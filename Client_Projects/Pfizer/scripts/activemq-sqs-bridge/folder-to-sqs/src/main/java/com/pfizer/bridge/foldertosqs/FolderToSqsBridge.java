package com.pfizer.bridge.foldertosqs;

import com.pfizer.bridge.common.Backoff;
import com.pfizer.bridge.common.BridgeRunner;
import com.pfizer.bridge.common.FailedMessageStore;
import com.pfizer.bridge.common.MessageFolder;
import com.pfizer.bridge.common.SqsPublisher;
import com.pfizer.bridge.common.XmlInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Scans the hand-off folder and pushes every {@code *.xml} file to SQS, oldest first.
 * A file leaves the folder (moved to {@code processed/} or deleted) only after SQS accepted it;
 * files SQS rejects permanently go to the failed-message store. Delivery is at-least-once.
 */
final class FolderToSqsBridge implements BridgeRunner.Bridge {

    private static final Logger LOG = LoggerFactory.getLogger(FolderToSqsBridge.class);

    private final MessageFolder folder;
    private final Optional<Path> processedDir;
    private final FailedMessageStore failedStore;
    private final SqsPublisher publisher;
    private final XmlInspector inspector;
    private final String siteId;
    private final String defaultQueue;
    private final Duration scanInterval;
    private final Backoff backoff;
    private volatile boolean running = true;

    FolderToSqsBridge(MessageFolder folder, Optional<Path> processedDir, FailedMessageStore failedStore,
                      SqsPublisher publisher, XmlInspector inspector, String siteId, String defaultQueue,
                      Duration scanInterval, Backoff backoff) {
        this.folder = folder;
        this.processedDir = processedDir;
        this.failedStore = failedStore;
        this.publisher = publisher;
        this.inspector = inspector;
        this.siteId = siteId;
        this.defaultQueue = defaultQueue;
        this.scanInterval = scanInterval;
        this.backoff = backoff;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        try {
            while (running) {
                List<Path> files;
                try {
                    files = folder.pending();
                } catch (IOException e) {
                    LOG.error("Cannot list {}: {}", folder.directory(), e.getMessage());
                    files = List.of();
                }
                if (!files.isEmpty()) {
                    LOG.info("Found {} file(s) in {}", files.size(), folder.directory());
                }
                for (Path file : files) {
                    if (!running) {
                        break;
                    }
                    process(file);
                }
                if (!Backoff.sleepWhile(scanInterval.toMillis(), () -> running)) {
                    break;
                }
            }
        } finally {
            publisher.close();
        }
    }

    private void process(Path file) {
        MessageFolder.Pending msg;
        try {
            msg = folder.read(file, siteId, defaultQueue);
        } catch (IOException e) {
            LOG.warn("Cannot read {}, will retry on the next scan: {}", file, e.getMessage());
            return;
        }

        XmlInspector.Result info = inspector.inspect(msg.body());
        if (info.schemaValid().filter(valid -> !valid).isPresent()) {
            LOG.warn("File {} failed XSD validation (forwarding anyway, SchemaValid=false): {}", file.getFileName(), info.validationError());
        }

        while (running) {
            try {
                String sqsId = publisher.send(msg.body(), msg.metadata().toSqsAttributes(info), msg.dedupKey());
                backoff.reset();
                LOG.info("Pushed {} (PSUniqueExceptionID={}) -> SQS {}", file.getFileName(), info.exceptionId().orElse("?"), sqsId);
                finish(file, null, null);
                return;
            } catch (SqsPublisher.RejectedException e) {
                LOG.error("SQS permanently rejected {}: {}", file.getFileName(), e.getMessage());
                finish(file, msg.body(), e.getMessage());
                return;
            } catch (SdkException e) {
                long delay = backoff.nextDelayMs();
                LOG.warn("SQS send failed for {}, retrying in {} ms: {}", file.getFileName(), delay, e.getMessage());
                if (!Backoff.sleep(delay)) {
                    running = false;
                }
            }
        }
        LOG.info("Stopping before {} was pushed; it stays in the folder.", file.getFileName());
    }

    /** Removes the file from the folder; a non-null {@code rejectReason} sends it to the failed-message store. */
    private void finish(Path file, String body, String rejectReason) {
        while (running) {
            try {
                if (rejectReason != null) {
                    Path saved = failedStore.save(file.getFileName().toString(), body, rejectReason);
                    MessageFolder.delete(file);
                    LOG.error("{} saved to {} and removed from the folder.", file.getFileName(), saved);
                } else if (processedDir.isPresent()) {
                    MessageFolder.moveTo(file, processedDir.get());
                } else {
                    MessageFolder.delete(file);
                }
                return;
            } catch (IOException e) {
                long delay = backoff.nextDelayMs();
                LOG.error("Cannot move/delete {}, retrying in {} ms (it may be sent again): {}", file, delay, e.getMessage());
                if (!Backoff.sleep(delay)) {
                    running = false;
                }
            }
        }
    }
}
