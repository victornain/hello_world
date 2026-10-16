package com.pfizer.bridge.foldertosqs;

import com.pfizer.bridge.common.Backoff;
import com.pfizer.bridge.common.BridgeRunner;
import com.pfizer.bridge.common.EnvConfig;
import com.pfizer.bridge.common.FailedMessageStore;
import com.pfizer.bridge.common.MessageFolder;
import com.pfizer.bridge.common.SqsPublisher;
import com.pfizer.bridge.common.SqsSettings;
import com.pfizer.bridge.common.XmlInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

/** Entry point of the Pfizer service: reads XML files from the hand-off folder and pushes them to AWS SQS. */
public final class FolderToSqsApp {

    private static final Logger LOG = LoggerFactory.getLogger(FolderToSqsApp.class);
    private static final String DEFAULT_QUEUE = "PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST";
    private static final Duration DEFAULT_SCAN_INTERVAL = Duration.ofSeconds(30);

    private FolderToSqsApp() {
    }

    public static void main(String[] args) {
        EnvConfig env;
        FolderToSqsBridge bridge;
        try {
            env = EnvConfig.fromSystem(args);
            SqsSettings sqs = SqsSettings.fromEnv(env);
            String siteId = env.required("SITE_ID");
            Path folderDir = Path.of(env.optional("MESSAGE_FOLDER", "/data/messages"));
            MessageFolder folder = new MessageFolder(folderDir);
            boolean deleteAfterSend = env.boolValue("DELETE_AFTER_SEND", false);
            Optional<Path> processedDir = deleteAfterSend
                    ? Optional.empty()
                    : Optional.of(Path.of(env.optional("PROCESSED_DIR", folderDir.resolve("processed").toString())));
            FailedMessageStore failedStore = new FailedMessageStore(Path.of(env.optional("FAILED_MESSAGE_DIR", "/data/failed")));
            XmlInspector inspector = new XmlInspector(env.optional("XSD_PATH").map(Path::of));
            Duration scanInterval = env.durationValue("FOLDER_POLL_INTERVAL", DEFAULT_SCAN_INTERVAL);
            if (scanInterval.isZero()) {
                scanInterval = Duration.ofSeconds(1);
            }

            LOG.info("Starting folder-to-sqs for site={} | folder={}, scanInterval={}, afterSend={} | SQS: {}",
                    siteId, folderDir.toAbsolutePath(), scanInterval,
                    processedDir.map(p -> "move to " + p.toAbsolutePath()).orElse("delete"), sqs.describe());

            SqsPublisher publisher = new SqsPublisher(sqs.createClient(), sqs, siteId,
                    env.intValue("SQS_MAX_MESSAGE_BYTES", 262144));
            bridge = new FolderToSqsBridge(folder, processedDir, failedStore, publisher, inspector, siteId,
                    env.optional("ACTIVEMQ_QUEUE", DEFAULT_QUEUE), scanInterval, Backoff.fromEnv(env));
        } catch (Exception e) {
            LOG.error("Startup/configuration error: {}", e.getMessage(), e);
            System.exit(2);
            return;
        }
        BridgeRunner.run(bridge, env.longValue("SHUTDOWN_GRACE_MS", 15000));
    }
}
