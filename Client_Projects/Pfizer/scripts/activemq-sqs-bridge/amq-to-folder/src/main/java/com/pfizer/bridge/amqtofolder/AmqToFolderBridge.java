package com.pfizer.bridge.amqtofolder;

import com.pfizer.bridge.common.ActiveMqPoller;
import com.pfizer.bridge.common.Backoff;
import com.pfizer.bridge.common.BridgeRunner;
import com.pfizer.bridge.common.FailedMessageStore;
import com.pfizer.bridge.common.MessageFolder;
import com.pfizer.bridge.common.MessageMetadata;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Polls the ActiveMQ queue and writes each message to the hand-off folder.
 * A message is acknowledged only after its file is on disk, so nothing is lost if the service stops mid-flight.
 */
final class AmqToFolderBridge implements BridgeRunner.Bridge {

    private static final Logger LOG = LoggerFactory.getLogger(AmqToFolderBridge.class);

    private final ActiveMqPoller poller;
    private final String queueName;
    private final String siteId;
    private final MessageFolder folder;
    private final FailedMessageStore failedStore;
    private final Backoff diskBackoff;

    AmqToFolderBridge(ActiveMQConnectionFactory factory, String queueName, String siteId, MessageFolder folder,
                      FailedMessageStore failedStore, long receiveTimeoutMs, Duration pollInterval,
                      Backoff connectionBackoff, Backoff diskBackoff) {
        this.queueName = queueName;
        this.siteId = siteId;
        this.folder = folder;
        this.failedStore = failedStore;
        this.diskBackoff = diskBackoff;
        this.poller = new ActiveMqPoller(factory, queueName, receiveTimeoutMs, pollInterval, connectionBackoff, this::process);
    }

    @Override
    public void stop() {
        poller.stop();
    }

    @Override
    public void run() {
        poller.run();
    }

    private void process(Message message) throws JMSException {
        String jmsId = MessageMetadata.jmsMessageId(message);
        String body = MessageMetadata.readBody(message);
        boolean unsupported = body == null;
        MessageMetadata metadata = unsupported ? null : MessageMetadata.fromJms(message, siteId, queueName);

        while (poller.isRunning()) {
            try {
                Path saved = unsupported
                        ? failedStore.save(jmsId, message.toString(), "Unsupported JMS message type: " + message.getClass().getName())
                        : folder.write(body, metadata);
                message.acknowledge();
                diskBackoff.reset();
                if (unsupported) {
                    LOG.error("JMS {} has an unsupported type; saved to {} and acknowledged.", jmsId, saved);
                } else {
                    LOG.info("Saved JMS {} -> {}", jmsId, saved);
                }
                return;
            } catch (IOException e) {
                long delay = diskBackoff.nextDelayMs();
                LOG.error("Cannot write JMS {} to disk, retrying in {} ms: {}", jmsId, delay, e.getMessage());
                if (!Backoff.sleep(delay)) {
                    poller.stop();
                }
            }
        }
        LOG.info("Stopping before JMS {} was saved; left unacknowledged for redelivery.", jmsId);
    }
}
