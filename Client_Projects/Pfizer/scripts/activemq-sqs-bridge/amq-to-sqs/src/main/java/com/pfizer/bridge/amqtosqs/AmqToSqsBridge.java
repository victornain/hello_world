package com.pfizer.bridge.amqtosqs;

import com.pfizer.bridge.common.ActiveMqPoller;
import com.pfizer.bridge.common.Backoff;
import com.pfizer.bridge.common.BridgeRunner;
import com.pfizer.bridge.common.FailedMessageStore;
import com.pfizer.bridge.common.MessageMetadata;
import com.pfizer.bridge.common.SqsPublisher;
import com.pfizer.bridge.common.XmlInspector;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import javax.jms.JMSException;
import javax.jms.Message;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

/**
 * Reads the ActiveMQ queue one message at a time (continuously or every poll interval) and forwards each to SQS.
 * A message is acknowledged only after SQS accepted it (or it was saved to the failed-message store),
 * so the broker keeps it if the bridge crashes mid-flight. Delivery is at-least-once.
 */
final class AmqToSqsBridge implements BridgeRunner.Bridge {

    private static final Logger LOG = LoggerFactory.getLogger(AmqToSqsBridge.class);

    private final ActiveMqPoller poller;
    private final String queueName;
    private final String siteId;
    private final SqsPublisher publisher;
    private final XmlInspector inspector;
    private final FailedMessageStore failedStore;
    private final Backoff sqsBackoff;

    AmqToSqsBridge(ActiveMQConnectionFactory factory, String queueName, String siteId, SqsPublisher publisher,
                   XmlInspector inspector, FailedMessageStore failedStore, long receiveTimeoutMs,
                   Duration pollInterval, Backoff connectionBackoff, Backoff sqsBackoff) {
        this.queueName = queueName;
        this.siteId = siteId;
        this.publisher = publisher;
        this.inspector = inspector;
        this.failedStore = failedStore;
        this.sqsBackoff = sqsBackoff;
        this.poller = new ActiveMqPoller(factory, queueName, receiveTimeoutMs, pollInterval, connectionBackoff, this::process);
    }

    @Override
    public void stop() {
        poller.stop();
    }

    @Override
    public void run() {
        try {
            poller.run();
        } finally {
            publisher.close();
        }
    }

    private void process(Message message) throws JMSException {
        String jmsId = MessageMetadata.jmsMessageId(message);
        String body = MessageMetadata.readBody(message);
        if (body == null) {
            storeAndAck(message, jmsId, message.toString(), "Unsupported JMS message type: " + message.getClass().getName());
            return;
        }

        XmlInspector.Result info = inspector.inspect(body);
        if (info.schemaValid().filter(valid -> !valid).isPresent()) {
            LOG.warn("Message {} failed XSD validation (forwarding anyway, SchemaValid=false): {}", jmsId, info.validationError());
        }
        Map<String, MessageAttributeValue> attributes =
                MessageMetadata.fromJms(message, siteId, queueName).toSqsAttributes(info);

        while (poller.isRunning()) {
            try {
                String sqsId = publisher.send(body, attributes, jmsId);
                message.acknowledge();
                sqsBackoff.reset();
                LOG.info("Forwarded JMS {} (PSUniqueExceptionID={}) -> SQS {}", jmsId, info.exceptionId().orElse("?"), sqsId);
                return;
            } catch (SqsPublisher.RejectedException e) {
                LOG.error("SQS permanently rejected JMS {}: {}", jmsId, e.getMessage());
                storeAndAck(message, jmsId, body, e.getMessage());
                return;
            } catch (SdkException e) {
                long delay = sqsBackoff.nextDelayMs();
                LOG.warn("SQS send failed for JMS {}, retrying in {} ms: {}", jmsId, delay, e.getMessage());
                if (!Backoff.sleep(delay)) {
                    poller.stop();
                }
            }
        }
        LOG.info("Stopping before JMS {} was forwarded; left unacknowledged for redelivery.", jmsId);
    }

    private void storeAndAck(Message message, String jmsId, String body, String reason) throws JMSException {
        while (poller.isRunning()) {
            try {
                Path saved = failedStore.save(jmsId, body, reason);
                message.acknowledge();
                LOG.error("JMS {} saved to {} and acknowledged.", jmsId, saved);
                return;
            } catch (IOException e) {
                long delay = sqsBackoff.nextDelayMs();
                LOG.error("Cannot write failed message {} to disk, retrying in {} ms: {}", jmsId, delay, e.getMessage());
                if (!Backoff.sleep(delay)) {
                    poller.stop();
                }
            }
        }
    }
}
