package com.pfizer.bridge.amqtosqs;

import com.pfizer.bridge.common.ActiveMqPoller;
import com.pfizer.bridge.common.Backoff;
import com.pfizer.bridge.common.BridgeRunner;
import com.pfizer.bridge.common.MessageMetadata;
import com.pfizer.bridge.common.XmlInspector;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import javax.jms.JMSException;
import javax.jms.Message;
import java.time.Duration;
import java.util.Map;

/**
 * Reads the ActiveMQ queue one message at a time (continuously or every poll interval) and forwards each to SQS.
 * A message is acknowledged only after SQS accepts it, so the broker keeps it if forwarding fails.
 * Delivery is at-least-once.
 */
final class AmqToSqsBridge implements BridgeRunner.Bridge {

    private static final Logger LOG = LoggerFactory.getLogger(AmqToSqsBridge.class);

    private final ActiveMqPoller poller;
    private final String queueName;
    private final String siteId;
    private final PythonSqsPublisher publisher;
    private final XmlInspector inspector;
    private final Backoff sqsBackoff;

    AmqToSqsBridge(ActiveMQConnectionFactory factory, String queueName, String siteId, PythonSqsPublisher publisher,
                   XmlInspector inspector, long receiveTimeoutMs,
                   Duration pollInterval, Backoff connectionBackoff, Backoff sqsBackoff) {
        this.queueName = queueName;
        this.siteId = siteId;
        this.publisher = publisher;
        this.inspector = inspector;
        this.sqsBackoff = sqsBackoff;
        this.poller = new ActiveMqPoller(factory, queueName, receiveTimeoutMs, pollInterval, connectionBackoff, this::process);
    }

    @Override
    public void stop() {
        poller.stop();
        publisher.stop();
    }

    String siteId() {
        return siteId;
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
            LOG.error("Unsupported JMS message type for site={} jmsId={}; leaving it unacknowledged",
                    siteId, jmsId);
            return;
        }

        XmlInspector.Result info = inspector.inspect(body);
        if (info.schemaValid().filter(valid -> !valid).isPresent()) {
            LOG.warn("Message failed XSD validation for site={} jmsId={} (forwarding anyway)", siteId, jmsId);
        }
        Map<String, MessageAttributeValue> attributes =
                MessageMetadata.fromJms(message, siteId, queueName).toSqsAttributes(info);

        while (poller.isRunning()) {
            try {
                String sqsId = publisher.send(body, attributes, jmsId);
                message.acknowledge();
                sqsBackoff.reset();
                LOG.info("Forwarded message for site={} jmsId={} to SQS id={}", siteId, jmsId, sqsId);
                return;
            } catch (PythonSqsPublisher.PublishException e) {
                long delay = sqsBackoff.nextDelayMs();
                LOG.warn("boto3 send_message failed for site={} jmsId={} code={}, retrying in {} ms",
                        siteId, jmsId, e.errorCode(), delay);
                if (!Backoff.sleep(delay)) {
                    poller.stop();
                }
            }
        }
        LOG.info("Stopping before forwarding for site={} jmsId={}; left unacknowledged", siteId, jmsId);
    }
}
