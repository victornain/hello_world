package com.pfizer.bridge.sqstoamq;

import com.pfizer.bridge.common.Backoff;
import com.pfizer.bridge.common.BridgeRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import javax.jms.JMSException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Long-polls SQS and publishes each message to ActiveMQ. The SQS message is deleted only after the
 * broker accepted it; on failure it reappears after the visibility timeout and, after the queue's
 * maxReceiveCount, moves to the SQS DLQ (redrive policy configured on the AWS side). At-least-once.
 */
final class SqsToAmqBridge implements BridgeRunner.Bridge {

    private static final Logger LOG = LoggerFactory.getLogger(SqsToAmqBridge.class);

    private final SqsClient sqs;
    private final String queueUrl;
    private final ReceiveMessageRequest receiveRequest;
    private final ActiveMqPublisher publisher;
    private final Backoff sqsBackoff;
    private final Backoff amqBackoff;
    private volatile boolean running = true;

    SqsToAmqBridge(SqsClient sqs, String queueUrl, ReceiveMessageRequest receiveRequest, ActiveMqPublisher publisher,
                   Backoff sqsBackoff, Backoff amqBackoff) {
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.receiveRequest = receiveRequest;
        this.publisher = publisher;
        this.sqsBackoff = sqsBackoff;
        this.amqBackoff = amqBackoff;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        try {
            while (running) {
                List<Message> messages;
                try {
                    messages = sqs.receiveMessage(receiveRequest).messages();
                    sqsBackoff.reset();
                } catch (SdkException e) {
                    long delay = sqsBackoff.nextDelayMs();
                    LOG.error("SQS receive failed, retrying in {} ms: {}", delay, e.getMessage());
                    if (!Backoff.sleep(delay)) {
                        break;
                    }
                    continue;
                }
                for (Message message : messages) {
                    // Unprocessed messages of this batch become visible again after the visibility timeout
                    if (!running || !forward(message)) {
                        break;
                    }
                }
            }
        } finally {
            publisher.close();
            sqs.close();
        }
    }

    private boolean forward(Message message) {
        try {
            publisher.publish(message.body(), toJmsProperties(message));
            amqBackoff.reset();
        } catch (JMSException e) {
            publisher.reset();
            long delay = amqBackoff.nextDelayMs();
            LOG.error("ActiveMQ publish failed for SQS {}, retrying in {} ms: {}", message.messageId(), delay, e.getMessage());
            if (!Backoff.sleep(delay)) {
                running = false;
            }
            return false;
        }

        try {
            sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(message.receiptHandle()).build());
            LOG.info("Forwarded SQS {} -> ActiveMQ", message.messageId());
        } catch (SdkException e) {
            LOG.warn("Published SQS {} but delete failed; it will be redelivered (possible duplicate): {}",
                    message.messageId(), e.getMessage());
        }
        return true;
    }

    private static Map<String, String> toJmsProperties(Message message) {
        Map<String, String> props = new HashMap<>();
        for (Map.Entry<String, MessageAttributeValue> attr : message.messageAttributes().entrySet()) {
            String type = attr.getValue().dataType();
            if (type != null && (type.startsWith("String") || type.startsWith("Number"))) {
                props.put(toJmsPropertyName(attr.getKey()), attr.getValue().stringValue());
            }
        }
        props.put("SqsMessageId", message.messageId());
        props.putIfAbsent("ContentType", "text/xml");
        return props;
    }

    /** JMS property names must be Java identifiers and must not use the reserved JMS prefix. */
    static String toJmsPropertyName(String name) {
        String sanitized = name.replaceAll("[^A-Za-z0-9_$]", "_");
        if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
            sanitized = "_" + sanitized;
        }
        return sanitized.toUpperCase().startsWith("JMS") ? "X" + sanitized : sanitized;
    }
}
