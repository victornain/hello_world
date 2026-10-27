package com.pfizer.bridge.common;

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Origin of a message (site, queue, JMS id/timestamp, content type), captured from JMS and turned into SQS attributes.
 */
public record MessageMetadata(String sourceSite, String sourceQueue, Optional<String> jmsMessageId,
                              long jmsTimestamp, String contentType) {

    private static final String DEFAULT_CONTENT_TYPE = "text/xml";

    public static MessageMetadata fromJms(Message message, String siteId, String queueName) throws JMSException {
        String contentType = message.getStringProperty("ContentType");
        return new MessageMetadata(siteId, queueName, Optional.of(jmsMessageId(message)), message.getJMSTimestamp(),
                contentType != null && !contentType.isBlank() ? contentType : DEFAULT_CONTENT_TYPE);
    }

    public static String jmsMessageId(Message message) throws JMSException {
        return message.getJMSMessageID() != null ? message.getJMSMessageID() : "no-id-" + UUID.randomUUID();
    }

    /** @return the body of a Text/Bytes message (bytes decoded as UTF-8), or null for other message types. */
    public static String readBody(Message message) throws JMSException {
        if (message instanceof TextMessage text) {
            return text.getText() == null ? "" : text.getText();
        }
        if (message instanceof BytesMessage bytes) {
            byte[] data = new byte[(int) bytes.getBodyLength()];
            bytes.readBytes(data);
            return new String(data, StandardCharsets.UTF_8);
        }
        return null;
    }

    public Map<String, MessageAttributeValue> toSqsAttributes(XmlInspector.Result info) {
        Map<String, MessageAttributeValue> attrs = new HashMap<>();
        putString(attrs, "site", sourceSite);
        putString(attrs, "SourceSite", sourceSite);
        putString(attrs, "SourceQueue", sourceQueue);
        jmsMessageId.ifPresent(id -> putString(attrs, "JmsMessageId", id));
        attrs.put("JmsTimestamp", MessageAttributeValue.builder()
                .dataType("Number").stringValue(Long.toString(jmsTimestamp)).build());
        putString(attrs, "ContentType", contentType);
        info.exceptionId().ifPresent(id -> putString(attrs, "PSUniqueExceptionID", id));
        info.schemaValid().ifPresent(valid -> putString(attrs, "SchemaValid", valid.toString()));
        return attrs;
    }

    private static void putString(Map<String, MessageAttributeValue> attrs, String name, String value) {
        if (value != null && !value.isBlank()) {
            attrs.put(name, MessageAttributeValue.builder().dataType("String").stringValue(value).build());
        }
    }
}
