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
import java.util.Properties;
import java.util.UUID;

/**
 * Origin of a message (site, queue, JMS id/timestamp, content type). Captured from JMS, persisted next to the
 * XML file in the hand-off folder, and turned into SQS message attributes; every path produces the same attributes.
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

    public Properties toProperties() {
        Properties p = new Properties();
        p.setProperty("SourceSite", sourceSite);
        p.setProperty("SourceQueue", sourceQueue);
        jmsMessageId.ifPresent(id -> p.setProperty("JmsMessageId", id));
        p.setProperty("JmsTimestamp", Long.toString(jmsTimestamp));
        p.setProperty("ContentType", contentType);
        return p;
    }

    /** Missing values fall back to {@code defaultSite}/{@code defaultQueue}, so plain XML files dropped by hand also work. */
    public static MessageMetadata fromProperties(Properties p, String defaultSite, String defaultQueue) {
        long ts;
        try {
            ts = Long.parseLong(p.getProperty("JmsTimestamp", "0").trim());
        } catch (NumberFormatException e) {
            ts = 0;
        }
        return new MessageMetadata(
                nonBlank(p.getProperty("SourceSite")).orElse(defaultSite),
                nonBlank(p.getProperty("SourceQueue")).orElse(defaultQueue),
                nonBlank(p.getProperty("JmsMessageId")),
                ts,
                nonBlank(p.getProperty("ContentType")).orElse(DEFAULT_CONTENT_TYPE));
    }

    public Map<String, MessageAttributeValue> toSqsAttributes(XmlInspector.Result info) {
        Map<String, MessageAttributeValue> attrs = new HashMap<>();
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

    private static Optional<String> nonBlank(String value) {
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value.trim());
    }

    private static void putString(Map<String, MessageAttributeValue> attrs, String name, String value) {
        if (value != null && !value.isBlank()) {
            attrs.put(name, MessageAttributeValue.builder().dataType("String").stringValue(value).build());
        }
    }
}
