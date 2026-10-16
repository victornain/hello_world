package com.pfizer.bridge.sqstoamq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Map;

/** Lazily (re)connecting ActiveMQ producer. Not thread-safe. */
final class ActiveMqPublisher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMqPublisher.class);

    private final ActiveMQConnectionFactory factory;
    private final String queueName;
    private final int deliveryMode;
    private final long timeToLiveMs;

    private Connection connection;
    private Session session;
    private MessageProducer producer;

    ActiveMqPublisher(ActiveMQConnectionFactory factory, String queueName, int deliveryMode, long timeToLiveMs) {
        this.factory = factory;
        this.queueName = queueName;
        this.deliveryMode = deliveryMode;
        this.timeToLiveMs = timeToLiveMs;
    }

    void publish(String body, Map<String, String> properties) throws JMSException {
        ensureConnected();
        TextMessage message = session.createTextMessage(body);
        for (Map.Entry<String, String> property : properties.entrySet()) {
            message.setStringProperty(property.getKey(), property.getValue());
        }
        // Persistent sends are synchronous: returns only after the broker accepted the message
        producer.send(message, deliveryMode, Message.DEFAULT_PRIORITY, timeToLiveMs);
    }

    private void ensureConnected() throws JMSException {
        if (producer != null) {
            return;
        }
        try {
            connection = factory.createConnection();
            connection.setExceptionListener(e -> LOG.warn("ActiveMQ connection problem: {}", e.getMessage()));
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = session.createProducer(session.createQueue(queueName));
            LOG.info("Connected, producing to {}", queueName);
        } catch (JMSException e) {
            reset();
            throw e;
        }
    }

    void reset() {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                LOG.debug("Error closing ActiveMQ connection", e);
            }
        }
        connection = null;
        session = null;
        producer = null;
    }

    @Override
    public void close() {
        reset();
    }
}
