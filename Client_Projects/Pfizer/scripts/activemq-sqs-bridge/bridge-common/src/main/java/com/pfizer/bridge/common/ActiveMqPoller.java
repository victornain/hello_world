package com.pfizer.bridge.common;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.time.Duration;

/**
 * Reads an ActiveMQ queue one message at a time with {@code INDIVIDUAL_ACKNOWLEDGE}; the handler acknowledges.
 * <ul>
 *   <li>{@code pollInterval == 0}: continuous consumption on a long-lived connection.</li>
 *   <li>{@code pollInterval > 0}: every interval, connect, drain the queue until it is empty, disconnect and wait.</li>
 * </ul>
 * Broken connections are rebuilt with backoff in both modes.
 */
public final class ActiveMqPoller {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMqPoller.class);

    /** Processes and acknowledges one message. Leave it unacknowledged to have the broker redeliver it. */
    @FunctionalInterface
    public interface Handler {
        void handle(Message message) throws JMSException;
    }

    private final ActiveMQConnectionFactory factory;
    private final String queueName;
    private final long receiveTimeoutMs;
    private final Duration pollInterval;
    private final Backoff connectionBackoff;
    private final Handler handler;
    private volatile boolean running = true;

    public ActiveMqPoller(ActiveMQConnectionFactory factory, String queueName, long receiveTimeoutMs,
                          Duration pollInterval, Backoff connectionBackoff, Handler handler) {
        this.factory = factory;
        this.queueName = queueName;
        this.receiveTimeoutMs = receiveTimeoutMs;
        this.pollInterval = pollInterval;
        this.connectionBackoff = connectionBackoff;
        this.handler = handler;
    }

    public boolean isRunning() {
        return running;
    }

    public void stop() {
        running = false;
    }

    public String describeMode() {
        return pollInterval.isZero() ? "continuous" : "every " + pollInterval;
    }

    public void run() {
        boolean polling = !pollInterval.isZero();
        LOG.info("Reading ActiveMQ queue {} ({})", queueName, describeMode());
        while (running) {
            Connection connection = null;
            try {
                connection = factory.createConnection();
                connection.setExceptionListener(e -> LOG.warn("ActiveMQ connection problem: {}", e.getMessage()));
                connection.start();
                Session session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));
                if (!polling) {
                    LOG.info("Connected, consuming from {}", queueName);
                }
                connectionBackoff.reset();

                int count = 0;
                while (running) {
                    Message message = consumer.receive(receiveTimeoutMs);
                    if (message == null) {
                        if (polling) {
                            break;
                        }
                        continue;
                    }
                    handler.handle(message);
                    count++;
                }
                if (polling) {
                    LOG.info("Poll of {} finished: {} message(s); next poll in {}", queueName, count, pollInterval);
                }
            } catch (JMSException e) {
                if (!running) {
                    break;
                }
                long delay = connectionBackoff.nextDelayMs();
                LOG.error("ActiveMQ error, reconnecting in {} ms: {}", delay, e.getMessage(), e);
                if (!Backoff.sleep(delay)) {
                    break;
                }
                continue;
            } finally {
                closeQuietly(connection);
            }
            if (polling && !Backoff.sleepWhile(pollInterval.toMillis(), this::isRunning)) {
                break;
            }
        }
    }

    private static void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (JMSException e) {
            LOG.debug("Error closing ActiveMQ connection", e);
        }
    }
}
