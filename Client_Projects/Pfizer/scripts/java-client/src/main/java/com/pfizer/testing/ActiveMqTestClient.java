package com.pfizer.testing;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * OpenWire round-trip test client for the PharmaSuite exception ingestion queues.
 *
 * Java port of activemq_test_client.py — the broker only speaks native ActiveMQ OpenWire
 * (confirmed via a failed AMQP 1.0 handshake), so this uses the official activemq-client
 * library directly instead of a Python/JPype bridge. See PlanActiveMQ_a_SQL.md,
 * "Local Testing Infrastructure" section, for context.
 *
 * Target: Justin's internal validation broker, tcp://aznaampstalos1d:61646
 * (plain tcp on 61646, NOT the ssl/61647 production endpoint).
 *
 * This is a connectivity test only, not the production connector described in
 * Phase 2/5 of PlanActiveMQ_a_SQL.md. The outbound payload is a placeholder — the
 * real response schema PRC expects is still an open question in that plan.
 *
 * Credentials (only needed if the broker enforces auth; leave unset for anonymous):
 *   set ACTIVEMQ_USER=...
 *   set ACTIVEMQ_PASSWORD=...
 */
public class ActiveMqTestClient {

    private static final String DEFAULT_BROKER_URL = "tcp://aznaampstalos1d:61646";
    private static final String INBOUND_QUEUE = "PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST";
    private static final String OUTBOUND_QUEUE = "EXCEPTION_RECORD_EXTERNAL_RESPONSE";

    // Placeholder test payload only — the real outbound/response schema is unconfirmed (see Phase 5 open questions).
    private static final String SAMPLE_RESPONSE_XML = """
            <ProcessRecordedException xmlns="http://www.mesa.org/xml/B2MML-V0600" xmlns:b2m="http://www.mesa.org/xml/B2MML-V0600-AllExtensions">
              <ApplicationArea>
                <Sender>
                  <LogicalID>QMS</LogicalID>
                </Sender>
                <Receiver>
                  <LogicalID>MES</LogicalID>
                </Receiver>
              </ApplicationArea>
              <DataArea>
                <PSUniqueExceptionID>TEST0000001 - 001</PSUniqueExceptionID>
                <OrderOrWorkflowIdentifier>TEST0000001</OrderOrWorkflowIdentifier>
                <ExceptionIdentifier>001</ExceptionIdentifier>
                <ExceptionDescription>Connectivity test message from ActiveMqTestClient.java</ExceptionDescription>
              </DataArea>
            </ProcessRecordedException>""";

    public static void main(String[] args) throws Exception {
        Options opts = Options.parse(args);

        String brokerUrl = System.getenv().getOrDefault("ACTIVEMQ_URL", DEFAULT_BROKER_URL);
        String user = "admin";// System.getenv("ACTIVEMQ_USER");
        String password = "admin";// System.getenv("ACTIVEMQ_PASSWORD");

        if (!opts.skipConsume) {
            consumeInbound(brokerUrl, user, password, opts.consumeLimit, opts.timeoutMs);
        }

        if (!opts.skipSend) {
            String payload = opts.payloadFile != null
                    ? Files.readString(opts.payloadFile)
                    : SAMPLE_RESPONSE_XML;
            sendTestResponse(brokerUrl, user, password, payload);
        }
    }

    /** Drains up to {@code limit} messages from the inbound queue, acking each, and returns the raw bodies. */
    private static List<String> consumeInbound(String brokerUrl, String user, String password, int limit, long timeoutMs) throws JMSException {
        List<String> bodies = new ArrayList<>();
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = (user != null && password != null)
                ? factory.createConnection(user, password)
                : factory.createConnection();

        System.out.printf("Connecting to %s to drain up to %d message(s) from %s%n", brokerUrl, limit, INBOUND_QUEUE);
        try {
            connection.start();
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Queue queue = session.createQueue(INBOUND_QUEUE);
            MessageConsumer consumer = session.createConsumer(queue);

            for (int i = 0; i < limit; i++) {
                Message message = consumer.receive(timeoutMs);
                if (message == null) {
                    System.out.printf("No more messages after %d received.%n", i);
                    break;
                }
                String body = (message instanceof TextMessage textMessage) ? textMessage.getText() : message.toString();
                bodies.add(body);
                message.acknowledge();
                System.out.printf("Received message %d:%n%s%n", i + 1, body);
            }

            consumer.close();
            session.close();
        } finally {
            connection.close();
        }
        return bodies;
    }

    /** Publishes a single test message to the outbound queue. */
    private static void sendTestResponse(String brokerUrl, String user, String password, String xmlPayload) throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = (user != null && password != null)
                ? factory.createConnection(user, password)
                : factory.createConnection();

        System.out.printf("Connecting to %s to send a test message to %s%n", brokerUrl, OUTBOUND_QUEUE);
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(OUTBOUND_QUEUE);
            MessageProducer producer = session.createProducer(queue);
            TextMessage message = session.createTextMessage(xmlPayload);
            message.setStringProperty("ContentType", "text/xml");
            producer.send(message);
            producer.close();
            session.close();
            System.out.printf("Test message sent to %s%n", OUTBOUND_QUEUE);
        } finally {
            connection.close();
        }
    }

    /** Minimal CLI argument parsing, mirroring activemq_test_client.py's flags. */
    private static final class Options {
        int consumeLimit = 10;
        long timeoutMs = 5000;
        boolean skipConsume = false;
        boolean skipSend = false;
        Path payloadFile = null;

        static Options parse(String[] args) {
            Options o = new Options();
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--consume-limit" -> o.consumeLimit = Integer.parseInt(args[++i]);
                    case "--timeout" -> o.timeoutMs = (long) (Double.parseDouble(args[++i]) * 1000);
                    case "--skip-consume" -> o.skipConsume = true;
                    case "--skip-send" -> o.skipSend = true;
                    case "--payload-file" -> o.payloadFile = Paths.get(args[++i]);
                    default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
                }
            }
            return o;
        }
    }
}
