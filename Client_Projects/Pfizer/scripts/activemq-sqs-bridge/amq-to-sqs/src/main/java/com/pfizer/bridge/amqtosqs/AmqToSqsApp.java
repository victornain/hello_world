package com.pfizer.bridge.amqtosqs;

import com.pfizer.bridge.common.ActiveMqSettings;
import com.pfizer.bridge.common.Backoff;
import com.pfizer.bridge.common.BridgeRunner;
import com.pfizer.bridge.common.EnvConfig;
import com.pfizer.bridge.common.FailedMessageStore;
import com.pfizer.bridge.common.SqsPublisher;
import com.pfizer.bridge.common.SqsSettings;
import com.pfizer.bridge.common.XmlInspector;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;

/** Entry point: ActiveMQ inbound queue -> AWS SQS (one instance per PharmaSuite site). */
public final class AmqToSqsApp {

    private static final Logger LOG = LoggerFactory.getLogger(AmqToSqsApp.class);
    private static final String DEFAULT_QUEUE = "PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST";

    private AmqToSqsApp() {
    }

    public static void main(String[] args) {
        EnvConfig env;
        AmqToSqsBridge bridge;
        try {
            env = EnvConfig.fromSystem(args);
            ActiveMqSettings amq = ActiveMqSettings.fromEnv(env, DEFAULT_QUEUE);
            SqsSettings sqs = SqsSettings.fromEnv(env);
            String siteId = env.required("SITE_ID");
            XmlInspector inspector = new XmlInspector(env.optional("XSD_PATH").map(Path::of));
            FailedMessageStore failedStore = new FailedMessageStore(Path.of(env.optional("FAILED_MESSAGE_DIR", "/data/failed")));
            ActiveMQConnectionFactory factory = amq.createConnectionFactory();
            Duration pollInterval = env.durationValue("ACTIVEMQ_POLL_INTERVAL", Duration.ZERO);

            LOG.info("Starting amq-to-sqs for site={} | ActiveMQ: {}, pollInterval={} | SQS: {}",
                    siteId, amq.describe(), pollInterval, sqs.describe());

            SqsPublisher publisher = new SqsPublisher(sqs.createClient(), sqs, siteId,
                    env.intValue("SQS_MAX_MESSAGE_BYTES", 262144));
            bridge = new AmqToSqsBridge(factory, amq.queue(), siteId, publisher, inspector, failedStore,
                    env.longValue("ACTIVEMQ_RECEIVE_TIMEOUT_MS", 1000), pollInterval,
                    Backoff.fromEnv(env), Backoff.fromEnv(env));
        } catch (Exception e) {
            LOG.error("Startup/configuration error: {}", e.getMessage(), e);
            System.exit(2);
            return;
        }
        BridgeRunner.run(bridge, env.longValue("SHUTDOWN_GRACE_MS", 15000));
    }
}
