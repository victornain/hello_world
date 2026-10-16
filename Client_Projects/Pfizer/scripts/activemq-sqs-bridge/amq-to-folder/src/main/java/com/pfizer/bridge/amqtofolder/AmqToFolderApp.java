package com.pfizer.bridge.amqtofolder;

import com.pfizer.bridge.common.ActiveMqSettings;
import com.pfizer.bridge.common.Backoff;
import com.pfizer.bridge.common.BridgeRunner;
import com.pfizer.bridge.common.EnvConfig;
import com.pfizer.bridge.common.FailedMessageStore;
import com.pfizer.bridge.common.MessageFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;

/** Entry point of the local service: polls the ActiveMQ inbound queue and saves each XML message to a folder. */
public final class AmqToFolderApp {

    private static final Logger LOG = LoggerFactory.getLogger(AmqToFolderApp.class);
    private static final String DEFAULT_QUEUE = "PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST";
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMinutes(5);

    private AmqToFolderApp() {
    }

    public static void main(String[] args) {
        EnvConfig env;
        AmqToFolderBridge bridge;
        try {
            env = EnvConfig.fromSystem(args);
            ActiveMqSettings amq = ActiveMqSettings.fromEnv(env, DEFAULT_QUEUE);
            String siteId = env.required("SITE_ID");
            MessageFolder folder = new MessageFolder(Path.of(env.optional("MESSAGE_FOLDER", "/data/messages")));
            FailedMessageStore failedStore = new FailedMessageStore(Path.of(env.optional("FAILED_MESSAGE_DIR", "/data/failed")));
            Duration pollInterval = env.durationValue("ACTIVEMQ_POLL_INTERVAL", DEFAULT_POLL_INTERVAL);

            LOG.info("Starting amq-to-folder for site={} | ActiveMQ: {}, pollInterval={} | folder={}",
                    siteId, amq.describe(), pollInterval, folder.directory().toAbsolutePath());

            bridge = new AmqToFolderBridge(amq.createConnectionFactory(), amq.queue(), siteId, folder, failedStore,
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
