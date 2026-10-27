package com.pfizer.bridge.amqtosqs;

import com.pfizer.bridge.common.ActiveMqSettings;
import com.pfizer.bridge.common.Backoff;
import com.pfizer.bridge.common.BridgeRunner;
import com.pfizer.bridge.common.EnvConfig;
import com.pfizer.bridge.common.XmlInspector;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Entry point: ActiveMQ inbound queues from configured sites -> their corresponding SQS queues. */
public final class AmqToSqsApp {

    private static final Logger LOG = LoggerFactory.getLogger(AmqToSqsApp.class);
    private static final String DEFAULT_QUEUE = "PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST";

    private AmqToSqsApp() {
    }

    public static void main(String[] args) {
        List<AmqToSqsBridge> bridges = new ArrayList<>();
        List<PythonSqsPublisher> publishers = new ArrayList<>();
        MultiSiteBridge multiSiteBridge;
        EnvConfig processEnv = EnvConfig.fromSystem();
        try {
            String environment = parseEnvironment(args);
            Path configPath = Path.of(processEnv.optional("INGESTION_CONFIG", "./ingestion.yaml"));
            IngestionConfig config = IngestionConfig.load(configPath, environment);
            Map<String, IngestionConfig.BrokerCredentials> credentials = loadCredentials(config);
            XmlInspector inspector = new XmlInspector(processEnv.optional("XSD_PATH").map(Path::of));

            for (IngestionConfig.Site site : config.sites()) {
                EnvConfig siteEnv = siteEnvironment(config, site, credentials.get(site.site()));
                ActiveMqSettings amq = ActiveMqSettings.fromEnv(siteEnv, DEFAULT_QUEUE);
                ActiveMQConnectionFactory factory = amq.createConnectionFactory();
                PythonSqsPublisher publisher = new PythonSqsPublisher(
                    site.site(), site.sqsQueueUrl(), config.region(),
                    siteEnv.optional("SQS_ENDPOINT_OVERRIDE").orElse(null),
                    siteEnv.intValue("SQS_MAX_MESSAGE_BYTES", 262144),
                    siteEnv.optional("PYTHON_EXECUTABLE", "python3"),
                    Path.of(siteEnv.optional("PYTHON_PUBLISHER_SCRIPT", "amq-to-sqs/src/main/python/sqs_publisher.py")));
                publishers.add(publisher);
                Duration pollInterval = siteEnv.durationValue("ACTIVEMQ_POLL_INTERVAL", Duration.ZERO);

                bridges.add(new AmqToSqsBridge(factory, amq.queue(), site.site(), publisher, inspector,
                        siteEnv.longValue("ACTIVEMQ_RECEIVE_TIMEOUT_MS", 1000), pollInterval,
                        Backoff.fromEnv(siteEnv), Backoff.fromEnv(siteEnv)));
                LOG.info("Configured ActiveMQ-to-SQS ingestion for site={}", site.site());
            }
            multiSiteBridge = new MultiSiteBridge(bridges);
        } catch (Exception e) {
            LOG.error("Startup/configuration error: {}", e.getMessage(), e);
            bridges.forEach(AmqToSqsBridge::stop);
            publishers.forEach(PythonSqsPublisher::close);
            System.exit(2);
            return;
        }
        BridgeRunner.run(multiSiteBridge, processEnv.longValue("SHUTDOWN_GRACE_MS", 15000));
    }

    private static String parseEnvironment(String[] args) {
        String environment = null;
        for (int index = 0; index < args.length; index++) {
            String argument = args[index];
            if (argument.startsWith("--env=")) {
                environment = argument.substring("--env=".length());
            } else if (argument.equals("--env") && index + 1 < args.length) {
                environment = args[++index];
            } else {
                throw new IllegalArgumentException("Usage: amq-to-sqs --env DEV|QA|PROD");
            }
        }
        if (environment == null || environment.isBlank()) {
            throw new IllegalArgumentException("Missing required argument --env DEV|QA|PROD");
        }
        return environment.trim().toUpperCase(java.util.Locale.ROOT);
    }

    private static Map<String, IngestionConfig.BrokerCredentials> loadCredentials(IngestionConfig config) {
        Map<String, IngestionConfig.BrokerCredentials> credentials = new HashMap<>();
        List<IngestionConfig.Site> sitesWithSecrets = config.sites().stream()
                .filter(site -> site.credentialsSecret() != null)
                .toList();
        if (sitesWithSecrets.isEmpty()) {
            return credentials;
        }

        try (SecretsManagerClient client = SecretsManagerClient.builder()
                .region(Region.of(config.region()))
                .build()) {
            for (IngestionConfig.Site site : sitesWithSecrets) {
                String secret = client.getSecretValue(GetSecretValueRequest.builder()
                        .secretId(site.credentialsSecret())
                        .build()).secretString();
                credentials.put(site.site(), IngestionConfig.parseCredentials(site.site(), secret));
            }
        }
        return credentials;
    }

    private static EnvConfig siteEnvironment(IngestionConfig config, IngestionConfig.Site site,
                                             IngestionConfig.BrokerCredentials credentials) {
        Map<String, String> values = new HashMap<>(System.getenv());
        values.put("AWS_REGION", config.region());
        values.put("SQS_QUEUE_URL", site.sqsQueueUrl());
        values.put("ACTIVEMQ_URL", site.brokerUrl());
        values.put("ACTIVEMQ_QUEUE", site.brokerQueue());
        if (credentials != null) {
            values.remove("ACTIVEMQ_PASSWORD_FILE");
            values.put("ACTIVEMQ_USER", credentials.username());
            values.put("ACTIVEMQ_PASSWORD", credentials.password());
        }
        return new EnvConfig(values);
    }
}
