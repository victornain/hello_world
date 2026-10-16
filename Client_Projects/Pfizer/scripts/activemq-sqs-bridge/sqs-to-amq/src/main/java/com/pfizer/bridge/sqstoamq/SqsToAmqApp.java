package com.pfizer.bridge.sqstoamq;

import com.pfizer.bridge.common.ActiveMqSettings;
import com.pfizer.bridge.common.Backoff;
import com.pfizer.bridge.common.BridgeRunner;
import com.pfizer.bridge.common.EnvConfig;
import com.pfizer.bridge.common.SqsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import javax.jms.DeliveryMode;

/** Entry point: AWS SQS -> ActiveMQ outbound queue (PRC push-back, Phase 5). */
public final class SqsToAmqApp {

    private static final Logger LOG = LoggerFactory.getLogger(SqsToAmqApp.class);
    private static final String DEFAULT_QUEUE = "EXCEPTION_RECORD_EXTERNAL_RESPONSE";

    private SqsToAmqApp() {
    }

    public static void main(String[] args) {
        EnvConfig env;
        SqsToAmqBridge bridge;
        try {
            env = EnvConfig.fromSystem(args);
            ActiveMqSettings amq = ActiveMqSettings.fromEnv(env, DEFAULT_QUEUE);
            SqsSettings sqs = SqsSettings.fromEnv(env);

            ReceiveMessageRequest.Builder receive = ReceiveMessageRequest.builder()
                    .queueUrl(sqs.queueUrl())
                    .maxNumberOfMessages(env.intValue("SQS_MAX_MESSAGES", 10))
                    .waitTimeSeconds(env.intValue("SQS_WAIT_TIME_SECONDS", 20))
                    .messageAttributeNames("All");
            int visibility = env.intValue("SQS_VISIBILITY_TIMEOUT_SECONDS", 0);
            if (visibility > 0) {
                receive.visibilityTimeout(visibility);
            }

            ActiveMqPublisher publisher = new ActiveMqPublisher(amq.createConnectionFactory(), amq.queue(),
                    env.boolValue("ACTIVEMQ_PERSISTENT", true) ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT,
                    env.longValue("ACTIVEMQ_MESSAGE_TTL_MS", 0));

            LOG.info("Starting sqs-to-amq | SQS: {} | ActiveMQ: {}", sqs.describe(), amq.describe());
            bridge = new SqsToAmqBridge(sqs.createClient(), sqs.queueUrl(), receive.build(), publisher,
                    Backoff.fromEnv(env), Backoff.fromEnv(env));
        } catch (Exception e) {
            LOG.error("Startup/configuration error: {}", e.getMessage(), e);
            System.exit(2);
            return;
        }
        BridgeRunner.run(bridge, env.longValue("SHUTDOWN_GRACE_MS", 30000));
    }
}
