package com.pfizer.bridge.common;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;

import java.util.Optional;

/** ActiveMQ (OpenWire) connection settings, shared by both bridge directions. */
public final class ActiveMqSettings {

    private final String brokerUrl;
    private final Optional<String> user;
    private final Optional<String> password;
    private final String queue;
    private final boolean useFailover;
    private final String failoverOptions;
    private final Optional<String> trustStore;
    private final Optional<String> trustStorePassword;
    private final Optional<String> keyStore;
    private final Optional<String> keyStorePassword;
    private final int maxRedeliveries;

    private ActiveMqSettings(EnvConfig env, String defaultQueue) {
        this.brokerUrl = env.required("ACTIVEMQ_URL");
        this.user = env.optional("ACTIVEMQ_USER");
        this.password = env.secret("ACTIVEMQ_PASSWORD");
        this.queue = env.optional("ACTIVEMQ_QUEUE", defaultQueue);
        this.useFailover = env.boolValue("ACTIVEMQ_USE_FAILOVER", true);
        // timeout makes blocked sends fail instead of hanging forever while the broker is down
        this.failoverOptions = env.optional("ACTIVEMQ_FAILOVER_OPTIONS",
                "initialReconnectDelay=1000&maxReconnectDelay=30000&useExponentialBackOff=true&timeout=30000");
        this.trustStore = env.optional("ACTIVEMQ_SSL_TRUSTSTORE");
        this.trustStorePassword = env.secret("ACTIVEMQ_SSL_TRUSTSTORE_PASSWORD");
        this.keyStore = env.optional("ACTIVEMQ_SSL_KEYSTORE");
        this.keyStorePassword = env.secret("ACTIVEMQ_SSL_KEYSTORE_PASSWORD");
        this.maxRedeliveries = env.intValue("ACTIVEMQ_MAX_REDELIVERIES", -1);
    }

    public static ActiveMqSettings fromEnv(EnvConfig env, String defaultQueue) {
        return new ActiveMqSettings(env, defaultQueue);
    }

    public String queue() {
        return queue;
    }

    public String effectiveBrokerUrl() {
        if (!useFailover || brokerUrl.startsWith("failover:")) {
            return brokerUrl;
        }
        return "failover:(" + brokerUrl + ")?" + failoverOptions;
    }

    public ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        String url = effectiveBrokerUrl();
        ActiveMQConnectionFactory factory;
        if (url.contains("ssl://")) {
            ActiveMQSslConnectionFactory ssl = new ActiveMQSslConnectionFactory(url);
            if (trustStore.isPresent()) {
                ssl.setTrustStore(trustStore.get());
                trustStorePassword.ifPresent(ssl::setTrustStorePassword);
            }
            if (keyStore.isPresent()) {
                ssl.setKeyStore(keyStore.get());
                keyStorePassword.ifPresent(ssl::setKeyStorePassword);
            }
            factory = ssl;
        } else {
            factory = new ActiveMQConnectionFactory(url);
        }
        user.ifPresent(factory::setUserName);
        password.ifPresent(factory::setPassword);
        // One in-flight message at a time: nothing sits unacked in a local buffer if the bridge dies
        factory.getPrefetchPolicy().setQueuePrefetch(1);
        factory.getRedeliveryPolicy().setMaximumRedeliveries(maxRedeliveries);
        // Advisory topics often require extra broker permissions we don't need
        factory.setWatchTopicAdvisories(false);
        return factory;
    }

    /** Safe-to-log summary (no secrets). */
    public String describe() {
        return "broker=" + effectiveBrokerUrl()
                + ", queue=" + queue
                + ", user=" + user.orElse("<anonymous>")
                + ", truststore=" + trustStore.orElse("<none>")
                + ", keystore=" + keyStore.orElse("<none>");
    }
}
