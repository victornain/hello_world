package com.pfizer.bridge.common;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

import java.net.URI;
import java.util.Optional;

/**
 * SQS settings. Credentials come from the AWS default chain (env vars, AWS_PROFILE,
 * web identity, container/instance role). The SDK only sees real environment variables, so
 * AWS_PROFILE / AWS_ACCESS_KEY_ID set in a {@code --config} file are applied explicitly here.
 */
public final class SqsSettings {

    private final String region;
    private final String queueUrl;
    private final Optional<String> endpointOverride;
    private final Optional<String> profile;
    private final Optional<String> accessKeyId;
    private final Optional<String> secretAccessKey;
    private final Optional<String> sessionToken;

    private SqsSettings(EnvConfig env) {
        this.region = env.required("AWS_REGION");
        this.queueUrl = env.required("SQS_QUEUE_URL");
        this.endpointOverride = env.optional("SQS_ENDPOINT_OVERRIDE");
        this.profile = env.optional("AWS_PROFILE");
        this.accessKeyId = env.optional("AWS_ACCESS_KEY_ID");
        this.secretAccessKey = env.secret("AWS_SECRET_ACCESS_KEY");
        this.sessionToken = env.secret("AWS_SESSION_TOKEN");
    }

    private AwsCredentialsProvider credentialsProvider() {
        if (accessKeyId.isPresent() && secretAccessKey.isPresent()) {
            return StaticCredentialsProvider.create(sessionToken.isPresent()
                    ? AwsSessionCredentials.create(accessKeyId.get(), secretAccessKey.get(), sessionToken.get())
                    : AwsBasicCredentials.create(accessKeyId.get(), secretAccessKey.get()));
        }
        if (profile.isPresent()) {
            return ProfileCredentialsProvider.create(profile.get());
        }
        return DefaultCredentialsProvider.create();
    }

    public static SqsSettings fromEnv(EnvConfig env) {
        return new SqsSettings(env);
    }

    public String queueUrl() {
        return queueUrl;
    }

    public boolean isFifo() {
        return queueUrl.endsWith(".fifo");
    }

    public SqsClient createClient() {
        SqsClientBuilder builder = SqsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(credentialsProvider());
        endpointOverride.ifPresent(e -> builder.endpointOverride(URI.create(e)));
        return builder.build();
    }

    public String describe() {
        return "region=" + region + ", queueUrl=" + queueUrl + ", fifo=" + isFifo()
                + endpointOverride.map(e -> ", endpoint=" + e).orElse("");
    }
}
