package com.pfizer.bridge.common;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.InvalidMessageContentsException;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;

/** Sends one message to SQS. Throws {@link RejectedException} for errors that retrying cannot fix. */
public final class SqsPublisher implements AutoCloseable {

    private static final Set<String> NON_RETRYABLE_CODES = Set.of(
            "InvalidMessageContents", "InvalidParameterValue", "InvalidAttributeValue", "InvalidAttributeName");

    private final SqsClient client;
    private final String queueUrl;
    private final boolean fifo;
    private final String siteId;
    private final int maxBodyBytes;

    public SqsPublisher(SqsClient client, SqsSettings settings, String siteId, int maxBodyBytes) {
        this.client = client;
        this.queueUrl = settings.queueUrl();
        this.fifo = settings.isFifo();
        this.siteId = siteId;
        this.maxBodyBytes = maxBodyBytes;
    }

    /** @return the SQS message id. Other {@code SdkException}s are transient and should be retried. */
    public String send(String body, Map<String, MessageAttributeValue> attributes, String dedupKey) throws RejectedException {
        if (body.isEmpty()) {
            throw new RejectedException("Empty message body");
        }
        int size = body.getBytes(StandardCharsets.UTF_8).length;
        if (size > maxBodyBytes) {
            throw new RejectedException("Body is " + size + " bytes, exceeds SQS limit of " + maxBodyBytes);
        }

        SendMessageRequest.Builder request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(body)
                .messageAttributes(attributes);
        if (fifo) {
            // Same JMS message redelivered after a crash => same dedup id => SQS drops the duplicate (5-min window)
            request.messageGroupId(siteId).messageDeduplicationId(sha256(siteId + "|" + dedupKey));
        }

        try {
            return client.sendMessage(request.build()).messageId();
        } catch (InvalidMessageContentsException e) {
            throw new RejectedException(e.getMessage());
        } catch (SqsException e) {
            String code = e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null;
            if (code != null && NON_RETRYABLE_CODES.contains(code)) {
                throw new RejectedException(code + ": " + e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static final class RejectedException extends Exception {
        public RejectedException(String message) {
            super(message);
        }
    }
}
