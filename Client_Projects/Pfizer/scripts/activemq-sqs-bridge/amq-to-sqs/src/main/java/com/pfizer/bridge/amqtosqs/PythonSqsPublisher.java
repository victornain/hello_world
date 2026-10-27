package com.pfizer.bridge.amqtosqs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;

/** Sends SQS requests to a persistent Python worker that publishes through boto3. */
final class PythonSqsPublisher implements AutoCloseable {

    private final String site;
    private final String queueUrl;
    private final String region;
    private final String endpointUrl;
    private final int maxBodyBytes;
    private final String pythonExecutable;
    private final Path scriptPath;
    private final ObjectMapper json = new ObjectMapper();

    private volatile Process process;
    private BufferedWriter input;
    private BufferedReader output;

    PythonSqsPublisher(String site, String queueUrl, String region, String endpointUrl, int maxBodyBytes,
                       String pythonExecutable, Path scriptPath) {
        this.site = site;
        this.queueUrl = queueUrl;
        this.region = region;
        this.endpointUrl = endpointUrl;
        this.maxBodyBytes = maxBodyBytes;
        this.pythonExecutable = pythonExecutable;
        this.scriptPath = scriptPath;
    }

    synchronized String send(String body, Map<String, MessageAttributeValue> attributes, String jmsMessageId)
            throws PublishException {
        try {
            ensureStarted();
            ObjectNode request = json.createObjectNode()
                    .put("body", body)
                    .put("jms_message_id", jmsMessageId);
            ObjectNode requestAttributes = request.putObject("message_attributes");
            attributes.forEach((name, value) -> {
                ObjectNode attribute = requestAttributes.putObject(name).put("DataType", value.dataType());
                if (value.stringValue() != null) {
                    attribute.put("StringValue", value.stringValue());
                } else if (value.binaryValue() != null) {
                    attribute.put("BinaryValueBase64", Base64.getEncoder().encodeToString(value.binaryValue().asByteArray()));
                }
            });
            input.write(json.writeValueAsString(request));
            input.newLine();
            input.flush();

            String responseLine = output.readLine();
            if (responseLine == null) {
                discardWorker();
                throw new PublishException("Python publisher exited without a response");
            }
            JsonNode response = json.readTree(responseLine);
            if (!response.path("ok").asBoolean()) {
                throw new PublishException(response.path("error_code").asText("PythonPublisherError"));
            }
            return response.path("message_id").asText();
        } catch (IOException e) {
            discardWorker();
            throw new PublishException("PythonBoto3Unavailable", e);
        }
    }

    void stop() {
        Process current = process;
        if (current != null) {
            current.destroy();
        }
    }

    @Override
    public synchronized void close() {
        Process current = process;
        if (current == null) {
            return;
        }
        try {
            if (current.isAlive() && input != null) {
                input.write("{\"shutdown\":true}");
                input.newLine();
                input.flush();
                current.waitFor(2, java.util.concurrent.TimeUnit.SECONDS);
            }
        } catch (IOException e) {
            current.destroy();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            current.destroy();
        } finally {
            if (current.isAlive()) {
                current.destroyForcibly();
            }
            closeStreams();
            process = null;
        }
    }

    private void ensureStarted() throws IOException {
        if (process != null && process.isAlive()) {
            return;
        }
        closeStreams();
        ProcessBuilder builder = new ProcessBuilder(
                pythonExecutable, scriptPath.toString(),
                "--site", site,
                "--queue-url", queueUrl,
                "--region", region,
                "--max-body-bytes", Integer.toString(maxBodyBytes));
        if (endpointUrl != null && !endpointUrl.isBlank()) {
            builder.command().add("--endpoint-url");
            builder.command().add(endpointUrl);
        }
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        process = builder.start();
        input = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
        output = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
    }

    private void discardWorker() {
        Process current = process;
        if (current != null) {
            current.destroyForcibly();
        }
        closeStreams();
        process = null;
    }

    private void closeStreams() {
        try {
            if (input != null) {
                input.close();
            }
        } catch (IOException ignored) {
        }
        try {
            if (output != null) {
                output.close();
            }
        } catch (IOException ignored) {
        }
        input = null;
        output = null;
    }

    static final class PublishException extends Exception {
        private final String errorCode;

        PublishException(String errorCode) {
            super(errorCode);
            this.errorCode = errorCode;
        }

        PublishException(String errorCode, Throwable cause) {
            super(errorCode, cause);
            this.errorCode = errorCode;
        }

        String errorCode() {
            return errorCode;
        }
    }
}