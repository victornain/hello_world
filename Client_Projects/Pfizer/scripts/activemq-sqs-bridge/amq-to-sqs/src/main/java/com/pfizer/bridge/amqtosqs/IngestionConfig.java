package com.pfizer.bridge.amqtosqs;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Validated view of the selected environment in ingestion.yaml. */
record IngestionConfig(String region, List<Site> sites) {

    static IngestionConfig load(Path path, String environment) {
        Object document;
        try (InputStream input = Files.newInputStream(path)) {
            LoaderOptions options = new LoaderOptions();
            options.setAllowDuplicateKeys(false);
            document = new Yaml(new SafeConstructor(options)).load(input);
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException("Cannot read ingestion config " + path + ": " + e.getMessage(), e);
        }

        Map<?, ?> root = asMap(document, "configuration root");
        Map<?, ?> aws = requiredMap(root, "aws", "configuration");
        String region = requiredString(aws, "region", "aws");
        Map<?, ?> environments = requiredMap(root, "environments", "configuration");
        Object environmentValue = environments.get(environment);
        if (environmentValue == null) {
            throw new IllegalArgumentException("Environment " + environment + " is not defined in " + path);
        }

        Map<?, ?> selected = asMap(environmentValue, "environment " + environment);
        Object sitesValue = selected.get("sites");
        if (!(sitesValue instanceof List<?> siteEntries) || siteEntries.isEmpty()) {
            throw new IllegalArgumentException("Environment " + environment + " must contain at least one site");
        }

        List<Site> sites = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (int index = 0; index < siteEntries.size(); index++) {
            String context = "environment " + environment + " site entry " + (index + 1);
            Map<?, ?> entry = asMap(siteEntries.get(index), context);
            String site = requiredString(entry, "site", context);
            if (!names.add(site.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("Duplicate site '" + site + "' in environment " + environment);
            }
            Map<?, ?> activemq = requiredMap(entry, "activemq", "site " + site);
            String brokerUrl = requiredString(activemq, "url", "site " + site + " activemq");
            String brokerQueue = requiredString(activemq, "queue", "site " + site + " activemq");
            String secret = optionalString(activemq, "credentials_secret", "site " + site + " activemq");
            Map<?, ?> sqs = requiredMap(entry, "sqs", "site " + site);
            String queueUrl = requiredString(sqs, "queue_url", "site " + site + " sqs");
            validateQueueUrl(queueUrl, site);
            sites.add(new Site(site, brokerUrl, brokerQueue, secret, queueUrl));
        }
        return new IngestionConfig(region, List.copyOf(sites));
    }

    static BrokerCredentials parseCredentials(String site, String secret) {
        Object document;
        try {
            LoaderOptions options = new LoaderOptions();
            options.setAllowDuplicateKeys(false);
            document = new Yaml(new SafeConstructor(options)).load(new StringReader(secret));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Credentials secret for site " + site
                    + " must be a JSON object with username and password", e);
        }
        Map<?, ?> values = asMap(document, "credentials secret for site " + site);
        return new BrokerCredentials(
                requiredString(values, "username", "credentials secret for site " + site),
                requiredString(values, "password", "credentials secret for site " + site));
    }

    private static void validateQueueUrl(String queueUrl, String site) {
        try {
            URI uri = URI.create(queueUrl);
            String scheme = uri.getScheme();
                if ((!"https".equalsIgnoreCase(scheme) && !"http".equalsIgnoreCase(scheme)) || uri.getHost() == null) {
                throw new IllegalArgumentException("SQS queue_url for site " + site + " must be an http(s) URL with a host");
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid SQS queue_url for site " + site, e);
        }
    }

    private static Map<?, ?> requiredMap(Map<?, ?> values, String key, String context) {
        Object value = values.get(key);
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("Missing or invalid " + key + " for " + context);
        }
        return map;
    }

    private static Map<?, ?> asMap(Object value, String context) {
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("Expected a mapping for " + context);
        }
        return map;
    }

    private static String requiredString(Map<?, ?> values, String key, String context) {
        String value = optionalString(values, key, context);
        if (value == null) {
            throw new IllegalArgumentException("Missing required " + key + " for " + context);
        }
        return value;
    }

    private static String optionalString(Map<?, ?> values, String key, String context) {
        Object value = values.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String stringValue) || stringValue.isBlank()) {
            throw new IllegalArgumentException("Invalid " + key + " for " + context);
        }
        return stringValue.trim();
    }

    record Site(String site, String brokerUrl, String brokerQueue, String credentialsSecret, String sqsQueueUrl) {
    }

    record BrokerCredentials(String username, String password) {
    }
}