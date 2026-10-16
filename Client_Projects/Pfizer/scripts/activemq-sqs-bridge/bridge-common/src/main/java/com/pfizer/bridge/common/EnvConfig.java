package com.pfizer.bridge.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Typed access to configuration; fails fast with a clear message on bad input.
 * Values come from environment variables and, optionally, an external env-style file
 * ({@code --config <file>} or {@code BRIDGE_CONFIG_FILE}); environment variables win over the file.
 */
public final class EnvConfig {

    public static final String CONFIG_FILE_VAR = "BRIDGE_CONFIG_FILE";
    private static final Pattern DURATION = Pattern.compile("(\\d+)\\s*(ms|s|m|h|d)");

    private final Map<String, String> env;

    public EnvConfig(Map<String, String> env) {
        this.env = env;
    }

    public static EnvConfig fromSystem() {
        return new EnvConfig(System.getenv());
    }

    /** Environment variables layered over the optional config file given by {@code --config} or {@code BRIDGE_CONFIG_FILE}. */
    public static EnvConfig fromSystem(String[] args) {
        Optional<String> file = configFileArg(args).or(() -> new EnvConfig(System.getenv()).optional(CONFIG_FILE_VAR));
        if (file.isEmpty()) {
            return fromSystem();
        }
        Map<String, String> merged = new HashMap<>(readEnvFile(Path.of(file.get())));
        merged.putAll(System.getenv());
        return new EnvConfig(merged);
    }

    private static Optional<String> configFileArg(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--config=")) {
                return Optional.of(args[i].substring("--config=".length()));
            }
            if (args[i].equals("--config")) {
                if (i + 1 >= args.length) {
                    throw new ConfigException("--config requires a file path");
                }
                return Optional.of(args[i + 1]);
            }
        }
        return Optional.empty();
    }

    /** Parses the same format as {@code docker --env-file}: KEY=VALUE lines, '#' comment lines, blank lines. */
    static Map<String, String> readEnvFile(Path path) {
        List<String> lines;
        try {
            lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ConfigException("Cannot read config file " + path + ": " + e.getMessage(), e);
        }
        Map<String, String> values = new HashMap<>();
        for (int n = 0; n < lines.size(); n++) {
            String line = lines.get(n).strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            int eq = line.indexOf('=');
            if (eq <= 0) {
                throw new ConfigException("Invalid line " + (n + 1) + " in " + path + " (expected KEY=VALUE): " + line);
            }
            values.put(line.substring(0, eq).strip(), line.substring(eq + 1).strip());
        }
        return values;
    }

    public Optional<String> optional(String key) {
        String value = env.get(key);
        return (value == null || value.isBlank()) ? Optional.empty() : Optional.of(value.trim());
    }

    public String optional(String key, String defaultValue) {
        return optional(key).orElse(defaultValue);
    }

    public String required(String key) {
        return optional(key).orElseThrow(() -> new ConfigException("Missing required environment variable: " + key));
    }

    /** Reads {@code key}, or the contents of the file named by {@code key_FILE} (Docker/Kubernetes secrets). */
    public Optional<String> secret(String key) {
        Optional<String> file = optional(key + "_FILE");
        if (file.isPresent()) {
            try {
                return Optional.of(Files.readString(Path.of(file.get()), StandardCharsets.UTF_8).trim());
            } catch (IOException e) {
                throw new ConfigException("Cannot read secret file for " + key + ": " + file.get(), e);
            }
        }
        return optional(key);
    }

    public int intValue(String key, int defaultValue) {
        return optional(key).map(v -> parse(key, v, Integer::parseInt)).orElse(defaultValue);
    }

    public long longValue(String key, long defaultValue) {
        return optional(key).map(v -> parse(key, v, Long::parseLong)).orElse(defaultValue);
    }

    public boolean boolValue(String key, boolean defaultValue) {
        return optional(key).map(v -> {
            if (v.equalsIgnoreCase("true") || v.equalsIgnoreCase("false")) {
                return Boolean.parseBoolean(v);
            }
            throw new ConfigException("Invalid boolean for " + key + ": " + v);
        }).orElse(defaultValue);
    }

    /**
     * Reads a duration such as {@code 500ms}, {@code 30s}, {@code 5m}, {@code 1h}, {@code 1d}, {@code 1h30m}
     * or ISO-8601 ({@code PT5M}). A bare {@code 0} is accepted; other bare numbers are rejected as ambiguous.
     */
    public Duration durationValue(String key, Duration defaultValue) {
        return optional(key).map(v -> parseDuration(key, v)).orElse(defaultValue);
    }

    static Duration parseDuration(String key, String value) {
        String v = value.trim().toLowerCase(Locale.ROOT);
        if (v.equals("0")) {
            return Duration.ZERO;
        }
        if (v.startsWith("p")) {
            try {
                return nonNegative(key, value, Duration.parse(value.trim().toUpperCase(Locale.ROOT)));
            } catch (DateTimeParseException e) {
                throw new ConfigException("Invalid duration for " + key + ": " + value, e);
            }
        }
        Matcher m = DURATION.matcher(v);
        Duration total = Duration.ZERO;
        int end = 0;
        while (m.find() && m.start() == end) {
            long amount = Long.parseLong(m.group(1));
            total = total.plus(switch (m.group(2)) {
                case "ms" -> Duration.ofMillis(amount);
                case "s" -> Duration.ofSeconds(amount);
                case "m" -> Duration.ofMinutes(amount);
                case "h" -> Duration.ofHours(amount);
                default -> Duration.ofDays(amount);
            });
            end = m.end();
            while (end < v.length() && v.charAt(end) == ' ') {
                end++;
            }
        }
        if (end == 0 || end != v.length()) {
            throw new ConfigException("Invalid duration for " + key + ": '" + value
                    + "' (use a unit, e.g. 30s, 5m, 10m, 1h, or ISO-8601 PT5M; 0 disables)");
        }
        return total;
    }

    private static Duration nonNegative(String key, String value, Duration d) {
        if (d.isNegative()) {
            throw new ConfigException("Duration for " + key + " must not be negative: " + value);
        }
        return d;
    }

    private static <T> T parse(String key, String value, java.util.function.Function<String, T> parser) {
        try {
            return parser.apply(value);
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid number for " + key + ": " + value, e);
        }
    }

    public static final class ConfigException extends RuntimeException {
        public ConfigException(String message) {
            super(message);
        }

        public ConfigException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
