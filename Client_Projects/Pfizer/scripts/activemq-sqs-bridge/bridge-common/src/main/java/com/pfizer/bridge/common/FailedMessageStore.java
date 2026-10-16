package com.pfizer.bridge.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Last-resort local store for messages that can never be forwarded (e.g. too large for SQS, unsupported JMS type).
 * The broker has no persistence, so dropping them would lose data; mount this directory as a volume.
 */
public final class FailedMessageStore {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final Path directory;

    public FailedMessageStore(Path directory) {
        this.directory = directory;
    }

    public Path save(String messageId, String body, String reason) throws IOException {
        Files.createDirectories(directory);
        String baseName = TS.format(Instant.now()) + "_" + messageId.replaceAll("[^A-Za-z0-9._-]", "_");
        Path bodyFile = directory.resolve(baseName + ".msg");
        Files.writeString(bodyFile, body, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        Files.writeString(directory.resolve(baseName + ".reason.txt"), reason, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        return bodyFile;
    }
}
