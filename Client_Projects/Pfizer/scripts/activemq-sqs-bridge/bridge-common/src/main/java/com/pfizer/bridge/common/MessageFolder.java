package com.pfizer.bridge.common;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Hand-off folder between the local service (amq-to-folder) and the Pfizer service (folder-to-sqs).
 * <p>
 * Each message is {@code <timestamp>_<jmsId>.xml} plus an optional {@code <same>.properties} metadata sidecar.
 * The sidecar is written first and the XML is renamed into place atomically, so a reader that only picks up
 * {@code *.xml} never sees a partial file. Names derive from the JMS timestamp and id, so a redelivered message
 * maps to the same file (idempotent) and sorting by name keeps arrival order.
 */
public final class MessageFolder {

    public static final String XML_SUFFIX = ".xml";
    public static final String META_SUFFIX = ".properties";
    private static final String TMP_SUFFIX = ".tmp";
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS'Z'").withZone(ZoneOffset.UTC);

    /** An XML file waiting in the folder, with its metadata. */
    public record Pending(Path xmlFile, String body, MessageMetadata metadata) {
        /** Stable key for SQS FIFO de-duplication: the JMS id, or the file name for hand-dropped files. */
        public String dedupKey() {
            return metadata.jmsMessageId().orElse(xmlFile.getFileName().toString());
        }
    }

    private final Path directory;

    public MessageFolder(Path directory) {
        this.directory = directory;
    }

    public Path directory() {
        return directory;
    }

    /** @return the XML file holding the message; if it already exists (redelivery) it is left untouched. */
    public Path write(String body, MessageMetadata metadata) throws IOException {
        Files.createDirectories(directory);
        Instant ts = metadata.jmsTimestamp() > 0 ? Instant.ofEpochMilli(metadata.jmsTimestamp()) : Instant.now();
        String id = metadata.jmsMessageId().orElseGet(() -> UUID.randomUUID().toString());
        String baseName = TS.format(ts) + "_" + id.replaceAll("[^A-Za-z0-9._-]", "_");
        Path xml = directory.resolve(baseName + XML_SUFFIX);
        if (Files.exists(xml)) {
            return xml;
        }

        Path metaTmp = directory.resolve(baseName + META_SUFFIX + TMP_SUFFIX);
        try (Writer w = Files.newBufferedWriter(metaTmp, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, StandardOpenOption.DSYNC)) {
            metadata.toProperties().store(w, "Metadata for " + baseName + XML_SUFFIX);
        }
        move(metaTmp, directory.resolve(baseName + META_SUFFIX));

        Path xmlTmp = directory.resolve(baseName + XML_SUFFIX + TMP_SUFFIX);
        Files.writeString(xmlTmp, body, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, StandardOpenOption.DSYNC);
        move(xmlTmp, xml);
        return xml;
    }

    /** @return {@code *.xml} files directly in the folder, oldest name first. */
    public List<Path> pending() throws IOException {
        List<Path> files = new ArrayList<>();
        if (!Files.isDirectory(directory)) {
            return files;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*" + XML_SUFFIX)) {
            for (Path p : stream) {
                if (Files.isRegularFile(p)) {
                    files.add(p);
                }
            }
        }
        files.sort((a, b) -> a.getFileName().toString().compareTo(b.getFileName().toString()));
        return files;
    }

    public Pending read(Path xml, String defaultSite, String defaultQueue) throws IOException {
        String body = Files.readString(xml, StandardCharsets.UTF_8);
        Properties props = new Properties();
        Path meta = sidecar(xml);
        if (Files.exists(meta)) {
            try (Reader r = Files.newBufferedReader(meta, StandardCharsets.UTF_8)) {
                props.load(r);
            }
        }
        return new Pending(xml, body, MessageMetadata.fromProperties(props, defaultSite, defaultQueue));
    }

    /** Moves the XML (first) and its sidecar into {@code targetDir}. */
    public static Path moveTo(Path xml, Path targetDir) throws IOException {
        Files.createDirectories(targetDir);
        Path target = targetDir.resolve(xml.getFileName());
        Files.move(xml, target, StandardCopyOption.REPLACE_EXISTING);
        Path meta = sidecar(xml);
        if (Files.exists(meta)) {
            Files.move(meta, targetDir.resolve(meta.getFileName()), StandardCopyOption.REPLACE_EXISTING);
        }
        return target;
    }

    /** Deletes the XML (first) and its sidecar. */
    public static void delete(Path xml) throws IOException {
        Files.deleteIfExists(xml);
        Files.deleteIfExists(sidecar(xml));
    }

    public static Path sidecar(Path xml) {
        String name = xml.getFileName().toString();
        String base = name.endsWith(XML_SUFFIX) ? name.substring(0, name.length() - XML_SUFFIX.length()) : name;
        return xml.resolveSibling(base + META_SUFFIX);
    }

    private static void move(Path from, Path to) throws IOException {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
