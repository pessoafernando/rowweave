package io.rowweave.soak;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Reconstructs committed Sink order from append-only transactional files. */
final class StreamSoakOracle {
    private StreamSoakOracle() { }

    static List<String> visibleRows(Path root) throws IOException {
        if (!Files.isDirectory(root)) return Collections.emptyList();
        List<Path> files = new ArrayList<>();
        try (java.util.stream.Stream<Path> stream = Files.list(root)) {
            stream.filter(path -> path.getFileName().toString()
                            .startsWith("visible-"))
                    .forEach(files::add);
        }
        files.sort(Comparator
                .comparing(StreamSoakOracle::modifiedTime)
                .thenComparing(path -> path.getFileName().toString()));
        List<String> rows = new ArrayList<>();
        for (Path file : files) {
            for (String row : Files.readAllLines(file,
                    StandardCharsets.UTF_8)) {
                if (!row.isEmpty()) rows.add(row);
            }
        }
        return Collections.unmodifiableList(rows);
    }

    static String digest(List<String> rows) {
        try {
            MessageDigest digest = sha256();
            for (String row : rows) {
                update(digest, row);
            }
            return hex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    static Comparison compare(Path expected, Path actualRoot)
            throws IOException {
        MessageDigest expectedDigest;
        MessageDigest actualDigest;
        try {
            expectedDigest = sha256();
            actualDigest = sha256();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
        long expectedRows = 0L;
        long actualRows = 0L;
        try (BufferedReader expectedReader = Files.newBufferedReader(
                expected, StandardCharsets.UTF_8);
             VisibleReader actualReader = new VisibleReader(actualRoot)) {
            while (true) {
                String expectedRow = expectedReader.readLine();
                String actualRow = actualReader.readLine();
                if (expectedRow != null) {
                    expectedRows = Math.addExact(expectedRows, 1L);
                    update(expectedDigest, expectedRow);
                }
                if (actualRow != null) {
                    actualRows = Math.addExact(actualRows, 1L);
                    update(actualDigest, actualRow);
                }
                if (expectedRow == null || actualRow == null) {
                    if (expectedRow == null && actualRow == null) {
                        return new Comparison(Status.MATCH, expectedRows,
                                actualRows, hex(expectedDigest.digest()),
                                hex(actualDigest.digest()));
                    }
                    if (actualRow == null) {
                        for (String remaining;
                                (remaining = expectedReader.readLine())
                                != null; ) {
                            expectedRows = Math.addExact(expectedRows, 1L);
                            update(expectedDigest, remaining);
                        }
                        return new Comparison(Status.ACTUAL_PREFIX,
                                expectedRows, actualRows,
                                hex(expectedDigest.digest()),
                                hex(actualDigest.digest()));
                    }
                    return new Comparison(Status.DIVERGED, expectedRows,
                            actualRows, hex(expectedDigest.digest()),
                            hex(actualDigest.digest()));
                }
                if (!expectedRow.equals(actualRow)) {
                    return new Comparison(Status.DIVERGED, expectedRows,
                            actualRows, hex(expectedDigest.digest()),
                            hex(actualDigest.digest()));
                }
            }
        }
    }

    enum Status { MATCH, ACTUAL_PREFIX, DIVERGED }

    static final class Comparison {
        final Status status;
        final long expectedRows;
        final long actualRows;
        final String expectedDigest;
        final String actualDigest;

        Comparison(Status status, long expectedRows, long actualRows,
                String expectedDigest, String actualDigest) {
            this.status = status;
            this.expectedRows = expectedRows;
            this.actualRows = actualRows;
            this.expectedDigest = expectedDigest;
            this.actualDigest = actualDigest;
        }
    }

    private static final class VisibleReader implements Closeable {
        private final List<Path> files;
        private int nextFile;
        private BufferedReader current;

        VisibleReader(Path root) throws IOException {
            files = visibleFiles(root);
        }

        String readLine() throws IOException {
            while (true) {
                if (current == null) {
                    if (nextFile >= files.size()) return null;
                    current = Files.newBufferedReader(
                            files.get(nextFile++), StandardCharsets.UTF_8);
                }
                String row = current.readLine();
                if (row != null && !row.isEmpty()) return row;
                if (row != null) continue;
                current.close();
                current = null;
            }
        }

        @Override public void close() throws IOException {
            if (current != null) current.close();
        }
    }

    private static List<Path> visibleFiles(Path root) throws IOException {
        if (!Files.isDirectory(root)) return Collections.emptyList();
        List<Path> files = new ArrayList<>();
        try (java.util.stream.Stream<Path> stream = Files.list(root)) {
            stream.filter(path -> path.getFileName().toString()
                            .startsWith("visible-"))
                    .forEach(files::add);
        }
        files.sort(Comparator
                .comparing(StreamSoakOracle::modifiedTime)
                .thenComparing(path -> path.getFileName().toString()));
        return files;
    }

    private static MessageDigest sha256()
            throws java.security.NoSuchAlgorithmException {
        return MessageDigest.getInstance("SHA-256");
    }

    private static void update(MessageDigest digest, String row) {
        digest.update(row.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) '\n');
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 0xf, 16));
            result.append(Character.forDigit(value & 0xf, 16));
        }
        return result.toString();
    }

    private static FileTime modifiedTime(Path path) {
        try { return Files.getLastModifiedTime(path); }
        catch (IOException failure) {
            throw new IllegalStateException(
                    "STREAM_SOAK_ORACLE_FILE_METADATA_UNAVAILABLE", failure);
        }
    }
}
