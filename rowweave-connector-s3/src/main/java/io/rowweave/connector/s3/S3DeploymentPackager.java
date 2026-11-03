package io.rowweave.connector.s3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.CRC32;

/** Builds the strict single-image archive consumed by RowWeave deployment trust. */
public final class S3DeploymentPackager {
    public static final String SOURCE_SERVICE_RESOURCE =
            "META-INF/services/io.rowweave.stream.spi.StreamSourceProvider";
    public static final String SOURCE_CONTRACT_RESOURCE =
            "META-INF/rowweave/stream-connector/contracts/s3-source-v1.bin";

    private static final long MAX_ENTRY_BYTES = 16L * 1024 * 1024;
    private static final long MAX_IMAGE_BYTES = 64L * 1024 * 1024;
    private static final int MAX_ENTRIES = 100_000;

    private S3DeploymentPackager() { }

    public static void main(String[] arguments) {
        if (arguments == null || arguments.length != 2) throw invalid();
        pack(java.nio.file.Paths.get(arguments[0]),
                java.nio.file.Paths.get(arguments[1]));
    }

    public static void pack(Path shadedJar, Path outputJar) {
        if (shadedJar == null || outputJar == null) throw invalid();
        Path input = shadedJar.toAbsolutePath().normalize();
        Path output = outputJar.toAbsolutePath().normalize();
        if (input.equals(output) || !Files.isRegularFile(input)
                || output.getParent() == null
                || !Files.isDirectory(output.getParent())) {
            throw invalid();
        }
        Path temporary = null;
        try {
            Map<String, byte[]> entries = read(input);
            entries.put(SOURCE_SERVICE_RESOURCE,
                    (S3ConnectorProvider.class.getName() + '='
                            + SOURCE_CONTRACT_RESOURCE + '\n')
                            .getBytes(StandardCharsets.UTF_8));
            entries.put(SOURCE_CONTRACT_RESOURCE,
                    S3ConnectorContract.encodedContract());
            temporary = Files.createTempFile(output.getParent(),
                    ".rowweave-s3-deployment-", ".jar.tmp");
            write(temporary, entries);
            if (Files.size(temporary) > MAX_IMAGE_BYTES) throw invalid();
            try {
                Files.move(temporary, output,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                Files.move(temporary, output,
                        StandardCopyOption.REPLACE_EXISTING);
            }
            temporary = null;
        } catch (RuntimeException failure) {
            if (failure instanceof IllegalArgumentException
                    && "S3_DEPLOYMENT_ARCHIVE_INVALID".equals(
                    failure.getMessage())) {
                throw failure;
            }
            throw invalid();
        } catch (IOException failure) {
            throw invalid();
        } finally {
            if (temporary != null) {
                try { Files.deleteIfExists(temporary); }
                catch (IOException ignored) { }
            }
        }
    }

    private static Map<String, byte[]> read(Path input) throws IOException {
        Map<String, byte[]> result = new TreeMap<>();
        Set<String> allNames = new HashSet<>();
        long expanded = 0L;
        int count = 0;
        try (JarFile jar = new JarFile(input.toFile(), false)) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                validateName(name);
                if (!allNames.add(name) || ++count > MAX_ENTRIES) {
                    throw invalid();
                }
                if (entry.isDirectory() || skip(name)) continue;
                long size = entry.getSize();
                if (size < 0 || size > MAX_ENTRY_BYTES) throw invalid();
                expanded = Math.addExact(expanded, size);
                if (expanded > MAX_IMAGE_BYTES) throw invalid();
                byte[] bytes = readExactly(jar.getInputStream(entry), size);
                if (result.put(name, bytes) != null) throw invalid();
            }
        }
        if (!result.containsKey(
                S3ConnectorProvider.class.getName().replace('.', '/')
                        + ".class")) {
            throw invalid();
        }
        return result;
    }

    private static boolean skip(String name) {
        if (SOURCE_SERVICE_RESOURCE.equals(name)
                || SOURCE_CONTRACT_RESOURCE.equals(name)
                || (S3DeploymentPackager.class.getName().replace('.', '/')
                        + ".class").equals(name)
                || "META-INF/MANIFEST.MF".equalsIgnoreCase(name)
                || name.startsWith("META-INF/versions/")
                || "module-info.class".equals(name)) {
            return true;
        }
        String upper = name.toUpperCase(java.util.Locale.ROOT);
        if (upper.startsWith("META-INF/")
                && (upper.endsWith(".SF") || upper.endsWith(".RSA")
                || upper.endsWith(".DSA") || upper.endsWith(".EC"))) {
            return true;
        }
        return (name.startsWith("io/rowweave/")
                && !name.startsWith("io/rowweave/connector/s3/"))
                || name.startsWith("org/slf4j/");
    }

    private static void validateName(String name) {
        if (name == null || name.isEmpty() || name.length() > 4096
                || name.startsWith("/") || name.indexOf('\\') >= 0
                || name.contains("//") || name.contains("/../")
                || name.contains("/./") || name.startsWith("../")
                || name.startsWith("./") || name.endsWith("/..")
                || name.endsWith("/.") || name.indexOf('\0') >= 0) {
            throw invalid();
        }
    }

    private static byte[] readExactly(InputStream input, long expected)
            throws IOException {
        try (InputStream stream = input) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    Math.toIntExact(expected));
            byte[] buffer = new byte[64 * 1024];
            long count = 0L;
            while (true) {
                int read = stream.read(buffer);
                if (read < 0) break;
                if (read == 0) continue;
                count = Math.addExact(count, read);
                if (count > expected) throw invalid();
                bytes.write(buffer, 0, read);
            }
            if (count != expected) throw invalid();
            return bytes.toByteArray();
        }
    }

    private static void write(Path target, Map<String, byte[]> entries)
            throws IOException {
        try (JarOutputStream output = new JarOutputStream(
                Files.newOutputStream(target))) {
            for (Map.Entry<String, byte[]> value : entries.entrySet()) {
                byte[] bytes = value.getValue();
                CRC32 crc = new CRC32();
                crc.update(bytes);
                JarEntry entry = new JarEntry(value.getKey());
                entry.setMethod(JarEntry.STORED);
                entry.setSize(bytes.length);
                entry.setCompressedSize(bytes.length);
                entry.setCrc(crc.getValue());
                entry.setTime(0L);
                output.putNextEntry(entry);
                output.write(bytes);
                output.closeEntry();
            }
        }
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException(
                "S3_DEPLOYMENT_ARCHIVE_INVALID");
    }
}
