package io.rowweave.stream;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Exported Stream Java signatures are frozen by the public API contract. */
class StreamPublicApiSignatureBaselineTest {
    private static final String STREAM_SIGNATURE_DIGEST =
            "13ea84fe5613fac1aab6244d0b0568c2ab392878190732378aa04c74b9027e4e";

    @Test
    void exportedStreamSignaturesStayBinaryAndSourceCompatible()
            throws Exception {
        String signatures = signatures();
        assertFalse(signatures.contains("io.rowweave.stream.cluster"));
        assertFalse(signatures.contains("io.rowweave.stream.contract"));
        assertFalse(signatures.contains("io.rowweave.stream.runtime"));
        assertFalse(signatures.contains("io.rowweave.internal"));
        assertEquals(STREAM_SIGNATURE_DIGEST, sha256(signatures), signatures);
    }

    private static String signatures() throws Exception {
        Path source = sourceRoot();
        Set<Class<?>> types = new LinkedHashSet<>();
        try (Stream<Path> files = Files.walk(source)) {
            files.filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> exportedSource(source, path))
                    .sorted()
                    .forEach(path -> addType(types, source, path));
        }
        List<String> lines = new ArrayList<>();
        for (Class<?> type : types) addSignatures(lines, type);
        Collections.sort(lines);
        return String.join("\n", lines) + "\n";
    }

    private static boolean exportedSource(Path root, Path path) {
        String relative = root.relativize(path).toString()
                .replace(java.io.File.separatorChar, '/');
        if (relative.startsWith("stream/api/")
                || relative.startsWith("stream/spi/")) {
            return true;
        }
        return relative.equals("RowWeave.java")
                || relative.equals("DataStream.java")
                || relative.startsWith("Stream");
    }

    private static void addType(
            Set<Class<?>> types, Path root, Path path) {
        String name = root.relativize(path).toString()
                .replace(java.io.File.separatorChar, '.')
                .replaceAll("\\.java$", "");
        try {
            addPublic(types, Class.forName("io.rowweave." + name));
        } catch (ClassNotFoundException failure) {
            throw new IllegalStateException("cannot load public API " + name,
                    failure);
        }
    }

    private static void addPublic(Set<Class<?>> types, Class<?> type) {
        if (!Modifier.isPublic(type.getModifiers()) || !types.add(type)) return;
        for (Class<?> nested : type.getDeclaredClasses()) {
            addPublic(types, nested);
        }
    }

    private static void addSignatures(
            List<String> lines, Class<?> type) {
        lines.add("TYPE " + type.toGenericString());
        if (type.getGenericSuperclass() != null) {
            lines.add("SUPER " + type.getName() + " "
                    + type.getGenericSuperclass().getTypeName());
        }
        List<String> interfaces = new ArrayList<>();
        for (java.lang.reflect.Type value : type.getGenericInterfaces()) {
            interfaces.add(value.getTypeName());
        }
        Collections.sort(interfaces);
        for (String value : interfaces) {
            lines.add("INTERFACE " + type.getName() + " " + value);
        }
        for (Constructor<?> constructor : type.getDeclaredConstructors()) {
            if (Modifier.isPublic(constructor.getModifiers())
                    && !constructor.isSynthetic()) {
                lines.add("CONSTRUCTOR " + constructor.toGenericString());
            }
        }
        for (Method method : type.getDeclaredMethods()) {
            if (Modifier.isPublic(method.getModifiers())
                    && !method.isSynthetic() && !method.isBridge()
                    && includeRowWeaveMethod(type, method)) {
                lines.add("METHOD " + method.toGenericString());
            }
        }
        for (Field field : type.getDeclaredFields()) {
            if (Modifier.isPublic(field.getModifiers())
                    && !field.isSynthetic()) {
                lines.add("FIELD " + field.toGenericString());
            }
        }
    }

    private static boolean includeRowWeaveMethod(Class<?> type, Method method) {
        return !"io.rowweave.RowWeave".equals(type.getName())
                || method.getName().toLowerCase(java.util.Locale.ROOT)
                .contains("stream");
    }

    private static String sha256(String value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                value.getBytes(StandardCharsets.UTF_8));
        StringBuilder text = new StringBuilder(64);
        for (byte current : digest) {
            text.append(String.format("%02x", current & 0xff));
        }
        return text.toString();
    }

    private static Path sourceRoot() {
        Path current = Paths.get("").toAbsolutePath().normalize();
        Path direct = current.resolve("src/main/java/io/rowweave");
        if (Files.isDirectory(direct)) return direct;
        Path reactor = current.resolve(
                "rowweave-core/src/main/java/io/rowweave");
        if (Files.isDirectory(reactor)) return reactor;
        throw new IllegalStateException("RowWeave source root is unavailable");
    }
}
