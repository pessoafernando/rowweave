package io.rowweave.release;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReleaseGoldenCorpusTest {

    @Test
    void canonicalCorpusLocksIndependentPublicFramesAndProjections()
            throws Exception {
        List<String> lines = resource("/compat/golden-corpus-v1.tsv");
        assertEquals("RWXXCORPUS1", lines.get(0));
        assertEquals(9, lines.size());

        List<String> keys = new ArrayList<>();
        for (int index = 1; index < lines.size(); index++) {
            String[] fields = lines.get(index).split("\\t", -1);
            assertEquals(7, fields.length);
            String key = fields[0] + "|" + fields[1] + "|" + fields[2];
            keys.add(key);

            assertEquals(ReleaseComponentContract.Scope.PUBLIC_FROZEN,
                    ReleaseContractCatalog.requirePublicV1Component(fields[1]).scope());

            List<String> frameLines = resource("/" + fields[4]);
            assertEquals(1, frameLines.size());
            String frameHex = frameLines.get(0);
            assertTrue(frameHex.matches("[0-9a-f]+"));
            assertEquals(0, frameHex.length() % 2);
            assertEquals(fields[5], sha256(fromHex(frameHex)));
            assertEquals(frameHex, run(fields[3], "emit"));
            assertEquals(fields[6], run(fields[3], "project", frameHex));
        }

        List<String> sorted = new ArrayList<>(keys);
        Collections.sort(sorted);
        assertEquals(sorted, keys);
        assertTrue(keys.get(0).startsWith("rowweave-1.0.0|"));
        assertTrue(keys.get(1).startsWith("rowweave-1.1.0|"));
        assertTrue(keys.contains("rowweave-1.2.0|rowweave.function-provider|2"));
        assertTrue(keys.contains("rowweave-1.2.0|rowweave.portable-batch-plan|2"));
        assertTrue(keys.contains(
                "rowweave-1.2.0|rowweave.portable-connector-provider|2"));
        assertTrue(keys.contains(
                "rowweave-1.3.0|rowweave.store-format-header|3"));
        assertTrue(keys.contains(
                "rowweave-1.3.0|rowweave.upgrade-authority|3"));
        assertTrue(keys.contains(
                "rowweave-1.4.0|rowweave.control-protocol|4"));
    }

    private static List<String> resource(String name) throws Exception {
        InputStream input = ReleaseGoldenCorpusTest.class.getResourceAsStream(name);
        if (input == null) throw new AssertionError("missing resource " + name);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                input, StandardCharsets.US_ASCII))) {
            return reader.lines().collect(Collectors.toList());
        }
    }

    private static String run(String mainClass, String... arguments)
            throws Exception {
        String[] command = new String[arguments.length + 4];
        command[0] = Paths.get(System.getProperty("java.home"), "bin", "java")
                .toString();
        command[1] = "-cp";
        command[2] = System.getProperty("java.class.path");
        command[3] = mainClass;
        System.arraycopy(arguments, 0, command, 4, arguments.length);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                process.getInputStream(), StandardCharsets.UTF_8))) {
            output = reader.lines().collect(Collectors.joining("\n")).trim();
        }
        assertEquals(0, process.waitFor(), output);
        return output;
    }

    private static String sha256(byte[] bytes) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder value = new StringBuilder(digest.length * 2);
        for (byte current : digest) {
            value.append(Character.forDigit((current >>> 4) & 0xf, 16));
            value.append(Character.forDigit(current & 0xf, 16));
        }
        return value.toString();
    }

    private static byte[] fromHex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) Integer.parseInt(
                    value.substring(index * 2, index * 2 + 2), 16);
        }
        return bytes;
    }
}
