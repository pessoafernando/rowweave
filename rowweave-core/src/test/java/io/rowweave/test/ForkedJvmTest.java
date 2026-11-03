package io.rowweave.test;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Arrays;
import java.lang.management.ManagementFactory;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ForkedJvmTest {

    @Test
    void surefireOwnsOneCompleteEnvelopeWithoutPollutingChildJvmEnvironment() {
        List<String> arguments = ManagementFactory.getRuntimeMXBean()
                .getInputArguments();
        assertEnvelope(arguments, "");
        for (String prefix : envelopePrefixes()) {
            assertEquals(1L, arguments.stream()
                    .filter(value -> value.startsWith(prefix)).count(), prefix);
        }
    }

    @Test
    void surefireAndFailsafeBothKeepCoverageAndTheirOwnCompleteEnvelope()
            throws Exception {
        Element root = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder().parse(Paths.get("..", "pom.xml")
                        .toFile()).getDocumentElement();
        NodeList plugins = root.getElementsByTagName("plugin");
        int found = 0;
        for (int index = 0; index < plugins.getLength(); index++) {
            Element plugin = (Element) plugins.item(index);
            String artifact = plugin.getElementsByTagName("artifactId")
                    .item(0).getTextContent();
            if (!"maven-surefire-plugin".equals(artifact)
                    && !"maven-failsafe-plugin".equals(artifact)) continue;
            NodeList argLines = plugin.getElementsByTagName("argLine");
            assertEquals(1, argLines.getLength(), artifact);
            String argLine = argLines.item(0).getTextContent();
            assertTrue(argLine.contains("@{argLine}"), artifact);
            assertTrue(argLine.contains("-Djava.io.tmpdir=${java.io.tmpdir}"),
                    artifact);
            List<String> arguments = Arrays.asList(argLine.trim().split("\\s+"));
            assertEnvelope(arguments, "");
            for (String prefix : envelopePrefixes()) {
                assertEquals(1L, arguments.stream()
                        .filter(value -> value.startsWith(prefix)).count(),
                        artifact + ":" + prefix);
            }
            found++;
        }
        assertEquals(2, found);
    }

    private static List<String> envelopePrefixes() {
        return Arrays.asList("-Xms", "-Xmx", "-Xss",
                "-XX:MaxDirectMemorySize=", "-XX:MaxMetaspaceSize=",
                "-XX:ReservedCodeCacheSize=", "-XX:ActiveProcessorCount=",
                "-Drowweave.process.memoryLimitBytes=");
    }

    @Test
    void surefireStartsWithConfiguredTemporaryDirectory() throws Exception {
        Path configured = Paths.get(System.getProperty("java.io.tmpdir"))
                .toRealPath();
        Path created = Files.createTempDirectory("rowweave-surefire-tmp-");
        try {
            assertEquals(configured, created.getParent().toRealPath());
        } finally {
            Files.deleteIfExists(created);
        }
    }

    @Test
    void javaAndKeytoolCommandsCarryCompleteResourceEnvelope() {
        List<String> java = ForkedJvm.command("example.Main");
        assertEnvelope(java, "");
        assertTrue(java.contains("-Djava.io.tmpdir="
                + System.getProperty("java.io.tmpdir")));

        List<String> keytool = ForkedJvm.keytoolCommand("-help");
        assertEnvelope(keytool, "-J");
        assertTrue(keytool.contains("-J-Djava.io.tmpdir="
                + System.getProperty("java.io.tmpdir")));
    }

    @Test
    void explicitOverridesReplaceHardCapsInsteadOfDuplicatingThem() {
        List<String> command = ForkedJvm.baseCommand();
        ForkedJvm.appendJvmArguments(command, Arrays.asList(
                "-Xmx3g", "-Drowweave.process.memoryLimitBytes=9663676416",
                "-Djava.io.tmpdir=/tmp/rowweave-explicit"));
        assertTrue(command.contains("-Xmx3g"));
        assertTrue(command.contains(
                "-Drowweave.process.memoryLimitBytes=9663676416"));
        assertTrue(command.stream().filter(value -> value.startsWith("-Xmx")).count() == 1L);
        assertTrue(command.stream().filter(value -> value.startsWith(
                "-Drowweave.process.memoryLimitBytes=")).count() == 1L);
        assertTrue(command.contains("-Djava.io.tmpdir=/tmp/rowweave-explicit"));
        assertTrue(command.stream().filter(value -> value.startsWith(
                "-Djava.io.tmpdir=")).count() == 1L);
    }

    private static void assertEnvelope(List<String> command, String prefix) {
        assertTrue(command.stream().anyMatch(value -> value.startsWith(prefix + "-Xms")));
        assertTrue(command.stream().anyMatch(value -> value.startsWith(prefix + "-Xmx")));
        assertTrue(command.stream().anyMatch(value ->
                value.startsWith(prefix + "-XX:MaxDirectMemorySize=")));
        assertTrue(command.stream().anyMatch(value ->
                value.startsWith(prefix + "-XX:MaxMetaspaceSize=")));
        assertTrue(command.stream().anyMatch(value ->
                value.startsWith(prefix + "-XX:ReservedCodeCacheSize=")));
        assertTrue(command.stream().anyMatch(value -> value.startsWith(prefix + "-Xss")));
        assertTrue(command.stream().anyMatch(value ->
                value.startsWith(prefix + "-XX:ActiveProcessorCount=")));
        if (prefix.isEmpty()) {
            assertTrue(command.stream().anyMatch(value -> value.startsWith(
                    "-Drowweave.process.memoryLimitBytes=")));
        }
    }
}
