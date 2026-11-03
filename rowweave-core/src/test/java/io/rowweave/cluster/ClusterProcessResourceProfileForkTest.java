package io.rowweave.cluster;

import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** fresh-process master/worker/combined resource certification. */
class ClusterProcessResourceProfileForkTest {

    @Test
    void roleProfilesStayBelowDeclaredJvmAndProcessBounds() throws Exception {
        for (String role : new String[] {"master", "worker", "combined"}) {
            assertRole(role);
        }
    }

    private static void assertRole(String role) throws Exception {
        List<String> command = new ArrayList<>(ForkedJvm.baseCommand());
        ForkedJvm.appendJvmArguments(command, java.util.Arrays.asList(
                "-Xms256m", "-Xmx2048m",
                "-XX:MaxDirectMemorySize=256m",
                "-XX:MaxMetaspaceSize=256m",
                "-XX:ReservedCodeCacheSize=128m",
                "-Xss1m", "-XX:ActiveProcessorCount=4",
                "-Drowweave.process.memoryLimitBytes=4294967296"));
        command.add("-Drowweave.worker.memory=268435456");
        command.add("-Drowweave.worker.slots=2");
        command.add("-Drowweave.worker.maxStageThreads=16");
        command.add("-Drowweave.control.maxConnections=8");
        command.add("-Drowweave.shuffle.maxConnections=8");
        command.add("-Drowweave.shuffle.maxIngressHandlers=8");
        command.add("-Drowweave.shuffle.ioThreads=2");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(ClusterProcessResourceProbe.class.getName());
        command.add(role);

        Process child = new ProcessBuilder(command).redirectErrorStream(true).start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                child.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter input = new BufferedWriter(new OutputStreamWriter(
                     child.getOutputStream(), StandardCharsets.UTF_8))) {
            String profileLine = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
            while (System.nanoTime() < deadline) {
                String line = reader.readLine();
                if (line == null) break;
                output.append(line).append('\n');
                if (line.startsWith("ROLE_PROFILE ")) {
                    profileLine = line;
                    break;
                }
            }
            assertTrue(profileLine != null, output.toString());
            Map<String, Long> profile = parse(profileLine);
            long rssBytes = exactRssBytes(child);
            System.out.println(profileLine + " rssBytes=" + rssBytes);
            assertTrue(profile.get("heapUsed") < profile.get("heapCap"), profileLine);
            assertTrue(profile.get("directUsed") < profile.get("directCap"), profileLine);
            assertTrue(profile.get("metaspaceUsed") < profile.get("metaspaceCap"), profileLine);
            assertTrue(profile.get("codeUsed") < profile.get("codeCap"), profileLine);
            assertTrue(profile.get("rowweaveThreads") <= profile.get("threadUpper"), profileLine);
            assertTrue(rssBytes < profile.get("rssRequired"),
                    profileLine + " rssBytes=" + rssBytes);
            assertTrue(profile.get("rssRequired") <= profile.get("processLimit"), profileLine);
            input.newLine();
            input.flush();
        }
        assertTrue(child.waitFor(30, TimeUnit.SECONDS), output.toString());
        assertEquals(0, child.exitValue(), output.toString());
    }

    private static long exactRssBytes(Process child) throws Exception {
        long pid = (Long) Process.class.getMethod("pid").invoke(child);
        Process ps = new ProcessBuilder("ps", "-o", "rss=", "-p",
                Long.toString(pid)).redirectErrorStream(true).start();
        String line;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                ps.getInputStream(), StandardCharsets.UTF_8))) {
            line = reader.readLine();
        }
        assertTrue(ps.waitFor(10, TimeUnit.SECONDS), "exact-child RSS probe timed out");
        assertEquals(0, ps.exitValue(), "exact-child RSS probe failed");
        return Long.parseLong(line.trim()) * 1024L;
    }

    private static Map<String, Long> parse(String line) {
        Map<String, Long> values = new LinkedHashMap<>();
        for (String token : line.split(" ")) {
            int separator = token.indexOf('=');
            if (separator > 0 && !"mode".equals(token.substring(0, separator))) {
                values.put(token.substring(0, separator),
                        Long.parseLong(token.substring(separator + 1)));
            }
        }
        return values;
    }
}
