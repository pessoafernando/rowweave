package io.rowweave.cluster.registry.zk;

import io.rowweave.cluster.JvmCapacityEnvelope;
import io.rowweave.test.ForkedJvm;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Curator 5.9.0 session reserve certified in an isolated JVM. */
public class ZkThreadReserveProfileTest {

    @Test
    void twoIndependentRoleSessionsFitTwoFixedVendorReserves() throws Exception {
        List<String> command = new ArrayList<>(ForkedJvm.baseCommand());
        ForkedJvm.appendJvmArguments(command, java.util.Arrays.asList(
                "-Xms128m", "-Xmx768m",
                "-XX:MaxDirectMemorySize=128m",
                "-XX:MaxMetaspaceSize=256m",
                "-XX:ReservedCodeCacheSize=128m",
                "-Xss1m", "-XX:ActiveProcessorCount=4",
                "-Drowweave.process.memoryLimitBytes=3221225472"));
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(ZkThreadReserveProfileTest.class.getName());
        command.add("probe");

        Process child = new ProcessBuilder(command).redirectErrorStream(true).start();
        StringBuilder output = new StringBuilder();
        String profileLine = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                child.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter input = new BufferedWriter(new OutputStreamWriter(
                     child.getOutputStream(), StandardCharsets.UTF_8))) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45L);
            while (System.nanoTime() < deadline) {
                String line = reader.readLine();
                if (line == null) break;
                output.append(line).append('\n');
                if (line.startsWith("ZK_PROCESS_PROFILE ")) {
                    profileLine = line;
                    break;
                }
            }
            assertTrue(profileLine != null, output.toString());
            Map<String, Long> profile = parse(profileLine);
            assertTrue(profile.get("masterAdded") > 0L
                    && profile.get("masterAdded") <= 16L, profileLine);
            assertTrue(profile.get("twoSessionsAdded") >= profile.get("masterAdded")
                    && profile.get("twoSessionsAdded") <= 32L, profileLine);
            assertTrue(profile.get("heapUsed") < profile.get("heapCap"), profileLine);
            assertTrue(profile.get("directUsed") < profile.get("directCap"), profileLine);
            assertTrue(profile.get("metaspaceUsed") < profile.get("metaspaceCap"), profileLine);
            assertTrue(profile.get("codeUsed") < profile.get("codeCap"), profileLine);
            long rssBytes = exactRssBytes(child);
            assertTrue(rssBytes < profile.get("rssRequired"), profileLine);
            assertTrue(profile.get("rssRequired") <= profile.get("processLimit"), profileLine);
            System.out.println(profileLine + " rssBytes=" + rssBytes);
            input.newLine();
            input.flush();
        } catch (Throwable failure) {
            child.destroyForcibly();
            throw failure;
        }
        assertTrue(child.waitFor(30L, TimeUnit.SECONDS), output.toString());
        assertEquals(0, child.exitValue(), output.toString());
    }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1 || !"probe".equals(arguments[0])) {
            throw new IllegalArgumentException("expected probe mode");
        }
        try (TestingServer server = new TestingServer(true)) {
            Set<Long> baseline = liveThreadIds();
            try (ZkServiceRegistry masterSession =
                         new ZkServiceRegistry(server.getConnectString())) {
                int masterAdded = additions(baseline, liveThreadIds());
                try (ZkServiceRegistry workerSession =
                             new ZkServiceRegistry(server.getConnectString())) {
                    int twoSessionsAdded = additions(baseline, liveThreadIds());
                    printProfile(masterAdded, twoSessionsAdded);
                    System.out.flush();
                    System.in.read();
                }
            }
        }
    }

    private static void printProfile(int masterAdded, int twoSessionsAdded) {
        String curatorVersion = org.apache.curator.framework.CuratorFramework.class
                .getPackage().getImplementationVersion();
        assertEquals("5.9.0", curatorVersion,
                "thread reserve must be re-certified on Curator upgrade");
        JvmCapacityEnvelope capacity = JvmCapacityEnvelope.capture(System.getProperties());
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long metaspace = 0L;
        long code = 0L;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            String name = pool.getName();
            long used = pool.getUsage() == null ? 0L : pool.getUsage().getUsed();
            if (name.contains("Metaspace") || name.contains("Compressed Class")) {
                metaspace += used;
            } else if (pool.getType() == MemoryType.NON_HEAP) {
                code += used;
            }
        }
        long direct = 0L;
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(
                BufferPoolMXBean.class)) {
            if ("direct".equals(pool.getName())) direct = pool.getMemoryUsed();
        }
        // ClusterMain process base (3) + two Curator role-session reserves.
        long threadUpper = 35L;
        java.lang.management.ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        System.out.println("ZK_PROCESS_PROFILE"
                + " curatorVersion=59"
                + " masterAdded=" + masterAdded
                + " twoSessionsAdded=" + twoSessionsAdded
                + " reservePerSession=16"
                + " heapUsed=" + heap.getUsed()
                + " directUsed=" + direct
                + " metaspaceUsed=" + metaspace
                + " codeUsed=" + code
                + " liveThreads=" + threads.getThreadCount()
                + " peakThreads=" + threads.getPeakThreadCount()
                + " threadUpper=" + threadUpper
                + " rssRequired=" + capacity.rssRequiredBytes(threadUpper)
                + " processLimit=" + capacity.processMemoryLimitBytes()
                + " heapCap=" + capacity.maxHeapBytes()
                + " directCap=" + capacity.maxDirectMemoryBytes()
                + " metaspaceCap=" + capacity.maxMetaspaceBytes()
                + " codeCap=" + capacity.reservedCodeCacheBytes());
    }

    private static int additions(Set<Long> baseline, Set<Long> current) {
        Set<Long> added = new LinkedHashSet<>(current);
        added.removeAll(baseline);
        return added.size();
    }

    private static Set<Long> liveThreadIds() {
        Set<Long> ids = new LinkedHashSet<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive()) ids.add(thread.getId());
        }
        return ids;
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
        assertTrue(ps.waitFor(10L, TimeUnit.SECONDS), "exact-child RSS probe timed out");
        assertEquals(0, ps.exitValue(), "exact-child RSS probe failed");
        return Long.parseLong(line.trim()) * 1024L;
    }

    private static Map<String, Long> parse(String line) {
        Map<String, Long> values = new LinkedHashMap<>();
        for (String token : line.split(" ")) {
            int separator = token.indexOf('=');
            if (separator > 0) {
                values.put(token.substring(0, separator),
                        Long.parseLong(token.substring(separator + 1)));
            }
        }
        return values;
    }
}
