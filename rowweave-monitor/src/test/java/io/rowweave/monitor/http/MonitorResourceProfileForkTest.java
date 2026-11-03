package io.rowweave.monitor.http;

import io.rowweave.cluster.JvmCapacityEnvelope;
import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.test.ForkedJvm;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** monitor sizing and non-heap/RSS evidence from an isolated JVM. */
public class MonitorResourceProfileForkTest {

    @Test
    void monitorFreshProcessStaysWithinDeclaredEnvelope() throws Exception {
        List<String> command = new ArrayList<>(ForkedJvm.baseCommand());
        ForkedJvm.appendJvmArguments(command, java.util.Arrays.asList(
                "-Xms128m", "-Xmx512m",
                "-XX:MaxDirectMemorySize=128m",
                "-XX:MaxMetaspaceSize=128m",
                "-XX:ReservedCodeCacheSize=64m",
                "-Xss1m", "-XX:ActiveProcessorCount=4",
                "-Drowweave.process.memoryLimitBytes=2147483648"));
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(MonitorResourceProfileForkTest.class.getName());
        command.add("probe");

        Process child = new ProcessBuilder(command).redirectErrorStream(true).start();
        StringBuilder output = new StringBuilder();
        String profileLine = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                child.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter input = new BufferedWriter(new OutputStreamWriter(
                     child.getOutputStream(), StandardCharsets.UTF_8))) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
            while (System.nanoTime() < deadline) {
                String line = reader.readLine();
                if (line == null) break;
                output.append(line).append('\n');
                if (line.startsWith("MONITOR_PROCESS_PROFILE ")) {
                    profileLine = line;
                    break;
                }
            }
            assertTrue(profileLine != null, output.toString());
            Map<String, Long> profile = parse(profileLine);
            long rssBytes = exactRssBytes(child);
            assertEquals(4L, profile.get("monitorThreads"));
            assertTrue(profile.get("heapUsed") < profile.get("heapCap"), profileLine);
            assertTrue(profile.get("directUsed") < profile.get("directCap"), profileLine);
            assertTrue(profile.get("metaspaceUsed") < profile.get("metaspaceCap"), profileLine);
            assertTrue(profile.get("codeUsed") < profile.get("codeCap"), profileLine);
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
        MonitorHttpServer server = new MonitorHttpServer(
                new InMemoryJobMonitor(10), 0, null, 3);
        ExecutorService clients = Executors.newFixedThreadPool(12);
        try {
            int port = server.start();
            List<Future<?>> calls = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                calls.add(clients.submit(() -> request(port)));
            }
            for (Future<?> call : calls) call.get();
            clients.shutdown();
            assertTrue(clients.awaitTermination(5L, TimeUnit.SECONDS));
            printProfile();
            System.out.flush();
            System.in.read();
        } finally {
            clients.shutdownNow();
            server.stop();
        }
    }

    private static void printProfile() {
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
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        int monitorThreads = 0;
        for (ThreadInfo info : threads.getThreadInfo(threads.getAllThreadIds())) {
            if (info != null && info.getThreadName().startsWith("rowweave-monitor-")) {
                monitorThreads++;
            }
        }
        // ClusterMain process base (3) + monitor boss/I/O (1 + 3).
        long threadUpper = 7L;
        System.out.println("MONITOR_PROCESS_PROFILE"
                + " heapUsed=" + heap.getUsed()
                + " directUsed=" + direct
                + " metaspaceUsed=" + metaspace
                + " codeUsed=" + code
                + " liveThreads=" + threads.getThreadCount()
                + " peakThreads=" + threads.getPeakThreadCount()
                + " monitorThreads=" + monitorThreads
                + " threadUpper=" + threadUpper
                + " rssRequired=" + capacity.rssRequiredBytes(threadUpper)
                + " processLimit=" + capacity.processMemoryLimitBytes()
                + " heapCap=" + capacity.maxHeapBytes()
                + " directCap=" + capacity.maxDirectMemoryBytes()
                + " metaspaceCap=" + capacity.maxMetaspaceBytes()
                + " codeCap=" + capacity.reservedCodeCacheBytes());
    }

    private static void request(int port) {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(
                    "http://127.0.0.1:" + port + "/api/jobs").openConnection();
            connection.setConnectTimeout(2_000);
            connection.setReadTimeout(2_000);
            try (InputStream input = connection.getInputStream()) {
                while (input.read() >= 0) {
                    // drain
                }
            }
        } catch (Exception failure) {
            throw new RuntimeException(failure);
        } finally {
            if (connection != null) connection.disconnect();
        }
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
