package io.rowweave.monitor.http;

import io.rowweave.monitor.InMemoryJobMonitor;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** monitor Netty threads are explicit, named and fully released. */
class MonitorThreadSizingTest {

    @Test
    void occupiedPortReleasesFailedLauncherThreads() throws Exception {
        try (java.net.ServerSocket occupied = new java.net.ServerSocket(
                0, 1, java.net.InetAddress.getByName("127.0.0.1"))) {
            for (int i = 0; i < 3; i++) {
                MonitorHttpServer server = new MonitorHttpServer(
                        new InMemoryJobMonitor(4), occupied.getLocalPort(), null, 1);
                try {
                    assertThrows(RuntimeException.class, server::start);
                    awaitNamedCount("rowweave-monitor-boss-", 0);
                    awaitNamedCount("rowweave-monitor-io-", 0);
                } finally {
                    server.stop();
                }
            }
        }
    }

    @Test
    void explicitIoThreadCountMatchesLiveNamedThreads() throws Exception {
        MonitorHttpServer server = new MonitorHttpServer(
                new InMemoryJobMonitor(10), 0, null, 3);
        int port = server.start();
        ExecutorService clients = Executors.newFixedThreadPool(12);
        try {
            List<Future<?>> calls = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                calls.add(clients.submit(() -> request(port)));
            }
            for (Future<?> call : calls) call.get();
            awaitNamedCount("rowweave-monitor-boss-", 1);
            awaitNamedCount("rowweave-monitor-io-", 3);
        } finally {
            clients.shutdownNow();
            server.stop();
        }
        awaitNamedCount("rowweave-monitor-boss-", 0);
        awaitNamedCount("rowweave-monitor-io-", 0);
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

    private static void awaitNamedCount(String prefix, int expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000L;
        int actual;
        do {
            actual = namedCount(prefix);
            if (actual == expected) return;
            Thread.sleep(10L);
        } while (System.currentTimeMillis() < deadline);
        assertEquals(expected, actual, prefix);
    }

    private static int namedCount(String prefix) {
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && thread.getName().startsWith(prefix)) count++;
        }
        return count;
    }
}
