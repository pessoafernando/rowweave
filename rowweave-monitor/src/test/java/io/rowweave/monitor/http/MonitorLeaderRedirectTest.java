package io.rowweave.monitor.http;

import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.monitor.JobView;
import io.rowweave.monitor.MonitorBootstrap;
import io.rowweave.monitor.MonitorLauncher;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Two cluster modes. A standby monitor answers with a 302 redirect to the leader, while the
 * leader answers directly. SPI discovery is covered too: MonitorBootstrap.discover finds
 * NettyMonitorLauncher.
 */
class MonitorLeaderRedirectTest {

    private static int statusNoRedirect(int port, String path) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL("http://127.0.0.1:" + port + path).openConnection();
        c.setInstanceFollowRedirects(false);  // Do not follow redirects automatically; the point is the 302 itself.
        c.setConnectTimeout(3000);
        c.setReadTimeout(3000);
        return c.getResponseCode();
    }

    private static String locationHeader(int port, String path) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL("http://127.0.0.1:" + port + path).openConnection();
        c.setInstanceFollowRedirects(false);
        c.setConnectTimeout(3000);
        c.setReadTimeout(3000);
        c.getResponseCode();
        return c.getHeaderField("Location");
    }

    private static String body(int port, String path) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL("http://127.0.0.1:" + port + path).openConnection();
        c.setConnectTimeout(3000);
        c.setReadTimeout(3000);
        int code = c.getResponseCode();
        StringBuilder sb = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(
                code < 400 ? c.getInputStream() : c.getErrorStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) sb.append(line);
        }
        return sb.toString();
    }

    /** Leader: isLeader() is true, so the monitor answers with data directly. */
    @Test
    void leaderRespondsDirectly() throws Exception {
        InMemoryJobMonitor m = new InMemoryJobMonitor(100);
        m.onJobSubmit(JobView.builder("j1", "j").build());
        MonitorLauncher.LeaderAddressResolver leader = new MonitorLauncher.LeaderAddressResolver() {
            @Override public boolean isLeader() { return true; }
            @Override public String leaderHost() { return "127.0.0.1:9999"; }
        };
        MonitorHttpServer server = new MonitorHttpServer(m, 0, leader);
        int port = server.start();
        try {
            assertEquals(200, statusNoRedirect(port, "/api/jobs"), "leader answers 200 directly");
            assertTrue(body(port, "/api/jobs").contains("\"jobId\":\"j1\""));
        } finally {
            server.stop();
        }
    }

    /** Standby: isLeader() is false, so the monitor redirects with 302 to the leader monitor address. */
    @Test
    void standbyRedirectsToLeader() throws Exception {
        InMemoryJobMonitor m = new InMemoryJobMonitor(100);
        MonitorLauncher.LeaderAddressResolver standby = new MonitorLauncher.LeaderAddressResolver() {
            @Override public boolean isLeader() { return false; }
            @Override public String leaderHost() { return "10.0.0.5:8080"; }
        };
        MonitorHttpServer server = new MonitorHttpServer(m, 0, standby);
        int port = server.start();
        try {
            assertEquals(302, statusNoRedirect(port, "/api/jobs"), "standby answers 302");
            String loc = locationHeader(port, "/api/jobs");
            assertNotNull(loc, "a Location header is present");
            assertTrue(loc.contains("10.0.0.5:8080"), "redirects to the leader address, actual:" + loc);
            assertTrue(loc.endsWith("/api/jobs"), "keeps the original path");
        } finally {
            server.stop();
        }
    }

    /** Standby with no leader answers 503. */
    @Test
    void standbyNoLeaderReturns503() throws Exception {
        MonitorLauncher.LeaderAddressResolver noLeader = new MonitorLauncher.LeaderAddressResolver() {
            @Override public boolean isLeader() { return false; }
            @Override public String leaderHost() { return null; }
        };
        MonitorHttpServer server = new MonitorHttpServer(new InMemoryJobMonitor(100), 0, noLeader);
        int port = server.start();
        try {
            assertEquals(503, statusNoRedirect(port, "/api/jobs"), "no leader yields 503");
        } finally {
            server.stop();
        }
    }

    /** SPI: MonitorBootstrap.discover finds NettyMonitorLauncher, since the monitor jar is on the classpath. */
    @Test
    void spiDiscoversLauncher() {
        MonitorLauncher launcher = MonitorBootstrap.discover();
        assertNotNull(launcher, "SPI discovers the MonitorLauncher implementation");
        assertEquals("NettyMonitorLauncher", launcher.getClass().getSimpleName());
    }

    /** Single-machine mode: leaderResolver is null, so nothing is redirected and the monitor answers directly. */
    @Test
    void singleMachineNoRedirect() throws Exception {
        MonitorHttpServer server = new MonitorHttpServer(new InMemoryJobMonitor(100), 0, null);
        int port = server.start();
        try {
            assertEquals(200, statusNoRedirect(port, "/api/jobs"), "single process answers 200 without a redirect");
        } finally {
            server.stop();
        }
    }
}
