package io.rowweave.monitor.http;

import io.rowweave.monitor.InMemoryJobMonitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Transport security defaults: loopback-first, read-only, and explicit cross-origin opt-in. */
class MonitorSecurityTest {
    private final Properties properties = new Properties();
    private InMemoryJobMonitor monitor;
    private MonitorHttpServer server;

    @BeforeEach
    void setUp() {
        monitor = new InMemoryJobMonitor(4);
    }

    @AfterEach
    void restoreProperties() {
        if (server != null) server.stop();
        for (Object key : properties.keySet()) System.clearProperty((String) key);
    }

    private void set(String key, String value) {
        properties.setProperty(key, value);
        System.setProperty(key, value);
    }

    @Test
    void defaultBindAddressIsLoopback() {
        server = new MonitorHttpServer(monitor, 0, null);
        int port = server.start();
        InetSocketAddress address = server.localAddress();
        assertEquals(port, address.getPort());
        assertTrue(address.getAddress().isLoopbackAddress());
    }

    @Test
    void wildcardBindRequiresExplicitOverride() {
        set("rowweave.monitor.bindAddress", "0.0.0.0");
        server = new MonitorHttpServer(monitor, 0, null);
        int port = server.start();
        InetSocketAddress address = server.localAddress();
        assertEquals(port, address.getPort());
        assertTrue(address.getAddress().isAnyLocalAddress());
    }

    @Test
    void wildcardOriginConfigurationFailsFast() {
        set("rowweave.monitor.allowedOrigins", "https://allowed.example.org,*");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new MonitorHttpServer(monitor, 0, null).start());
    }

    @Test
    void nonGetRequestsAreRejectedWithAllowHeader() throws Exception {
        server = new MonitorHttpServer(monitor, 0, null);
        int port = server.start();
        Response response = request(port, "POST", "/", null);
        assertEquals(405, response.status);
        assertEquals("GET", response.header("Allow"));
        assertEquals("application/json; charset=UTF-8", response.contentType);
        assertTrue(response.body.contains("\"error\":\"method not allowed\""));
    }

    @Test
    void defaultResponseHasNoWildcardCors() throws Exception {
        server = new MonitorHttpServer(monitor, 0, null);
        int port = server.start();
        Response response = request(port, "GET", "/api/overview", null);
        assertEquals(200, response.status);
        assertFalse(response.hasHeader("access-control-allow-origin"));
        assertFalse(response.body.contains("\"*\""));
    }

    @Test
    void configuredOriginIsEchoedOnlyWhenRequested() throws Exception {
        set("rowweave.monitor.allowedOrigins", "https://monitor.example.org");
        server = new MonitorHttpServer(monitor, 0, null);
        int port = server.start();
        Response config = request(port, "GET", "/api/config", null);
        assertTrue(config.body.contains("https://monitor.example.org"), config.body);
        Response denied = rawGetWithOrigin(port, "https://other.example.org");
        assertFalse(denied.hasHeader("access-control-allow-origin"));
        Response allowed = rawGetWithOrigin(port, "https://monitor.example.org");
        assertEquals("https://monitor.example.org",
                allowed.header("access-control-allow-origin"));
    }

    private Response rawGetWithOrigin(int port, String origin) throws Exception {
        try (java.net.Socket socket = new java.net.Socket("127.0.0.1", port)) {
            socket.setSoTimeout(2000);
            java.io.OutputStream output = socket.getOutputStream();
            String request = "GET /api/overview HTTP/1.1\r\n"
                    + "Host: 127.0.0.1:" + port + "\r\n"
                    + "Origin: " + origin + "\r\n"
                    + "Connection: close\r\n\r\n";
            output.write(request.getBytes(StandardCharsets.US_ASCII));
            output.flush();
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            InputStream input = socket.getInputStream();
            byte[] buffer = new byte[1024];
            int read;
            while ((read = input.read(buffer)) != -1) bytes.write(buffer, 0, read);
            String response = new String(bytes.toByteArray(), StandardCharsets.UTF_8);
            int headerEnd = response.indexOf("\r\n\r\n");
            String headers = response.substring(0, headerEnd);
            java.util.Map<String, java.util.List<String>> map =
                    new java.util.LinkedHashMap<>();
            for (String line : headers.split("\r\n")) {
                int separator = line.indexOf(':');
                if (separator <= 0) continue;
                map.computeIfAbsent(line.substring(0, separator).trim().toLowerCase(
                                java.util.Locale.ROOT), ignored -> new java.util.ArrayList<>())
                        .add(line.substring(separator + 1).trim());
            }
            int status = Integer.parseInt(headers.split(" ")[1]);
            return new Response(status, header(map, "content-type"), map,
                    response.substring(headerEnd + 4));
        }
    }

    private static String header(java.util.Map<String, java.util.List<String>> headers,
                                 String name) {
        java.util.List<String> values = headers.get(name.toLowerCase(java.util.Locale.ROOT));
        return values == null || values.isEmpty() ? null : values.get(0);
    }

    private Response request(int port, String method, String path, String origin) throws Exception {
        HttpURLConnection connection =
                (HttpURLConnection) new URL("http://127.0.0.1:" + port + path).openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout(2000);
        connection.setReadTimeout(2000);
        connection.setInstanceFollowRedirects(false);
        if (origin != null) connection.setRequestProperty("Origin", origin);
        if ("POST".equals(method)) connection.setDoOutput(true);
        int status = connection.getResponseCode();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        InputStream input = status < 400 ? connection.getInputStream() : connection.getErrorStream();
        byte[] buffer = new byte[1024];
        try (InputStream stream = input) {
            int read;
            while ((read = stream.read(buffer)) != -1) output.write(buffer, 0, read);
        }
        return new Response(status, connection.getContentType(),
                connection.getHeaderFields(), new String(output.toByteArray(), StandardCharsets.UTF_8));
    }

    private static final class Response {
        private final int status;
        private final String contentType;
        private final java.util.Map<String, java.util.List<String>> headerNames;
        private final String body;

        private Response(int status, String contentType,
                         java.util.Map<String, java.util.List<String>> headers, String body) {
            this.status = status;
            this.contentType = contentType;
            this.headerNames = headers;
            this.body = body;
        }

        private boolean hasHeader(String name) { return header(name) != null; }

        private String header(String name) {
            java.util.List<String> values = headerNames.get(
                    name.toLowerCase(java.util.Locale.ROOT));
            return values == null || values.isEmpty() ? null : values.get(0);
        }
    }
}
