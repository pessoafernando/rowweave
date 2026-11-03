package io.rowweave.stream.api;

import java.util.Objects;

/** Public address of one already-started Stream Worker control endpoint. */
public final class StreamWorkerEndpoint {
    private final String host;
    private final int port;

    public StreamWorkerEndpoint(String host, int port) {
        if (host == null || host.trim().isEmpty() || host.length() > 255
                || port < 1 || port > 65_535) {
            throw new IllegalArgumentException(
                    "Stream Worker endpoint is invalid");
        }
        this.host = host;
        this.port = port;
    }

    public String host() { return host; }
    public int port() { return port; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamWorkerEndpoint)) return false;
        StreamWorkerEndpoint that = (StreamWorkerEndpoint) other;
        return port == that.port && host.equals(that.host);
    }

    @Override public int hashCode() {
        return Objects.hash(host, port);
    }
}
