package io.rowweave.cluster;

import java.util.Objects;

/**
 * A network endpoint: an immutable host and port value object. It is the single return type of
 * master election and addressing, both the self record of {@link MasterElection} and {@code
 * MasterLocator#locateLeader()}.
 */
public final class Endpoint {

    private final String host;
    private final int port;

    public Endpoint(String host, int port) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
    }

    public String host() { return host; }
    public int port() { return port; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Endpoint)) return false;
        Endpoint e = (Endpoint) o;
        return port == e.port && host.equals(e.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
