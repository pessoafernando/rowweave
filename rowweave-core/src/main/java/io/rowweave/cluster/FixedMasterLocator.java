package io.rowweave.cluster;

import java.util.Objects;

/**
 * Master addressing by a fixed host and port, which keeps the behaviour of the existing {@code
 * (host,port)} constructors, now wrapped inside this class by the older client and worker
 * constructors. Thedefault when high availability is not in use.
 */
public final class FixedMasterLocator implements MasterLocator {

    private final Endpoint endpoint;

    public FixedMasterLocator(Endpoint endpoint) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
    }

    @Override
    public Endpoint locateLeader() {
        return endpoint;
    }
}
