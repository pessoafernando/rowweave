package io.rowweave;

import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.stream.cluster.RegistryStreamWorkerDirectory;
import io.rowweave.stream.cluster.StreamWorkerCapabilityProfile;

import java.util.concurrent.atomic.AtomicBoolean;

/** Owned registration of one exact Stream Worker incarnation. */
public final class StreamWorkerRegistryLease implements AutoCloseable {
    private final ServiceRegistry registry;
    private final StreamWorkerCapabilityProfile profile;
    private final AtomicBoolean closed = new AtomicBoolean();

    StreamWorkerRegistryLease(ServiceRegistry registry,
            StreamWorkerCapabilityProfile profile) {
        this.registry = registry;
        this.profile = profile;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            RegistryStreamWorkerDirectory.deregisterExact(registry, profile);
        }
    }
}
