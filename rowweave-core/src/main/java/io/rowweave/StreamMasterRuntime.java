package io.rowweave;

import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.RegistryRoleElection;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.deployment.StreamConnectorProviderRegistry;
import io.rowweave.deployment.VerifiedDeploymentSet;
import io.rowweave.stream.api.StreamWorkerEndpoint;
import io.rowweave.stream.cluster.RemoteStreamSourceSinkRunFactory;
import io.rowweave.stream.cluster.RemoteStreamWorkerEndpoint;
import io.rowweave.stream.cluster.LeadershipBoundStreamProductRunFactory;
import io.rowweave.stream.cluster.StreamProductJobMasterServer;
import io.rowweave.stream.cluster.StreamProductRunFactory;
import io.rowweave.stream.cluster.StreamProductJobStateStore;
import io.rowweave.stream.cluster.StreamClusterRunStateStore;
import io.rowweave.stream.cluster.StreamClusterRunStateStoreProvider;
import io.rowweave.stream.cluster.StreamWorkerDirectory;
import io.rowweave.stream.runtime.StreamCheckpointAuthority;
import io.rowweave.stream.runtime.StreamCheckpointAuthorityProvider;
import io.rowweave.stream.runtime.StreamCheckpointCatalog;
import io.rowweave.stream.runtime.StreamCheckpointCatalogProvider;
import io.rowweave.cluster.rpc.ControlSecurityConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Owned public lifecycle for a durable Stream Master over independent Workers. */
public final class StreamMasterRuntime implements AutoCloseable {
    private final StreamProductJobMasterServer delegate;
    private final Ownership ownership;
    private final String advertisedHost;
    private final RegistryRoleElection election;
    private final AtomicBoolean closed = new AtomicBoolean();

    private StreamMasterRuntime(
            StreamProductJobMasterServer delegate, Ownership ownership,
            String advertisedHost, RegistryRoleElection election) {
        this.delegate = delegate;
        this.ownership = ownership;
        this.advertisedHost = advertisedHost;
        this.election = election;
    }

    static StreamMasterRuntime start(
            Path requestedRoot, Collection<StreamWorkerEndpoint> workers) {
        return start(requestedRoot, workers,
                ControlSecurityConfig.fromProperties(new java.util.Properties()),
                0);
    }

    static StreamMasterRuntime start(Path requestedRoot,
            Collection<StreamWorkerEndpoint> workers,
            ControlSecurityConfig security, int port) {
        return start(requestedRoot, workers, security, port,
                (StreamConnectorProviderRegistry) null);
    }

    static StreamMasterRuntime start(Path requestedRoot,
            Collection<StreamWorkerEndpoint> workers,
            ControlSecurityConfig security, int port,
            VerifiedDeploymentSet deployments) {
        return start(requestedRoot, workers, security, port,
                deployments == null ? null
                        : StreamConnectorProviderRegistry.scan(deployments,
                        StreamConnectorProviderRegistry.Role.STATIC_ONLY));
    }

    static StreamMasterRuntime start(Path requestedRoot,
            Collection<StreamWorkerEndpoint> workers,
            ControlSecurityConfig security, int port,
            StreamConnectorProviderRegistry connectorRegistry) {
        if (requestedRoot == null || workers == null || workers.isEmpty()
                || security == null || port < 0 || port > 65_535) {
            throw new IllegalArgumentException(
                    "Stream Master state root, endpoint and Workers are required");
        }
        security.requireImplementedTransport();
        security.requireLocalRole(
                io.rowweave.cluster.rpc.ControlRole.MASTER);
        List<RemoteStreamWorkerEndpoint> endpoints = new ArrayList<>();
        for (StreamWorkerEndpoint worker : workers) {
            if (worker == null) {
                throw new IllegalArgumentException(
                        "Stream cluster Worker endpoint is required");
            }
            endpoints.add(RemoteStreamWorkerEndpoint.connect(
                    worker.host(), worker.port(), security));
        }
        return start(requestedRoot, () -> endpoints, security, port,
                null, null, null, connectorRegistry);
    }

    static StreamMasterRuntime start(Path requestedRoot,
            StreamWorkerDirectory workers,
            ControlSecurityConfig security, int port) {
        return start(requestedRoot, workers, security, port, null);
    }

    static StreamMasterRuntime start(Path requestedRoot,
            StreamWorkerDirectory workers,
            ControlSecurityConfig security, int port,
            StreamConnectorProviderRegistry connectorRegistry) {
        return start(requestedRoot, workers, security, port, null, null, null,
                connectorRegistry);
    }

    static StreamMasterRuntime startHa(Path requestedRoot,
            StreamWorkerDirectory workers,
            ControlSecurityConfig security, int port,
            ServiceRegistry electionRegistry) {
        if (electionRegistry == null) {
            throw new IllegalArgumentException(
                    "Stream Master election registry is required");
        }
        return start(requestedRoot, workers, security, port,
                electionRegistry, null, null, null);
    }

    static StreamMasterRuntime startHa(Path requestedRoot,
            StreamWorkerDirectory workers,
            ControlSecurityConfig security, int port,
            ServiceRegistry electionRegistry,
            StreamProductJobStateStore productJobStateStore) {
        if (electionRegistry == null || productJobStateStore == null) {
            throw new IllegalArgumentException(
                    "Stream Master election and product authority are required");
        }
        return start(requestedRoot, workers, security, port,
                electionRegistry, productJobStateStore, null, null);
    }

    static StreamMasterRuntime startHa(Path requestedRoot,
            StreamWorkerDirectory workers,
            ControlSecurityConfig security, int port,
            ServiceRegistry electionRegistry,
            StreamProductJobStateStore productJobStateStore,
            Path sharedCheckpointRoot) {
        return startHa(requestedRoot, workers, security, port,
                electionRegistry, productJobStateStore,
                sharedCheckpointRoot, null);
    }

    static StreamMasterRuntime startHa(Path requestedRoot,
            StreamWorkerDirectory workers,
            ControlSecurityConfig security, int port,
            ServiceRegistry electionRegistry,
            StreamProductJobStateStore productJobStateStore,
            Path sharedCheckpointRoot,
            StreamConnectorProviderRegistry connectorRegistry) {
        if (electionRegistry == null || productJobStateStore == null
                || sharedCheckpointRoot == null) {
            throw new IllegalArgumentException(
                    "Stream Master HA authorities and checkpoint root are required");
        }
        return start(requestedRoot, workers, security, port,
                electionRegistry, productJobStateStore,
                sharedCheckpointRoot, connectorRegistry);
    }

    private static StreamMasterRuntime start(Path requestedRoot,
            StreamWorkerDirectory workers,
            ControlSecurityConfig security, int port,
            ServiceRegistry electionRegistry,
            StreamProductJobStateStore productJobStateStore,
            Path sharedCheckpointRoot,
            StreamConnectorProviderRegistry connectorRegistry) {
        if (requestedRoot == null || workers == null || security == null
                || port < 0 || port > 65_535) {
            throw new IllegalArgumentException(
                    "Stream Master state root, endpoint and Worker directory are required");
        }
        security.requireImplementedTransport();
        security.requireLocalRole(
                io.rowweave.cluster.rpc.ControlRole.MASTER);
        Ownership ownership = Ownership.acquire(requestedRoot);
        StreamProductJobMasterServer master = null;
        RegistryRoleElection election = null;
        try {
            StreamClusterRunStateStore sharedRuns = null;
            StreamCheckpointAuthority sharedCheckpoints = null;
            StreamCheckpointCatalog sharedCheckpointCatalog = null;
            if (electionRegistry != null) {
                if (!(electionRegistry instanceof
                        StreamClusterRunStateStoreProvider)
                        || !(electionRegistry instanceof
                        StreamCheckpointAuthorityProvider)
                        || !(electionRegistry instanceof
                        StreamCheckpointCatalogProvider)) {
                    throw new IllegalStateException(
                            "HA Stream registry lacks run/checkpoint authority");
                }
                sharedRuns = ((StreamClusterRunStateStoreProvider)
                        electionRegistry).streamClusterRunStateStore();
                sharedCheckpoints = ((StreamCheckpointAuthorityProvider)
                        electionRegistry).streamCheckpointAuthority();
                sharedCheckpointCatalog = ((StreamCheckpointCatalogProvider)
                        electionRegistry).streamCheckpointCatalog();
            }
            final StreamClusterRunStateStore runStore = sharedRuns;
            final StreamCheckpointAuthority checkpointAuthority =
                    sharedCheckpoints;
            final StreamCheckpointCatalog checkpointCatalog =
                    sharedCheckpointCatalog;
            final Path checkpointRoot = sharedCheckpointRoot == null
                    ? ownership.root.resolve("checkpoints")
                    : sharedCheckpointRoot.toAbsolutePath().normalize();
            try {
                Files.createDirectories(checkpointRoot);
            } catch (IOException failure) {
                throw new IllegalStateException(
                        "Stream checkpoint root is unavailable", failure);
            }
            Supplier<RemoteStreamSourceSinkRunFactory> runFactory = () -> {
                long term = runStore == null
                        ? ownership.nextCoordinatorTerm()
                        : runStore.nextCoordinatorTerm();
                String incarnation = UUID.randomUUID().toString();
                return runStore == null
                        ? new RemoteStreamSourceSinkRunFactory(
                        ownership.root.resolve("runs"), term,
                        incarnation, workers, checkpointRoot)
                        : new RemoteStreamSourceSinkRunFactory(
                        runStore, checkpointAuthority, checkpointCatalog,
                        term, incarnation, workers,
                        checkpointRoot);
            };
            RemoteStreamSourceSinkRunFactory initialRuns = runFactory.get();
            LeadershipBoundStreamProductRunFactory leadershipRuns =
                    electionRegistry == null ? null
                    : new LeadershipBoundStreamProductRunFactory(
                            initialRuns, runFactory);
            StreamProductRunFactory runs = leadershipRuns == null
                    ? initialRuns : leadershipRuns;
            AtomicBoolean activeLeader = new AtomicBoolean(
                    electionRegistry == null);
            master = productJobStateStore == null
                    ? new StreamProductJobMasterServer(
                    ownership.root.resolve("jobs"), runs, security, port,
                    activeLeader::get, connectorRegistry)
                    : new StreamProductJobMasterServer(
                    productJobStateStore, runs, security, port,
                    activeLeader::get, connectorRegistry);
            if (electionRegistry != null) {
                election = new RegistryRoleElection(electionRegistry,
                        io.rowweave.stream.cluster.StreamClusterRoles
                                .MASTER_LEADER_ROLE_ID,
                        new Endpoint(security.advertisedHost(), master.port()),
                        security.securityMetadata());
                election.addLeadershipListener(active -> {
                    if (!active) {
                        activeLeader.set(false);
                        return;
                    }
                    leadershipRuns.activate();
                    activeLeader.set(true);
                });
                election.start();
            }
            return new StreamMasterRuntime(master, ownership,
                    security.advertisedHost(), election);
        } catch (RuntimeException | Error failure) {
            if (election != null) election.close();
            if (master != null) master.close();
            ownership.closeAfterFailure(failure);
            throw failure;
        }
    }

    public String host() { return advertisedHost; }

    public int port() {
        requireOpen();
        return delegate.port();
    }

    /** Whether this Master currently owns the HA role (always true without HA). */
    public boolean isLeader() {
        requireOpen();
        return election == null || election.isLeader();
    }

    public StreamJobClient createJobClient() {
        requireOpen();
        return RowWeave.createRealClusterStreamJobClient(host(), port());
    }

    public StreamJobClient createJobClient(ControlSecurityConfig security) {
        requireOpen();
        return RowWeave.createRealClusterStreamJobClient(host(), port(), security);
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        RuntimeException failure = null;
        if (election != null) {
            try {
                election.close();
            } catch (RuntimeException value) {
                failure = value;
            }
        }
        try {
            delegate.close();
        } catch (RuntimeException value) {
            if (failure == null) failure = value;
            else failure.addSuppressed(value);
        }
        try {
            ownership.close();
        } catch (RuntimeException value) {
            if (failure == null) failure = value;
            else failure.addSuppressed(value);
        }
        if (failure != null) throw failure;
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Stream Master is closed");
        }
    }

    private static final class Ownership implements AutoCloseable {
        private final Path root;
        private final FileChannel lockChannel;
        private final FileLock lock;

        private Ownership(Path root, FileChannel lockChannel, FileLock lock) {
            this.root = root;
            this.lockChannel = lockChannel;
            this.lock = lock;
        }

        private static Ownership acquire(Path requestedRoot) {
            FileChannel channel = null;
            try {
                Files.createDirectories(requestedRoot);
                Path root = requestedRoot.toRealPath();
                channel = FileChannel.open(root.resolve("stream-master.lock"),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock lock;
                try {
                    lock = channel.tryLock();
                } catch (OverlappingFileLockException overlapping) {
                    lock = null;
                }
                if (lock == null) {
                    channel.close();
                    throw new IllegalStateException(
                            "Stream Master state root is already owned");
                }
                return new Ownership(root, channel, lock);
            } catch (IllegalStateException failure) {
                throw failure;
            } catch (IOException failure) {
                if (channel != null) try {
                    channel.close();
                } catch (IOException close) {
                    failure.addSuppressed(close);
                }
                throw new IllegalStateException(
                        "Stream Master state root is unavailable", failure);
            }
        }

        private long nextCoordinatorTerm() {
            Path path = root.resolve("coordinator.term");
            try (FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE)) {
                long current;
                if (channel.size() == 0L) {
                    current = 0L;
                } else if (channel.size() == 8L) {
                    ByteBuffer frame = ByteBuffer.allocate(8);
                    while (frame.hasRemaining()) {
                        if (channel.read(frame) < 0) {
                            throw new IOException(
                                    "coordinator term is truncated");
                        }
                    }
                    frame.flip();
                    current = frame.getLong();
                    if (current < 1L) {
                        throw new IOException(
                                "coordinator term is invalid");
                    }
                } else {
                    throw new IOException(
                            "coordinator term has an invalid length");
                }
                long next = Math.addExact(current, 1L);
                ByteBuffer frame = ByteBuffer.allocate(8).putLong(next);
                frame.flip();
                channel.position(0L);
                while (frame.hasRemaining()) channel.write(frame);
                channel.truncate(8L);
                channel.force(true);
                return next;
            } catch (IOException | ArithmeticException failure) {
                throw new IllegalStateException(
                        "Stream coordinator term is unavailable", failure);
            }
        }

        private void closeAfterFailure(Throwable primary) {
            try {
                close();
            } catch (RuntimeException close) {
                primary.addSuppressed(close);
            }
        }

        @Override public void close() {
            IOException failure = null;
            try {
                lock.release();
            } catch (IOException value) {
                failure = value;
            }
            try {
                lockChannel.close();
            } catch (IOException value) {
                if (failure == null) failure = value;
                else failure.addSuppressed(value);
            }
            if (failure != null) {
                throw new IllegalStateException(
                        "Stream Master ownership release failed", failure);
            }
        }
    }
}
