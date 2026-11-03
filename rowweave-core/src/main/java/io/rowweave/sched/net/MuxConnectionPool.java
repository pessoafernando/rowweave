package io.rowweave.sched.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.rowweave.DataSchema;
import io.rowweave.cluster.DataPlaneConfig;
import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.cluster.rpc.DataPlaneSecurityContext;
import io.rowweave.mem.RowSerializer;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.sched.Channel;
import io.rowweave.sched.ChannelPoisonedException;
import io.rowweave.sched.stream.StreamEvent;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One physical TCP connection per remote worker endpoint, which targets and producers share as
 * logical {@link Channel} objects. Credit, failures and end of stream are all routed by
 * targetId, and the physical connection closes only after the last logical handle has been
 * released.
 */
public final class MuxConnectionPool implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MuxConnectionPool.class);
    private static final int AUTHENTICATED_READ_BYTES = 16 * 1024;

    @FunctionalInterface
    interface EventLoopGroupFactory {
        EventLoopGroup create();
    }

    @FunctionalInterface
    interface ConstructionProbe {
        void afterGroupCreated();
    }

    @FunctionalInterface
    interface CloseAction {
        void close();
    }

    private static final ConstructionProbe NOOP_CONSTRUCTION_PROBE = () -> { };
    private static final EventLoopGroupFactory DEFAULT_GROUP_FACTORY = () ->
            new NioEventLoopGroup(1, new NamedDaemonThreadFactory("rowweave-mux-sink-io"));

    private final Object lock = new Object();
    private final EventLoopGroup group;
    private final DataPlaneConfig dataPlaneConfig;
    private final DataPlaneSecurityContext dataPlaneSecurity;
    private final DataPlaneHandshakeAdmission handshakeAdmission;
    private final WorkerDataIdentity localDataIdentity;
    private final OutboundShuffleCohortRegistry outboundCohorts;
    private final CloseAction outboundCohortCloser;
    private final Map<Endpoint, MuxConnection> connections = new HashMap<>();
    private final Map<CohortKey, CohortRoot> cohorts = new HashMap<>();
    private boolean closed;

    public MuxConnectionPool() {
        this(createDefault(false, null, null, null, null, null,
                DEFAULT_GROUP_FACTORY, NOOP_CONSTRUCTION_PROBE));
    }

    public MuxConnectionPool(DataPlaneConfig dataPlaneConfig,
                             DataPlaneSecurityContext dataPlaneSecurity,
                             DataPlaneHandshakeAdmission handshakeAdmission,
                             WorkerDataIdentity localDataIdentity) {
        this(createDefault(true, dataPlaneConfig, dataPlaneSecurity, handshakeAdmission,
                localDataIdentity, ControlResidentBudget.processWide(),
                DEFAULT_GROUP_FACTORY, NOOP_CONSTRUCTION_PROBE));
    }

    /**
     * Injection point for tests; the pool still owns the lifecycle of a group it is handed and
     * closes it along with itself.
     */
    MuxConnectionPool(EventLoopGroup group) {
        this(createFromOwnedGroup(group, false, null, null, null, null, null));
    }

    MuxConnectionPool(EventLoopGroup group, DataPlaneConfig dataPlaneConfig,
                      DataPlaneSecurityContext dataPlaneSecurity,
                      DataPlaneHandshakeAdmission handshakeAdmission,
                      WorkerDataIdentity localDataIdentity) {
        this(createFromOwnedGroup(group, false, dataPlaneConfig, dataPlaneSecurity,
                handshakeAdmission, localDataIdentity, ControlResidentBudget.processWide()));
    }

    MuxConnectionPool(EventLoopGroup group, DataPlaneConfig dataPlaneConfig,
                      DataPlaneSecurityContext dataPlaneSecurity,
                      DataPlaneHandshakeAdmission handshakeAdmission,
                      WorkerDataIdentity localDataIdentity,
                      ControlResidentBudget controlResidentBudget) {
        this(createFromOwnedGroup(group, false, dataPlaneConfig, dataPlaneSecurity,
                handshakeAdmission, localDataIdentity, controlResidentBudget));
    }

    MuxConnectionPool(EventLoopGroup group, CloseAction outboundCohortCloser) {
        this(new Construction(Objects.requireNonNull(group, "group"), null,
                null, null, null, null,
                Objects.requireNonNull(outboundCohortCloser, "outboundCohortCloser")));
    }

    private MuxConnectionPool(Construction construction) {
        this.group = construction.group;
        this.dataPlaneConfig = construction.dataPlaneConfig;
        this.dataPlaneSecurity = construction.dataPlaneSecurity;
        this.handshakeAdmission = construction.handshakeAdmission;
        this.localDataIdentity = construction.localDataIdentity;
        this.outboundCohorts = construction.outboundCohorts;
        this.outboundCohortCloser = construction.outboundCohortCloser;
    }

    static MuxConnectionPool createDefaultForTesting(
            DataPlaneConfig dataPlaneConfig,
            DataPlaneSecurityContext dataPlaneSecurity,
            DataPlaneHandshakeAdmission handshakeAdmission,
            WorkerDataIdentity localDataIdentity,
            ControlResidentBudget controlResidentBudget,
            EventLoopGroupFactory groupFactory,
            ConstructionProbe probe) {
        return new MuxConnectionPool(createDefault(true, dataPlaneConfig, dataPlaneSecurity,
                handshakeAdmission, localDataIdentity, controlResidentBudget,
                groupFactory, probe));
    }

    private static Construction createDefault(
            boolean requireDataPlaneConfig,
            DataPlaneConfig dataPlaneConfig,
            DataPlaneSecurityContext dataPlaneSecurity,
            DataPlaneHandshakeAdmission handshakeAdmission,
            WorkerDataIdentity localDataIdentity,
            ControlResidentBudget controlResidentBudget,
            EventLoopGroupFactory groupFactory,
            ConstructionProbe probe) {
        ValidatedDependencies dependencies = validate(requireDataPlaneConfig, dataPlaneConfig,
                dataPlaneSecurity, handshakeAdmission, localDataIdentity, controlResidentBudget);
        Objects.requireNonNull(groupFactory, "groupFactory");
        Objects.requireNonNull(probe, "probe");
        EventLoopGroup group = null;
        try {
            group = Objects.requireNonNull(groupFactory.create(), "created group");
            probe.afterGroupCreated();
            return construction(group, dependencies);
        } catch (Throwable failure) {
            shutdownAfterConstructionFailure(group, failure);
            rethrow(failure);
            throw new AssertionError("unreachable");
        }
    }

    private static Construction createFromOwnedGroup(
            EventLoopGroup group,
            boolean requireDataPlaneConfig,
            DataPlaneConfig dataPlaneConfig,
            DataPlaneSecurityContext dataPlaneSecurity,
            DataPlaneHandshakeAdmission handshakeAdmission,
            WorkerDataIdentity localDataIdentity,
            ControlResidentBudget controlResidentBudget) {
        Objects.requireNonNull(group, "group");
        try {
            ValidatedDependencies dependencies = validate(requireDataPlaneConfig, dataPlaneConfig,
                    dataPlaneSecurity, handshakeAdmission, localDataIdentity, controlResidentBudget);
            return construction(group, dependencies);
        } catch (Throwable failure) {
            shutdownAfterConstructionFailure(group, failure);
            rethrow(failure);
            throw new AssertionError("unreachable");
        }
    }

    private static ValidatedDependencies validate(
            boolean requireDataPlaneConfig,
            DataPlaneConfig dataPlaneConfig,
            DataPlaneSecurityContext dataPlaneSecurity,
            DataPlaneHandshakeAdmission handshakeAdmission,
            WorkerDataIdentity localDataIdentity,
            ControlResidentBudget controlResidentBudget) {
        if (requireDataPlaneConfig) {
            Objects.requireNonNull(dataPlaneConfig, "dataPlaneConfig");
        }
        boolean mtls = dataPlaneConfig != null
                && dataPlaneConfig.protocol() == DataPlaneConfig.Protocol.SHUFFLE_MTLS_V1;
        if (mtls && (dataPlaneSecurity == null || handshakeAdmission == null
                || localDataIdentity == null)) {
            throw new IllegalArgumentException(
                    "MTLS mux pool requires shared TLS context, admission and local identity");
        }
        if (!mtls && (dataPlaneSecurity != null || handshakeAdmission != null
                || localDataIdentity != null)) {
            throw new IllegalArgumentException(
                    "plain mux pool must not receive MTLS capabilities");
        }
        if (mtls) {
            Objects.requireNonNull(controlResidentBudget, "controlResidentBudget");
        }
        return new ValidatedDependencies(dataPlaneConfig, dataPlaneSecurity,
                handshakeAdmission, localDataIdentity, controlResidentBudget, mtls);
    }

    private static Construction construction(EventLoopGroup group,
                                             ValidatedDependencies dependencies) {
        OutboundShuffleCohortRegistry outbound = dependencies.mtls
                ? new OutboundShuffleCohortRegistry(dependencies.controlResidentBudget) : null;
        CloseAction closer = outbound == null ? null : outbound::close;
        return new Construction(group, dependencies.dataPlaneConfig,
                dependencies.dataPlaneSecurity, dependencies.handshakeAdmission,
                dependencies.localDataIdentity, outbound, closer);
    }

    private static void shutdownAfterConstructionFailure(EventLoopGroup group, Throwable primary) {
        if (group == null) return;
        RuntimeException cleanup = NettyTransportShutdown.shutdownGroups(
                "MuxConnectionPool construction", group);
        if (cleanup != null) primary.addSuppressed(cleanup);
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        throw new RuntimeException(failure);
    }

    private static final class ValidatedDependencies {
        private final DataPlaneConfig dataPlaneConfig;
        private final DataPlaneSecurityContext dataPlaneSecurity;
        private final DataPlaneHandshakeAdmission handshakeAdmission;
        private final WorkerDataIdentity localDataIdentity;
        private final ControlResidentBudget controlResidentBudget;
        private final boolean mtls;

        private ValidatedDependencies(DataPlaneConfig dataPlaneConfig,
                                      DataPlaneSecurityContext dataPlaneSecurity,
                                      DataPlaneHandshakeAdmission handshakeAdmission,
                                      WorkerDataIdentity localDataIdentity,
                                      ControlResidentBudget controlResidentBudget,
                                      boolean mtls) {
            this.dataPlaneConfig = dataPlaneConfig;
            this.dataPlaneSecurity = dataPlaneSecurity;
            this.handshakeAdmission = handshakeAdmission;
            this.localDataIdentity = localDataIdentity;
            this.controlResidentBudget = controlResidentBudget;
            this.mtls = mtls;
        }
    }

    private static final class Construction {
        private final EventLoopGroup group;
        private final DataPlaneConfig dataPlaneConfig;
        private final DataPlaneSecurityContext dataPlaneSecurity;
        private final DataPlaneHandshakeAdmission handshakeAdmission;
        private final WorkerDataIdentity localDataIdentity;
        private final OutboundShuffleCohortRegistry outboundCohorts;
        private final CloseAction outboundCohortCloser;

        private Construction(EventLoopGroup group,
                             DataPlaneConfig dataPlaneConfig,
                             DataPlaneSecurityContext dataPlaneSecurity,
                             DataPlaneHandshakeAdmission handshakeAdmission,
                             WorkerDataIdentity localDataIdentity,
                             OutboundShuffleCohortRegistry outboundCohorts,
                             CloseAction outboundCohortCloser) {
            this.group = group;
            this.dataPlaneConfig = dataPlaneConfig;
            this.dataPlaneSecurity = dataPlaneSecurity;
            this.handshakeAdmission = handshakeAdmission;
            this.localDataIdentity = localDataIdentity;
            this.outboundCohorts = outboundCohorts;
            this.outboundCohortCloser = outboundCohortCloser;
        }
    }

    public OutboundShuffleCohortRegistry.PreparedHandle prepareOutboundCohorts(
            String assignmentId, RunAttemptFence fence,
            List<ShuffleEndpoint> endpoints) {
        if (!isMtls() || outboundCohorts == null) {
            throw new IllegalStateException(
                    "outbound shuffle cohorts require the MTLS data plane");
        }
        return outboundCohorts.prepare(assignmentId, fence, endpoints);
    }

    public void cancelOutboundAttempt(RunAttemptFence fence) {
        if (outboundCohorts == null) return;
        List<byte[]> cancelled = outboundCohorts.cancelAttempt(fence);
        java.util.List<TargetState> poisoned = new java.util.ArrayList<>();
        java.util.List<MuxConnection> closeConnections = new java.util.ArrayList<>();
        synchronized (lock) {
            for (byte[] cohortId : cancelled) {
                CohortRoot cohort = cohorts.remove(new CohortKey(cohortId));
                if (cohort == null || cohort.retired) continue;
                cohort.retired = true;
                MuxConnection connection = cohort.connection;
                if (connection == null) continue;
                synchronized (connection.targets) {
                    for (TargetKey key : cohort.targetKeys) {
                        TargetState target = connection.targets.remove(key);
                        if (target != null) poisoned.add(target);
                    }
                }
                if (connection.references > 0) connection.references--;
                if (connection.references == 0) {
                    connections.remove(connection.endpoint, connection);
                    connection.closing = true;
                    closeConnections.add(connection);
                }
            }
        }
        IllegalStateException cancelledFailure =
                new IllegalStateException("outbound shuffle attempt cancelled");
        for (TargetState target : poisoned) target.poison(cancelledFailure);
        for (MuxConnection connection : closeConnections) connection.closePhysical();
    }

    public Channel acquire(DataSchema dataSchema, String host, int port, UUID targetId, int window,
                           int keepaliveMs, int soTimeoutMs) {
        if (isMtls()) {
            throw new IllegalArgumentException(
                    "MTLS mux acquire requires an authenticated ShuffleEndpoint");
        }
        return acquire(dataSchema, host, port, targetId, null,
                null, 0, null, 0, null, window, keepaliveMs, soTimeoutMs);
    }

    public Channel acquire(DataSchema dataSchema, ShuffleEndpoint endpoint, int window,
                           int keepaliveMs, int soTimeoutMs) {
        if (endpoint == null) {
            throw new IllegalArgumentException("shuffle endpoint is required");
        }
        OutboundShuffleCohortRegistry.StreamHandle outbound = isMtls()
                ? outboundCohorts.open(endpoint) : null;
        try {
            return acquire(dataSchema, endpoint.host(), endpoint.port(), endpoint.targetId(),
                    endpoint.destinationIdentity(), endpoint.producerGrant(),
                    endpoint.producerOrdinal(), endpoint.cohortId(),
                    endpoint.expectedOrdinalCount(), outbound,
                    window, keepaliveMs, soTimeoutMs);
        } catch (RuntimeException | Error failure) {
            if (outbound != null) outbound.close();
            throw failure;
        }
    }

    private Channel acquire(DataSchema dataSchema, String host, int port, UUID targetId,
                            WorkerDataIdentity destinationIdentity,
                            ShuffleProducerGrant producerGrant,
                            int producerOrdinal, byte[] cohortId,
                            int expectedOrdinalCount,
                            OutboundShuffleCohortRegistry.StreamHandle outbound,
                            int window,
                            int keepaliveMs, int soTimeoutMs) {
        if (dataSchema == null || host == null || targetId == null || Frame.NO_TARGET.equals(targetId)) {
            throw new IllegalArgumentException("dataSchema, host and non-zero targetId are required");
        }
        if (port <= 0 || window <= 0) {
            throw new IllegalArgumentException("port and window must be > 0");
        }
        if (isMtls() && (destinationIdentity == null || producerGrant == null)) {
            throw new IllegalArgumentException(
                    "MTLS mux endpoint requires exact destination identity and producer grant");
        }
        if (isMtls()) {
            ShuffleProducerGrant.requireOrdinal(producerOrdinal);
            if (cohortId == null || cohortId.length != 32
                    || expectedOrdinalCount < 1
                    || expectedOrdinalCount
                    > ShuffleProducerGrant.MAX_PRODUCERS_PER_TARGET) {
                throw new IllegalArgumentException("invalid outbound shuffle cohort");
            }
        }
        if (!isMtls() && (destinationIdentity != null || producerGrant != null
                || producerOrdinal != 0 || cohortId != null
                || expectedOrdinalCount != 0)) {
            throw new IllegalArgumentException(
                    "plain mux endpoint must not carry authenticated capabilities");
        }
        Endpoint endpoint = new Endpoint(host, port, destinationIdentity);
        MuxConnection connection;
        CohortRoot cohort = null;
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("mux connection pool closed");
            }
            if (outbound != null) outbound.requireActive();
            retireReplacedEndpointLocked(endpoint);
            if (isMtls()) {
                CohortKey key = new CohortKey(cohortId);
                cohort = cohorts.get(key);
                if (cohort == null) {
                    cohort = new CohortRoot(key, endpoint, targetId,
                            expectedOrdinalCount);
                    cohorts.put(key, cohort);
                } else {
                    cohort.requireCompatible(endpoint, targetId, expectedOrdinalCount);
                }
                if (!cohort.registeredOrdinals.add(producerOrdinal)) {
                    throw new IllegalStateException(
                            "authorized shuffle route is already opened");
                }
                connection = cohort.connection;
                if (connection != null && !connection.isUsable()) {
                    cohort.registeredOrdinals.remove(producerOrdinal);
                    throw new IllegalStateException(
                            "authorized shuffle cohort connection is terminal");
                }
            } else {
                connection = null;
            }
            if (connection == null) {
                connection = connections.get(endpoint);
                if (connection == null || !connection.isUsable()) {
                    connection = new MuxConnection(endpoint, window, keepaliveMs, soTimeoutMs);
                    connections.put(endpoint, connection);
                }
                if (cohort != null) {
                    cohort.connection = connection;
                    connection.references++; // cohort root survives gaps between active handles
                }
            }
            connection.references++;
            connection.logicalStreams.incrementAndGet();
        }
        TargetState target;
        try {
            target = connection.target(targetId, producerOrdinal, dataSchema,
                    cohort == null ? null : cohort.credit, outbound);
            if (cohort != null) {
                synchronized (lock) {
                    cohort.targetKeys.add(target.key);
                }
            }
        } catch (RuntimeException failure) {
            release(connection, null, cohort, producerOrdinal, false);
            throw failure;
        }
        byte[] openPayload = producerGrant == null ? new byte[0] : producerGrant.copyBytes();
        ChannelFuture opened = connection.write(new Frame(
                FrameCodec.FRAME_OPEN, targetId, producerOrdinal, openPayload));
        opened.addListener(future -> {
            if (openPayload.length > 0) java.util.Arrays.fill(openPayload, (byte) 0);
            if (!future.isSuccess()) {
                target.poison(openFailure(future.cause(), future.isCancelled()));
            }
        });
        // After OPEN, RECORD is bounded by the zero initial credit, so the stage thread never blocks
        // waiting for the local EventLoop to flush. Only a failure already known to be immediate is
        // rejected synchronously; a pending failure is delivered by the listener, which poisons the
        // target and wakes anyone waiting for credit.
        if (opened.isDone() && !opened.isSuccess()) {
            release(connection, target, cohort, producerOrdinal, true);
            Throwable failure = openFailure(opened.cause(), opened.isCancelled());
            throw new ChannelPoisonedException("mux OPEN failed: " + failure.getMessage());
        }
        return new LogicalTargetHandle(connection, target, cohort, outbound);
    }

    private boolean isMtls() {
        return dataPlaneConfig != null
                && dataPlaneConfig.protocol() == DataPlaneConfig.Protocol.SHUFFLE_MTLS_V1;
    }

    private void retireReplacedEndpointLocked(Endpoint requested) {
        Iterator<Map.Entry<Endpoint, MuxConnection>> iterator =
                connections.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Endpoint, MuxConnection> entry = iterator.next();
            Endpoint existing = entry.getKey();
            if (existing.sameAddress(requested) && !existing.equals(requested)) {
                MuxConnection stale = entry.getValue();
                iterator.remove();
                stale.closing = true;
                stale.poisonAll(new IllegalStateException(
                        "destination Worker generation was replaced"));
                stale.closePhysical();
            }
        }
    }

    private static Throwable openFailure(Throwable cause, boolean cancelled) {
        if (cause != null) {
            return cause;
        }
        return new IllegalStateException(cancelled
                ? "OPEN write was cancelled"
                : "OPEN write failed without cause");
    }

    /** How many endpoints, and therefore physical TCP connections, are currently active. */
    public int connectionCount() {
        synchronized (lock) {
            return connections.size();
        }
    }

    private void release(MuxConnection connection, TargetState target,
                         CohortRoot cohort, int producerOrdinal,
                         boolean terminalOrdinal) {
        if (target != null) {
            synchronized (connection.targets) {
                if (target.handles > 0) {
                    target.handles--;
                }
                if (target.handles == 0 && cohort == null) {
                    connection.targets.remove(target.key, target);
                }
            }
        }
        boolean closeConnection = false;
        synchronized (lock) {
            if (cohort != null) {
                if (terminalOrdinal) {
                    cohort.terminalOrdinals.add(producerOrdinal);
                } else {
                    cohort.registeredOrdinals.remove(producerOrdinal);
                    if (cohort.registeredOrdinals.isEmpty()
                            && cohort.terminalOrdinals.isEmpty()
                            && cohort.credit.outstandingBytes() == 0L) {
                        cohort.retired = true;
                        cohorts.remove(cohort.key, cohort);
                        if (cohort.connection != null
                                && cohort.connection.references > 0) {
                            cohort.connection.references--;
                        }
                    }
                }
            }
            if (connection.references > 0) {
                connection.references--;
            }
            retireCohortIfCompleteLocked(cohort);
            if (connection.references == 0) {
                connections.remove(connection.endpoint, connection);
                connection.closing = true;
                closeConnection = true;
            }
        }
        if (closeConnection) {
            connection.closePhysical();
        }
    }

    private void retireCohortIfCompleteLocked(CohortRoot cohort) {
        if (cohort == null || cohort.retired
                || cohort.terminalOrdinals.size() != cohort.expectedOrdinalCount
                || cohort.credit.outstandingBytes() != 0L) {
            return;
        }
        cohort.retired = true;
        cohorts.remove(cohort.key, cohort);
        MuxConnection connection = cohort.connection;
        if (connection != null) {
            synchronized (connection.targets) {
                for (TargetKey key : cohort.targetKeys) {
                    connection.targets.remove(key);
                }
            }
        }
        if (connection != null && connection.references > 0) {
            connection.references--;
        }
    }

    private void creditDrained(CohortRoot cohort) {
        boolean closeConnection = false;
        MuxConnection connection;
        synchronized (lock) {
            retireCohortIfCompleteLocked(cohort);
            connection = cohort.connection;
            if (connection != null && connection.references == 0) {
                connections.remove(connection.endpoint, connection);
                connection.closing = true;
                closeConnection = true;
            }
        }
        if (closeConnection) connection.closePhysical();
    }

    @Override
    public void close() {
        Map<Endpoint, MuxConnection> snapshot;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            snapshot = new HashMap<>(connections);
            connections.clear();
            cohorts.clear();
            for (MuxConnection connection : snapshot.values()) {
                connection.closing = true;
            }
        }
        Throwable primary = null;
        for (MuxConnection connection : snapshot.values()) {
            try {
                connection.poisonAll(new IllegalStateException("mux connection pool closed"));
                connection.closePhysical();
            } catch (Throwable failure) {
                primary = aggregate(primary, failure);
            }
        }
        if (outboundCohortCloser != null) {
            try {
                outboundCohortCloser.close();
            } catch (Throwable failure) {
                primary = aggregate(primary, failure);
            }
        }
        RuntimeException groupFailure =
                NettyTransportShutdown.shutdownGroups("MuxConnectionPool", group);
        if (groupFailure != null) {
            primary = aggregate(primary, groupFailure);
        }
        if (primary != null) rethrow(primary);
    }

    private static Throwable aggregate(Throwable primary, Throwable next) {
        if (primary == null) return next;
        if (primary != next) primary.addSuppressed(next);
        return primary;
    }

    private final class LogicalTargetHandle implements Channel {
        private final MuxConnection connection;
        private final TargetState target;
        private final CohortRoot cohort;
        private final OutboundShuffleCohortRegistry.StreamHandle outbound;
        private boolean terminal;

        LogicalTargetHandle(MuxConnection connection, TargetState target,
                            CohortRoot cohort,
                            OutboundShuffleCohortRegistry.StreamHandle outbound) {
            this.connection = connection;
            this.target = target;
            this.cohort = cohort;
            this.outbound = outbound;
        }

        @Override
        public void send(StreamEvent event) {
            synchronized (this) {
                if (terminal) {
                    throw new IllegalStateException("send on closed mux target handle");
                }
            }
            if (event.isError()) {
                fail(event.cause());
                return;
            }
            if (!event.isRecord()) {
                return;
            }
            byte[] payload = target.serializer.serialize(event.row());
            int wireBytes = FrameCodec.headerBytes(connection.protocol()) + payload.length;
            boolean exhausted = target.acquireCredit(wireBytes);
            if (outbound != null) outbound.recordSent(wireBytes);
            try {
                connection.writeRecord(target.key, payload, exhausted);
            } catch (RuntimeException | Error failure) {
                if (outbound != null) outbound.recordReturned(wireBytes);
                throw failure;
            }
        }

        @Override
        public StreamEvent poll() {
            throw new UnsupportedOperationException("mux target handle is send-only");
        }

        @Override
        public void close() {
            synchronized (this) {
                if (terminal) {
                    return;
                }
                terminal = true;
            }
            ChannelFuture eos = connection.write(new Frame(
                    FrameCodec.FRAME_EOS, target.targetId,
                    target.producerOrdinal, new byte[0]));
            eos.awaitUninterruptibly(NettyTransportShutdown.timeoutMs(), TimeUnit.MILLISECONDS);
            if (!eos.isSuccess()) {
                target.poison(eos.cause());
            }
            if (outbound != null) outbound.close();
            release(connection, target, cohort, target.producerOrdinal, true);
        }

        @Override
        public void fail(Throwable cause) {
            synchronized (this) {
                if (terminal) {
                    return;
                }
                terminal = true;
            }
            Throwable failure = cause == null ? new RuntimeException("mux target failed") : cause;
            target.poison(failure);
            byte[] message = String.valueOf(failure.getMessage()).getBytes(StandardCharsets.UTF_8);
            connection.write(new Frame(FrameCodec.FRAME_ERROR, target.targetId,
                    target.producerOrdinal, message))
                    .awaitUninterruptibly(NettyTransportShutdown.timeoutMs(), TimeUnit.MILLISECONDS);
            if (outbound != null) outbound.close();
            release(connection, target, cohort, target.producerOrdinal, true);
        }
    }

    private final class MuxConnection {
        final Endpoint endpoint;
        final io.netty.channel.Channel channel;
        final Map<TargetKey, TargetState> targets = new HashMap<>();
        final Object stagingLock = new Object();
        final int flushThreshold;
        byte[] staging;
        int stagingLength;
        int references;
        volatile boolean closing;
        final long connectStartedNanos = System.nanoTime();
        final AtomicInteger logicalStreams = new AtomicInteger();
        final AtomicLong records = new AtomicLong();
        final AtomicLong wireBytes = new AtomicLong();
        final AtomicLong firstRecordNanos = new AtomicLong();
        final AtomicLong lastRecordNanos = new AtomicLong();
        final AtomicBoolean lifetimeReported = new AtomicBoolean();
        final long connectedNanos;

        MuxConnection(Endpoint endpoint, int window, int keepaliveMs, int soTimeoutMs) {
            this.endpoint = endpoint;
            this.flushThreshold = window;
            this.staging = new byte[Math.min(Math.max(window, 4096), 64 * 1024)];
            final long deadline = connectionDeadline();
            DataPlaneHandshakeAdmission.Lease lease = null;
            CompletableFuture<WorkerDataIdentity> hello = new CompletableFuture<>();
            if (isMtls()) {
                try {
                    lease = handshakeAdmission.acquireOutbound(deadline);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "interrupted awaiting outbound data-plane admission", interrupted);
                } catch (TimeoutException timeout) {
                    throw new IllegalStateException(
                            "outbound data-plane admission deadline expired", timeout);
                }
            }
            final DataPlaneHandshakeAdmission.Lease acquiredLease = lease;
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                            remainingMillis(deadline))
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            if (!isMtls()) {
                                installClientDataPipeline(ch, soTimeoutMs, null);
                                return;
                            }
                            DataPlaneSecurityContext.AdmittedTlsChannel tls =
                                    dataPlaneSecurity.installClientHandler(
                                            ch.pipeline(), endpoint.host, endpoint.port,
                                            acquiredLease, deadline);
                            ch.pipeline().addLast(new WorkerDataHelloHandler(
                                    tls, localDataIdentity, deadline,
                                    peer -> {
                                        dataPlaneSecurity.requireWorkerPeer(
                                                tls, endpoint.identity
                                                        .certificateFingerprint());
                                        if (!endpoint.identity.equals(peer)) {
                                            throw new IllegalArgumentException(
                                                    "destination Worker hello identity mismatch");
                                        }
                                    },
                                    (context, peer) -> installClientDataPipeline(
                                            ch, soTimeoutMs, context.name()),
                                    new WorkerDataHelloHandler.Outcome() {
                                        @Override
                                        public void complete(WorkerDataIdentity peer) {
                                            hello.complete(peer);
                                        }

                                        @Override
                                        public void fail(Throwable failure) {
                                            hello.completeExceptionally(failure);
                                        }
                                    }));
                        }
                    });
            io.netty.channel.Channel connected = null;
            try {
                ChannelFuture connect = bootstrap.connect(endpoint.host, endpoint.port);
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L
                        || !connect.awaitUninterruptibly(remaining, TimeUnit.NANOSECONDS)) {
                    connect.cancel(false);
                    throw new IllegalStateException(
                            "data-plane connection creation deadline expired");
                }
                if (!connect.isSuccess()) {
                    throw new IllegalStateException(
                            "data-plane socket connect failed", connect.cause());
                }
                connected = connect.channel();
                if (isMtls()) awaitHello(hello, deadline);
                this.channel = connected;
                this.connectedNanos = System.nanoTime();
                LOG.info("ROWWEAVE_SHUFFLE_CONNECTION_OPEN protocol={} connectNanos={}",
                        protocol(), Math.max(0L, connectedNanos - connectStartedNanos));
            } catch (RuntimeException failure) {
                if (connected != null) connected.close();
                throw new RuntimeException("failed to connect mux shuffle to " + endpoint, failure);
            } finally {
                if (lease != null) lease.close();
            }
            channel.closeFuture().addListener(future -> {
                if (!closing) {
                    poisonAll(new IllegalStateException("mux physical connection closed"));
                }
                synchronized (targets) {
                    targets.clear();
                }
            });
            if (keepaliveMs > 0) {
                channel.eventLoop().scheduleAtFixedRate(() -> {
                    if (isUsable()) {
                        write(new Frame(FrameCodec.FRAME_KEEPALIVE,
                                Frame.NO_TARGET, 0, new byte[0]));
                    }
                }, keepaliveMs, keepaliveMs, TimeUnit.MILLISECONDS);
            }
            if (SocketChannelSink.FLUSH_INTERVAL_MS > 0) {
                channel.eventLoop().scheduleAtFixedRate(this::timedFlush,
                        SocketChannelSink.FLUSH_INTERVAL_MS, SocketChannelSink.FLUSH_INTERVAL_MS,
                        TimeUnit.MILLISECONDS);
            }
        }

        private void installClientDataPipeline(SocketChannel ch, int soTimeoutMs,
                                               String afterHandlerName) {
            io.netty.channel.ChannelPipeline pipeline = ch.pipeline();
            DataPlaneRegistration.Protocol wireProtocol = protocol();
            configureInboundFrameAllocation(ch, wireProtocol);
            if (afterHandlerName == null) {
                if (soTimeoutMs > 0) {
                    pipeline.addLast(new IdleStateHandler(
                            soTimeoutMs, 0, 0, TimeUnit.MILLISECONDS));
                }
                pipeline.addLast(new FrameEncoder(wireProtocol));
                pipeline.addLast(newInboundFrameDecoder(wireProtocol));
                pipeline.addLast(new ClientInboundHandler(MuxConnection.this));
                return;
            }
            String previous = afterHandlerName;
            if (soTimeoutMs > 0) {
                pipeline.addAfter(previous, "rowweave-client-data-idle",
                        new IdleStateHandler(soTimeoutMs, 0, 0, TimeUnit.MILLISECONDS));
                previous = "rowweave-client-data-idle";
            }
            pipeline.addAfter(previous, "rowweave-client-data-encoder",
                    new FrameEncoder(wireProtocol));
            pipeline.addAfter("rowweave-client-data-encoder",
                    "rowweave-client-data-decoder", newInboundFrameDecoder(wireProtocol));
            pipeline.addAfter("rowweave-client-data-decoder",
                    "rowweave-client-data-handler", new ClientInboundHandler(MuxConnection.this));
        }

        boolean isUsable() {
            return !closing && channel.isActive();
        }

        DataPlaneRegistration.Protocol protocol() {
            return isMtls()
                    ? DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1
                    : DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0;
        }

        TargetState target(UUID targetId, int producerOrdinal, DataSchema dataSchema,
                           ConnectionTargetCredit sharedCredit,
                           OutboundShuffleCohortRegistry.StreamHandle outbound) {
            TargetKey key = new TargetKey(targetId, producerOrdinal);
            synchronized (targets) {
                TargetState existing = targets.get(key);
                if (existing != null) {
                    if (isMtls()) {
                        throw new IllegalStateException(
                                "authorized shuffle route is already opened");
                    }
                    existing.requireCompatible(dataSchema);
                    existing.handles++;
                    return existing;
                }
                TargetState created = new TargetState(key, dataSchema,
                        sharedCredit == null ? new ConnectionTargetCredit(null) : sharedCredit,
                        outbound);
                created.handles = 1;
                targets.put(key, created);
                return created;
            }
        }

        ChannelFuture write(Frame frame) {
            if (!isUsable()) {
                io.netty.channel.ChannelPromise failed = channel.newPromise();
                failed.setFailure(new IllegalStateException("mux physical connection closed"));
                return failed;
            }
            // Draining RECORD and enqueuing the control frame must be one critical section; otherwise
            // a concurrent producer could insert a RECORD ahead of its own OPEN.
            synchronized (stagingLock) {
                byte[] pending = stagingLength == 0 ? null : drainStagingLocked();
                if (pending != null) {
                    writeBytesLocked(pending, false);
                }
                return channel.writeAndFlush(frame);
            }
        }

        /** RECORD hot path: encode into the per-connection staging buffer on the caller thread, and form one Netty outbound entry only when the window fills or the credit runs out. */
        void writeRecord(TargetKey key, byte[] payload, boolean exhausted) {
            synchronized (stagingLock) {
                int frameBytes = FrameCodec.headerBytes(protocol()) + payload.length;
                ensureStagingCapacity(stagingLength + frameBytes);
                staging[stagingLength++] = FrameCodec.FRAME_RECORD;
                putLong(key.targetId.getMostSignificantBits());
                putLong(key.targetId.getLeastSignificantBits());
                if (isMtls()) putInt(key.producerOrdinal);
                putInt(payload.length);
                System.arraycopy(payload, 0, staging, stagingLength, payload.length);
                stagingLength += payload.length;
                long now = System.nanoTime();
                firstRecordNanos.compareAndSet(0L, now);
                lastRecordNanos.set(now);
                records.incrementAndGet();
                wireBytes.addAndGet(frameBytes);
                if (stagingLength >= flushThreshold || exhausted) {
                    writeBytesLocked(drainStagingLocked(), true);
                }
            }
        }

        private void timedFlush() {
            if (!isUsable()) {
                return;
            }
            synchronized (stagingLock) {
                if (stagingLength > 0) {
                    writeBytesLocked(drainStagingLocked(), true);
                }
            }
        }

        private byte[] drainStagingLocked() {
            byte[] pending = java.util.Arrays.copyOf(staging, stagingLength);
            stagingLength = 0;
            return pending;
        }

        /** Holding stagingLock: stage a batch of raw frames onto one Netty channel in lock order. */
        private void writeBytesLocked(byte[] pending, boolean flush) {
            if (!isUsable()) {
                poisonAll(new IllegalStateException("mux physical connection closed before staged write"));
                return;
            }
            ByteBuf buffer = channel.alloc().buffer(pending.length);
            buffer.writeBytes(pending);
            ChannelFuture future = flush ? channel.writeAndFlush(buffer) : channel.write(buffer);
            future.addListener(write -> {
                if (!write.isSuccess()) poisonAll(write.cause());
            });
        }

        private void ensureStagingCapacity(int required) {
            if (required <= staging.length) {
                return;
            }
            int capacity = staging.length;
            while (capacity < required) {
                int next = capacity << 1;
                if (next <= capacity) {
                    capacity = required;
                    break;
                }
                capacity = next;
            }
            staging = java.util.Arrays.copyOf(staging, capacity);
        }

        private void putLong(long value) {
            staging[stagingLength++] = (byte) (value >>> 56);
            staging[stagingLength++] = (byte) (value >>> 48);
            staging[stagingLength++] = (byte) (value >>> 40);
            staging[stagingLength++] = (byte) (value >>> 32);
            staging[stagingLength++] = (byte) (value >>> 24);
            staging[stagingLength++] = (byte) (value >>> 16);
            staging[stagingLength++] = (byte) (value >>> 8);
            staging[stagingLength++] = (byte) value;
        }

        private void putInt(int value) {
            staging[stagingLength++] = (byte) (value >>> 24);
            staging[stagingLength++] = (byte) (value >>> 16);
            staging[stagingLength++] = (byte) (value >>> 8);
            staging[stagingLength++] = (byte) value;
        }

        void poisonAll(Throwable cause) {
            synchronized (targets) {
                for (TargetState target : targets.values()) {
                    target.poison(cause);
                }
            }
        }

        void closePhysical() {
            reportLifetime();
            // End of stream already waited per handle for its writes to complete; the last reference
            // uses FIN for an orderly half-close, because a plain close would discard the trailing
            // frames still sitting in the kernel queue.
            if (channel instanceof SocketChannel && channel.isActive()) {
                ((SocketChannel) channel).shutdownOutput()
                        .awaitUninterruptibly(NettyTransportShutdown.timeoutMs(), TimeUnit.MILLISECONDS);
                // The stage close path does not wait for the peer's FIN: a shared server may handle the
                // trailing ingress later. Once the timeout passes, the EventLoop closes it anyway.
                io.netty.util.concurrent.ScheduledFuture<?> forcedClose = channel.eventLoop().schedule(() -> {
                    if (channel.isOpen()) {
                        channel.close();
                    }
                }, NettyTransportShutdown.timeoutMs(), TimeUnit.MILLISECONDS);
                channel.closeFuture().addListener(future -> forcedClose.cancel(false));
                return;
            }
            channel.close().awaitUninterruptibly(NettyTransportShutdown.timeoutMs(), TimeUnit.MILLISECONDS);
        }

        private void reportLifetime() {
            if (!lifetimeReported.compareAndSet(false, true)) {
                return;
            }
            long first = firstRecordNanos.get();
            long last = lastRecordNanos.get();
            long activeDataNanos = first == 0L ? 0L : Math.max(1L, last - first);
            LOG.info("ROWWEAVE_SHUFFLE_CONNECTION_LIFETIME protocol={} connectNanos={} "
                            + "logicalStreams={} records={} wireBytes={} activeDataNanos={} "
                            + "lifetimeNanos={}",
                    protocol(), Math.max(0L, connectedNanos - connectStartedNanos),
                    logicalStreams.get(), records.get(), wireBytes.get(), activeDataNanos,
                    Math.max(0L, System.nanoTime() - connectedNanos));
        }
    }

    static void configureInboundFrameAllocation(
            io.netty.channel.Channel channel, DataPlaneRegistration.Protocol protocol) {
        if (channel == null || protocol == null) {
            throw new IllegalArgumentException("channel and data protocol are required");
        }
        if (protocol == DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1) {
            channel.config().setRecvByteBufAllocator(
                    new FixedRecvByteBufAllocator(AUTHENTICATED_READ_BYTES));
        }
    }

    static io.netty.channel.ChannelHandler newInboundFrameDecoder(
            DataPlaneRegistration.Protocol protocol) {
        if (protocol == null) {
            throw new IllegalArgumentException("data protocol is required");
        }
        return protocol == DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1
                ? new BoundedV1ControlFrameDecoder()
                : new FrameDecoder(protocol);
    }

    private static final class TargetState {
        final TargetKey key;
        final UUID targetId;
        final int producerOrdinal;
        final DataSchema dataSchema;
        final RowSerializer serializer;
        final ConnectionTargetCredit creditPool;
        final OutboundShuffleCohortRegistry.StreamHandle outbound;
        Throwable failure;
        int handles; // guarded by owning connection.targets

        TargetState(TargetKey key, DataSchema dataSchema,
                    ConnectionTargetCredit creditPool,
                    OutboundShuffleCohortRegistry.StreamHandle outbound) {
            this.key = key;
            this.targetId = key.targetId;
            this.producerOrdinal = key.producerOrdinal;
            this.dataSchema = dataSchema;
            this.serializer = new RowSerializer(dataSchema);
            this.creditPool = creditPool;
            this.outbound = outbound;
        }

        boolean acquireCredit(int wireBytes) {
            return creditPool.acquire(this, wireBytes);
        }

        void grant(int bytes) {
            long returned = creditPool.grant(bytes);
            if (returned > 0L && outbound != null) {
                outbound.recordReturned(returned);
            }
        }

        void poison(Throwable cause) {
            creditPool.lock.lock();
            try {
                if (failure == null) {
                    failure = cause == null ? new RuntimeException("mux target failed") : cause;
                }
                creditPool.hasCredit.signalAll();
            } finally {
                creditPool.lock.unlock();
            }
        }

        void requireCompatible(DataSchema other) {
            if (dataSchema.getFieldCount() != other.getFieldCount()) {
                throw new IllegalArgumentException("target " + targetId + " row meta mismatch");
            }
            for (int i = 0; i < dataSchema.getFieldCount(); i++) {
                io.rowweave.Field left = dataSchema.getField(i);
                io.rowweave.Field right = other.getField(i);
                if (!Objects.equals(left.getName(), right.getName())
                        || !Objects.equals(left.getAlias(), right.getAlias())
                        || left.getDataType() != right.getDataType()) {
                    throw new IllegalArgumentException("target " + targetId + " row meta mismatch");
                }
            }
        }
    }

    private final class ConnectionTargetCredit {
        final ReentrantLock lock = new ReentrantLock();
        final Condition hasCredit = lock.newCondition();
        final CohortRoot cohort;
        long availableBytes;
        long outstandingBytes;

        ConnectionTargetCredit(CohortRoot cohort) {
            this.cohort = cohort;
        }

        boolean acquire(TargetState target, int wireBytes) {
            lock.lock();
            try {
                while (availableBytes <= 0L && target.failure == null) {
                    hasCredit.awaitUninterruptibly();
                }
                if (target.failure != null) {
                    throw new ChannelPoisonedException(
                            "mux target failed: " + target.failure.getMessage());
                }
                availableBytes -= wireBytes;
                outstandingBytes = Math.addExact(outstandingBytes, wireBytes);
                return availableBytes <= 0L;
            } finally {
                lock.unlock();
            }
        }

        long grant(int bytes) {
            if (bytes <= 0) return 0L;
            boolean drained;
            long returned;
            lock.lock();
            try {
                returned = Math.min(outstandingBytes, (long) bytes);
                outstandingBytes -= returned;
                availableBytes = Math.addExact(availableBytes, bytes);
                drained = outstandingBytes == 0L;
                hasCredit.signalAll();
            } finally {
                lock.unlock();
            }
            if (drained && cohort != null) creditDrained(cohort);
            return returned;
        }

        long outstandingBytes() {
            lock.lock();
            try {
                return outstandingBytes;
            } finally {
                lock.unlock();
            }
        }
    }

    private final class CohortRoot {
        final CohortKey key;
        final Endpoint endpoint;
        final UUID targetId;
        final int expectedOrdinalCount;
        final java.util.Set<Integer> registeredOrdinals = new java.util.HashSet<>();
        final java.util.Set<Integer> terminalOrdinals = new java.util.HashSet<>();
        final java.util.Set<TargetKey> targetKeys = new java.util.HashSet<>();
        final ConnectionTargetCredit credit;
        MuxConnection connection;
        boolean retired;

        CohortRoot(CohortKey key, Endpoint endpoint, UUID targetId,
                   int expectedOrdinalCount) {
            this.key = key;
            this.endpoint = endpoint;
            this.targetId = targetId;
            this.expectedOrdinalCount = expectedOrdinalCount;
            this.credit = new ConnectionTargetCredit(this);
        }

        void requireCompatible(Endpoint endpoint, UUID targetId,
                               int expectedOrdinalCount) {
            if (retired || !this.endpoint.equals(endpoint)
                    || !this.targetId.equals(targetId)
                    || this.expectedOrdinalCount != expectedOrdinalCount) {
                throw new IllegalStateException(
                        "outbound shuffle cohort descriptor conflict");
            }
        }
    }

    private static final class CohortKey {
        final byte[] value;
        CohortKey(byte[] value) { this.value = value.clone(); }
        @Override public boolean equals(Object other) {
            return this == other || other instanceof CohortKey
                    && java.util.Arrays.equals(value, ((CohortKey) other).value);
        }
        @Override public int hashCode() { return java.util.Arrays.hashCode(value); }
    }

    private static final class ClientInboundHandler extends SimpleChannelInboundHandler<Frame> {
        private final MuxConnection connection;

        ClientInboundHandler(MuxConnection connection) {
            this.connection = connection;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            if (frame.type() == FrameCodec.FRAME_KEEPALIVE) {
                return;
            }
            TargetState target;
            synchronized (connection.targets) {
                target = connection.targets.get(new TargetKey(
                        frame.targetId(), frame.producerOrdinal()));
            }
            if (target == null) {
                return;
            }
            if (frame.type() == FrameCodec.FRAME_CREDIT) {
                if (frame.payload().length != 4) {
                    target.poison(new IllegalStateException("invalid mux credit payload"));
                    return;
                }
                target.grant(ByteBuffer.wrap(frame.payload()).getInt());
            } else if (frame.type() == FrameCodec.FRAME_ERROR) {
                target.poison(new RuntimeException("downstream shuffle error: "
                        + new String(frame.payload(), StandardCharsets.UTF_8)));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (!connection.closing) {
                connection.poisonAll(new IllegalStateException("mux physical connection closed"));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
            if (event instanceof IdleStateEvent) {
                connection.poisonAll(new IllegalStateException("mux physical connection reader idle"));
                ctx.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            connection.poisonAll(cause);
            ctx.close();
        }
    }

    private static final class TargetKey {
        final UUID targetId;
        final int producerOrdinal;

        TargetKey(UUID targetId, int producerOrdinal) {
            this.targetId = targetId;
            this.producerOrdinal = producerOrdinal;
        }

        @Override public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof TargetKey)) return false;
            TargetKey that = (TargetKey) other;
            return producerOrdinal == that.producerOrdinal && targetId.equals(that.targetId);
        }

        @Override public int hashCode() {
            return 31 * targetId.hashCode() + producerOrdinal;
        }
    }

    private static final class Endpoint {
        final String host;
        final int port;
        final WorkerDataIdentity identity;

        Endpoint(String host, int port, WorkerDataIdentity identity) {
            this.host = host;
            this.port = port;
            this.identity = identity;
        }

        boolean sameAddress(Endpoint other) {
            return port == other.port && host.equals(other.host);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Endpoint)) return false;
            Endpoint endpoint = (Endpoint) other;
            return port == endpoint.port && host.equals(endpoint.host)
                    && Objects.equals(identity, endpoint.identity);
        }

        @Override
        public int hashCode() {
            return 31 * (31 * host.hashCode() + port) + Objects.hashCode(identity);
        }

        @Override
        public String toString() {
            return host + ':' + port
                    + (identity == null ? "" : "@" + identity.workerId()
                    + '/' + identity.incarnationId());
        }
    }

    private long connectionDeadline() {
        long timeoutMillis = dataPlaneConfig == null
                ? 10_000L
                : Math.min(dataPlaneConfig.connectTimeoutMillis(),
                        dataPlaneConfig.handshakeTimeoutMillis());
        long now = System.nanoTime();
        long budget = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        return now > Long.MAX_VALUE - budget ? Long.MAX_VALUE : now + budget;
    }

    private static int remainingMillis(long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0L) return 1;
        long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
        if (millis == 0L) millis = 1L;
        return (int) Math.min(Integer.MAX_VALUE, millis);
    }

    private static void awaitHello(CompletableFuture<WorkerDataIdentity> hello,
                                   long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0L) {
            throw new IllegalStateException("Worker hello deadline expired");
        }
        try {
            hello.get(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted awaiting Worker hello", interrupted);
        } catch (ExecutionException failure) {
            throw new IllegalStateException("Worker hello rejected", failure.getCause());
        } catch (TimeoutException timeout) {
            throw new IllegalStateException("Worker hello deadline expired", timeout);
        }
    }
}
