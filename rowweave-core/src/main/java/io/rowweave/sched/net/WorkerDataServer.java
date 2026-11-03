package io.rowweave.sched.net;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.rowweave.cluster.DataPlaneConfig;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.DataPlaneSecurityContext;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <b>single data port</b> Netty server of a worker. One worker runs one data server, bound exactly to
 * the address the control plane verified, and every upstream connects to it, so a shuffle edge does not need
 * a port of its own. Each connection runs {@link FrameDecoder} plus {@link DataDemuxHandler} in the
 * pipeline, and the handler looks the frame's targetId up in {@link DataInboundRegistry} to demux it to the
 * channel of the matching downstream stage.
 *
 * <p>The dataPort is reported with the worker registration, so the master can build a ShuffleEndpoint from
 * (host, dataPort, targetId) without any separate port report.
 *
 * <p><b>Thread model</b>: boss for accept, worker threads for I/O, which decode frames and do the O(1) demux,
 * plus a cached ingress executor. The registry await, the deserialization and the bounded send all happen in
 * the per connection-target ordered ingress, so the I/O EventLoop never blocks.
 *
 * <p><b>Non-blocking demux</b>: DataDemuxHandler only enqueues, and neither awaits nor sends on the EventLoop,
 * so a slow target blocks its own ingress and nothing else. <b>EOS does not close the connection</b>: the EOS
 * of one target reports producerDone on that channel alone. <b>Connection lifetime</b>: {@code close()} on the
 * server closes every child connection in one place.
 */
public final class WorkerDataServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerDataServer.class);
    private static final int AUTHENTICATED_READ_BYTES = 16 * 1024;

    private final DataInboundRegistry registry;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final EventExecutorGroup handlerGroup;
    private final BoundedShuffleIngressExecutor ingressExecutor;
    private final ShuffleIngressAdmissionConfig admission;
    private final ShuffleIngressResidentBudget residentBudget;
    private final DataPlaneConfig dataPlaneConfig;
    private final DataPlaneSecurityContext dataPlaneSecurity;
    private final DataPlaneHandshakeAdmission handshakeAdmission;
    private final WorkerDataIdentity localDataIdentity;
    private final DataInboundAuthorizationRegistry authorizationRegistry;
    private final ThreadPoolExecutor handshakeSetupExecutor;
    private final Channel serverChannel;
    private final ChannelGroup childChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
    private final Queue<Future<Void>> childTeardownBarriers = new ConcurrentLinkedQueue<>();
    private final AtomicInteger acceptedConnectionCount = new AtomicInteger();
    private final AtomicInteger activeConnectionCount = new AtomicInteger();
    private final AtomicInteger peakConnectionCount = new AtomicInteger();
    private final AtomicInteger peakTargetsPerConnection = new AtomicInteger();
    private final AtomicInteger peakPendingFramesPerTarget = new AtomicInteger();
    private final AtomicLong connectionGenerations = new AtomicLong();
    private final Object shutdownLock = new Object();
    private boolean shutdownComplete;
    private RuntimeException shutdownFailure;

    public WorkerDataServer(DataInboundRegistry registry, long awaitTimeoutMs) {
        this(registry, awaitTimeoutMs, 0, systemDataPlaneConfig());
    }

    /** A preferredPort of zero lets the OS choose, which suits tests and local runs; production can pin a fixed dataPort. */
    public WorkerDataServer(DataInboundRegistry registry, long awaitTimeoutMs, int preferredPort) {
        this(registry, awaitTimeoutMs, preferredPort, systemDataPlaneConfig());
    }

    WorkerDataServer(DataInboundRegistry registry, long awaitTimeoutMs, int preferredPort,
                     ShuffleIngressAdmissionConfig admission) {
        this(registry, awaitTimeoutMs, preferredPort, admission, ipv4Loopback(),
                null, null, null, null, null);
    }

    public WorkerDataServer(DataInboundRegistry registry, long awaitTimeoutMs, int preferredPort,
                            DataPlaneConfig dataPlaneConfig) {
        this(registry, awaitTimeoutMs, preferredPort,
                requireDataPlane(dataPlaneConfig).ingressAdmission(),
                dataPlaneConfig.bindAddress(), dataPlaneConfig, null, null, null, null);
    }

    /** Production MTLS constructor; server and client pool must share this exact context/admission. */
    public WorkerDataServer(DataInboundRegistry registry, long awaitTimeoutMs, int preferredPort,
                            DataPlaneConfig dataPlaneConfig,
                            DataPlaneSecurityContext dataPlaneSecurity,
                            DataPlaneHandshakeAdmission handshakeAdmission,
                            WorkerDataIdentity localDataIdentity) {
        this(registry, awaitTimeoutMs, preferredPort, dataPlaneConfig,
                dataPlaneSecurity, handshakeAdmission, localDataIdentity, null);
    }

    /** Production MTLS constructor with the pre-ACK consumer authorization registry. */
    public WorkerDataServer(DataInboundRegistry registry, long awaitTimeoutMs, int preferredPort,
                            DataPlaneConfig dataPlaneConfig,
                            DataPlaneSecurityContext dataPlaneSecurity,
                            DataPlaneHandshakeAdmission handshakeAdmission,
                            WorkerDataIdentity localDataIdentity,
                            DataInboundAuthorizationRegistry authorizationRegistry) {
        this(registry, awaitTimeoutMs, preferredPort,
                requireDataPlane(dataPlaneConfig).ingressAdmission(),
                dataPlaneConfig.bindAddress(), dataPlaneConfig, dataPlaneSecurity,
                handshakeAdmission, localDataIdentity, authorizationRegistry);
    }

    private WorkerDataServer(DataInboundRegistry registry, long awaitTimeoutMs,
                             int preferredPort,
                             ShuffleIngressAdmissionConfig admission,
                             InetAddress bindAddress,
                             DataPlaneConfig dataPlaneConfig,
                             DataPlaneSecurityContext dataPlaneSecurity,
                             DataPlaneHandshakeAdmission handshakeAdmission,
                             WorkerDataIdentity localDataIdentity,
                             DataInboundAuthorizationRegistry authorizationRegistry) {
        if (registry == null || admission == null || awaitTimeoutMs < 0L) {
            throw new IllegalArgumentException("registry/admission must not be null and await timeout must be >= 0");
        }
        if (bindAddress == null) {
            throw new IllegalArgumentException("worker data bind address must not be null");
        }
        this.registry = registry;
        this.admission = admission;
        this.dataPlaneConfig = dataPlaneConfig;
        boolean mtls = dataPlaneConfig != null
                && dataPlaneConfig.protocol() == DataPlaneConfig.Protocol.SHUFFLE_MTLS_V1;
        if (mtls && (dataPlaneSecurity == null || handshakeAdmission == null
                || localDataIdentity == null)) {
            throw new IllegalArgumentException(
                    "MTLS Worker data server requires shared TLS context, admission and identity");
        }
        if (!mtls && (dataPlaneSecurity != null || handshakeAdmission != null
                || localDataIdentity != null || authorizationRegistry != null)) {
            throw new IllegalArgumentException(
                    "LOOPBACK Worker data server must not receive MTLS capabilities");
        }
        this.dataPlaneSecurity = dataPlaneSecurity;
        this.handshakeAdmission = handshakeAdmission;
        this.localDataIdentity = localDataIdentity;
        this.authorizationRegistry = mtls
                ? (authorizationRegistry == null
                        ? new DataInboundAuthorizationRegistry()
                        : authorizationRegistry)
                : null;
        this.handshakeSetupExecutor = mtls
                ? new ThreadPoolExecutor(
                        handshakeAdmission.inboundCapacity(),
                        handshakeAdmission.inboundCapacity(),
                        0L, TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(handshakeAdmission.inboundCapacity()),
                        new NamedDaemonThreadFactory("rowweave-wds-tls-admission"),
                        new ThreadPoolExecutor.AbortPolicy())
                : null;
        this.residentBudget = new ShuffleIngressResidentBudget(admission.ingressResidentBytes());
        this.ingressExecutor = new BoundedShuffleIngressExecutor(admission.maxIngressHandlers());
        int ioThreads = dataPlaneConfig == null
                ? Math.max(2, Runtime.getRuntime().availableProcessors() / 2)
                : dataPlaneConfig.ioThreads();
        this.bossGroup = new NioEventLoopGroup(1, new NamedDaemonThreadFactory("rowweave-wds-boss"));
        this.workerGroup = new NioEventLoopGroup(ioThreads, new NamedDaemonThreadFactory("rowweave-wds-io"));
        // The demux itself is O(1) target queuing, so it is safe to leave on the I/O EventLoop; the blocking work runs on ingressExecutor.
        // There is no extra handlerGroup hop in between, which saves a cross-executor handoff for each batch of frames.
        this.handlerGroup = null;
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            acceptedConnectionCount.incrementAndGet();
                            int active = activeConnectionCount.incrementAndGet();
                            if (active > admission.maxConnections()) {
                                activeConnectionCount.decrementAndGet();
                                ch.close();
                                return;
                            }
                            updatePeak(peakConnectionCount, active);
                            AtomicBoolean released = new AtomicBoolean();
                            ch.closeFuture().addListener(ignored -> {
                                if (released.compareAndSet(false, true)) {
                                    activeConnectionCount.decrementAndGet();
                                }
                            });
                            childTeardownBarriers.add(NettyTransportShutdown.installTeardownBarrier(ch));
                            childChannels.add(ch);
                            if (isMtls()) initializeMtls(ch, awaitTimeoutMs);
                            else installDataPipeline(ch, awaitTimeoutMs, null, null, 0L);
                        }
                    });
            this.serverChannel = b.bind(new InetSocketAddress(bindAddress, preferredPort))
                    .syncUninterruptibly().channel();
        } catch (RuntimeException e) {
            RuntimeException cleanup = shutdownGroups();
            if (cleanup != null) {
                e.addSuppressed(cleanup);
            }
            throw new RuntimeException("failed to bind worker data server", e);
        }
    }

    /** The port actually bound, which is what the worker registration reports to the master. */
    public int port() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    private boolean isMtls() {
        return dataPlaneConfig != null
                && dataPlaneConfig.protocol() == DataPlaneConfig.Protocol.SHUFFLE_MTLS_V1;
    }

    private void initializeMtls(SocketChannel channel, long awaitTimeoutMs) {
        channel.config().setAutoRead(false);
        long deadline = deadlineAfterMillis(dataPlaneConfig.handshakeTimeoutMillis());
        try {
            handshakeSetupExecutor.execute(() -> {
                DataPlaneHandshakeAdmission.Lease lease = null;
                try {
                    lease = handshakeAdmission.acquireInbound(deadline);
                    DataPlaneHandshakeAdmission.Lease acquired = lease;
                    channel.eventLoop().execute(() -> installMtlsOnEventLoop(
                            channel, awaitTimeoutMs, deadline, acquired));
                    lease = null; // event-loop callback now owns the caller handle
                } catch (Throwable failure) {
                    channel.close();
                } finally {
                    if (lease != null) lease.close();
                }
            });
        } catch (RejectedExecutionException saturated) {
            channel.close();
        }
    }

    private void installMtlsOnEventLoop(SocketChannel channel, long awaitTimeoutMs,
                                        long deadline,
                                        DataPlaneHandshakeAdmission.Lease lease) {
        try {
            if (!channel.isOpen() || System.nanoTime() >= deadline) {
                lease.close();
                channel.close();
                return;
            }
            DataPlaneSecurityContext.AdmittedTlsChannel tls =
                    dataPlaneSecurity.installServerHandler(
                            channel.pipeline(), lease, deadline);
            channel.pipeline().addLast(new WorkerDataHelloHandler(
                    tls, localDataIdentity, deadline,
                    peer -> {
                        dataPlaneSecurity.requireWorkerPeer(
                                tls, peer.certificateFingerprint());
                        requireSameDataSet(peer);
                    },
                    (context, peer) -> installDataPipeline(
                            channel, awaitTimeoutMs, context.name(), peer,
                            nextConnectionGeneration()),
                    new ClosingHelloOutcome(channel)));
            channel.config().setAutoRead(true);
            channel.read();
        } catch (Throwable failure) {
            lease.close();
            channel.close();
        }
    }

    private void requireSameDataSet(WorkerDataIdentity peer) {
        if (peer.protocol() != localDataIdentity.protocol()
                || !localDataIdentity.deploymentSetId().equals(peer.deploymentSetId())
                || !localDataIdentity.authzSetId().equals(peer.authzSetId())) {
            throw new IllegalArgumentException("Worker hello data identity set mismatch");
        }
    }

    private long nextConnectionGeneration() {
        long generation = connectionGenerations.incrementAndGet();
        if (generation <= 0L) {
            throw new IllegalStateException("worker data connection generation exhausted");
        }
        return generation;
    }

    private void installDataPipeline(SocketChannel channel, long awaitTimeoutMs,
                                     String afterHandlerName,
                                     WorkerDataIdentity exactPeer,
                                     long connectionGeneration) {
        io.netty.channel.ChannelPipeline pipeline = channel.pipeline();
        ReadTimeoutHandler idle = new ReadTimeoutHandler(
                admission.idleReadTimeoutMillis(), TimeUnit.MILLISECONDS);
        io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol wireProtocol = isMtls()
                ? io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1
                : io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0;
        configureInboundFrameAllocation(channel, wireProtocol);
        ChannelHandler decoder = newInboundFrameDecoder(wireProtocol, residentBudget);
        FrameEncoder encoder = new FrameEncoder(wireProtocol);
        DataDemuxHandler demux = isMtls()
                ? DataDemuxHandler.authenticated(registry, authorizationRegistry,
                        exactPeer, connectionGeneration, awaitTimeoutMs, ingressExecutor,
                        admission.maxTargetsPerConnection(),
                        admission.maxPendingFramesPerTarget(),
                        value -> updatePeak(peakTargetsPerConnection, value),
                        value -> updatePeak(peakPendingFramesPerTarget, value))
                : new DataDemuxHandler(registry, awaitTimeoutMs, ingressExecutor,
                        admission.maxTargetsPerConnection(),
                        admission.maxPendingFramesPerTarget(),
                        value -> updatePeak(peakTargetsPerConnection, value),
                        value -> updatePeak(peakPendingFramesPerTarget, value));
        if (afterHandlerName == null) {
            pipeline.addLast(idle);
            pipeline.addLast(decoder);
            pipeline.addLast(encoder);
            pipeline.addLast(demux);
            return;
        }
        pipeline.addAfter(afterHandlerName, "rowweave-data-idle", idle);
        pipeline.addAfter("rowweave-data-idle", "rowweave-data-decoder", decoder);
        pipeline.addAfter("rowweave-data-decoder", "rowweave-data-encoder", encoder);
        pipeline.addAfter("rowweave-data-encoder", "rowweave-data-demux", demux);
    }

    static void configureInboundFrameAllocation(
            Channel channel,
            io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol protocol) {
        if (channel == null || protocol == null) {
            throw new IllegalArgumentException("channel and data protocol are required");
        }
        if (protocol
                == io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1) {
            channel.config().setRecvByteBufAllocator(
                    new FixedRecvByteBufAllocator(AUTHENTICATED_READ_BYTES));
        }
    }

    static ChannelHandler newInboundFrameDecoder(
            io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol protocol,
            ShuffleIngressResidentBudget residentBudget) {
        if (protocol == null || residentBudget == null) {
            throw new IllegalArgumentException("data protocol and resident budget are required");
        }
        if (protocol
                == io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1) {
            return new ReservedV1FrameDecoder(residentBudget);
        }
        return new FrameDecoder(protocol, residentBudget);
    }

    private static long deadlineAfterMillis(long timeoutMillis) {
        long now = System.nanoTime();
        long budget = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        return now > Long.MAX_VALUE - budget ? Long.MAX_VALUE : now + budget;
    }

    private static final class ClosingHelloOutcome implements WorkerDataHelloHandler.Outcome {
        private final Channel channel;

        private ClosingHelloOutcome(Channel channel) { this.channel = channel; }
        @Override public void complete(WorkerDataIdentity peer) { }
        @Override public void fail(Throwable failure) { channel.close(); }
    }

    InetSocketAddress boundAddress() {
        return (InetSocketAddress) serverChannel.localAddress();
    }

    /** Acceptance probe: the number of physical data connections accepted so far in this server's lifetime. */
    public int acceptedConnectionCount() {
        return acceptedConnectionCount.get();
    }

    AdmissionSnapshot admissionSnapshot() {
        ShuffleIngressResidentBudget.Snapshot resident = residentBudget.snapshot();
        return new AdmissionSnapshot(activeConnectionCount.get(),
                peakConnectionCount.get(), ingressExecutor.activeHandlers(),
                ingressExecutor.peakHandlers(), peakTargetsPerConnection.get(),
                peakPendingFramesPerTarget.get(), resident.usedBytes(), resident.peakBytes());
    }

    @Override
    public void close() {
        if (authorizationRegistry != null) authorizationRegistry.close();
        registry.close();
        synchronized (shutdownLock) {
            if (!shutdownComplete) {
                // Stop admission waiters first so they cannot enqueue TLS installation while
                // the Netty groups and accepted channels are being drained.
                shutdownHandshakeSetup();
                shutdownFailure = NettyTransportShutdown.shutdownServer(
                        "WorkerDataServer", serverChannel, childChannels, childTeardownBarriers,
                        bossGroup, workerGroup, handlerGroup);
                ingressExecutor.shutdownNow();
                try {
                    if (!ingressExecutor.awaitTermination(
                            NettyTransportShutdown.timeoutMs(), TimeUnit.MILLISECONDS)) {
                        RuntimeException timeout = new IllegalStateException(
                                "WorkerDataServer ingress executor shutdown timed out");
                        if (shutdownFailure == null) shutdownFailure = timeout;
                        else shutdownFailure.addSuppressed(timeout);
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    RuntimeException failure = new IllegalStateException(
                            "interrupted shutting down WorkerDataServer ingress executor", interrupted);
                    if (shutdownFailure == null) shutdownFailure = failure;
                    else shutdownFailure.addSuppressed(failure);
                }
                shutdownComplete = true;
            }
            if (shutdownFailure != null) {
                LOG.warn("WorkerDataServer shutdown incomplete", shutdownFailure);
            }
        }
    }

    private RuntimeException shutdownGroups() {
        ingressExecutor.shutdownNow();
        shutdownHandshakeSetup();
        return NettyTransportShutdown.shutdownGroups(
                "WorkerDataServer constructor", bossGroup, workerGroup, handlerGroup);
    }

    private void shutdownHandshakeSetup() {
        if (handshakeSetupExecutor == null) return;
        handshakeSetupExecutor.shutdownNow();
        try {
            if (!handshakeSetupExecutor.awaitTermination(
                    NettyTransportShutdown.timeoutMs(), TimeUnit.MILLISECONDS)) {
                LOG.warn("WorkerDataServer TLS admission executor did not terminate");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void updatePeak(AtomicInteger peak, int candidate) {
        int current = peak.get();
        while (candidate > current && !peak.compareAndSet(current, candidate)) {
            current = peak.get();
        }
    }

    private static DataPlaneConfig systemDataPlaneConfig() {
        Properties snapshot = new Properties();
        snapshot.putAll(System.getProperties());
        ControlSecurityConfig control = ControlSecurityConfig.fromProperties(snapshot);
        return DataPlaneConfig.fromProperties(
                snapshot, control, Runtime.getRuntime().maxMemory());
    }

    private static DataPlaneConfig requireDataPlane(DataPlaneConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("data plane config must not be null");
        }
        return config;
    }

    private static InetAddress ipv4Loopback() {
        try {
            return InetAddress.getByName("127.0.0.1");
        } catch (java.net.UnknownHostException impossible) {
            throw new IllegalStateException("IPv4 loopback is unavailable", impossible);
        }
    }

    static final class AdmissionSnapshot {
        private final int activeConnections;
        private final int peakConnections;
        private final int activeHandlers;
        private final int peakHandlers;
        private final int peakTargetsPerConnection;
        private final int peakPendingFramesPerTarget;
        private final long residentBytes;
        private final long peakResidentBytes;

        private AdmissionSnapshot(int activeConnections, int peakConnections,
                                  int activeHandlers, int peakHandlers,
                                  int peakTargetsPerConnection,
                                  int peakPendingFramesPerTarget,
                                  long residentBytes, long peakResidentBytes) {
            this.activeConnections = activeConnections;
            this.peakConnections = peakConnections;
            this.activeHandlers = activeHandlers;
            this.peakHandlers = peakHandlers;
            this.peakTargetsPerConnection = peakTargetsPerConnection;
            this.peakPendingFramesPerTarget = peakPendingFramesPerTarget;
            this.residentBytes = residentBytes;
            this.peakResidentBytes = peakResidentBytes;
        }

        int activeConnections() { return activeConnections; }
        int peakConnections() { return peakConnections; }
        int activeHandlers() { return activeHandlers; }
        int peakHandlers() { return peakHandlers; }
        int peakTargetsPerConnection() { return peakTargetsPerConnection; }
        int peakPendingFramesPerTarget() { return peakPendingFramesPerTarget; }
        long residentBytes() { return residentBytes; }
        long peakResidentBytes() { return peakResidentBytes; }

        @Override
        public String toString() {
            return "AdmissionSnapshot{connections=" + activeConnections + "/" + peakConnections
                    + ", handlers=" + activeHandlers + "/" + peakHandlers
                    + ", targetsPeak=" + peakTargetsPerConnection
                    + ", pendingPeak=" + peakPendingFramesPerTarget
                    + ", resident=" + residentBytes + "/" + peakResidentBytes + '}';
        }
    }
}
