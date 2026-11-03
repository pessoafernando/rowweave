package io.rowweave.sched.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.rowweave.DataRow;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.mem.RowSerializer;
import io.rowweave.sched.MultiProducerChannel;
import io.rowweave.sched.stream.StreamEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;

/**
 * The lightweight demux for a worker data connection. The handler only queues by targetId, and each
 * connection-target is served by its own ordered {@link TargetIngress}, which does the registry await, the
 * deserialization and the send that may block, so a slow target no longer heads-of-line blocks its siblings.
 *
 * <p>After an OPEN the connection follows the mux protocol, where credit is a Frame carrying a targetId and is
 * returned only after the downstream has really polled the record. A connection that never sent OPEN still
 * serves the older sender with a bare int credit, which is what mux off and the compatibility tests use.
 */
public final class DataDemuxHandler extends SimpleChannelInboundHandler<Frame> {

    private final DataInboundRegistry registry;
    private final long awaitTimeoutMs;
    private final Executor ingressExecutor;
    private final int maxTargetsPerConnection;
    private final int maxPendingFramesPerTarget;
    private final IntConsumer targetPeakObserver;
    private final IntConsumer pendingPeakObserver;
    private final DataPlaneRegistration.Protocol protocol;
    private final DataInboundAuthorizationRegistry authorizationRegistry;
    private final WorkerDataIdentity exactPeer;
    private final long connectionGeneration;
    private final Map<StreamKey, TargetIngress> ingresses = new HashMap<>();
    private final AtomicBoolean connectionRejected = new AtomicBoolean();

    public DataDemuxHandler(DataInboundRegistry registry, long awaitTimeoutMs) {
        this(registry, awaitTimeoutMs, Runnable::run);
    }

    /** The older constructor signature is kept; creditBatch belongs only to the legacy protocol, since the new mux must return exactly what each consumption took. */
    public DataDemuxHandler(DataInboundRegistry registry, long awaitTimeoutMs, long creditBatch) {
        this(registry, awaitTimeoutMs, Runnable::run);
    }

    public DataDemuxHandler(DataInboundRegistry registry, long awaitTimeoutMs, Executor ingressExecutor) {
        this(registry, awaitTimeoutMs, ingressExecutor, Integer.MAX_VALUE,
                Integer.MAX_VALUE, ignored -> { }, ignored -> { });
    }

    DataDemuxHandler(DataInboundRegistry registry, long awaitTimeoutMs,
                     Executor ingressExecutor,
                     int maxTargetsPerConnection,
                     int maxPendingFramesPerTarget,
                     IntConsumer targetPeakObserver,
                     IntConsumer pendingPeakObserver) {
        this(registry, awaitTimeoutMs, ingressExecutor, maxTargetsPerConnection,
                maxPendingFramesPerTarget, targetPeakObserver, pendingPeakObserver,
                DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0, null, null, 0L);
    }

    static DataDemuxHandler authenticated(
            DataInboundRegistry registry,
            DataInboundAuthorizationRegistry authorizationRegistry,
            WorkerDataIdentity exactPeer,
            long connectionGeneration,
            long awaitTimeoutMs,
            Executor ingressExecutor) {
        return authenticated(registry, authorizationRegistry, exactPeer,
                connectionGeneration, awaitTimeoutMs, ingressExecutor,
                Integer.MAX_VALUE, Integer.MAX_VALUE, ignored -> { }, ignored -> { });
    }

    static DataDemuxHandler authenticated(
            DataInboundRegistry registry,
            DataInboundAuthorizationRegistry authorizationRegistry,
            WorkerDataIdentity exactPeer,
            long connectionGeneration,
            long awaitTimeoutMs,
            Executor ingressExecutor,
            int maxTargetsPerConnection,
            int maxPendingFramesPerTarget,
            IntConsumer targetPeakObserver,
            IntConsumer pendingPeakObserver) {
        return new DataDemuxHandler(registry, awaitTimeoutMs, ingressExecutor,
                maxTargetsPerConnection, maxPendingFramesPerTarget,
                targetPeakObserver, pendingPeakObserver,
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                authorizationRegistry, exactPeer, connectionGeneration);
    }

    private DataDemuxHandler(DataInboundRegistry registry, long awaitTimeoutMs,
                     Executor ingressExecutor,
                     int maxTargetsPerConnection,
                     int maxPendingFramesPerTarget,
                     IntConsumer targetPeakObserver,
                     IntConsumer pendingPeakObserver,
                     DataPlaneRegistration.Protocol protocol,
                     DataInboundAuthorizationRegistry authorizationRegistry,
                     WorkerDataIdentity exactPeer,
                     long connectionGeneration) {
        if (registry == null || ingressExecutor == null || targetPeakObserver == null
                || pendingPeakObserver == null || protocol == null
                || maxTargetsPerConnection < 1
                || maxPendingFramesPerTarget < 1) {
            throw new IllegalArgumentException("invalid data demux admission");
        }
        boolean authenticated = protocol == DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1;
        if (authenticated && (authorizationRegistry == null || exactPeer == null
                || connectionGeneration <= 0L)) {
            throw new IllegalArgumentException("authenticated demux requires authorization and peer binding");
        }
        if (!authenticated && (authorizationRegistry != null || exactPeer != null
                || connectionGeneration != 0L)) {
            throw new IllegalArgumentException("plain demux must not receive authorization capabilities");
        }
        this.registry = registry;
        this.awaitTimeoutMs = awaitTimeoutMs;
        this.ingressExecutor = ingressExecutor;
        this.maxTargetsPerConnection = maxTargetsPerConnection;
        this.maxPendingFramesPerTarget = maxPendingFramesPerTarget;
        this.targetPeakObserver = targetPeakObserver;
        this.pendingPeakObserver = pendingPeakObserver;
        this.protocol = protocol;
        this.authorizationRegistry = authorizationRegistry;
        this.exactPeer = exactPeer;
        this.connectionGeneration = connectionGeneration;
    }

    /** Test and diagnostics probe; the map itself is only added to or removed from on the connection EventLoop. */
    int activeTargetCount() {
        return ingresses.size();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
        if (connectionRejected.get()) {
            frame.releaseIngressReservation();
            ctx.close();
            return;
        }
        if (frame.type() == FrameCodec.FRAME_KEEPALIVE && Frame.NO_TARGET.equals(frame.targetId())) {
            try {
                ctx.writeAndFlush(new Frame(FrameCodec.FRAME_KEEPALIVE,
                        Frame.NO_TARGET, new byte[0]));
            } finally {
                frame.releaseIngressReservation();
            }
            return;
        }
        if (Frame.NO_TARGET.equals(frame.targetId())) {
            frame.releaseIngressReservation();
            ctx.close();
            return;
        }
        StreamKey streamKey = new StreamKey(frame.targetId(),
                isAuthenticated() ? frame.producerOrdinal() : 0);
        TargetIngress ingress = ingresses.get(streamKey);
        if (ingress == null) {
            if (ingresses.size() >= maxTargetsPerConnection) {
                frame.releaseIngressReservation();
                rejectConnection(ctx, new IllegalStateException(
                        "shuffle ingress target admission exhausted: maxTargetsPerConnection="
                                + maxTargetsPerConnection));
                return;
            }
            ingress = new TargetIngress(streamKey, ctx.channel());
            ingresses.put(streamKey, ingress);
            targetPeakObserver.accept(ingresses.size());
        }
        ingress.enqueue(frame);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (!registry.isClosed() || isAuthenticated()) {
            IOException failure = new IOException("worker data connection closed before target EOS");
            for (TargetIngress ingress : ingresses.values()) {
                ingress.enqueueConnectionEnd(failure);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!registry.isClosed() || isAuthenticated()) {
            Throwable failure = cause instanceof IOException ? cause
                    : new IOException("worker data connection failed: " + cause.getMessage(), cause);
            for (TargetIngress ingress : ingresses.values()) {
                ingress.enqueueConnectionEnd(failure);
            }
        }
        ctx.close();
    }

    private void rejectConnection(ChannelHandlerContext ctx, Throwable cause) {
        if (connectionRejected.compareAndSet(false, true)) {
            abortAll(cause);
        }
        ctx.close();
    }

    private void rejectConnection(Channel channel, Throwable cause) {
        if (!connectionRejected.compareAndSet(false, true)) {
            return;
        }
        Runnable rejection = () -> {
            abortAll(cause);
            channel.close();
        };
        if (channel.eventLoop().inEventLoop()) {
            rejection.run();
            return;
        }
        try {
            channel.eventLoop().execute(rejection);
        } catch (RuntimeException eventLoopClosed) {
            // A terminated EventLoop can no longer change its handler map, so complete the reservation cleanup directly.
            abortAll(cause);
            channel.close();
        }
    }

    private void abortAll(Throwable cause) {
        TargetIngress[] snapshot = ingresses.values().toArray(new TargetIngress[0]);
        ingresses.clear();
        for (TargetIngress ingress : snapshot) {
            ingress.abort(cause);
        }
    }

    private boolean isAuthenticated() {
        return protocol == DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1;
    }

    private final class TargetIngress {
        private final StreamKey streamKey;
        private final UUID targetId;
        private final int producerOrdinal;
        private final Channel channel;
        private final ArrayDeque<Object> pending = new ArrayDeque<>();
        private boolean draining;
        private DataInboundRegistry.Entry entry;
        private RowSerializer serializer;
        private boolean mux;
        private boolean protocolChosen;
        private int openProducers;
        private boolean legacyEosSeen;
        private volatile boolean terminal;
        private final AtomicLong consumedCredit = new AtomicLong();
        private final AtomicBoolean creditFlushScheduled = new AtomicBoolean();
        private int pendingFrames;
        private DataInboundAuthorizationRegistry.AuthorizedStream authorizedStream;

        TargetIngress(StreamKey streamKey, Channel channel) {
            this.streamKey = streamKey;
            this.targetId = streamKey.targetId;
            this.producerOrdinal = streamKey.producerOrdinal;
            this.channel = channel;
        }

        void enqueue(Frame frame) {
            boolean schedule = false;
            boolean overflow = false;
            synchronized (pending) {
                if (terminal) {
                    frame.releaseIngressReservation();
                    return;
                }
                if (pendingFrames >= maxPendingFramesPerTarget) {
                    overflow = true;
                } else {
                    pending.addLast(frame);
                    pendingFrames++;
                    pendingPeakObserver.accept(pendingFrames);
                    if (!draining) {
                        draining = true;
                        schedule = true;
                    }
                }
            }
            if (overflow) {
                frame.releaseIngressReservation();
                rejectConnection(channel, new IllegalStateException(
                        "shuffle ingress pending frame admission exhausted: maxPendingFramesPerTarget="
                                + maxPendingFramesPerTarget));
            } else if (schedule) {
                submitDrain();
            }
        }

        void enqueueConnectionEnd(Throwable cause) {
            boolean schedule = false;
            synchronized (pending) {
                pending.addLast(new ConnectionEnd(cause));
                if (!draining) {
                    draining = true;
                    schedule = true;
                }
            }
            if (schedule) submitDrain();
        }

        void enqueueTargetEnd() {
            boolean schedule = false;
            synchronized (pending) {
                pending.addLast(TargetEnd.INSTANCE);
                if (!draining) {
                    draining = true;
                    schedule = true;
                }
            }
            if (schedule) submitDrain();
        }

        private void submitDrain() {
            try {
                ingressExecutor.execute(this::drain);
            } catch (RejectedExecutionException rejected) {
                synchronized (pending) {
                    draining = false;
                }
                rejectConnection(channel, new IllegalStateException(
                        "shuffle ingress handler admission exhausted", rejected));
            } catch (RuntimeException rejected) {
                synchronized (pending) {
                    draining = false;
                }
                rejectConnection(channel, rejected);
            }
        }

        private void drain() {
            while (true) {
                Object item;
                synchronized (pending) {
                    item = pending.pollFirst();
                    if (item == null) {
                        draining = false;
                        return;
                    }
                    if (item instanceof Frame) pendingFrames--;
                }
                try {
                    if (item == TargetEnd.INSTANCE) {
                        finishTarget();
                    } else if (item instanceof ConnectionEnd) {
                        onConnectionEnd(((ConnectionEnd) item).cause);
                    } else {
                        onFrame((Frame) item);
                    }
                } catch (Throwable failure) {
                    failTarget(failure);
                } finally {
                    if (item instanceof Frame) {
                        ((Frame) item).releaseIngressReservation();
                    }
                }
            }
        }

        private void abort(Throwable cause) {
            synchronized (pending) {
                terminal = true;
                Object item;
                while ((item = pending.pollFirst()) != null) {
                    if (item instanceof Frame) {
                        ((Frame) item).releaseIngressReservation();
                    }
                }
                pendingFrames = 0;
            }
            RuntimeException cancellation = cause instanceof RuntimeException
                    ? (RuntimeException) cause
                    : new IllegalStateException(String.valueOf(cause.getMessage()), cause);
            if (isAuthenticated()) {
                if (authorizedStream != null) authorizedStream.fail();
                if (entry != null) entry.channel.fail(cancellation);
            } else {
                try {
                    registry.cancel(targetId, cancellation);
                } catch (RuntimeException ignored) {
                    // The registry close or full path already published the same terminal state, and this method has released the reservation.
                }
            }
        }

        private void onFrame(Frame frame) throws IOException {
            if (terminal) {
                return;
            }
            if (isAuthenticated()) {
                onAuthenticatedFrame(frame);
                return;
            }
            if (!protocolChosen) {
                protocolChosen = true;
                mux = frame.type() == FrameCodec.FRAME_OPEN;
            }
            switch (frame.type()) {
                case FrameCodec.FRAME_OPEN:
                    if (!mux) {
                        throw new IllegalStateException("OPEN after legacy data for target " + targetId);
                    }
                    resolve();
                    openProducers++;
                    if (openProducers == 1) {
                        long initial = entry.windowBytes <= 0 ? 1L
                                : Math.max(1L, entry.windowBytes / entry.remoteProducerCount);
                        grantMux((int) Math.min(Integer.MAX_VALUE, initial));
                    }
                    break;
                case FrameCodec.FRAME_RECORD:
                    if (mux && openProducers <= 0) {
                        throw new IllegalStateException("RECORD before OPEN for target " + targetId);
                    }
                    deliverRecord(frame.payload());
                    break;
                case FrameCodec.FRAME_EOS:
                    resolve();
                    if (mux && openProducers <= 0) {
                        throw new IllegalStateException("EOS without OPEN for target " + targetId);
                    }
                    entry.channel.producerDone();
                    if (mux) {
                        openProducers--;
                    } else {
                        legacyEosSeen = true;
                    }
                    break;
                case FrameCodec.FRAME_ERROR:
                    resolve();
                    entry.channel.fail(new RuntimeException("upstream shuffle error: "
                            + new String(frame.payload(), StandardCharsets.UTF_8)));
                    finishTarget();
                    break;
                case FrameCodec.FRAME_KEEPALIVE:
                    break;
                default:
                    throw new IOException("unknown frame type " + frame.type());
            }
        }

        private void onAuthenticatedFrame(Frame frame) throws IOException {
            switch (frame.type()) {
                case FrameCodec.FRAME_OPEN:
                    if (authorizedStream != null
                            || frame.payload().length != ShuffleProducerGrant.BYTES) {
                        throw new DataInboundAuthorizationRegistry.AuthorizationRejectedException();
                    }
                    authorizedStream = authorizationRegistry.open(
                            targetId, producerOrdinal,
                            ShuffleProducerGrant.fromBytes(frame.payload()),
                            connectionGeneration, exactPeer);
                    resolve();
                    protocolChosen = true;
                    mux = true;
                    openProducers = 1;
                    grantMux(authorizedStream.claimInitialCredit(entry.windowBytes));
                    break;
                case FrameCodec.FRAME_RECORD:
                    requireAuthorizedOwner();
                    deliverRecord(frame.payload());
                    break;
                case FrameCodec.FRAME_EOS:
                    requireAuthorizedOwner();
                    entry.channel.producerDone();
                    openProducers = 0;
                    authorizedStream.markEos();
                    finishTarget();
                    break;
                case FrameCodec.FRAME_ERROR:
                    requireAuthorizedOwner();
                    entry.channel.fail(new RuntimeException("upstream shuffle stream failed"));
                    authorizedStream.fail();
                    finishTarget();
                    break;
                default:
                    throw new IOException("invalid authenticated target frame");
            }
        }

        private void requireAuthorizedOwner() {
            if (authorizedStream == null
                    || !authorizedStream.matchesOwner(connectionGeneration, exactPeer)) {
                throw new DataInboundAuthorizationRegistry.AuthorizationRejectedException();
            }
        }

        private void deliverRecord(byte[] payload) {
            resolve();
            DataRow row = serializer.deserialize(payload);
            int wireBytes = FrameCodec.headerBytes(protocol) + payload.length;
            if (mux) {
                entry.channel.send(StreamEvent.record(row), wireBytes, () -> returnConsumedCredit(wireBytes));
            } else {
                entry.channel.send(StreamEvent.record(row), wireBytes);
                grantLegacy(wireBytes);
            }
        }

        private void resolve() {
            if (entry == null) {
                entry = registry.await(targetId, awaitTimeoutMs);
                serializer = new RowSerializer(entry.dataSchema);
                entry.channel.onTerminal(this::enqueueTargetEnd);
            }
        }

        private void onConnectionEnd(Throwable cause) {
            if (terminal) {
                return;
            }
            if ((mux && openProducers > 0) || (!mux && protocolChosen && !legacyEosSeen)) {
                IOException incomplete = isAuthenticated()
                        ? new IOException(
                        "authenticated shuffle connection closed before stream terminal")
                        : new IOException(cause.getMessage() + " target=" + targetId
                        + " mux=" + mux + " openProducers=" + openProducers, cause);
                failTarget(incomplete);
            }
        }

        private void failTarget(Throwable cause) {
            if (terminal) {
                return;
            }
            if (isAuthenticated()) {
                if (authorizedStream != null) {
                    if (entry != null) entry.channel.fail(cause);
                    authorizedStream.fail();
                }
            } else {
                try {
                    resolve();
                    entry.channel.fail(cause);
                } catch (RuntimeException ignored) {
                    // A registry cancel or close is itself the terminal state of the target.
                }
            }
            if (mux && channel.isActive()) {
                byte[] payload = (isAuthenticated()
                        ? "shuffle stream rejected"
                        : String.valueOf(cause.getMessage())).getBytes(StandardCharsets.UTF_8);
                channel.writeAndFlush(new Frame(
                        FrameCodec.FRAME_ERROR, targetId, producerOrdinal, payload));
            } else if (isAuthenticated() && channel.isActive()) {
                channel.writeAndFlush(new Frame(FrameCodec.FRAME_ERROR,
                        targetId, producerOrdinal,
                        "shuffle stream rejected".getBytes(StandardCharsets.UTF_8)));
            }
            finishTarget();
        }

        /** Only the connection EventLoop may change the HashMap; a consumption callback may keep holding the ingress after EOS and return the credit already taken. */
        private void finishTarget() {
            if (terminal) {
                return;
            }
            terminal = true;
            if (authorizedStream != null) authorizedStream.fail();
            channel.eventLoop().execute(() -> ingresses.remove(streamKey, this));
        }

        private void grantMux(int bytes) {
            if (bytes <= 0 || !channel.isActive()) {
                return;
            }
            byte[] payload = ByteBuffer.allocate(4).putInt(bytes).array();
            channel.writeAndFlush(new Frame(
                    FrameCodec.FRAME_CREDIT, targetId, producerOrdinal, payload));
        }

        /**
         * A consumption callback can fire once per row, so the wire bytes are first tallied exactly and then
         * coalesced into one CREDIT frame per one-millisecond slice on the connection EventLoop. The total
         * returned is unchanged, and this keeps a writeAndFlush per row from cutting through mux throughput.
         */
        private void returnConsumedCredit(int bytes) {
            if (bytes <= 0 || !channel.isActive()) {
                return;
            }
            consumedCredit.addAndGet(bytes);
            scheduleCreditFlush();
        }

        private void scheduleCreditFlush() {
            if (!creditFlushScheduled.compareAndSet(false, true)) {
                return;
            }
            try {
                channel.eventLoop().schedule(this::flushConsumedCredit, 1L, TimeUnit.MILLISECONDS);
            } catch (RuntimeException closed) {
                creditFlushScheduled.set(false);
            }
        }

        private void flushConsumedCredit() {
            long pendingBytes = consumedCredit.getAndSet(0L);
            if (pendingBytes > 0 && channel.isActive()) {
                long remaining = pendingBytes;
                while (remaining > 0) {
                    int grant = (int) Math.min(Integer.MAX_VALUE, remaining);
                    grantMux(grant);
                    remaining -= grant;
                }
            }
            creditFlushScheduled.set(false);
            if (consumedCredit.get() > 0 && channel.isActive()) {
                scheduleCreditFlush();
            }
        }

        private void grantLegacy(int bytes) {
            if (bytes <= 0 || !channel.isActive()) {
                return;
            }
            ByteBuf credit = channel.alloc().buffer(4);
            credit.writeInt(bytes);
            channel.writeAndFlush(credit);
        }
    }

    private static final class StreamKey {
        final UUID targetId;
        final int producerOrdinal;

        StreamKey(UUID targetId, int producerOrdinal) {
            this.targetId = targetId;
            this.producerOrdinal = producerOrdinal;
        }

        @Override public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof StreamKey)) return false;
            StreamKey that = (StreamKey) other;
            return producerOrdinal == that.producerOrdinal && targetId.equals(that.targetId);
        }

        @Override public int hashCode() {
            return 31 * targetId.hashCode() + producerOrdinal;
        }
    }

    private static final class ConnectionEnd {
        final Throwable cause;

        ConnectionEnd(Throwable cause) {
            this.cause = cause;
        }
    }

    private enum TargetEnd {
        INSTANCE
    }
}
