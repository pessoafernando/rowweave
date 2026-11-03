package io.rowweave.sched.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import io.rowweave.DataSchema;
import io.rowweave.mem.RowSerializer;
import io.rowweave.sched.ChannelPoisonedException;
import io.rowweave.sched.stream.StreamEvent;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The socket-backed Channel sender. The **public API and wire protocol are unchanged**: {@code send} encodes
 * RECORD frames and writes them through the Netty channel, {@code close} emits EOS and half-closes gracefully,
 * and {@code fail} emits ERROR and closes the connection.
 *
 * <p><b>Application-level byte-credit backpressure</b>: {@code send} runs on the **caller thread** rather than the
 * EventLoop and keeps a lock plus Condition credit counter. A send parks once credit is at or below zero, where
 * one unit is one wire byte (the last row may overshoot into negative credit, and a single row is always
 * deliverable, so this cannot deadlock). Reading the credit is moved to the inbound handler on the EventLoop,
 * which picks up the **bare int** credit echoed by the receiver, adds it back and signals. Batching: writes do not
 * flush per frame and flush only once the staging buffer reaches the window size in bytes, which keeps the Netty
 * ChannelOutboundBuffer naturally bounded at about one window instead of growing without limit.
 *
 * <p><b>Wedged-peer backstop</b>: with keepaliveMs above zero, FRAME_KEEPALIVE is sent periodically on the
 * EventLoop and IdleStateHandler sets readerIdle to soTimeoutMs, so a receiver sending nothing at all poisons this
 * side and wakes a parked send. The EventLoop never blocks, so readerIdle cannot fire spuriously.
 *
 * <p><b>Graceful shutdown</b>: after EOS is written, shutdownOutput() sends the FIN, then the peer's drain and
 * close are awaited through closeFuture with a timeout backstop, and only then is the channel closed.
 */
public final class SocketChannelSink implements io.rowweave.sched.Channel, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SocketChannelSink.class);
    private static final String TEST_DROP_EOS_ONCE_PROPERTY =
            "rowweave.test.shuffle.dropEosOnce";
    private static final String TEST_DROP_EOS_CLAIM_PROPERTY =
            "rowweave.test.shuffle.dropEosClaim";
    private static final AtomicBoolean TEST_DROP_EOS_ONCE =
            new AtomicBoolean(testDropEosOnceEnabled());

    static final byte FRAME_RECORD = FrameCodec.FRAME_RECORD;
    static final byte FRAME_EOS = FrameCodec.FRAME_EOS;
    static final byte FRAME_ERROR = FrameCodec.FRAME_ERROR;
    static final byte FRAME_KEEPALIVE = FrameCodec.FRAME_KEEPALIVE;

    /** Keepalive off, meaning zero: no keepalive is sent and no readerIdle is set, which is the earlier behaviour trusting RST. */
    static final int KEEPALIVE_DISABLED = 0;

    /**
     * Time-sliced flush interval: even when the staging buffer has not reached its byte threshold, anything
     * buffered is sent once this interval passes. A slow streaming operator that emits small rows per call would
     * never fill a 1 MiB window, so without this it would hold its first frame until the buffer filled; now the
     * downstream receives the first frame promptly (READY then RUNNING) and first-row latency is bounded by about
     * flushIntervalMs. It is scheduled on the EventLoop, so the per-row hot path on the caller thread is untouched.
     */
    static final long FLUSH_INTERVAL_MS = Long.getLong("rowweave.shuffle.flushIntervalMs", 100L);

    private final RowSerializer serializer;
    private final int keepaliveMs;
    // The multiplexing routing key for outbound frames: NO_TARGET means disabled or unset, and when connecting to a worker dataPort this is set by withTargetId to the globally unique id of the downstream.
    private volatile java.util.UUID outboundTargetId = Frame.NO_TARGET;

    private final EventLoopGroup group;
    private final Channel channel;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition hasCredit = lock.newCondition();
    private long credit;  // Guarded by lock; the unit is wire bytes, and it may go negative on an overshoot
    private boolean poisoned;  // Guarded by lock: the receiving side died or this side failed
    private boolean closed;      // guarded by lock
    private final Object shutdownLock = new Object();
    private boolean shutdownComplete;
    private RuntimeException shutdownFailure;

    // Batched flush: the caller thread encodes frames into the staging buffer and issues **one** writeAndFlush to
    // Netty only when flushByteThreshold (the window) is reached. This removes the cross-thread MPSC enqueue, the
    // per-row ChannelOutboundBuffer entry and the per-row listener cost of a channel.write per row.
    // Guarded by `flushLock`, which serializes it with the flush done by close and fail.
    private final long flushByteThreshold;
    private final Object flushLock = new Object();
    private byte[] staging;  // Guarded by flushLock: the frame bytes held by the batch
    private int stagingLen;       // guarded by flushLock

    /** The older constructor: no keepalive and no readerIdle. */
    public SocketChannelSink(DataSchema dataSchema, String host, int port, int window) {
        this(dataSchema, host, port, window, KEEPALIVE_DISABLED, KEEPALIVE_DISABLED);
    }

    /**
     * Sets the multiplexing routing key of outbound frames, which is the globally unique targetId of the
     * downstream. On connecting to a worker dataPort, the server-side DataDemuxHandler uses it to route each
     * frame to the channel of the matching downstream stage. <b>It must be set before the first send</b>, as
     * a volatile assigned by a chained call straight after construction. Zero keeps the legacy single port.
     */
    public SocketChannelSink withTargetId(java.util.UUID targetId) {
        this.outboundTargetId = targetId == null ? Frame.NO_TARGET : targetId;
        return this;
    }

    public SocketChannelSink(DataSchema dataSchema, String host, int port, int window, int keepaliveMs, int soTimeoutMs) {
        if (window <= 0) {
            throw new IllegalArgumentException("window must be > 0");
        }
        this.serializer = new RowSerializer(dataSchema);
        this.credit = window;
        this.keepaliveMs = keepaliveMs;
        this.flushByteThreshold = window;
        // The staging buffer starts at the smaller of the window and 64K and doubles as needed; the ceiling is held near the window by credit rather than growing without bound.
        this.staging = new byte[(int) Math.min(Math.max(window, 4096), 64 * 1024)];

        this.group = new NioEventLoopGroup(1, new NamedDaemonThreadFactory("rowweave-sink-io"));
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)  // Unpooled, which puts no-OOM first
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            if (soTimeoutMs > 0) {
                                ch.pipeline().addLast(new IdleStateHandler(soTimeoutMs, 0, 0, TimeUnit.MILLISECONDS));
                            }
                            ch.pipeline().addLast(new FrameEncoder());       // outbound: Frame → bytes
                            ch.pipeline().addLast(new CreditIntDecoder());   // inbound: bytes → Integer credit
                            ch.pipeline().addLast(new CreditHandler());  // Inbound: adds the credit handler, with readerIdle poisoning
                        }
                    });
            this.channel = b.connect(host, port).syncUninterruptibly().channel();
        } catch (RuntimeException e) {
            RuntimeException cleanup = NettyTransportShutdown.shutdownGroups(
                    "SocketChannelSink constructor", group);
            if (cleanup != null) {
                e.addSuppressed(cleanup);
            }
            throw new RuntimeException("failed to connect shuffle sink to " + host + ":" + port, e);
        }
        if (keepaliveMs > 0) {
            channel.eventLoop().scheduleAtFixedRate(this::sendKeepalive, keepaliveMs, keepaliveMs, TimeUnit.MILLISECONDS);
        }
        // Time-slice flush: periodically hand out whatever remains in staging, so that below the byte threshold a slow trickle still goes out in time, which bounds first-row latency and lets the downstream move from READY to RUNNING promptly.
        if (FLUSH_INTERVAL_MS > 0) {
            channel.eventLoop().scheduleAtFixedRate(
                    this::timedFlush, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Time-sliced flush, scheduled periodically on the EventLoop: it sends the bytes accumulated in staging. It
     * runs on the EventLoop thread, and is serialized against the caller thread's flush by flushLock, which
     * drainStaging holds; the drained snapshot is then written with writeAndFlush, and since the EventLoop is
     * Netty's writer thread this writes directly with no cross-thread enqueue. After close or poison it is a
     * no-op, because close and fail already flushed whatever was left.
     */
    private void timedFlush() {
        lock.lock();
        boolean skip;
        try {
            skip = poisoned || closed;
        } finally {
            lock.unlock();
        }
        if (skip) {
            return;
        }
        byte[] pending;
        synchronized (flushLock) {
            pending = stagingLen > 0 ? drainStaging() : null;
        }
        if (pending != null) {
            flushBytes(pending, pending.length);
        }
    }

    private void sendKeepalive() {
        boolean skip;
        lock.lock();
        try {
            skip = poisoned || closed;
        } finally {
            lock.unlock();
        }
        if (!skip && channel.isActive()) {
            channel.writeAndFlush(new Frame(FRAME_KEEPALIVE, outboundTargetId, new byte[0]));  // Carries the targetId, for consistency, although the demux ignores KEEPALIVE
        }
    }

    private void poison() {
        lock.lock();
        try {
            poisoned = true;
            hasCredit.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void send(StreamEvent event) {
        if (event.isError()) {
            fail(event.cause());
            return;
        }
        if (!event.isRecord()) {
            return;  // EOS is sent by close; BARRIER and CANCEL never cross a shuffle, matching LocalChannel semantics
        }
        byte[] payload = serializer.serialize(event.row());
        int wireBytes = FrameCodec.HEADER_BYTES + payload.length;
        lock.lock();
        try {
            if (credit <= 0 && !poisoned && !closed) {
                // About to park, so flush what the batch holds first. The flush happens after the lock is released, since holding it while flushing risks interlocking with credit being added back.
                lock.unlock();
                try {
                    flushChannel();
                } finally {
                    lock.lock();
                }
            }
            while (credit <= 0 && !poisoned && !closed) {
                io.rowweave.sched.Task pipelineTask =
                        io.rowweave.sched.Task.currentPipelineTask();
                if (pipelineTask != null) pipelineTask.beginExternalBackpressure();
                try {
                    hasCredit.awaitUninterruptibly();
                } finally {
                    if (pipelineTask != null) pipelineTask.endExternalBackpressure();
                }
            }
            if (poisoned) {
                throw new ChannelPoisonedException("send on poisoned socket sink (downstream failed/closed)");
            }
            if (closed) {
                throw new IllegalStateException("send on closed socket sink");
            }
            credit -= wireBytes;  // Symmetric with the receiver and mux, so a zero-payload frame still charges the full header, and one row may overshoot into negative
        } finally {
            lock.unlock();
        }
        writeFrame(FRAME_RECORD, payload);
    }

    /**
     * Writes a RECORD frame, in batch mode: on the **caller thread** it encodes `[type][len][payload]` into the
     * staging buffer, and issues **one** writeAndFlush to Netty only once the window's worth of bytes is
     * accumulated. That collapses the per-row cross-thread channel.write (MPSC enqueue plus per-row
     * ChannelOutboundBuffer entry plus per-row listener) into roughly one per window. The byte credit bounds
     * staging at about one window naturally, so it cannot grow without limit.
     */
    private void writeFrame(byte type, byte[] payload) {
        byte[] toFlush = null;
        synchronized (flushLock) {
            appendFrame(type, payload);
            if (stagingLen >= flushByteThreshold) {
                toFlush = drainStaging();
            }
        }
        if (toFlush != null) {
            flushBytes(toFlush, toFlush.length);
        }
    }

    /** Appends {@code [type:1][targetId:16 (uuid most and least significant bits) big-endian][len:4 big-endian][payload]} to staging,
     * doubling the buffer when needed. Holds flushLock. The frame header carries the targetId, the
     * big-endian routing key for multiplexing, matching the byte layout of FrameEncoder and FrameDecoder. */
    private void appendFrame(byte type, byte[] payload) {
        int need = stagingLen + FrameCodec.HEADER_BYTES + payload.length;
        if (need > staging.length) {
            int cap = staging.length;
            while (cap < need) {
                cap <<= 1;
            }
            staging = java.util.Arrays.copyOf(staging, cap);
        }
        java.util.UUID tid = outboundTargetId;
        staging[stagingLen++] = type;
        putLong(tid.getMostSignificantBits());
        putLong(tid.getLeastSignificantBits());
        staging[stagingLen++] = (byte) (payload.length >>> 24);
        staging[stagingLen++] = (byte) (payload.length >>> 16);
        staging[stagingLen++] = (byte) (payload.length >>> 8);
        staging[stagingLen++] = (byte) payload.length;
        System.arraycopy(payload, 0, staging, stagingLen, payload.length);
        stagingLen += payload.length;
    }

    /** Writes one long big-endian into staging, used for the targetId in the frame header. Holds flushLock. */
    private void putLong(long v) {
        staging[stagingLen++] = (byte) (v >>> 56);
        staging[stagingLen++] = (byte) (v >>> 48);
        staging[stagingLen++] = (byte) (v >>> 40);
        staging[stagingLen++] = (byte) (v >>> 32);
        staging[stagingLen++] = (byte) (v >>> 24);
        staging[stagingLen++] = (byte) (v >>> 16);
        staging[stagingLen++] = (byte) (v >>> 8);
        staging[stagingLen++] = (byte) v;
    }

    /** Copies the stagingLen bytes accumulated in staging into a new array and resets it. Holds flushLock. */
    private byte[] drainStaging() {
        byte[] snap = java.util.Arrays.copyOf(staging, stagingLen);
        stagingLen = 0;
        return snap;
    }

    /** Hands the bytes held by the batch to Netty as <b>a single ByteBuf</b> through writeAndFlush, which is one cross-thread handoff and one outbound entry. On failure, poison. */
    private void flushBytes(byte[] bytes, int len) {
        try {
            ByteBuf buf = channel.alloc().buffer(len);
            buf.writeBytes(bytes, 0, len);
            channel.writeAndFlush(buf).addListener(WRITE_LISTENER);
        } catch (Throwable e) {
            poison();
            throw new ChannelPoisonedException("socket write failed: " + e.getMessage());
        }
    }

    /** Listener for a failed write future: when the peer closes the connection the write fails, and ClosedChannelException is checked, so poison. */
    private final io.netty.channel.ChannelFutureListener WRITE_LISTENER = future -> {
        if (!future.isSuccess()) {
            poison();
        }
    };

    /**
     * Writes a control frame, that is EOS, ERROR or KEEPALIVE: first flush whatever RECORDs the batch still
     * holds, keeping FIFO, then write the control frame and flush both together. Exceptions are not thrown,
     * since this is best effort. The returned write future of the control frame is what gracefulClose must
     * <b>await first</b>, so the EOS bytes are really on the wire before shutdownOutput sends the FIN,
     * otherwise the FIN arrives at the peer ahead of the EOS, which loses it and reports connection closed
     * before EOS.
     */
    private io.netty.channel.ChannelFuture writeFrameNoThrow(byte type, byte[] payload) {
        try {
            byte[] pending = null;
            synchronized (flushLock) {
                if (stagingLen > 0) {
                    pending = drainStaging();
                }
            }
            if (pending != null) {
                ByteBuf buf = channel.alloc().buffer(pending.length);
                buf.writeBytes(pending);
                channel.write(buf).addListener(WRITE_LISTENER);  // The RECORDs left in the batch go first, keeping FIFO
            }
            // A control frame, meaning EOS, ERROR or KEEPALIVE, also carries the outboundTargetId, otherwise the server cannot demux it at targetId 0
            // and the EOS never reaches this target's channel, so producerDone is lost and the downstream hangs. It matches the targetId of a RECORD from appendFrame.
            return channel.writeAndFlush(new Frame(type, outboundTargetId, payload));  // The control frame travels through FrameEncoder, and the flush covers everything
        } catch (Throwable ignored) {
            return null;  // The connection is already broken: the peer learns through its own socket exception
        }
    }

    /** Flushes what the batch holds, called before parking: writeAndFlush the bytes in staging and clear it. */
    private void flushChannel() {
        byte[] pending;
        synchronized (flushLock) {
            pending = stagingLen > 0 ? drainStaging() : null;
        }
        if (pending != null) {
            flushBytes(pending, pending.length);
        }
    }

    @Override
    public StreamEvent poll() {
        throw new UnsupportedOperationException("SocketChannelSink is send-only (poll on receiver side)");
    }

    @Override
    public void close() {
        boolean sendEos;
        lock.lock();
        try {
            sendEos = !closed && !poisoned;
            closed = true;
            hasCredit.signalAll();
        } finally {
            lock.unlock();
        }
        if (sendEos) {
            if (claimTestDroppedEos()) {
                LOG.warn("ROWWEAVE_TEST_DROP_EOS holding sender open until cancellation");
                awaitPeerCloseAfterTestDroppedEos();
            } else {
                io.netty.channel.ChannelFuture eosWritten =
                        writeFrameNoThrow(FRAME_EOS, new byte[0]);
                gracefulClose(eosWritten);
            }
        } else {
            closeChannel(null, false);
        }
    }

    private static boolean testDropEosOnceEnabled() {
        if (!Boolean.getBoolean(TEST_DROP_EOS_ONCE_PROPERTY)) {
            return false;
        }
        if (SocketChannelSink.class.getClassLoader()
                .getResource("rowweave-test-faults.enabled") == null) {
            throw new IllegalStateException(
                    TEST_DROP_EOS_ONCE_PROPERTY + " requires the test-only classpath marker");
        }
        return true;
    }

    private static boolean claimTestDroppedEos() {
        if (!TEST_DROP_EOS_ONCE.compareAndSet(true, false)) {
            return false;
        }
        String claimPath = System.getProperty(TEST_DROP_EOS_CLAIM_PROPERTY);
        if (claimPath == null || claimPath.isEmpty()) {
            return true;
        }
        try {
            java.nio.file.Files.createFile(java.nio.file.Paths.get(claimPath));
            return true;
        } catch (java.nio.file.FileAlreadyExistsException alreadyClaimed) {
            return false;
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("test EOS fault claim failed", failure);
        }
    }

    /** Test-only missing-EOS fault: cancellation closes the channel and releases this wait. */
    private void awaitPeerCloseAfterTestDroppedEos() {
        RuntimeException failure = awaitFuture(
                channel.closeFuture(), "test missing-EOS peer close", false);
        closeChannel(failure, true);
    }

    @Override
    public void fail(Throwable cause) {
        Throwable primary = cause == null ? new RuntimeException("socket sink failed") : cause;
        boolean sendError;
        lock.lock();
        try {
            sendError = !closed;
            poisoned = true;
            closed = true;
            hasCredit.signalAll();
        } finally {
            lock.unlock();
        }
        if (sendError) {
            String msg = String.valueOf(primary.getMessage());
            io.netty.channel.ChannelFuture errorWritten =
                    writeFrameNoThrow(FRAME_ERROR,
                            msg.getBytes(StandardCharsets.UTF_8));
            RuntimeException writeFailure = errorWritten == null
                    ? new IllegalStateException(
                    "ERROR write was not scheduled")
                    : awaitFuture(errorWritten, "ERROR write", false);
            if (writeFailure != null && writeFailure != primary) {
                primary.addSuppressed(writeFailure);
            }
        }
        closeChannel(primary, false);
    }

    /**
     * Graceful close: once the EOS has been sent, shutdownOutput sends a FIN rather than closing directly, so
     * the peer drains its unread data and closes after the EOS, with no RST discarding data, after which this
     * side's closeFuture completes and the groups shut down. A 30 second timeout is the backstop, so a wedged
     * peer is not waited on forever.
     */
    private void gracefulClose(io.netty.channel.ChannelFuture eosWritten) {
        RuntimeException failure = null;
        try {
            // Await the write first so the EOS frame bytes are really on the wire, then send the FIN, otherwise the FIN wins and the peer reports closed before EOS, losing it.
            if (eosWritten != null) {
                failure = awaitFuture(eosWritten, "EOS write", false);
            } else {
                failure = new IllegalStateException("EOS write was not scheduled");
            }
            if (channel instanceof SocketChannel && channel.isActive()) {
                // shutdownOutput may return a failed future because the peer already closed the connection after receiving the EOS,
                // which is the normal race on the graceful path, so <b>do not sync and do not throw</b>: it is benign, the EOS arrived and the connection is closing. Await best effort and ignore failure.
                RuntimeException outputFailure = awaitFuture(
                        ((SocketChannel) channel).shutdownOutput(), "shutdownOutput", true);
                failure = merge(failure, outputFailure);
            }
            RuntimeException peerCloseFailure = awaitFuture(channel.closeFuture(), "peer close", false);
            failure = merge(failure, peerCloseFailure);
        } catch (Throwable t) {
            failure = merge(failure, new IllegalStateException("graceful socket close failed", t));
        } finally {
            closeChannel(failure, true);
        }
    }

    private void closeChannel(Throwable primary, boolean reportPrimary) {
        synchronized (shutdownLock) {
            if (!shutdownComplete) {
                shutdownFailure = NettyTransportShutdown.shutdownClient(
                        "SocketChannelSink", channel, group);
                shutdownComplete = true;
            }
            if (shutdownFailure != null) {
                if (primary != null && primary != shutdownFailure) {
                    primary.addSuppressed(shutdownFailure);
                } else if (primary == null) {
                    LOG.warn("SocketChannelSink shutdown incomplete", shutdownFailure);
                }
            }
            if (reportPrimary && primary instanceof RuntimeException) {
                LOG.warn("SocketChannelSink graceful shutdown incomplete", primary);
            }
        }
    }

    /** allowFailedFuture is only for the benign race where the peer closes first during shutdownOutput after the EOS wrote successfully; a timeout is still a failure. */
    private static RuntimeException awaitFuture(io.netty.channel.ChannelFuture future,
                                                String operation,
                                                boolean allowFailedFuture) {
        long timeoutMs = NettyTransportShutdown.timeoutMs();
        if (!future.awaitUninterruptibly(timeoutMs, TimeUnit.MILLISECONDS)) {
            return new IllegalStateException(operation + " timed out after " + timeoutMs + " ms");
        }
        if (!future.isSuccess() && !allowFailedFuture) {
            Throwable cause = future.cause();
            return new IllegalStateException(operation + " failed", cause);
        }
        return null;
    }

    private static RuntimeException merge(RuntimeException first, RuntimeException next) {
        if (next == null) {
            return first;
        }
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }

    /** The credit returned by the receiving side is a <b>bare 4-byte int</b>, not a Frame, so one int is decoded for every 4 bytes accumulated. */
    private static final class CreditIntDecoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            while (in.readableBytes() >= 4) {
                out.add(in.readInt());
            }
        }
    }

    /** Receiving a credit int on the EventLoop: above zero adds it back and signals; zero is keepalive, liveness alone with no credit change; a broken connection or an idle period poisons. */
    private final class CreditHandler extends SimpleChannelInboundHandler<Integer> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Integer granted) {
            if (granted == 0) {
                return;  // Credit-stream keepalive: only resets readerIdle, and leaves credit alone
            }
            lock.lock();
            try {
                credit += granted;
                hasCredit.signalAll();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            poison();  // The receiving side closed the connection, either after a normal EOS or because it died, so poison this side and unpark a send that is parked, which avoids deadlock
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                poison();  // readerIdle: not one byte from the receiving side, keepalive included, so the peer is wedged and this side poisons, which is the equivalent of SO_TIMEOUT
                ctx.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            poison();
            ctx.close();
        }
    }
}
