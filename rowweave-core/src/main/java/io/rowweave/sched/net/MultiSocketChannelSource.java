package io.rowweave.sched.net;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.ScheduledFuture;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.sched.MultiProducerChannel;
import io.rowweave.sched.stream.StreamEvent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The receiving side of an N by M socket shuffle, with the <b>public API and the wire protocol
 * unchanged</b>. The ServerBootstrap accepts <b>M connections</b>, each running a {@link FrameDecoder}
 * plus a {@link ConnHandler} that decodes frames and sends into the <b>shared {@link
 * MultiProducerChannel}</b>, and {@code poll()} takes from the merged queue, which closes only when all
 * M sides have posted EOS and is poisoned by any error.
 *
 * <p><b>Backpressure and credit</b>: the handlers run on their own <b>multi-threaded
 * EventExecutorGroup</b>, since a connection may block in {@code queue.send()}. Per connection, byte
 * credit is accumulated into batches, flushed at half the window or when a read round comes back empty,
 * and sent back as the bare int credit protocol.
 *
 * <p><b>Accept guard</b>: all M connections must be accepted within acceptTimeoutMs, otherwise the queue
 * fails rather than hanging. <b>Poison</b>: a connection that fails, or disconnects before EOS, fails the
 * queue and closes every connection, so each producer's credit thread sees the break and is poisoned.
 * <b>Keepalive</b>: each connection is sent a zero credit periodically, scheduled off the I/O thread.
 */
public final class MultiSocketChannelSource implements io.rowweave.sched.Channel, AutoCloseable {

    private static final int DEFAULT_ACCEPT_TIMEOUT_MS = 30_000;
    private static final Logger LOG = LoggerFactory.getLogger(MultiSocketChannelSource.class);

    private final RowSerializer serializer;
    private final MultiProducerChannel queue;
    private final int producerCount;  // Number of socket connections, which drives the accept guard and the handler threads
    private final int keepaliveMs;
    private final long creditBatch;

    private final EventLoopGroup bossGroup;  // Null when purely local, since nothing is bound
    private final EventLoopGroup workerGroup;
    private final EventExecutorGroup handlerGroup;  // Multi-threaded: the handler of each connection may block in queue.send()
    private final Channel serverChannel;  // Null when purely local
    private final ChannelGroup childChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
    private final Queue<Future<Void>> childTeardownBarriers = new ConcurrentLinkedQueue<>();
    private final Object shutdownLock = new Object();
    private boolean shutdownComplete;
    private RuntimeException shutdownFailure;
    private final AtomicInteger acceptedCount = new AtomicInteger();
    private final ScheduledFuture<?> acceptGuard;
    private volatile boolean stopped;

    public MultiSocketChannelSource(DataSchema dataSchema, int producerCount, int capacity) {
        this(dataSchema, producerCount, capacity, DEFAULT_ACCEPT_TIMEOUT_MS);
    }

    public MultiSocketChannelSource(DataSchema dataSchema, int producerCount, int capacity, int acceptTimeoutMs) {
        this(dataSchema, producerCount, capacity, acceptTimeoutMs,
                SocketChannelSink.KEEPALIVE_DISABLED, SocketChannelSink.KEEPALIVE_DISABLED);
    }

    public MultiSocketChannelSource(DataSchema dataSchema, int producerCount, int capacity, int acceptTimeoutMs,
                                    int keepaliveMs, int soTimeoutMs) {
        this(dataSchema, producerCount, 0, capacity, acceptTimeoutMs, keepaliveMs, soTimeoutMs);
    }

    /**
     * The mixed fan-in constructor: {@code socketProducers} socket connections plus {@code
     * localProducers} in-process local producers, merged into one {@link MultiProducerChannel}, whose
     * producerCount is the sum and which holds the single EOS latch. A local producer takes the channel
     * through {@link #sharedChannel()} and sends directly, with zero wire bytes. With {@code
     * socketProducers == 0} the shuffle is purely local, so <b>no ServerSocket is bound and no Netty is
     * started</b>, and {@link #port()} throws.
     */
    public MultiSocketChannelSource(DataSchema dataSchema, int socketProducers, int localProducers, int capacity,
                                    int acceptTimeoutMs, int keepaliveMs, int soTimeoutMs) {
        this(dataSchema, socketProducers, localProducers, capacity, acceptTimeoutMs, keepaliveMs, soTimeoutMs, 0);
    }

    /**
     * {@code preferredPort} is the port the master allocated in advance, where zero means an OS-chosen
     * port. When the given port fails to bind, because it is taken, in TIME_WAIT, or held by an external
     * process, the bind <b>falls back to zero</b> and lets the OS choose, without failing the job, and
     * {@link #port()} always reports the port actually bound, which is what the PORT_REPORT fallback
     * uploads when the preferred bind failed. Preallocating ports is an optimization, so a conflict
     * degrades automatically and the rollout carries no regression.
     */
    public MultiSocketChannelSource(DataSchema dataSchema, int socketProducers, int localProducers, int capacity,
                                    int acceptTimeoutMs, int keepaliveMs, int soTimeoutMs, int preferredPort) {
        this(dataSchema, socketProducers, localProducers, capacity, acceptTimeoutMs,
                keepaliveMs, soTimeoutMs, preferredPort, false);
    }

    /**
     * The mux socket is accepted by WorkerDataServer, so although this object binds nothing, the merged
     * channel must still bound bytes by the wire window.
     */
    public static MultiSocketChannelSource muxInbound(DataSchema dataSchema, int totalProducers, int capacity,
                                                       int acceptTimeoutMs, int keepaliveMs, int soTimeoutMs) {
        return new MultiSocketChannelSource(dataSchema, 0, totalProducers, capacity, acceptTimeoutMs,
                keepaliveMs, soTimeoutMs, 0, true);
    }

    private MultiSocketChannelSource(DataSchema dataSchema, int socketProducers, int localProducers, int capacity,
                                     int acceptTimeoutMs, int keepaliveMs, int soTimeoutMs, int preferredPort,
                                     boolean muxRemoteByteBound) {
        if (socketProducers < 0 || localProducers < 0) {
            throw new IllegalArgumentException("producer counts must be >= 0");
        }
        int total = socketProducers + localProducers;
        if (total <= 0) {
            throw new IllegalArgumentException("socketProducers + localProducers must be > 0");
        }
        if (acceptTimeoutMs <= 0) {
            throw new IllegalArgumentException("acceptTimeoutMs must be > 0");
        }
        this.serializer = new RowSerializer(dataSchema);
        this.producerCount = socketProducers;
        long windowBytes = capacity;
        int maxRows = (int) Math.min(1 << 20, Math.max(1024, windowBytes / 16));
        // A purely local inbound (no socket producer) uses **row-count backpressure only** (maxBytes=0),
        // matching PushDagExecutor's LocalChannel: with no socket the wire byte count means nothing, so the byte
        // charge is dropped. A mixed setup (with sockets) keeps the byte bound, because sockets need it.
        long maxBytes = (socketProducers == 0 && !muxRemoteByteBound) ? 0L : windowBytes;
        this.queue = new MultiProducerChannel(maxRows, total, maxBytes);  // The merged latch is socket plus local producers
        this.keepaliveMs = keepaliveMs;
        this.creditBatch = Math.max(1, windowBytes / 2);

        if (socketProducers == 0) {
            // Purely local: no ServerSocket bound, no Netty group started, and no accept guard, because LocalShuffleRegistry awaits the local edges.
            this.bossGroup = null;
            this.workerGroup = null;
            this.handlerGroup = null;
            this.serverChannel = null;
            this.acceptGuard = null;
            return;
        }

        this.bossGroup = new NioEventLoopGroup(1, new NamedDaemonThreadFactory("rowweave-msrc-boss"));
        this.workerGroup = new NioEventLoopGroup(1, new NamedDaemonThreadFactory("rowweave-msrc-io"));
        // One handler thread per connection, which may block in queue.send; the thread count is socketProducers, at least one.
        this.handlerGroup = new DefaultEventExecutorGroup(
                Math.max(1, socketProducers), new NamedDaemonThreadFactory("rowweave-msrc-handler"));
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            childTeardownBarriers.add(NettyTransportShutdown.installTeardownBarrier(ch));
                            childChannels.add(ch);
                            acceptedCount.incrementAndGet();
                            ch.pipeline().addLast(new FrameDecoder());
                            ch.pipeline().addLast(handlerGroup, new ConnHandler(ch));
                            startKeepalive(ch);
                        }
                    });
            this.serverChannel = bindWithFallback(b, preferredPort);
        } catch (RuntimeException e) {
            RuntimeException cleanup = shutdownGroups();
            if (cleanup != null) {
                e.addSuppressed(cleanup);
            }
            throw new RuntimeException("failed to bind multi shuffle source", e);
        }
        // Accept guard: fail, rather than hang, if socketProducers have not all arrived within the timeout. Scheduled on the boss loop, so it does not block.
        this.acceptGuard = bossGroup.next().schedule(() -> {
            if (!stopped && acceptedCount.get() < producerCount) {
                queue.fail(new IOException("only " + acceptedCount.get() + "/" + producerCount
                        + " producers connected within timeout (accept guard, not hang)"));
            }
        }, acceptTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * The shared merged channel the downstream inbound reads, offered so a <b>local producer in the same
     * worker</b>, that is {@link io.rowweave.sched.LocalChannelSink}, can send directly with zero wire
     * bytes. The socket-side handlers write to it as well, and producerDone and fail converge at this one
     * point.
     */
    public MultiProducerChannel sharedChannel() {
        return queue;
    }

    /**
     * Tries to bind the preferred port first; when that fails, because it is taken, in TIME_WAIT, or held
     * by another process, Netty throws a wrapped {@code BindException} and the bind <b>falls back to
     * zero</b> for an OS-chosen port. A preferred port of zero binds zero straight away. The fallback does
     * not fail the job, since port preallocation is only an optimization, and on conflict the run degrades
     * to an OS-chosen port reported through PORT_REPORT.
     */
    private io.netty.channel.Channel bindWithFallback(ServerBootstrap b, int preferredPort) {
        // Bind to the <b>wildcard 0.0.0.0</b>, listening on every interface rather than only 127.0.0.1:
        // the advertised host of a worker is the specific interface IP that was probed, so when an upstream
        // connects to that IP the downstream must be listening on that interface. A wildcard bind covers the
        // probed IP and the loopback alike, so either address works for the upstream.
        // The same approach as Flink and Spark: bind every interface, advertise one specific address.
        if (preferredPort > 0) {
            try {
                return b.bind(new InetSocketAddress(preferredPort)).syncUninterruptibly().channel();
            } catch (Throwable conflict) {
                // The preferred port is taken: BindException is rethrown as it is by the Netty syncUninterruptibly, and is not a RuntimeException,
                // so fall back to an OS-chosen port by binding zero. The caller then reports the real value of port() through the PORT_REPORT fallback.
            }
        }
        return b.bind(new InetSocketAddress(0)).syncUninterruptibly().channel();
    }

    public int port() {
        if (serverChannel == null) {
            throw new IllegalStateException("pure-local MultiSocketChannelSource has no socket port");
        }
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @Override
    public StreamEvent poll() throws InterruptedException {
        return queue.poll();
    }

    @Override
    public void send(StreamEvent event) {
        throw new UnsupportedOperationException("MultiSocketChannelSource is poll-only (send on sender side)");
    }

    @Override
    public void close() {
        stopped = true;
        queue.close();
        closeAll(null);
    }

    @Override
    public void fail(Throwable cause) {
        stopped = true;
        Throwable primary = cause == null ? new RuntimeException("multi shuffle source failed") : cause;
        queue.fail(primary);  // Poison the merged queue, so the downstream polls the error
        closeAll(primary);  // Close all M connections, so each producer credit thread sees the break and is unparked by the poison
    }

    private void closeAll(Throwable primary) {
        if (acceptGuard != null) {
            acceptGuard.cancel(false);
        }
        if (serverChannel == null) {
            return;  // Purely local: there are no socket resources
        }
        synchronized (shutdownLock) {
            if (!shutdownComplete) {
                shutdownFailure = NettyTransportShutdown.shutdownServer(
                        "MultiSocketChannelSource", serverChannel, childChannels, childTeardownBarriers,
                        bossGroup, workerGroup, handlerGroup);
                shutdownComplete = true;
            }
            if (shutdownFailure != null) {
                if (primary != null && primary != shutdownFailure) {
                    primary.addSuppressed(shutdownFailure);
                } else if (primary == null) {
                    LOG.warn("MultiSocketChannelSource shutdown incomplete", shutdownFailure);
                }
            }
        }
    }

    private RuntimeException shutdownGroups() {
        return NettyTransportShutdown.shutdownGroups(
                "MultiSocketChannelSource constructor", bossGroup, workerGroup, handlerGroup);
    }

    private void startKeepalive(Channel ch) {
        if (keepaliveMs > 0) {
            ch.eventLoop().scheduleAtFixedRate(() -> {
                if (!stopped && ch.isActive()) {
                    grantCredit(ch, 0);
                }
            }, keepaliveMs, keepaliveMs, TimeUnit.MILLISECONDS);
        }
    }

    /** Sends a bare int credit back to the given connection, pairing with the credit reader in {@code SocketChannelSink}; zero means keepalive. */
    private static void grantCredit(Channel ch, int n) {
        if (ch == null || n < 0 || !ch.isActive()) {
            return;
        }
        ByteBuf buf = ch.alloc().buffer(4);
        buf.writeInt(n);
        ch.writeAndFlush(buf);
    }

    /**
     * The frame consumer for one connection, on its own thread: a RECORD goes to queue.send, which may
     * block, and accumulates the per-connection byte credit; an EOS grants what remains, reports
     * producerDone and closes the connection with a FIN; an ERROR, a disconnect before EOS, or any
     * exception fails the queue and poisons it for everyone.
     */
    private final class ConnHandler extends SimpleChannelInboundHandler<Frame> {
        private final Channel ch;
        private long pending;
        private boolean done;  // Already EOS or ERROR, so channelInactive no longer reads it as a disconnect before EOS

        ConnHandler(Channel ch) {
            this.ch = ch;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            byte type = frame.type();
            byte[] payload = frame.payload();
            switch (type) {
                case FrameCodec.FRAME_RECORD: {
                    DataRow row = serializer.deserialize(payload);
                    int frameBytes = FrameCodec.HEADER_BYTES + payload.length;
                    queue.send(StreamEvent.record(row), frameBytes);  // A full queue parks this handler thread, which stops granting, which stops the producer: the backpressure
                    pending += frameBytes;
                    if (pending >= creditBatch) {
                        grantCredit(ch, (int) pending);
                        pending = 0;
                    }
                    break;
                }
                case FrameCodec.FRAME_EOS:
                    if (pending > 0) {
                        grantCredit(ch, (int) pending);
                        pending = 0;
                    }
                    done = true;
                    queue.producerDone();  // The merged EOS is posted only once all M sides are done, which is the close latch
                    ctx.close();  // Once the data and the EOS have been read, close the connection with a FIN, so the sender shuts down gracefully with no RST
                    break;
                case FrameCodec.FRAME_ERROR:
                    done = true;
                    queue.fail(new RuntimeException(
                            "upstream shuffle error: " + new String(payload, StandardCharsets.UTF_8)));
                    ctx.close();
                    break;
                case FrameCodec.FRAME_KEEPALIVE:
                    break;  // Liveness only: nothing is enqueued and no credit is returned
                default:
                    queue.fail(new IOException("unknown frame type " + type));
                    ctx.close();
            }
        }

        /** The read round came back empty, so flush-on-empty returns the remaining credit, which keeps the producer from starving. */
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            if (pending > 0) {
                grantCredit(ch, (int) pending);
                pending = 0;
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (!done && !stopped) {
                queue.fail(new IOException("connection closed before EOS"));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (!stopped) {
                queue.fail(cause);
            }
            ctx.close();
        }
    }
}
