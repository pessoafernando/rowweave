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

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.sched.LocalChannel;
import io.rowweave.sched.stream.StreamEvent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The receiving side of a socket-backed Channel, with the <b>public API and the wire protocol
 * unchanged</b>. A ServerBootstrap accepts one connection, {@link FrameDecoder} decodes the frames and
 * {@link SourceHandler} feeds them into an internal {@link LocalChannel}.
 *
 * <p><b>Backpressure</b>: the primary form is <b>application-level byte credit</b>, which parks the
 * sender within a window of bytes. SourceHandler runs on <b>its own single-threaded EventExecutor</b>,
 * not the I/O EventLoop, so it <b>may block</b> in {@code queue.send()} when the queue is full. The no-OOM
 * guarantee follows from that: the byte credit keeps the sender at most about one window ahead, so the
 * unprocessed bytes an I/O thread has read in stay within one window, and a blocked handler cannot let them
 * pile up without bound.
 *
 * <p><b>Returning credit</b>: each frame processed accumulates byte credit, and when it reaches half the
 * window, or at {@code channelReadComplete} when the read round came back empty, it is sent back as a
 * <b>bare int</b>, which pairs with the credit reader in {@code SocketChannelSink}.
 *
 * <p><b>Poison and graceful close</b>: an ERROR frame, a connection closed before EOS, or a decode failure
 * all go to {@code queue.fail}. An EOS grants what credit remains, closes the queue and closes the
 * connection with a FIN. A keepalive, which is a zero-credit bare int, keeps the sender alive; this side
 * installs no reader read timeout, as initChannel explains.
 */
public final class SocketChannelSource implements io.rowweave.sched.Channel, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SocketChannelSource.class);

    private final RowSerializer serializer;
    private final LocalChannel queue;
    private final long creditBatch;
    private final int keepaliveMs;
    private final int soTimeoutMs;

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final EventExecutorGroup handlerGroup;  // Its own single thread, so the handler may block in queue.send()
    private final Channel serverChannel;
    private final ChannelGroup childChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
    private final Queue<Future<Void>> childTeardownBarriers = new ConcurrentLinkedQueue<>();
    private final Object shutdownLock = new Object();
    private boolean shutdownComplete;
    private RuntimeException shutdownFailure;
    private volatile Channel conn;
    private volatile boolean stopped;

    /** The older constructor: no keepalive and no SO_TIMEOUT, trusting RST. */
    public SocketChannelSource(DataSchema dataSchema, int window) {
        this(dataSchema, window, SocketChannelSink.KEEPALIVE_DISABLED, SocketChannelSink.KEEPALIVE_DISABLED);
    }

    /**
     * A positive keepaliveMs enables the backstop for a wedged peer: every keepaliveMs a zero credit is
     * sent back, which keeps the sender's credit loop alive. No read timeout is installed on this side,
     * because it would fire spuriously under the blocking-handler model; a wedged sender is caught by its
     * FIN or RST reaching channelInactive, and by the credit-read timeout on the sender side. The keepalive
     * is switched by keepaliveMs alone.
     */
    public SocketChannelSource(DataSchema dataSchema, int window, int keepaliveMs, int soTimeoutMs) {
        if (window <= 0) {
            throw new IllegalArgumentException("window must be > 0");
        }
        this.serializer = new RowSerializer(dataSchema);
        int maxRows = (int) Math.min(1 << 20, Math.max(1, (long) window / 16));
        this.queue = new LocalChannel(maxRows);
        this.creditBatch = Math.max(1, (long) window / 2);
        this.keepaliveMs = keepaliveMs;
        this.soTimeoutMs = soTimeoutMs;

        this.bossGroup = new NioEventLoopGroup(1, new NamedDaemonThreadFactory("rowweave-src-boss"));
        this.workerGroup = new NioEventLoopGroup(1, new NamedDaemonThreadFactory("rowweave-src-io"));
        this.handlerGroup = new DefaultEventExecutorGroup(1, new NamedDaemonThreadFactory("rowweave-src-handler"));
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT)  // Unpooled, which puts no-OOM first and keeps the lifecycle simple
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            childTeardownBarriers.add(NettyTransportShutdown.installTeardownBarrier(ch));
                            childChannels.add(ch);
                            conn = ch;
                            // Note: <b>no readerIdle timeout is set on the receiver</b>. Under the blocking-handler model, when
                            // backpressure is slow the handler parks in queue.send(), Netty suspends auto-read, and the I/O
                            // loop reads no bytes at all, even though the sender keepalive has reached the socket buffer, so a
                            // readerIdle would <b>fire in error</b>. A wedged sender is detected instead by the sender dying,
                            // or its FIN or RST arriving, which reaches channelInactive, and by the symmetric backstop of the
                            // sender's own creditIn SO_TIMEOUT in SocketChannelSink. This side installs no read timeout from
                            // soTimeoutMs and takes no part in keepalive scheduling, which keepaliveMs decides.
                            ch.pipeline().addLast(new FrameDecoder());
                            // The handler runs on its own single-threaded executor, so it may block in queue.send() without blocking the I/O EventLoop.
                            ch.pipeline().addLast(handlerGroup, new SourceHandler());
                            startKeepaliveIfEnabled();
                        }
                    });
            // Bind the wildcard 0.0.0.0, listening on every interface, so an upstream reaching the worker's advertised IP gets through, matching MultiSocketChannelSource.
            this.serverChannel = b.bind(new InetSocketAddress(0)).syncUninterruptibly().channel();
        } catch (RuntimeException e) {
            RuntimeException cleanup = shutdownGroups();
            if (cleanup != null) {
                e.addSuppressed(cleanup);
            }
            throw new RuntimeException("failed to bind shuffle source", e);
        }
    }

    /** The port this receiving side listens on, which is what the sender connects to. */
    public int port() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @Override
    public StreamEvent poll() throws InterruptedException {
        return queue.poll();
    }

    @Override
    public void send(StreamEvent event) {
        throw new UnsupportedOperationException("SocketChannelSource is poll-only (send on sender side)");
    }

    @Override
    public void close() {
        stopped = true;
        queue.close();
        closeChannels(null);
    }

    @Override
    public void fail(Throwable cause) {
        stopped = true;
        Throwable primary = cause == null ? new RuntimeException("shuffle source failed") : cause;
        queue.fail(primary);  // Poison the internal queue, so the downstream polls the error
        closeChannels(primary);  // Close the connection, so the sender credit thread sees the break, is poisoned and unparked
    }

    private void closeChannels(Throwable primary) {
        synchronized (shutdownLock) {
            if (!shutdownComplete) {
                shutdownFailure = NettyTransportShutdown.shutdownServer(
                        "SocketChannelSource", serverChannel, childChannels, childTeardownBarriers,
                        bossGroup, workerGroup, handlerGroup);
                shutdownComplete = true;
            }
            if (shutdownFailure != null) {
                if (primary != null && primary != shutdownFailure) {
                    primary.addSuppressed(shutdownFailure);
                } else if (primary == null) {
                    LOG.warn("SocketChannelSource shutdown incomplete", shutdownFailure);
                }
            }
        }
    }

    private RuntimeException shutdownGroups() {
        return NettyTransportShutdown.shutdownGroups(
                "SocketChannelSource constructor", bossGroup, workerGroup, handlerGroup);
    }

    /** Sends a bare int credit back, pairing with the credit reader in {@code SocketChannelSink}; zero is a keepalive, which is liveness only and adds no credit on the sender. */
    private void grantCredit(int n) {
        Channel c = conn;
        if (c == null || n < 0 || !c.isActive()) {
            return;
        }
        ByteBuf buf = c.alloc().buffer(4);
        buf.writeInt(n);
        c.writeAndFlush(buf);  // A bare int, not a Frame, since FrameEncoder is not in this pipeline
    }

    /**
     * The frame consumer, on its own single-threaded executor: a RECORD goes to queue.send, which may
     * block, and accumulates byte credit; an EOS grants the remaining credit and closes; an ERROR, a
     * disconnect before EOS, or an exception fails the queue. channelReadComplete grants what remains,
     * which is flush-on-empty and keeps the sender from starving.
     */
    private final class SourceHandler extends SimpleChannelInboundHandler<Frame> {
        private long pendingCredit;
        private boolean sawEos;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            byte type = frame.type();
            byte[] payload = frame.payload();
            switch (type) {
                case FrameCodec.FRAME_RECORD: {
                    DataRow row = serializer.deserialize(payload);
                    queue.send(StreamEvent.record(row));  // A full queue parks this handler thread, which is not the I/O thread, so no further credit is granted and the sender's credit runs out and parks
                    pendingCredit += FrameCodec.HEADER_BYTES + payload.length;
                    if (pendingCredit >= creditBatch) {
                        grantCredit((int) pendingCredit);
                        pendingCredit = 0;
                    }
                    break;
                }
                case FrameCodec.FRAME_EOS:
                    if (pendingCredit > 0) {
                        grantCredit((int) pendingCredit);
                        pendingCredit = 0;
                    }
                    sawEos = true;
                    queue.close();
                    ctx.close();  // Closing the connection with a FIN after the data and EOS, so the sender creditThread sees EOF and shuts down gracefully with no RST
                    break;
                case FrameCodec.FRAME_ERROR:
                    sawEos = true;  // Terminated explicitly, so channelInactive no longer reads it as a disconnect before EOS
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

        /** The read round came back empty, so flush-on-empty returns the remaining credit, which keeps things working when the window is under twice the largest row. */
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            if (pendingCredit > 0) {
                grantCredit((int) pendingCredit);
                pendingCredit = 0;
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (!sawEos && !stopped) {
                // The connection closed without an EOS, meaning the sender died, so poison the queue rather than treating it as a normal end of input.
                queue.fail(new IOException("connection closed before EOS"));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (!stopped) {
                queue.fail(cause instanceof Exception
                        ? new IOException("shuffle source read failed: " + cause.getMessage(), cause)
                        : new RuntimeException(cause));
            }
            ctx.close();
        }
    }

    /**
     * Keepalive: every keepaliveMs a zero credit is sent back, which keeps the sender's credit loop
     * alive. <b>It must be scheduled on the non-blocking I/O workerGroup</b>, whose EventLoop never blocks,
     * and never on the handlerGroup, whose single thread parks in queue.send() when backpressure is slow,
     * so no keepalive would go out and the sender would poison itself on a creditIn SO_TIMEOUT in error.
     * The earlier implementation used a dedicated keepalive thread, which for the same reason must not
     * share a lifetime with the blocking reader thread.
     */
    private void startKeepaliveIfEnabled() {
        if (keepaliveMs > 0) {
            workerGroup.next().scheduleAtFixedRate(() -> {
                if (!stopped) {
                    grantCredit(0);
                }
            }, keepaliveMs, keepaliveMs, TimeUnit.MILLISECONDS);
        }
    }
}
