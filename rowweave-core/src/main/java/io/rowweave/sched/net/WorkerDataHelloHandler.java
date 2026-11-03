package io.rowweave.sched.net;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import io.rowweave.cluster.rpc.DataPlaneSecurityContext;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TLS-internal bounded Worker hello. It never forwards application bytes until both the peer
 * identity and the TLS session have been verified.
 */
final class WorkerDataHelloHandler extends ChannelInboundHandlerAdapter {

    interface PeerVerifier {
        void verify(WorkerDataIdentity peer) throws Exception;
    }

    interface PipelineInstaller {
        void install(ChannelHandlerContext context, WorkerDataIdentity peer) throws Exception;
    }

    interface Outcome {
        void complete(WorkerDataIdentity peer);
        void fail(Throwable failure);
    }

    private final DataPlaneSecurityContext.AdmittedTlsChannel tls;
    private final byte[] localHello;
    private final long absoluteDeadlineNanos;
    private final PeerVerifier verifier;
    private final PipelineInstaller installer;
    private final Outcome outcome;
    private final byte[] peerHello = new byte[WorkerDataHello.MAX_FRAME_BYTES];
    private final AtomicBoolean terminal = new AtomicBoolean();
    private int received;
    private int expectedLength = -1;
    private boolean tlsComplete;
    private ScheduledFuture<?> timeout;

    WorkerDataHelloHandler(DataPlaneSecurityContext.AdmittedTlsChannel tls,
                           WorkerDataIdentity localIdentity,
                           long absoluteDeadlineNanos,
                           PeerVerifier verifier,
                           PipelineInstaller installer,
                           Outcome outcome) {
        if (tls == null || localIdentity == null || verifier == null
                || installer == null || outcome == null) {
            throw new IllegalArgumentException(
                    "TLS channel, Worker identities and hello callbacks are required");
        }
        this.tls = tls;
        this.localHello = WorkerDataHello.encode(localIdentity);
        this.absoluteDeadlineNanos = absoluteDeadlineNanos;
        this.verifier = verifier;
        this.installer = installer;
        this.outcome = outcome;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext context) {
        long remaining = absoluteDeadlineNanos - System.nanoTime();
        if (remaining <= 0L) {
            fail(context, new IllegalStateException("Worker hello deadline expired"));
            return;
        }
        timeout = context.executor().schedule(
                () -> fail(context, new IllegalStateException("Worker hello deadline expired")),
                remaining, TimeUnit.NANOSECONDS);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext context, Object event)
            throws Exception {
        if (event instanceof SslHandshakeCompletionEvent) {
            SslHandshakeCompletionEvent handshake = (SslHandshakeCompletionEvent) event;
            if (!handshake.isSuccess()) {
                fail(context, handshake.cause() == null
                        ? new IllegalStateException("data-plane TLS handshake failed")
                        : handshake.cause());
                return;
            }
            tlsComplete = true;
            context.writeAndFlush(Unpooled.wrappedBuffer(localHello));
        }
        context.fireUserEventTriggered(event);
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
        if (!(message instanceof ByteBuf)) {
            ReferenceCountUtil.release(message);
            fail(context, new IllegalArgumentException(
                    "Worker hello received an invalid plaintext message"));
            return;
        }
        ByteBuf input = (ByteBuf) message;
        try {
            if (!tlsComplete) {
                fail(context, new IllegalStateException(
                        "Worker hello arrived before TLS handshake completion"));
                return;
            }
            while (input.isReadable() && !terminal.get()) {
                int target = expectedLength < 0
                        ? WorkerDataHello.PREFIX_BYTES : expectedLength;
                int copy = Math.min(input.readableBytes(), target - received);
                if (copy > 0) {
                    input.readBytes(peerHello, received, copy);
                    received += copy;
                }
                if (expectedLength < 0 && received == WorkerDataHello.PREFIX_BYTES) {
                    expectedLength = WorkerDataHello.declaredLength(peerHello);
                }
                if (expectedLength > 0 && received == expectedLength) {
                    WorkerDataIdentity peer = WorkerDataHello.decode(
                            java.util.Arrays.copyOf(peerHello, expectedLength));
                    verifier.verify(peer);
                    installer.install(context, peer);
                    tls.releaseAfterAuthentication();
                    if (!terminal.compareAndSet(false, true)) return;
                    cancelTimeout();
                    outcome.complete(peer);
                    if (input.isReadable()) {
                        // Data handlers were installed immediately after this handler.
                        context.fireChannelRead(input.readRetainedSlice(input.readableBytes()));
                    }
                    context.pipeline().remove(this);
                }
            }
        } catch (Throwable failure) {
            fail(context, failure);
        } finally {
            input.release();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        if (!terminal.get()) {
            fail(context, new IllegalStateException(
                    "data-plane connection closed before Worker hello completed"));
        }
        super.channelInactive(context);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext context) throws Exception {
        cancelTimeout();
        if (!terminal.get() && context.channel().isOpen()) {
            fail(context, new IllegalStateException(
                    "Worker hello handler removed before authentication"));
        }
        super.handlerRemoved(context);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        fail(context, cause);
    }

    private void fail(ChannelHandlerContext context, Throwable failure) {
        if (terminal.compareAndSet(false, true)) {
            cancelTimeout();
            outcome.fail(failure == null
                    ? new IllegalStateException("Worker hello failed") : failure);
        }
        context.close();
    }

    private void cancelTimeout() {
        ScheduledFuture<?> scheduled = timeout;
        if (scheduled != null) scheduled.cancel(false);
    }
}
