package io.rowweave.cluster.rpc;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.rowweave.cluster.ProcessTlsEnvelope;
import io.rowweave.sched.net.DataPlaneHandshakeAdmission;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Read-only data plane TLS capability. It creates only a constrained JDK {@link SSLEngine} and
 * Netty {@link SslHandler}, and never exposes an SSLContext, a key, a password or a key or
 * trust manager.
 */
public final class DataPlaneSecurityContext {

    public static final int TLS_READ_BYTES = 16 * 1024;
    public static final int WRITE_LOW_WATER_MARK_BYTES = 32 * 1024;
    public static final int WRITE_HIGH_WATER_MARK_BYTES = 64 * 1024;

    private final ControlTlsContext tlsContext;
    private final long handshakeTimeoutMillis;
    private final byte[] localCertificateFingerprint;
    private final String deploymentSetId;
    private final String authzSetId;
    private final DataPlaneHandshakeAdmission admission;
    private final UnpooledByteBufAllocator dataPlaneAllocator;
    private final Runnable runtimeEnvelopeValidator;

    DataPlaneSecurityContext(ControlTlsContext tlsContext,
                             RpcSecurityMetadata securityMetadata,
                             long handshakeTimeoutMillis,
                             DataPlaneHandshakeAdmission admission) {
        this(tlsContext, securityMetadata, handshakeTimeoutMillis, admission, null);
    }

    DataPlaneSecurityContext(ControlTlsContext tlsContext,
                             RpcSecurityMetadata securityMetadata,
                             long handshakeTimeoutMillis,
                             DataPlaneHandshakeAdmission admission,
                             ProcessTlsEnvelope processTlsEnvelope) {
        if (tlsContext == null) {
            throw new IllegalArgumentException("data-plane TLS context is required");
        }
        if (securityMetadata == null) {
            throw new IllegalArgumentException("data-plane security metadata is required");
        }
        if (handshakeTimeoutMillis < 1_000L || handshakeTimeoutMillis > 60_000L) {
            throw new IllegalArgumentException(
                    "data-plane TLS handshake timeout must be in [1000,60000]");
        }
        if (admission == null) {
            throw new IllegalArgumentException(
                    "data-plane handshake admission is required");
        }
        EnumSet<ControlRole> localRoles = tlsContext.localRoles();
        if (!localRoles.contains(ControlRole.WORKER)) {
            throw new IllegalStateException(
                    "data-plane TLS local certificate requires WORKER role");
        }
        this.tlsContext = tlsContext;
        this.handshakeTimeoutMillis = handshakeTimeoutMillis;
        this.localCertificateFingerprint = decodeFingerprint(tlsContext.localFingerprint());
        this.deploymentSetId = securityMetadata.deploymentSetId();
        this.authzSetId = securityMetadata.authzSetId();
        this.admission = admission;
        this.runtimeEnvelopeValidator = processTlsEnvelope == null
                ? () -> { }
                : processTlsEnvelope::requireCurrentProcessMatch;
        this.runtimeEnvelopeValidator.run();
        // Heap preferred, leak wrapping off, and no cleaner-free direct buffers: the Netty default allocator property is not inherited.
        this.dataPlaneAllocator = new UnpooledByteBufAllocator(false, true, false);
    }

    public AdmittedTlsChannel installServerHandler(
            ChannelPipeline pipeline, DataPlaneHandshakeAdmission.Lease lease) {
        return installServerHandlerWithTimeout(
                pipeline, lease, handshakeTimeoutMillis);
    }

    public AdmittedTlsChannel installServerHandler(
            ChannelPipeline pipeline, DataPlaneHandshakeAdmission.Lease lease,
            long absoluteDeadlineNanos) {
        return installServerHandlerWithTimeout(
                pipeline, lease, remainingHandshakeMillis(absoluteDeadlineNanos));
    }

    private AdmittedTlsChannel installServerHandlerWithTimeout(
            ChannelPipeline pipeline, DataPlaneHandshakeAdmission.Lease lease,
            long timeoutMillis) {
        DataPlaneHandshakeAdmission.HandlerLease handlerLease = admission.claimForHandler(
                lease, DataPlaneHandshakeAdmission.Direction.INBOUND);
        try {
            runtimeEnvelopeValidator.run();
            SSLEngine engine = tlsContext.sslContext().createSSLEngine();
            engine.setUseClientMode(false);
            engine.setEnabledProtocols(ControlTlsContext.enabledDataPlaneProtocols(
                    engine.getSupportedProtocols()));
            engine.setEnabledCipherSuites(ControlTlsContext.enabledCiphers(
                    engine.getSupportedCipherSuites()));
            engine.setNeedClientAuth(true);
            return install(pipeline, handler(engine, handlerLease, timeoutMillis));
        } catch (RuntimeException | Error failure) {
            handlerLease.close();
            throw failure;
        }
    }

    /** Fixes the channel read chunk and the write backpressure watermark before the SslHandler is installed. */
    public void configureChannel(ChannelConfig channelConfig) {
        if (channelConfig == null) {
            throw new IllegalArgumentException("data-plane channel config is required");
        }
        runtimeEnvelopeValidator.run();
        channelConfig.setAllocator(dataPlaneAllocator);
        channelConfig.setRecvByteBufAllocator(
                new FixedRecvByteBufAllocator(TLS_READ_BYTES));
        channelConfig.setWriteBufferWaterMark(new WriteBufferWaterMark(
                WRITE_LOW_WATER_MARK_BYTES, WRITE_HIGH_WATER_MARK_BYTES));
    }

    public AdmittedTlsChannel installClientHandler(
            ChannelPipeline pipeline, String peerHost, int peerPort,
            DataPlaneHandshakeAdmission.Lease lease) {
        return installClientHandlerWithTimeout(
                pipeline, peerHost, peerPort, lease, handshakeTimeoutMillis);
    }

    public AdmittedTlsChannel installClientHandler(
            ChannelPipeline pipeline, String peerHost, int peerPort,
            DataPlaneHandshakeAdmission.Lease lease,
            long absoluteDeadlineNanos) {
        return installClientHandlerWithTimeout(pipeline, peerHost, peerPort, lease,
                remainingHandshakeMillis(absoluteDeadlineNanos));
    }

    private AdmittedTlsChannel installClientHandlerWithTimeout(
            ChannelPipeline pipeline, String peerHost, int peerPort,
            DataPlaneHandshakeAdmission.Lease lease, long timeoutMillis) {
        String host = peerHost == null ? null : peerHost.trim();
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("data-plane TLS peer host is required");
        }
        if (peerPort < 1 || peerPort > 65_535) {
            throw new IllegalArgumentException("data-plane TLS peer port is invalid");
        }
        DataPlaneHandshakeAdmission.HandlerLease handlerLease = admission.claimForHandler(
                lease, DataPlaneHandshakeAdmission.Direction.OUTBOUND);
        try {
            runtimeEnvelopeValidator.run();
            SSLEngine engine = tlsContext.sslContext().createSSLEngine(host, peerPort);
            engine.setUseClientMode(true);
            engine.setEnabledProtocols(ControlTlsContext.enabledDataPlaneProtocols(
                    engine.getSupportedProtocols()));
            engine.setEnabledCipherSuites(ControlTlsContext.enabledCiphers(
                    engine.getSupportedCipherSuites()));
            SSLParameters parameters = engine.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            engine.setSSLParameters(parameters);
            return install(pipeline, handler(engine, handlerLease, timeoutMillis));
        } catch (RuntimeException | Error failure) {
            handlerLease.close();
            throw failure;
        }
    }

    /** Returns a copy, used only to publish the local certificate identity in the control REGISTER. */
    public byte[] localCertificateFingerprint() {
        return localCertificateFingerprint.clone();
    }

    public boolean localWorkerRole() {
        return true;
    }

    public String deploymentSetId() {
        return deploymentSetId;
    }

    public String authzSetId() {
        return authzSetId;
    }

    /**
     * After a successful TLS handshake, binds the data plane twice: to the WORKER role and to
     * the expected fingerprint.
     */
    public void requireWorkerPeer(
            AdmittedTlsChannel channel, byte[] expectedFingerprint) throws IOException {
        if (channel == null) {
            throw new IOException("data-plane TLS channel is unavailable");
        }
        requireWorkerPeer(channel.handler.engine().getSession(), expectedFingerprint);
        channel.peerAuthorized.set(true);
    }

    public void requireWorkerPeer(SSLSession session, byte[] expectedFingerprint)
            throws IOException {
        if (session == null) {
            throw new IOException("data-plane TLS session is unavailable");
        }
        if (expectedFingerprint == null
                || expectedFingerprint.length != DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES) {
            throw new IllegalArgumentException(
                    "expected Worker certificate fingerprint must be exactly 32 bytes");
        }
        try {
            Certificate[] peer = session.getPeerCertificates();
            if (peer.length == 0 || !(peer[0] instanceof X509Certificate)) {
                throw new IOException("data-plane TLS peer has no X509 leaf certificate");
            }
            X509Certificate leaf = (X509Certificate) peer[0];
            leaf.checkValidity();
            String fingerprintHex = ControlTlsContext.fingerprint(leaf);
            if (!tlsContext.authzPolicy().roles(fingerprintHex).contains(ControlRole.WORKER)) {
                throw new IOException("data-plane TLS peer certificate lacks WORKER role");
            }
            byte[] actual = decodeFingerprint(fingerprintHex);
            if (!MessageDigest.isEqual(expectedFingerprint, actual)) {
                throw new IOException("data-plane TLS peer certificate fingerprint mismatch");
            }
        } catch (IOException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IOException("data-plane TLS peer certificate validation failed", failure);
        }
    }

    private GuardedSslHandler handler(
            SSLEngine engine, DataPlaneHandshakeAdmission.HandlerLease handlerLease,
            long timeoutMillis) {
        GuardedSslHandler handler = new GuardedSslHandler(
                engine, handlerLease.delegatedTaskExecutor(), handlerLease,
                timeoutMillis, dataPlaneAllocator,
                runtimeEnvelopeValidator);
        return handler;
    }

    private long remainingHandshakeMillis(long absoluteDeadlineNanos) {
        long remaining = absoluteDeadlineNanos - System.nanoTime();
        if (remaining <= 0L) {
            throw new IllegalStateException("data-plane handshake deadline expired");
        }
        long millis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remaining);
        if (millis == 0L) millis = 1L;
        return Math.min(handshakeTimeoutMillis, millis);
    }


    long allocatedHeapBytes() {
        return dataPlaneAllocator.metric().usedHeapMemory();
    }

    long allocatedDirectBytes() {
        return dataPlaneAllocator.metric().usedDirectMemory();
    }

    private AdmittedTlsChannel install(
            ChannelPipeline pipeline, GuardedSslHandler handler) {
        if (pipeline == null) {
            handler.abortBeforeInstallation();
            throw new IllegalArgumentException("data-plane TLS pipeline is required");
        }
        configureChannel(pipeline.channel().config());
        try {
            pipeline.addLast(handler);
            pipeline.addLast(new TlsPresenceGuard(handler));
            return new AdmittedTlsChannel(handler);
        } catch (RuntimeException | Error failure) {
            handler.abortBeforeInstallation();
            throw failure;
        }
    }

    /**
     * Placed immediately downstream of the TLS handler. When Netty unlinks first and delivers
     * handlerRemoved asynchronously, this blocks any concurrent event that would bypass TLS.
     * Malicious code in the same process could still tear down the whole pipeline, which is
     * outside the promise of an in-process sandbox for a worker.
     */
    private static final class TlsPresenceGuard extends ChannelDuplexHandler {
        private final GuardedSslHandler tlsHandler;

        private TlsPresenceGuard(GuardedSslHandler tlsHandler) {
            this.tlsHandler = tlsHandler;
        }

        @Override
        public void channelRead(ChannelHandlerContext context, Object message)
                throws Exception {
            if (!tlsPresent(context)) {
                ReferenceCountUtil.release(message);
                context.close();
                return;
            }
            context.fireChannelRead(message);
        }

        @Override
        public void write(ChannelHandlerContext context, Object message,
                          ChannelPromise promise) throws Exception {
            if (!tlsPresent(context)) {
                IllegalStateException failure = new IllegalStateException(
                        "data-plane TLS handler is absent from the channel pipeline");
                ReferenceCountUtil.release(message);
                promise.tryFailure(failure);
                context.close();
                return;
            }
            context.write(message, promise);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext context) throws Exception {
            context.close();
            super.handlerRemoved(context);
        }

        private boolean tlsPresent(ChannelHandlerContext context) {
            return context.pipeline().context(tlsHandler) != null
                    && context.channel().isOpen();
        }
    }

    /** Exposes only the read-only state a handshake or authentication needs, never the SslHandler or SSLEngine. */
    public static final class AdmittedTlsChannel {
        private final GuardedSslHandler handler;
        private final AtomicBoolean peerAuthorized = new AtomicBoolean();

        private AdmittedTlsChannel(GuardedSslHandler handler) {
            this.handler = handler;
        }

        public void whenHandshakeComplete(
                java.util.function.Consumer<HandshakeOutcome> listener) {
            if (listener == null) {
                throw new IllegalArgumentException("TLS handshake listener is required");
            }
            handler.whenHandshakeComplete(listener);
        }
        public boolean awaitHandshake(long timeout, java.util.concurrent.TimeUnit unit)
                throws InterruptedException {
            return handler.awaitHandshake(timeout, unit);
        }
        public void awaitHandshakeSuccess() throws Exception {
            handler.awaitHandshakeSuccess();
        }
        public boolean handshakeSucceeded() {
            return handler.handshakeSucceeded();
        }
        public boolean handshakeDone() { return handler.handshakeDone(); }
        public Throwable handshakeFailure() { return handler.handshakeFailure(); }
        public boolean clientMode() { return handler.engine().getUseClientMode(); }
        public boolean needClientAuth() { return handler.engine().getNeedClientAuth(); }
        public String peerHost() { return handler.engine().getPeerHost(); }
        public int peerPort() { return handler.engine().getPeerPort(); }
        public String endpointIdentificationAlgorithm() {
            return handler.engine().getSSLParameters()
                    .getEndpointIdentificationAlgorithm();
        }
        public long handshakeTimeoutMillis() {
            return handler.getHandshakeTimeoutMillis();
        }
        public String[] enabledProtocols() {
            return handler.engine().getEnabledProtocols();
        }
        public String[] enabledCipherSuites() {
            return handler.engine().getEnabledCipherSuites();
        }
        public String engineImplementationName() {
            return handler.engineImplementationName();
        }
        public Certificate[] peerCertificates() throws Exception {
            return handler.engine().getSession().getPeerCertificates().clone();
        }
        public void releaseAfterAuthentication() {
            if (!peerAuthorized.get()) {
                throw new IllegalStateException(
                        "data-plane TLS peer authorization is incomplete");
            }
            handler.releaseAfterAuthentication();
        }
    }

    public static final class HandshakeOutcome {
        private final boolean success;
        private final Throwable failure;

        private HandshakeOutcome(boolean success, Throwable failure) {
            this.success = success;
            this.failure = failure;
        }

        public boolean success() { return success; }
        public Throwable failure() { return failure; }
    }

    /** The handshake credit is settled by the handler itself on success, failure, removal and channel close, without caller compensation. */
    private static final class GuardedSslHandler extends SslHandler {
        private final DataPlaneHandshakeAdmission.HandlerLease lease;
        private final ByteBufAllocator expectedAllocator;
        private final Runnable runtimeEnvelopeValidator;
        private final AtomicBoolean released = new AtomicBoolean();
        private final SSLEngine readOnlyEngine;
        private final Future<Channel> readOnlyHandshakeFuture;
        private final String engineImplementationName;
        private final Object lifecycle = new Object();
        private volatile ChannelHandlerContext context;
        private boolean installationForbidden;

        private GuardedSslHandler(SSLEngine engine, java.util.concurrent.Executor executor,
                                  DataPlaneHandshakeAdmission.HandlerLease lease,
                                  long handshakeTimeoutMillis,
                                  ByteBufAllocator expectedAllocator,
                                  Runnable runtimeEnvelopeValidator) {
            super(engine, executor);
            this.lease = lease;
            this.expectedAllocator = expectedAllocator;
            this.runtimeEnvelopeValidator = runtimeEnvelopeValidator;
            this.readOnlyEngine = new ReadOnlySslEngine(engine);
            this.readOnlyHandshakeFuture = new ReadOnlyFuture<>(super.handshakeFuture());
            this.engineImplementationName = engine.getClass().getName();
            super.setHandshakeTimeoutMillis(handshakeTimeoutMillis);
            super.handshakeFuture().addListener(done -> {
                if (!done.isSuccess()) terminateFailedHandshake();
            });
        }

        @Override
        public Future<Channel> handshakeFuture() {
            return readOnlyHandshakeFuture;
        }

        private void whenHandshakeComplete(
                java.util.function.Consumer<HandshakeOutcome> listener) {
            super.handshakeFuture().addListener(done -> {
                try {
                    listener.accept(new HandshakeOutcome(
                            done.isSuccess(), done.cause()));
                } catch (Throwable callbackFailure) {
                    terminateCallbackFailure(callbackFailure);
                }
            });
        }

        private boolean awaitHandshake(long timeout, java.util.concurrent.TimeUnit unit)
                throws InterruptedException {
            return super.handshakeFuture().await(timeout, unit);
        }

        private void awaitHandshakeSuccess() throws Exception {
            super.handshakeFuture().sync();
        }

        private boolean handshakeSucceeded() {
            return super.handshakeFuture().isSuccess();
        }

        private boolean handshakeDone() { return super.handshakeFuture().isDone(); }

        private Throwable handshakeFailure() { return super.handshakeFuture().cause(); }

        /** An immutable view, so a caller cannot change protocols or authentication mode, or drive the engine, after the factory returns. */
        @Override
        public SSLEngine engine() {
            return readOnlyEngine;
        }

        @Override
        public void setHandshakeTimeout(long timeout, java.util.concurrent.TimeUnit unit) {
            throw new UnsupportedOperationException(
                    "data-plane TLS handshake timeout is immutable");
        }

        @Override
        public void setHandshakeTimeoutMillis(long timeoutMillis) {
            throw new UnsupportedOperationException(
                    "data-plane TLS handshake timeout is immutable");
        }

        public String engineImplementationName() {
            return engineImplementationName;
        }

        private void abortBeforeInstallation() {
            synchronized (lifecycle) {
                if (context != null) {
                    context.close();
                    return;
                }
                installationForbidden = true;
            }
            releaseLease();
        }

        /**
         * The credit is returned only after TLS peer authentication completes. The production
         * pipeline adds a bounded hello completion bit on top of that, so this method must not
         * be called before the hello is done.
         */
        public void releaseAfterAuthentication() {
            try {
                runtimeEnvelopeValidator.run();
            } catch (RuntimeException failure) {
                closeForTerminalRelease();
                throw failure;
            }
            SSLSession session = super.engine().getSession();
            boolean authenticated = super.handshakeFuture().isSuccess()
                    && session.isValid()
                    && "TLSv1.3".equals(session.getProtocol())
                    && !"SSL_NULL_WITH_NULL_NULL".equals(session.getCipherSuite());
            try {
                authenticated &= session.getPeerCertificates().length > 0;
            } catch (javax.net.ssl.SSLPeerUnverifiedException unverified) {
                authenticated = false;
            }
            if (!authenticated) {
                throw new IllegalStateException(
                        "TLS admission cannot be released before authenticated handshake success");
            }
            releaseLease();
        }

        @Override
        public void handlerAdded(ChannelHandlerContext context) throws Exception {
            synchronized (lifecycle) {
                if (installationForbidden || released.get()) {
                    context.close();
                    throw new IllegalStateException(
                            "terminal data-plane TLS handler cannot be installed");
                }
                this.context = context;
            }
            try {
                requireCertifiedAllocator(context);
                super.handlerAdded(context);
            } catch (Exception | Error failure) {
                context.close();
                throw failure;
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext context) throws Exception {
            requireCertifiedAllocator(context);
            super.channelActive(context);
        }

        @Override
        public void channelRead(ChannelHandlerContext context, Object message)
                throws Exception {
            try {
                requireCertifiedAllocator(context);
            } catch (RuntimeException failure) {
                ReferenceCountUtil.release(message);
                throw failure;
            }
            super.channelRead(context, message);
        }

        @Override
        public void write(ChannelHandlerContext context, Object message,
                          ChannelPromise promise) throws Exception {
            try {
                requireCertifiedAllocator(context);
            } catch (RuntimeException failure) {
                ReferenceCountUtil.release(message);
                promise.tryFailure(failure);
                return;
            }
            super.write(context, message, promise);
        }

        @Override
        public void handlerRemoved0(ChannelHandlerContext context) throws Exception {
            try {
                super.handlerRemoved0(context);
            } finally {
                // Removing the TLS handler from an active channel would downgrade it to plaintext, so the channel must already be terminal.
                context.close();
                releaseLease();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) throws Exception {
            super.channelInactive(context);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause)
                throws Exception {
            try {
                super.exceptionCaught(context, cause);
            } finally {
                // SslHandler propagates ordinary exceptions only, so the channel closes first and handlerRemoved0 settles the lease after TLS cleanup.
                context.close();
            }
        }

        /** Active renegotiation has no second admission credit and must fail closed; the data plane protocol enables TLS 1.3 only. */
        @Override
        public Future<Channel> renegotiate() {
            ChannelHandlerContext current;
            synchronized (lifecycle) {
                current = context;
                if (current == null) installationForbidden = true;
            }
            if (current == null) {
                releaseLease();
                return io.netty.util.concurrent.ImmediateEventExecutor.INSTANCE
                        .newFailedFuture(renegotiationRejected());
            }
            Promise<Channel> promise = current.executor().newPromise();
            return rejectRenegotiation(promise, current);
        }

        @Override
        public Future<Channel> renegotiate(Promise<Channel> promise) {
            if (promise == null) {
                throw new IllegalArgumentException("renegotiation promise is required");
            }
            ChannelHandlerContext current;
            synchronized (lifecycle) {
                current = context;
                if (current == null) installationForbidden = true;
            }
            if (current == null) releaseLease();
            return rejectRenegotiation(promise, current);
        }

        private Future<Channel> rejectRenegotiation(
                Promise<Channel> promise, ChannelHandlerContext current) {
            javax.net.ssl.SSLException failure = renegotiationRejected();
            if (current != null) current.close();
            boolean rejected;
            try {
                rejected = promise.tryFailure(failure);
            } catch (Throwable hostilePromise) {
                if (current != null) {
                    return current.executor().newFailedFuture(failure);
                }
                return io.netty.util.concurrent.ImmediateEventExecutor.INSTANCE
                        .newFailedFuture(failure);
            }
            if (rejected) return promise;
            if (current != null) return current.executor().newFailedFuture(failure);
            return io.netty.util.concurrent.ImmediateEventExecutor.INSTANCE
                    .newFailedFuture(failure);
        }

        private void terminateFailedHandshake() {
            ChannelHandlerContext current;
            synchronized (lifecycle) {
                current = context;
                if (current == null) installationForbidden = true;
            }
            if (current == null) releaseLease();
            else current.close();
        }

        private void terminateCallbackFailure(Throwable failure) {
            ChannelHandlerContext current;
            synchronized (lifecycle) {
                current = context;
                if (current == null) installationForbidden = true;
            }
            if (current == null) {
                releaseLease();
            } else {
                current.close();
                current.fireExceptionCaught(failure);
            }
        }

        private static javax.net.ssl.SSLException renegotiationRejected() {
            return new javax.net.ssl.SSLException(
                    "data-plane TLS renegotiation is forbidden");
        }

        private void releaseLease() {
            if (released.compareAndSet(false, true)) lease.close();
        }

        private void requireCertifiedAllocator(ChannelHandlerContext context) {
            if (context.alloc() == expectedAllocator) return;
            context.close();
            throw new IllegalStateException(
                    "data-plane TLS channel allocator differs from certified allocator");
        }

        private void closeForTerminalRelease() {
            ChannelHandlerContext current;
            synchronized (lifecycle) {
                current = context;
                if (current == null) installationForbidden = true;
            }
            if (current == null) releaseLease();
            else current.close();
        }
    }

    /** The SslHandler still holds the real engine; the published engine() returns a view that blocks every change and drive call. */
    private static final class ReadOnlySslEngine extends SSLEngine {
        private final SSLEngine delegate;

        private ReadOnlySslEngine(SSLEngine delegate) {
            super(delegate.getPeerHost(), delegate.getPeerPort());
            this.delegate = delegate;
        }

        @Override
        public SSLEngineResult wrap(ByteBuffer[] sources, int offset, int length,
                                    ByteBuffer target) {
            throw immutable();
        }

        @Override
        public SSLEngineResult unwrap(ByteBuffer source, ByteBuffer[] targets,
                                      int offset, int length) {
            throw immutable();
        }

        @Override
        public Runnable getDelegatedTask() { throw immutable(); }

        @Override
        public void closeInbound() { throw immutable(); }

        @Override
        public boolean isInboundDone() { return delegate.isInboundDone(); }

        @Override
        public void closeOutbound() { throw immutable(); }

        @Override
        public boolean isOutboundDone() { return delegate.isOutboundDone(); }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public String[] getEnabledCipherSuites() {
            return delegate.getEnabledCipherSuites();
        }

        @Override
        public void setEnabledCipherSuites(String[] suites) { throw immutable(); }

        @Override
        public String[] getSupportedProtocols() { return delegate.getSupportedProtocols(); }

        @Override
        public String[] getEnabledProtocols() { return delegate.getEnabledProtocols(); }

        @Override
        public void setEnabledProtocols(String[] protocols) { throw immutable(); }

        @Override
        public SSLSession getSession() { return delegate.getSession(); }

        @Override
        public SSLSession getHandshakeSession() { return delegate.getHandshakeSession(); }

        @Override
        public void beginHandshake() { throw immutable(); }

        @Override
        public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
            return delegate.getHandshakeStatus();
        }

        @Override
        public void setUseClientMode(boolean mode) { throw immutable(); }

        @Override
        public boolean getUseClientMode() { return delegate.getUseClientMode(); }

        @Override
        public void setNeedClientAuth(boolean need) { throw immutable(); }

        @Override
        public boolean getNeedClientAuth() { return delegate.getNeedClientAuth(); }

        @Override
        public void setWantClientAuth(boolean want) { throw immutable(); }

        @Override
        public boolean getWantClientAuth() { return delegate.getWantClientAuth(); }

        @Override
        public void setEnableSessionCreation(boolean flag) { throw immutable(); }

        @Override
        public boolean getEnableSessionCreation() {
            return delegate.getEnableSessionCreation();
        }

        @Override
        public SSLParameters getSSLParameters() { return delegate.getSSLParameters(); }

        @Override
        public void setSSLParameters(SSLParameters parameters) { throw immutable(); }

        private static UnsupportedOperationException immutable() {
            return new UnsupportedOperationException(
                    "data-plane TLS engine is immutable and handler-owned");
        }
    }

    /** Read-only shell around Netty's LazyChannelPromise; the underlying promise in the listener argument is not leaked. */
    @SuppressWarnings("unchecked")
    private static final class ReadOnlyFuture<V> implements Future<V> {
        private final Future<V> delegate;

        private ReadOnlyFuture(Future<V> delegate) {
            this.delegate = delegate;
        }

        @Override public boolean isSuccess() { return delegate.isSuccess(); }
        @Override public boolean isCancellable() { return false; }
        @Override public Throwable cause() { return delegate.cause(); }
        @Override
        public Future<V> addListener(
                io.netty.util.concurrent.GenericFutureListener<? extends Future<? super V>> listener) {
            throw immutableFuture();
        }
        @Override
        public Future<V> addListeners(
                io.netty.util.concurrent.GenericFutureListener<? extends Future<? super V>>... listeners) {
            throw immutableFuture();
        }
        @Override
        public Future<V> removeListener(
                io.netty.util.concurrent.GenericFutureListener<? extends Future<? super V>> listener) {
            throw immutableFuture();
        }
        @Override
        public Future<V> removeListeners(
                io.netty.util.concurrent.GenericFutureListener<? extends Future<? super V>>... listeners) {
            throw immutableFuture();
        }
        @Override public Future<V> sync() throws InterruptedException {
            delegate.sync();
            return this;
        }
        @Override public Future<V> syncUninterruptibly() {
            delegate.syncUninterruptibly();
            return this;
        }
        @Override public Future<V> await() throws InterruptedException {
            delegate.await();
            return this;
        }
        @Override public Future<V> awaitUninterruptibly() {
            delegate.awaitUninterruptibly();
            return this;
        }
        @Override public boolean await(long timeout, java.util.concurrent.TimeUnit unit)
                throws InterruptedException { return delegate.await(timeout, unit); }
        @Override public boolean await(long timeoutMillis) throws InterruptedException {
            return delegate.await(timeoutMillis);
        }
        @Override public boolean awaitUninterruptibly(
                long timeout, java.util.concurrent.TimeUnit unit) {
            return delegate.awaitUninterruptibly(timeout, unit);
        }
        @Override public boolean awaitUninterruptibly(long timeoutMillis) {
            return delegate.awaitUninterruptibly(timeoutMillis);
        }
        @Override public V getNow() { return delegate.getNow(); }
        @Override public boolean cancel(boolean mayInterruptIfRunning) { return false; }
        @Override public boolean isCancelled() { return delegate.isCancelled(); }
        @Override public boolean isDone() { return delegate.isDone(); }
        @Override public V get() throws java.util.concurrent.ExecutionException,
                InterruptedException { return delegate.get(); }
        @Override public V get(long timeout, java.util.concurrent.TimeUnit unit)
                throws java.util.concurrent.ExecutionException, InterruptedException,
                java.util.concurrent.TimeoutException {
            return delegate.get(timeout, unit);
        }

        private static UnsupportedOperationException immutableFuture() {
            return new UnsupportedOperationException(
                    "data-plane TLS handshake future is read-only");
        }
    }

    private static byte[] decodeFingerprint(String hex) {
        if (hex == null || hex.length() != 64) {
            throw new IllegalStateException("certificate fingerprint is unavailable");
        }
        byte[] decoded = new byte[DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES];
        for (int i = 0; i < decoded.length; i++) {
            int high = Character.digit(hex.charAt(i * 2), 16);
            int low = Character.digit(hex.charAt(i * 2 + 1), 16);
            if (high < 0 || low < 0) {
                throw new IllegalStateException("certificate fingerprint is invalid");
            }
            decoded[i] = (byte) ((high << 4) | low);
        }
        return decoded;
    }
}
