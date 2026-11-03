package io.rowweave.stream.cluster;

import io.rowweave.cluster.rpc.ControlRole;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.RoleAuthenticatedSocketTransport;
import io.rowweave.cluster.rpc.RpcSecurityMetadata;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamEnvelopeCodec;
import io.rowweave.stream.contract.StreamEnvelopeSequenceGuard;
import io.rowweave.stream.runtime.StreamChannelCancelledException;
import io.rowweave.stream.runtime.StreamChannelFailedException;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamChannelOutput;
import io.rowweave.stream.runtime.StreamChannelMetrics;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One-producer/one-consumer Stream socket lane with explicit event and byte credit.
 * This protocol is intentionally independent from the frozen Batch shuffle frame domain.
 */
public final class StreamSocketChannel {
    private static final byte[] MAGIC =
            "RWXXSDV1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SECURITY_MAGIC =
            "RWXXSLI1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    public enum FrameKind {
        DATA(1), CREDIT(2), CLOSE(3), CANCEL(4), ERROR(5);

        private final int wireId;

        FrameKind(int wireId) {
            this.wireId = wireId;
        }

        public int wireId() {
            return wireId;
        }
    }

    private static final int DATA = FrameKind.DATA.wireId();
    private static final int CREDIT = FrameKind.CREDIT.wireId();
    private static final int CLOSE = FrameKind.CLOSE.wireId();
    private static final int CANCEL = FrameKind.CANCEL.wireId();
    private static final int ERROR = FrameKind.ERROR.wireId();
    private static final int CREDIT_BYTES = 12;
    private static final int MAX_ERROR_BYTES = 128;
    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;
    private static final int HANDSHAKE_TIMEOUT_MILLIS = 30_000;

    private StreamSocketChannel() { }

    public static byte[] protocolMagic() {
        return MAGIC.clone();
    }

    public static Pair openLoopback(
            StreamChannelIdentity identity, int maximumEvents,
            int maximumFrameBytes, long maximumBytes) throws IOException {
        requireLimits(identity, maximumEvents, maximumFrameBytes, maximumBytes);
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket server = new ServerSocket(0, 1, loopback)) {
            Socket client = new Socket(loopback, server.getLocalPort());
            Socket accepted = null;
            try {
                accepted = server.accept();
                configure(client);
                configure(accepted);
                Sender sender = new Sender(client, identity, maximumEvents,
                        maximumFrameBytes, maximumBytes);
                Receiver receiver = new Receiver(accepted, identity, maximumEvents,
                        maximumFrameBytes, maximumBytes);
                sender.completeHandshake();
                return new Pair(sender, receiver, server.getLocalPort());
            } catch (Throwable failure) {
                closeQuietly(client);
                closeQuietly(accepted);
                if (failure instanceof IOException) {
                    throw (IOException) failure;
                }
                if (failure instanceof RuntimeException) {
                    throw (RuntimeException) failure;
                }
                if (failure instanceof Error) {
                    throw (Error) failure;
                }
                throw new IOException("Stream socket channel open failed", failure);
            }
        }
    }

    public static Listener bindLoopback(
            StreamChannelIdentity identity, int maximumEvents,
            int maximumFrameBytes, long maximumBytes) throws IOException {
        return bind(InetAddress.getLoopbackAddress(), identity,
                maximumEvents, maximumFrameBytes, maximumBytes);
    }

    /** Binds a data lane on the Worker address selected by placement. */
    public static Listener bind(InetAddress address,
            StreamChannelIdentity identity, int maximumEvents,
            int maximumFrameBytes, long maximumBytes) throws IOException {
        requireLimits(identity, maximumEvents, maximumFrameBytes, maximumBytes);
        if (address == null || address.isAnyLocalAddress()) {
            throw new IllegalArgumentException(
                    "Stream lane bind address must be concrete");
        }
        ServerSocket server = new ServerSocket(
                0, 1, address);
        return new Listener(server, null, null, identity, maximumEvents,
                maximumFrameBytes, maximumBytes);
    }

    /** Binds a Worker data lane with the configured LOOPBACK or mTLS identity. */
    public static Listener bind(ControlSecurityConfig security,
            StreamChannelIdentity identity, int maximumEvents,
            int maximumFrameBytes, long maximumBytes) throws IOException {
        return bind(security, security == null ? null
                        : security.securityMetadata(),
                identity, maximumEvents, maximumFrameBytes, maximumBytes);
    }

    static Listener bind(ControlSecurityConfig security,
            RpcSecurityMetadata securityMetadata,
            StreamChannelIdentity identity, int maximumEvents,
            int maximumFrameBytes, long maximumBytes) throws IOException {
        requireLimits(identity, maximumEvents, maximumFrameBytes, maximumBytes);
        if (security == null) {
            throw new IllegalArgumentException(
                    "Stream lane security is required");
        }
        ServerSocket server = RoleAuthenticatedSocketTransport.bind(
                security, ControlRole.WORKER, 0, 1);
        return new Listener(server, security, securityMetadata,
                identity, maximumEvents,
                maximumFrameBytes, maximumBytes);
    }

    public static Sender connectLoopback(
            int port, StreamChannelIdentity identity, int maximumEvents,
            int maximumFrameBytes, long maximumBytes) throws IOException {
        return connect(InetAddress.getLoopbackAddress().getHostAddress(),
                port, identity, maximumEvents, maximumFrameBytes,
                maximumBytes);
    }

    /** Connects a data lane to the Worker address selected by placement. */
    public static Sender connect(String host, int port,
            StreamChannelIdentity identity, int maximumEvents,
            int maximumFrameBytes, long maximumBytes) throws IOException {
        requireLimits(identity, maximumEvents, maximumFrameBytes, maximumBytes);
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("Stream lane host is required");
        }
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException(
                "Stream socket port is invalid");
        }
        Socket socket = new Socket();
        try {
            socket.connect(new java.net.InetSocketAddress(host, port), 5_000);
            configure(socket);
            Sender sender = new Sender(
                    socket, identity, maximumEvents,
                    maximumFrameBytes, maximumBytes);
            sender.completeHandshake();
            return sender;
        } catch (Throwable failure) {
            closeQuietly(socket);
            if (failure instanceof IOException) {
                throw (IOException) failure;
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new IOException("Stream socket connect failed", failure);
        }
    }

    /** Connects a Worker data lane without a plaintext production fallback. */
    public static Sender connect(ControlSecurityConfig security,
            String host, int port, StreamChannelIdentity identity,
            int maximumEvents, int maximumFrameBytes,
            long maximumBytes) throws IOException {
        return connect(security, security == null ? null
                        : security.securityMetadata(),
                host, port, identity, maximumEvents, maximumFrameBytes,
                maximumBytes);
    }

    static Sender connect(ControlSecurityConfig security,
            RpcSecurityMetadata securityMetadata,
            String host, int port, StreamChannelIdentity identity,
            int maximumEvents, int maximumFrameBytes,
            long maximumBytes) throws IOException {
        requireLimits(identity, maximumEvents, maximumFrameBytes, maximumBytes);
        if (security == null) {
            throw new IllegalArgumentException(
                    "Stream lane security is required");
        }
        Socket socket = null;
        try {
            socket = RoleAuthenticatedSocketTransport.connect(
                    security, ControlRole.WORKER, ControlRole.WORKER,
                    host, port, CONNECT_TIMEOUT_MILLIS,
                    HANDSHAKE_TIMEOUT_MILLIS);
            configure(socket);
            exchangeSecurityIdentityAsClient(socket, securityMetadata);
            Sender sender = new Sender(socket, identity, maximumEvents,
                    maximumFrameBytes, maximumBytes);
            sender.completeHandshake();
            return sender;
        } catch (Throwable failure) {
            closeQuietly(socket);
            if (failure instanceof IOException) {
                throw (IOException) failure;
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            if (failure instanceof Error) throw (Error) failure;
            throw new IOException(
                    "authenticated Stream socket connect failed", failure);
        }
    }

    public static final class Listener implements AutoCloseable {
        private final ServerSocket server;
        private final ControlSecurityConfig security;
        private final RpcSecurityMetadata securityMetadata;
        private final StreamChannelIdentity identity;
        private final int maximumEvents;
        private final int maximumFrameBytes;
        private final long maximumBytes;
        private final AtomicBoolean accepted = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        private Listener(
                ServerSocket server, ControlSecurityConfig security,
                RpcSecurityMetadata securityMetadata,
                StreamChannelIdentity identity,
                int maximumEvents, int maximumFrameBytes,
                long maximumBytes) {
            this.server = server;
            this.security = security;
            this.securityMetadata = securityMetadata;
            this.identity = identity;
            this.maximumEvents = maximumEvents;
            this.maximumFrameBytes = maximumFrameBytes;
            this.maximumBytes = maximumBytes;
        }

        public int localPort() {
            return server.getLocalPort();
        }

        public Receiver accept() throws IOException {
            if (closed.get() || !accepted.compareAndSet(false, true)) {
                throw new IllegalStateException(
                        "Stream socket listener accepts exactly one channel");
            }
            Socket socket = server.accept();
            try {
                if (security != null) {
                    RoleAuthenticatedSocketTransport.authenticateAccepted(
                            security, ControlRole.WORKER, socket,
                            HANDSHAKE_TIMEOUT_MILLIS);
                }
                configure(socket);
                if (security != null) {
                    exchangeSecurityIdentityAsServer(
                            socket, securityMetadata);
                }
                return new Receiver(
                        socket, identity, maximumEvents,
                        maximumFrameBytes, maximumBytes);
            } catch (Throwable failure) {
                closeQuietly(socket);
                if (failure instanceof IOException) {
                    throw (IOException) failure;
                }
                if (failure instanceof RuntimeException) {
                    throw (RuntimeException) failure;
                }
                if (failure instanceof Error) {
                    throw (Error) failure;
                }
                throw new IOException("Stream socket accept failed", failure);
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                try {
                    server.close();
                } catch (IOException ignored) {
                    // Listener close is idempotent and best-effort.
                }
            }
        }
    }

    public static final class Pair implements AutoCloseable {
        private final Sender sender;
        private final Receiver receiver;
        private final int localPort;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Pair(Sender sender, Receiver receiver, int localPort) {
            this.sender = sender;
            this.receiver = receiver;
            this.localPort = localPort;
        }

        public Sender sender() { return sender; }
        public Receiver receiver() { return receiver; }
        public int localPort() { return localPort; }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            sender.dispose();
            receiver.dispose();
        }
    }

    public static final class Sender
            implements AutoCloseable, StreamChannelOutput {
        private final Socket socket;
        private final DataInputStream controlInput;
        private final DataOutputStream dataOutput;
        private final Object outputLock = new Object();
        private final StreamChannelIdentity identity;
        private final StreamEnvelopeSequenceGuard sequenceGuard;
        private final int maximumEvents;
        private final long maximumBytes;
        private final int maximumFrameBytes;
        private final ReentrantLock creditLock = new ReentrantLock();
        private final Condition creditChanged = creditLock.newCondition();
        private final AtomicBoolean disposed = new AtomicBoolean();

        private int availableEvents;
        private long availableBytes;
        private int outstandingEvents;
        private long outstandingBytes;
        private int maximumObservedEvents;
        private long maximumObservedBytes;
        private long parkCount;
        private long blockedNanos;
        private boolean closed;
        private boolean cancelled;
        private Throwable failure;
        private Thread controlReader;

        private Sender(
                Socket socket, StreamChannelIdentity identity,
                int maximumEvents, int maximumFrameBytes,
                long maximumBytes) throws IOException {
            this.socket = socket;
            this.identity = identity;
            this.sequenceGuard = new StreamEnvelopeSequenceGuard(identity);
            this.maximumEvents = maximumEvents;
            this.maximumFrameBytes = maximumFrameBytes;
            this.maximumBytes = maximumBytes;
            this.availableEvents = maximumEvents;
            this.availableBytes = maximumBytes;
            this.controlInput = new DataInputStream(socket.getInputStream());
            this.dataOutput = new DataOutputStream(socket.getOutputStream());
            writeHello(dataOutput, identity, maximumEvents,
                    maximumFrameBytes, maximumBytes);
        }

        private void completeHandshake() throws IOException {
            readAck(controlInput);
            socket.setSoTimeout(0);
            controlReader = new Thread(this::readControls,
                    "rowweave-stream-socket-credit-" + identity.channelId());
            controlReader.setDaemon(true);
            controlReader.start();
        }

        public void send(StreamEnvelope envelope) {
            if (envelope == null) {
                throw new IllegalArgumentException("Stream envelope is required");
            }
            byte[] frame = StreamEnvelopeCodec.encode(envelope);
            if (frame.length > maximumFrameBytes) {
                throw new IllegalArgumentException(
                        "encoded Stream envelope exceeds socket frame limit");
            }
            creditLock.lock();
            try {
                requireWritable();
                sequenceGuard.accept(envelope);
                boolean counted = false;
                long blockedAt = 0L;
                while ((availableEvents == 0 || availableBytes < frame.length)
                        && !cancelled && failure == null && !closed) {
                    if (!counted) {
                        parkCount++;
                        counted = true;
                        blockedAt = System.nanoTime();
                    }
                    creditChanged.awaitUninterruptibly();
                }
                if (counted) {
                    long elapsed = Math.max(
                            1L, System.nanoTime() - blockedAt);
                    blockedNanos = blockedNanos > Long.MAX_VALUE - elapsed
                            ? Long.MAX_VALUE : blockedNanos + elapsed;
                }
                requireWritable();
                availableEvents--;
                availableBytes -= frame.length;
                outstandingEvents++;
                outstandingBytes += frame.length;
                maximumObservedEvents = Math.max(
                        maximumObservedEvents, outstandingEvents);
                maximumObservedBytes = Math.max(
                        maximumObservedBytes, outstandingBytes);
            } finally {
                creditLock.unlock();
            }
            try {
                writeFrame(dataOutput, outputLock, DATA, frame);
            } catch (IOException writeFailure) {
                failLocal(writeFailure);
                throw new StreamChannelFailedException(writeFailure);
            }
        }

        @Override
        public StreamChannelIdentity channelIdentity() {
            return identity;
        }

        @Override
        public void close() {
            creditLock.lock();
            try {
                if (closed || cancelled || failure != null) {
                    return;
                }
                closed = true;
                creditChanged.signalAll();
            } finally {
                creditLock.unlock();
            }
            try {
                writeFrame(dataOutput, outputLock, CLOSE, new byte[0]);
            } catch (IOException writeFailure) {
                failLocal(writeFailure);
                throw new StreamChannelFailedException(writeFailure);
            }
        }

        public void cancel() {
            if (!markCancelled()) {
                return;
            }
            try {
                writeFrame(dataOutput, outputLock, CANCEL, new byte[0]);
            } catch (IOException ignored) {
                // Local cancellation is already terminal and wakes blocked senders.
            }
        }

        public void fail(Throwable cause) {
            if (cause == null) {
                throw new IllegalArgumentException(
                        "Stream socket channel failure cause is required");
            }
            if (!failLocal(cause)) {
                return;
            }
            try {
                writeFrame(dataOutput, outputLock, ERROR,
                        "STREAM_CHANNEL_FAILED".getBytes(StandardCharsets.US_ASCII));
            } catch (IOException ignored) {
                // Failure is already terminal locally.
            }
        }

        public long parkCount() {
            creditLock.lock();
            try { return parkCount; } finally { creditLock.unlock(); }
        }

        public int outstandingEvents() {
            creditLock.lock();
            try { return outstandingEvents; } finally { creditLock.unlock(); }
        }

        public long outstandingBytes() {
            creditLock.lock();
            try { return outstandingBytes; } finally { creditLock.unlock(); }
        }

        public int maximumObservedEvents() {
            creditLock.lock();
            try { return maximumObservedEvents; } finally { creditLock.unlock(); }
        }

        public long maximumObservedBytes() {
            creditLock.lock();
            try { return maximumObservedBytes; } finally { creditLock.unlock(); }
        }

        @Override
        public StreamChannelMetrics metrics() {
            creditLock.lock();
            try {
                return StreamChannelMetrics.available(
                        outstandingEvents, outstandingBytes,
                        maximumObservedEvents, maximumObservedBytes,
                        blockedNanos);
            } finally {
                creditLock.unlock();
            }
        }

        private void readControls() {
            try {
                while (!disposed.get()) {
                    int type = controlInput.readInt();
                    int length = controlInput.readInt();
                    if (type == CREDIT) {
                        if (length != CREDIT_BYTES) {
                            throw new IOException("invalid Stream credit frame");
                        }
                        onCredit(controlInput.readInt(), controlInput.readLong());
                    } else if (type == CANCEL && length == 0) {
                        markCancelled();
                    } else if (type == ERROR) {
                        readBounded(controlInput, length, MAX_ERROR_BYTES);
                        failLocal(new IllegalStateException(
                                "remote Stream socket channel failed"));
                    } else {
                        throw new IOException("unknown Stream reverse frame");
                    }
                }
            } catch (EOFException closedPeer) {
                if (!disposed.get() && !closed && !cancelled) {
                    failLocal(closedPeer);
                }
            } catch (IOException protocolFailure) {
                if (!disposed.get() && !cancelled) {
                    failLocal(protocolFailure);
                }
            }
        }

        private void onCredit(int events, long bytes) throws IOException {
            creditLock.lock();
            try {
                if (events <= 0 || bytes <= 0L
                        || events > outstandingEvents || bytes > outstandingBytes
                        || availableEvents + events > maximumEvents
                        || availableBytes + bytes > maximumBytes) {
                    throw new IOException("invalid socket Stream credit");
                }
                outstandingEvents -= events;
                outstandingBytes -= bytes;
                availableEvents += events;
                availableBytes += bytes;
                creditChanged.signalAll();
            } finally {
                creditLock.unlock();
            }
        }

        private boolean markCancelled() {
            creditLock.lock();
            try {
                if (cancelled || failure != null) {
                    return false;
                }
                cancelled = true;
                creditChanged.signalAll();
                return true;
            } finally {
                creditLock.unlock();
            }
        }

        private boolean failLocal(Throwable cause) {
            creditLock.lock();
            try {
                if (cancelled || failure != null) {
                    return false;
                }
                failure = cause;
                creditChanged.signalAll();
                return true;
            } finally {
                creditLock.unlock();
            }
        }

        private void requireWritable() {
            if (cancelled) {
                throw new StreamChannelCancelledException();
            }
            if (failure != null) {
                throw new StreamChannelFailedException(failure);
            }
            if (closed) {
                throw new IllegalStateException(
                        "send on closed socket Stream channel");
            }
        }

        private void dispose() {
            if (disposed.compareAndSet(false, true)) {
                closeQuietly(socket);
            }
        }
    }

    public static final class Receiver
            implements AutoCloseable, StreamChannelInput {
        private final Socket socket;
        private final DataInputStream dataInput;
        private final DataOutputStream controlOutput;
        private final Object outputLock = new Object();
        private final StreamChannelIdentity identity;
        private final StreamEnvelopeSequenceGuard sequenceGuard;
        private final int maximumFrameBytes;
        private final AtomicBoolean disposed = new AtomicBoolean();
        private volatile boolean cancelled;
        private volatile boolean normalClosed;
        private volatile Throwable failure;

        private Receiver(
                Socket socket, StreamChannelIdentity expectedIdentity,
                int maximumEvents, int maximumFrameBytes,
                long maximumBytes) throws IOException {
            this.socket = socket;
            this.identity = expectedIdentity;
            this.sequenceGuard = new StreamEnvelopeSequenceGuard(expectedIdentity);
            this.maximumFrameBytes = maximumFrameBytes;
            this.dataInput = new DataInputStream(socket.getInputStream());
            this.controlOutput = new DataOutputStream(socket.getOutputStream());
            readAndValidateHello(dataInput, expectedIdentity, maximumEvents,
                    maximumFrameBytes, maximumBytes);
            writeAck(controlOutput);
            socket.setSoTimeout(0);
        }

        public StreamEnvelope receive() throws InterruptedException {
            if (cancelled) {
                throw new StreamChannelCancelledException();
            }
            if (failure != null) {
                throw new StreamChannelFailedException(failure);
            }
            if (normalClosed) {
                return null;
            }
            try {
                int type = dataInput.readInt();
                int length = dataInput.readInt();
                if (type == DATA) {
                    byte[] encoded = readBounded(
                            dataInput, length, maximumFrameBytes);
                    StreamEnvelope envelope = StreamEnvelopeCodec.decode(encoded);
                    sequenceGuard.accept(envelope);
                    sendCredit(encoded.length);
                    return envelope;
                }
                if (type == CLOSE && length == 0) {
                    normalClosed = true;
                    return null;
                }
                if (type == CANCEL && length == 0) {
                    cancelled = true;
                    throw new StreamChannelCancelledException();
                }
                if (type == ERROR) {
                    readBounded(dataInput, length, MAX_ERROR_BYTES);
                    failure = new IllegalStateException(
                            "remote Stream socket channel failed");
                    throw new StreamChannelFailedException(failure);
                }
                throw new IOException("unknown Stream forward frame");
            } catch (EOFException closedPeer) {
                if (cancelled) {
                    throw new StreamChannelCancelledException();
                }
                failure = closedPeer;
                throw new StreamChannelFailedException(closedPeer);
            } catch (SocketTimeoutException timeout) {
                return null;
            } catch (IOException protocolFailure) {
                if (cancelled) {
                    throw new StreamChannelCancelledException();
                }
                failure = protocolFailure;
                throw new StreamChannelFailedException(protocolFailure);
            }
        }

        @Override
        public StreamEnvelope poll(long timeoutMillis)
                throws InterruptedException {
            if (timeoutMillis < 0L) {
                throw new IllegalArgumentException(
                        "Stream channel poll timeout must not be negative");
            }
            if (cancelled) throw new StreamChannelCancelledException();
            if (failure != null) throw new StreamChannelFailedException(failure);
            if (normalClosed) return null;
            try {
                socket.setSoTimeout((int) Math.min(Integer.MAX_VALUE,
                        Math.max(1L, timeoutMillis)));
            } catch (IOException setupFailure) {
                failure = setupFailure;
                throw new StreamChannelFailedException(setupFailure);
            }
            try {
                return receive();
            } finally {
                try {
                    socket.setSoTimeout(0);
                } catch (IOException ignored) {
                    // receive/cancel will report the terminal socket state.
                }
            }
        }

        @Override
        public StreamChannelIdentity channelIdentity() {
            return identity;
        }

        @Override
        public void close() {
            dispose();
        }

        public void cancel() {
            if (cancelled || failure != null) {
                return;
            }
            cancelled = true;
            try {
                writeFrame(controlOutput, outputLock, CANCEL, new byte[0]);
            } catch (IOException ignored) {
                // Local cancellation is already terminal.
            } finally {
                // A thread may already be blocked in dataInput.readInt().
                // Interrupting that thread does not wake a socket read, so the
                // receiver must close its own endpoint after the reverse
                // CANCEL has been flushed. The peer still observes CANCEL,
                // while the local read deterministically converges.
                dispose();
            }
        }

        public void fail(Throwable cause) {
            if (cause == null) {
                throw new IllegalArgumentException(
                        "Stream socket channel failure cause is required");
            }
            if (cancelled || failure != null) {
                return;
            }
            failure = cause;
            try {
                writeFrame(controlOutput, outputLock, ERROR,
                        "STREAM_CHANNEL_FAILED".getBytes(StandardCharsets.US_ASCII));
            } catch (IOException ignored) {
                // Local failure is already terminal.
            }
        }

        private void sendCredit(int bytes) throws IOException {
            synchronized (outputLock) {
                controlOutput.writeInt(CREDIT);
                controlOutput.writeInt(CREDIT_BYTES);
                controlOutput.writeInt(1);
                controlOutput.writeLong(bytes);
                controlOutput.flush();
            }
        }

        private void dispose() {
            if (disposed.compareAndSet(false, true)) {
                closeQuietly(socket);
            }
        }
    }

    private static void requireLimits(
            StreamChannelIdentity identity, int maximumEvents,
            int maximumFrameBytes, long maximumBytes) {
        if (identity == null || maximumEvents <= 0
                || maximumFrameBytes <= 0
                || maximumFrameBytes > StreamEnvelopeCodec.MAX_FRAME_BYTES
                || maximumBytes < maximumFrameBytes) {
            throw new IllegalArgumentException(
                    "Stream socket channel limits are invalid");
        }
    }

    private static void configure(Socket socket) throws IOException {
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(2_000);
    }

    private static void writeHello(
            DataOutputStream output, StreamChannelIdentity identity,
            int maximumEvents, int maximumFrameBytes,
            long maximumBytes) throws IOException {
        output.write(MAGIC);
        output.writeInt(VERSION);
        writeText(output, identity.runId());
        writeText(output, identity.attemptId());
        output.writeLong(identity.fenceToken());
        output.writeLong(identity.assignmentGeneration());
        writeText(output, identity.channelId());
        output.writeLong(identity.channelEpoch());
        output.writeInt(maximumEvents);
        output.writeInt(maximumFrameBytes);
        output.writeLong(maximumBytes);
        output.flush();
    }

    private static void exchangeSecurityIdentityAsClient(
            Socket socket, RpcSecurityMetadata expected) throws IOException {
        DataOutputStream output = new DataOutputStream(
                socket.getOutputStream());
        DataInputStream input = new DataInputStream(socket.getInputStream());
        writeSecurityIdentity(output, expected);
        requireSecurityIdentity(input, expected);
    }

    private static void exchangeSecurityIdentityAsServer(
            Socket socket, RpcSecurityMetadata expected) throws IOException {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        DataOutputStream output = new DataOutputStream(
                socket.getOutputStream());
        requireSecurityIdentity(input, expected);
        writeSecurityIdentity(output, expected);
    }

    private static void writeSecurityIdentity(DataOutputStream output,
            RpcSecurityMetadata metadata) throws IOException {
        output.write(SECURITY_MAGIC);
        output.writeInt(1);
        output.writeBoolean(metadata != null);
        if (metadata != null) {
            writeText(output, metadata.deploymentSetId());
            writeText(output, metadata.authzSetId());
        }
        output.flush();
    }

    private static void requireSecurityIdentity(DataInputStream input,
            RpcSecurityMetadata expected) throws IOException {
        byte[] magic = new byte[SECURITY_MAGIC.length];
        input.readFully(magic);
        if (!java.security.MessageDigest.isEqual(SECURITY_MAGIC, magic)
                || input.readInt() != 1) {
            throw new IOException(
                    "Stream data-lane security hello is incompatible");
        }
        RpcSecurityMetadata actual = null;
        if (input.readBoolean()) {
            try {
                actual = RpcSecurityMetadata.of(
                        readText(input), readText(input));
            } catch (IllegalArgumentException invalid) {
                throw new IOException(
                        "Stream data-lane security identity is invalid",
                        invalid);
            }
        }
        if (!java.util.Objects.equals(expected, actual)) {
            throw new IOException(
                    "Stream data-lane deployment/authz identity mismatch");
        }
    }

    private static void readAndValidateHello(
            DataInputStream input, StreamChannelIdentity expectedIdentity,
            int maximumEvents, int maximumFrameBytes,
            long maximumBytes) throws IOException {
        byte[] magic = new byte[MAGIC.length];
        input.readFully(magic);
        if (!java.security.MessageDigest.isEqual(MAGIC, magic)
                || input.readInt() != VERSION) {
            throw new IOException("Stream socket hello is incompatible");
        }
        StreamChannelIdentity actual = new StreamChannelIdentity(
                readText(input), readText(input), input.readLong(),
                input.readLong(), readText(input), input.readLong());
        if (!expectedIdentity.equals(actual)
                || maximumEvents != input.readInt()
                || maximumFrameBytes != input.readInt()
                || maximumBytes != input.readLong()) {
            throw new IOException("Stream socket hello identity or limits differ");
        }
    }

    private static void writeAck(DataOutputStream output) throws IOException {
        output.write(MAGIC);
        output.writeInt(VERSION);
        output.flush();
    }

    private static void readAck(DataInputStream input) throws IOException {
        byte[] magic = new byte[MAGIC.length];
        input.readFully(magic);
        if (!java.security.MessageDigest.isEqual(MAGIC, magic)
                || input.readInt() != VERSION) {
            throw new IOException("Stream socket hello ACK is incompatible");
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        StreamControlMessage.text(value, "Stream socket identity");
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > StreamControlMessage.MAX_TEXT_BYTES) {
            throw new IOException("Stream socket identity length is invalid");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
            return StreamControlMessage.text(value, "Stream socket identity");
        } catch (CharacterCodingException | IllegalArgumentException invalid) {
            throw new IOException("Stream socket identity is invalid", invalid);
        }
    }

    private static byte[] readBounded(
            DataInputStream input, int length, int maximum) throws IOException {
        if (length < 0 || length > maximum) {
            throw new IOException("Stream socket frame exceeds its limit");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return bytes;
    }

    private static void writeFrame(
            DataOutputStream output, Object lock, int type, byte[] payload)
            throws IOException {
        synchronized (lock) {
            output.writeInt(type);
            output.writeInt(payload.length);
            output.write(payload);
            output.flush();
        }
    }

    private static void closeQuietly(Socket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException ignored) {
            // Best-effort cleanup after the lane already became terminal.
        }
    }
}
