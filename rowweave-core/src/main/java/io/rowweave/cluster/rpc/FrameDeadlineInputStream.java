package io.rowweave.cluster.rpc;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;

/** Monotonic absolute deadline per frame; byte-by-byte progress cannot extend the total limit. */
final class FrameDeadlineInputStream extends InputStream {
    private final Socket socket;
    private final InputStream source;
    private final long deadlineNanos;
    private long frameDeadline;

    FrameDeadlineInputStream(Socket socket, InputStream source, long deadlineMillis) {
        this.socket = socket;
        this.source = source;
        this.deadlineNanos = TimeUnit.MILLISECONDS.toNanos(deadlineMillis);
    }

    void beginFrame() {
        frameDeadline = System.nanoTime() + deadlineNanos;
    }

    long frameDeadlineNanos() {
        return frameDeadline;
    }

    @Override public int read() throws IOException {
        updateTimeout();
        return source.read();
    }

    @Override public int read(byte[] bytes, int offset, int length) throws IOException {
        updateTimeout();
        return source.read(bytes, offset, length);
    }

    private void updateTimeout() throws IOException {
        long remaining = frameDeadline - System.nanoTime();
        if (remaining <= 0L) throw new SocketTimeoutException("RPC absolute frame deadline exceeded");
        long millis = TimeUnit.NANOSECONDS.toMillis(remaining);
        socket.setSoTimeout((int) Math.max(1L, Math.min(Integer.MAX_VALUE, millis)));
    }
}
