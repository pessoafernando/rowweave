package io.rowweave.cluster.rpc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Linearizes connection registration and close for RpcServer, rejecting and self-closing a late socket. */
final class RpcConnectionRegistry implements AutoCloseable {

    private final Object lifecycle = new Object();
    private final Set<Socket> sockets = new LinkedHashSet<>();
    private boolean closed;

    /** Takes ownership of the socket while OPEN; once CLOSED the caller must not hand off, because the socket is closed before return. */
    boolean register(Socket socket) {
        return register(socket, Integer.MAX_VALUE);
    }

    /** Takes ownership while OPEN and below the connection limit; at capacity or closed the socket is closed before return. */
    boolean register(Socket socket, int maxConnections) {
        if (socket == null) {
            throw new IllegalArgumentException("socket must not be null");
        }
        synchronized (lifecycle) {
            if (!closed && sockets.size() < maxConnections) {
                sockets.add(socket);
                return true;
            }
        }
        closeLate(socket);
        return false;
    }

    /** Idempotently returns ownership once the connection's handler thread has finished. */
    void unregister(Socket socket) {
        if (socket == null) {
            return;
        }
        synchronized (lifecycle) {
            sockets.remove(socket);
        }
    }

    int size() {
        synchronized (lifecycle) {
            return sockets.size();
        }
    }

    boolean isClosed() {
        synchronized (lifecycle) {
            return closed;
        }
    }

    @Override
    public void close() {
        List<Socket> owned;
        synchronized (lifecycle) {
            if (closed) {
                return;
            }
            closed = true;
            owned = new ArrayList<>(sockets);
            sockets.clear();
        }
        IOException firstFailure = null;
        for (Socket socket : owned) {
            try {
                socket.close();
            } catch (IOException failure) {
                if (firstFailure == null) {
                    firstFailure = failure;
                } else {
                    firstFailure.addSuppressed(failure);
                }
            }
        }
        if (firstFailure != null) {
            throw new UncheckedIOException("failed to close RPC connections", firstFailure);
        }
    }

    private static void closeLate(Socket socket) {
        try {
            socket.close();
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to close late RPC connection", failure);
        }
    }
}
