package io.rowweave.cluster.rpc;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Factories for socket-backed and process-local role endpoints. */
public final class RpcRoleEndpoints {

    private RpcRoleEndpoints() {
    }

    public static RpcRoleEndpoint socket(RpcServer server) {
        if (server == null) {
            throw new IllegalArgumentException("RPC server is required");
        }
        return new SocketEndpoint(server);
    }

    public static RpcRoleEndpoint inProcess(
            String host, Function<RpcMessage, RpcMessage> handler, int maxHandlers) {
        return new InProcessEndpoint(host, handler, maxHandlers);
    }

    private static final class SocketEndpoint implements RpcRoleEndpoint {
        private final RpcServer server;

        private SocketEndpoint(RpcServer server) {
            this.server = server;
        }

        @Override
        public int port() {
            return server.port();
        }

        @Override
        public void activate() {
            server.activate();
        }

        @Override
        public boolean hasNetworkListener() {
            return !server.isClosedForDiagnostics();
        }

        @Override
        public boolean acceptThreadAlive() {
            return server.acceptThreadAlive();
        }

        @Override
        public int handlerThreadCount() {
            return server.connectionThreadStartCount();
        }

        @Override
        public void close() {
            server.close();
        }
    }

    private static final class InProcessEndpoint implements RpcRoleEndpoint {
        private final String host;
        private final int logicalPort;
        private final Function<RpcMessage, RpcMessage> handler;
        private final int maxHandlers;
        private final AtomicBoolean activated = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();
        private volatile InProcessRpcRegistry.Registration registration;

        private InProcessEndpoint(
                String host, Function<RpcMessage, RpcMessage> handler, int maxHandlers) {
            if (host == null || handler == null) {
                throw new IllegalArgumentException("host and handler are required");
            }
            this.host = host;
            this.logicalPort = InProcessRpcRegistry.allocateLogicalPort();
            this.handler = handler;
            this.maxHandlers = maxHandlers;
        }

        @Override
        public int port() {
            return logicalPort;
        }

        @Override
        public void activate() {
            if (closed.get()) {
                throw new IllegalStateException("in-process RPC endpoint is closed");
            }
            if (activated.compareAndSet(false, true)) {
                try {
                    registration = InProcessRpcRegistry.registerLogical(
                            host, logicalPort, handler, maxHandlers);
                } catch (RuntimeException | Error failure) {
                    activated.set(false);
                    throw failure;
                }
            }
        }

        @Override
        public boolean hasNetworkListener() {
            return false;
        }

        @Override
        public boolean acceptThreadAlive() {
            return false;
        }

        @Override
        public int handlerThreadCount() {
            InProcessRpcRegistry.Registration current = registration;
            return current == null ? 0 : current.handlerThreadCount();
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            InProcessRpcRegistry.Registration current = registration;
            if (current != null) {
                current.close();
            }
        }
    }
}
