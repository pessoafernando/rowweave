package io.rowweave.cluster.rpc;

import java.util.function.Function;

/**
 * Abstraction over sending one {@link RpcMessage} to an endpoint and getting the response back,
 * decoupled from the concrete transport. Control plane traffic runs per stage and per job
 * rather than per row, so the abstraction costs nothing.<p>Two implementations: {@link
 * RpcClient} uses a socket, which is what a cluster across JVMs uses, and {@link
 * InProcessRpcTransport} is the local path, delivering to the target handler's pool without a
 * socket or serialization.
 */
public interface RpcTransport extends AutoCloseable {

    /** Sends the request and waits for the response, throwing on a timeout or a lost connection instead of hanging. */
    RpcMessage call(RpcMessage request);

    /**
     * Consumes the response while the transport still holds its resident budget. A socket
     * implementation must not let a response carrying a byte[] payload escape this scope; an
     * in-process implementation has no wire materialization, so its default delegates to a
     * plain call.
     */
    default <T> T callScoped(RpcMessage request,
                             Function<? super RpcMessage, ? extends T> consumer) {
        if (consumer == null) throw new IllegalArgumentException("response consumer must not be null");
        return consumer.apply(call(request));
    }

    /** One-way notification with no response awaited, such as a heartbeat. */
    void send(RpcMessage request);

    @Override
    void close();
}
