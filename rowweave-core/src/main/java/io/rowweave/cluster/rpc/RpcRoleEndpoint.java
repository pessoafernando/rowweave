package io.rowweave.cluster.rpc;

/** Lifecycle boundary between a cluster role and its control-plane transport endpoint. */
public interface RpcRoleEndpoint extends AutoCloseable {

    int port();

    void activate();

    boolean hasNetworkListener();

    boolean acceptThreadAlive();

    int handlerThreadCount();

    @Override
    void close();
}
