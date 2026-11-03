package io.rowweave.cluster.registry.zk;

import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.registry.ServiceRegistryProvider;

/**
 * The ZooKeeper answer to {@link ServiceRegistryProvider}: asking for kind "zk" builds a
 * {@link ZkServiceRegistry} against the given connect string. A provider is how ClusterMain chooses
 * a registry, with {@code -Drowweave.registry=zk} and {@code -Drowweave.zk.connectString=...}, rather
 * than a compiled-in branch. Curator is a dependency of this module alone.
 */
public final class ZkServiceRegistryProvider implements ServiceRegistryProvider {

    @Override
    public String kind() {
        return "zk";
    }

    @Override
    public ServiceRegistry create(String connectString) {
        if (connectString == null || connectString.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "The zk registry requires -Drowweave.zk.connectString=<host:port,...>");
        }
        return new ZkServiceRegistry(connectString.trim());
    }
}
