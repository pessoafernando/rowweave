package io.rowweave.cluster.registry;

/**
 * Factory SPI for {@link ServiceRegistry} (following the MonitorLauncher SPI pattern): core defines the interface and depends on no implementation module
 * (narrow dependency boundary: Curator and ZK dependencies are isolated in rowweave-registry-zk). An implementation registers through
 * {@code META-INF/services/io.rowweave.cluster.registry.ServiceRegistryProvider}, and
 * {@link ServiceRegistryBootstrap} discovers it with {@link java.util.ServiceLoader} by {@link #kind()}.
 *
 * <p>ClusterMain chooses the registry from {@code -Drowweave.registry=<kind>}: {@code inprocess} (the default, built into core, no SPI) or
 * {@code zk} (supplied by rowweave-registry-zk, connecting through {@code -Drowweave.zk.connectString}).
 */
public interface ServiceRegistryProvider {

    /** Identifies the registry kind this provider builds (such as {@code "zk"}), matching the value of {@code -Drowweave.registry}. */
    String kind();

    /**
     * Builds the {@link ServiceRegistry} instance.
     * @param connectString the connection string (for ZK, {@code "host:port,host:port,..."}); an implementation interprets it as needed and may ignore it.
     */
    ServiceRegistry create(String connectString);
}
