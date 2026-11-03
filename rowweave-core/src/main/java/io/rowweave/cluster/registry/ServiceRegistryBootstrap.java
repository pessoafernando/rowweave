package io.rowweave.cluster.registry;

import java.util.ServiceLoader;

/**
 * Registry bootstrap (following the MonitorBootstrap SPI pattern): discovers a {@link ServiceRegistryProvider} by {@code kind} through
 * {@link ServiceLoader} and builds the {@link ServiceRegistry} from it, so that core imports no concrete implementation (narrow dependency boundary).
 *
 * <p>{@code kind="inprocess"} (or null or empty) returns {@link InProcessRegistry} directly (built into core, no SPI needed);
 * any other value (such as {@code "zk"}) looks for a matching provider on the classpath and fails fast when none is found, reporting explicitly instead of falling back silently.
 */
public final class ServiceRegistryBootstrap {

    public static final String KIND_INPROCESS = "inprocess";

    private ServiceRegistryBootstrap() { }

    /**
     * Builds the registry.
     * @param kind the registry kind (inprocess, zk, and so on); null or empty selects inprocess.
     * @param connectString the connection string (ignored by inprocess; for zk a list of host:port entries).
     */
    public static ServiceRegistry create(String kind, String connectString) {
        if (kind == null || kind.trim().isEmpty() || KIND_INPROCESS.equalsIgnoreCase(kind.trim())) {
            return new InProcessRegistry();
        }
        String want = kind.trim();
        ServiceLoader<ServiceRegistryProvider> loader = ServiceLoader.load(ServiceRegistryProvider.class);
        for (ServiceRegistryProvider p : loader) {
            if (want.equalsIgnoreCase(p.kind())) {
                return p.create(connectString);
            }
        }
        throw new IllegalStateException("no ServiceRegistryProvider for kind=" + want
                + " (the implementation JAR is missing from the classpath; for example rowweave-registry-zk)");
    }
}
