package io.rowweave.cluster;

/**
 * Abstraction over finding the master, so that a client or worker never hardcodes a host and
 * port: {@link#locateLeader()} returns the address of the master currently active, which makes
 * a failover betweenprimary and standby transparent. The two implementations are {@link
 * FixedMasterLocator}, a static addressthat keeps the behaviour of an existing {@code (host,
 * port)} pair, and {@link RegistryMasterLocator},which looks up the leader role record in an
 * {@link io.rowweave.cluster.registry.ServiceRegistry}.<p>Withno active leader this throws
 * {@link IllegalStateException} rather than returning null in silence. Everycall re-resolves,
 * so after a leader fails over the next call sees the new one, which is what makesfailover
 * transparent.
 */
public interface MasterLocator {

    /** Address of the master currently active; with no leader this throws {@link IllegalStateException} rather than failing silently. */
    Endpoint locateLeader();
}
