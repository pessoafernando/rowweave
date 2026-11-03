package io.rowweave.cluster;

import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.RpcSecurityMetadata;

import java.util.Objects;

/** Resolves the current holder of one named registry role on every call. */
public final class RegistryRoleLocator implements MasterLocator {
    private final ServiceRegistry registry;
    private final String roleId;
    private final RpcSecurityMetadata expectedSecurityMetadata;

    public RegistryRoleLocator(ServiceRegistry registry, String roleId,
            RpcSecurityMetadata expectedSecurityMetadata) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.roleId = RegistryRoleElection.roleId(roleId);
        this.expectedSecurityMetadata = expectedSecurityMetadata;
    }

    @Override
    public Endpoint locateLeader() {
        RegistryWorker active = registry.worker(roleId);
        if (active == null) {
            throw new IllegalStateException(
                    "no active registry role holder: " + roleId);
        }
        if (expectedSecurityMetadata != null
                && !expectedSecurityMetadata.equals(
                active.securityMetadata())) {
            throw new SecurityException(
                    "registry role deployment/authz identity mismatch");
        }
        return new Endpoint(active.host(), active.port());
    }

    public long termToken() {
        RegistryWorker active = registry.worker(roleId);
        if (active == null) {
            throw new IllegalStateException(
                    "no active registry role holder: " + roleId);
        }
        return RegistryRoleElection.termToken(active);
    }
}
