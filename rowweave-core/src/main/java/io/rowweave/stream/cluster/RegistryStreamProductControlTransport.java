package io.rowweave.stream.cluster;

import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.RegistryRoleLocator;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.stream.api.StreamControlException;

/** Resolves the active named Stream Master role for every control call. */
final class RegistryStreamProductControlTransport
        implements StreamProductControlTransport {
    private final RegistryRoleLocator locator;
    private final ControlSecurityConfig security;

    RegistryStreamProductControlTransport(ServiceRegistry registry,
            ControlSecurityConfig security) {
        if (registry == null || security == null) {
            throw new IllegalArgumentException(
                    "Stream registry and client security are required");
        }
        this.locator = new RegistryRoleLocator(registry,
                StreamClusterRoles.MASTER_LEADER_ROLE_ID,
                security.securityMetadata());
        this.security = security;
    }

    @Override
    public StreamProductJobView call(
            StreamProductControlWire.Request request) {
        final Endpoint leader;
        try {
            leader = locator.locateLeader();
        } catch (SecurityException mismatch) {
            throw mismatch;
        } catch (RuntimeException unavailable) {
            throw new StreamControlException(
                    StreamControlException.Code.TRANSPORT_FAILURE,
                    "active Stream Master is unavailable", unavailable);
        }
        return new SocketStreamProductControlTransport(
                leader.host(), leader.port(), security).call(request);
    }
}
