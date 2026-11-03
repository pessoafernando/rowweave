package io.rowweave.cluster.rpc;

/** Immutable deployment and authorization identity for the secure wire format. */
public final class RpcSecurityMetadata {
    private final String deploymentSetId;
    private final String authzSetId;

    private RpcSecurityMetadata(String deploymentSetId, String authzSetId) {
        this.deploymentSetId = requireSetId("deploymentSetId", deploymentSetId);
        this.authzSetId = requireSetId("authzSetId", authzSetId);
    }

    public static RpcSecurityMetadata of(String deploymentSetId, String authzSetId) {
        return new RpcSecurityMetadata(deploymentSetId, authzSetId);
    }

    public String deploymentSetId() { return deploymentSetId; }
    public String authzSetId() { return authzSetId; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof RpcSecurityMetadata)) return false;
        RpcSecurityMetadata that = (RpcSecurityMetadata) other;
        return deploymentSetId.equals(that.deploymentSetId)
                && authzSetId.equals(that.authzSetId);
    }

    @Override
    public int hashCode() {
        return 31 * deploymentSetId.hashCode() + authzSetId.hashCode();
    }

    private static String requireSetId(String name, String value) {
        if (value == null || value.length() != 64) {
            throw new IllegalArgumentException(name + " must be exact 64 lowercase hex");
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                throw new IllegalArgumentException(name + " must be exact 64 lowercase hex");
            }
        }
        return value;
    }
}
