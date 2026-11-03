package io.rowweave.cluster.rpc;

/** Exact local/peer identity fence shared by socket and in-process transports. */
public final class RpcSecurityFence {
    private RpcSecurityFence() { }

    public static RpcMessage outbound(RpcMessage message, RpcSecurityMetadata expected,
                                      boolean required) {
        if (message == null) throw new IllegalArgumentException("RPC message is required");
        RpcSecurityMetadata actual = message.securityMetadata();
        if (actual == null && expected != null) return message.withSecurityMetadata(expected);
        requireExact(actual, expected, required);
        return message;
    }

    public static RpcMessage inbound(RpcMessage message, RpcSecurityMetadata expected,
                                     boolean required) {
        if (message == null) throw new SecurityException("RPC message is missing");
        requireExact(message.securityMetadata(), expected, required);
        return message;
    }

    private static void requireExact(RpcSecurityMetadata actual,
                                     RpcSecurityMetadata expected, boolean required) {
        if (actual == null) {
            if (required) throw new SecurityException("secure RPC identity is required");
            return;
        }
        if (expected == null
                || !actual.deploymentSetId().equals(expected.deploymentSetId())
                || !actual.authzSetId().equals(expected.authzSetId())) {
            throw new SecurityException("secure RPC deployment/authz identity mismatch");
        }
    }
}
