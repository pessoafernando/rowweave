package io.rowweave.cluster.rpc;

/** Deterministic fail-closed rejection raised before business RPC traffic. */
public final class ProtocolNegotiationException extends IllegalArgumentException {
    private final String code;

    ProtocolNegotiationException(String code, String message) {
        super(code + ": " + message);
        this.code = code;
    }

    public String code() { return code; }
}
