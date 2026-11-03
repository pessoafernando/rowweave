package io.rowweave.cluster.rpc;

/** Authorization roles carried by an mTLS leaf certificate; the trust chain by itself grants no RPC rights. */
public enum ControlRole {
    CLIENT,
    MASTER,
    WORKER
}
