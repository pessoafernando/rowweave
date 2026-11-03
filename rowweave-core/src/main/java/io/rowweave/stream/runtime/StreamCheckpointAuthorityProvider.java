package io.rowweave.stream.runtime;

/** Registry capability exposing shared Stream checkpoint decision authority. */
public interface StreamCheckpointAuthorityProvider {
    StreamCheckpointAuthority streamCheckpointAuthority();
}
