package io.rowweave;

/** Read-only bounded broadcast map visible from keyed and timer callbacks. */
public interface StreamReadOnlyBroadcastState {

    byte[] get(byte[] key);

    boolean contains(byte[] key);

    StreamStateCursor entries();
}
