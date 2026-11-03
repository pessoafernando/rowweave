package io.rowweave;

/** Canonical bounded map scoped to the current key. */
public interface StreamMapState {

    byte[] get(byte[] mapKey);

    boolean contains(byte[] mapKey);

    void put(byte[] mapKey, byte[] value);

    void remove(byte[] mapKey);

    StreamStateCursor entries();

    void clear();
}
