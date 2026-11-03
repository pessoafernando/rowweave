package io.rowweave;

/** Broadcast map mutable only within the declared broadcast-side callback. */
public interface StreamBroadcastState extends StreamReadOnlyBroadcastState {

    void put(byte[] key, byte[] value);

    void remove(byte[] key);

    void clear();
}
