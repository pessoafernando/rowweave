package io.rowweave;

/** Keyed-side context with read-only access to an exact broadcast version. */
public interface StreamKeyedBroadcastProcessContext
        extends StreamProcessContext {

    StreamReadOnlyBroadcastState broadcastState(String stateName);
}
