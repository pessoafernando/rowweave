package io.rowweave;

/** Timer callback context with read-only broadcast-state access. */
public interface StreamBroadcastTimerContext extends StreamTimerContext {

    StreamReadOnlyBroadcastState broadcastState(String stateName);
}
