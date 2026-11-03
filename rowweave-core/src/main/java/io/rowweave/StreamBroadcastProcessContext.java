package io.rowweave;

/** Non-keyed broadcast-side callback context. */
public interface StreamBroadcastProcessContext {

    boolean hasRecordTimestamp();

    long recordTimestamp();

    long currentWatermark();

    long currentProcessingTime();

    StreamBroadcastState broadcastState(String stateName);
}
