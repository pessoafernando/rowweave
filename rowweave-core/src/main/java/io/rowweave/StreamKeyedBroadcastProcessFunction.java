package io.rowweave;

import java.io.Serializable;

/**
 * Portable keyed/broadcast two-input process function.
 *
 * <p>The keyed callback and timers can only read broadcast state. The
 * broadcast callback is the sole mutation authority and has no keyed-state or
 * timer surface.
 */
public interface StreamKeyedBroadcastProcessFunction extends Serializable {

    void processKeyed(DataRow value,
            StreamKeyedBroadcastProcessContext context,
            StreamCollector output);

    void processBroadcast(DataRow value,
            StreamBroadcastProcessContext context,
            StreamCollector output);

    default void onTimer(long timestamp,
            StreamBroadcastTimerContext context,
            StreamCollector output) { }
}
