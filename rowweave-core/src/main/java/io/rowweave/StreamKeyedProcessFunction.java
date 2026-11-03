package io.rowweave;

import java.io.Serializable;

/**
 * Portable single-input keyed process function.
 *
 * <p>The function can only reach state, timers and outputs declared by its
 * {@code StreamProcessSpec}. Runtime services, threads and checkpoint handles
 * are deliberately absent from this API.
 */
public interface StreamKeyedProcessFunction extends Serializable {

    void processElement(DataRow value, StreamProcessContext context,
            StreamCollector output);

    default void onTimer(long timestamp, StreamTimerContext context,
            StreamCollector output) { }
}
