package io.rowweave;

import java.io.Serializable;

/**
 * Portable symmetric two-input keyed process function.
 *
 * <p>Both inputs are partitioned by the same canonical key contract. Their
 * callbacks therefore share the managed state and timer families declared by
 * the enclosing {@code StreamCoProcessSpec}.
 */
public interface StreamKeyedCoProcessFunction extends Serializable {

    void processLeft(DataRow value, StreamProcessContext context,
            StreamCollector output);

    void processRight(DataRow value, StreamProcessContext context,
            StreamCollector output);

    default void onTimer(long timestamp, StreamTimerContext context,
            StreamCollector output) { }
}
