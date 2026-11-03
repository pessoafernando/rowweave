package io.rowweave;

/** Bounded mailbox context for one keyed process element callback. */
public interface StreamProcessContext {

    /** Defensive row containing only the declared key fields. */
    DataRow currentKey();

    boolean hasRecordTimestamp();

    /** Returns the event timestamp; invalid when {@link #hasRecordTimestamp()} is false. */
    long recordTimestamp();

    long currentWatermark();

    /** Monotonic processing time controlled by the Worker mailbox. */
    long currentProcessingTime();

    StreamTimerService timerService();

    StreamValueState valueState(String stateName);

    StreamMapState mapState(String stateName);

    StreamListState listState(String stateName);

    StreamAggregatingState aggregatingState(String stateName);
}
