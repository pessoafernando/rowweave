package io.rowweave;

/** Callback-scoped identity and mailbox time for one keyed window firing. */
public interface StreamWindowContext {

    /** Defensive row containing only the declared key fields. */
    DataRow currentKey();

    /** Inclusive event/processing timestamp or count ordinal. */
    long windowStart();

    /** Exclusive event/processing timestamp or count ordinal. */
    long windowEnd();

    long currentWatermark();

    /** Monotonic processing time controlled by the Worker mailbox. */
    long currentProcessingTime();
}
