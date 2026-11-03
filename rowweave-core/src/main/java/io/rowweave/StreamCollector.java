package io.rowweave;

import io.rowweave.stream.api.StreamOutputTag;

/** Callback-scoped output for the main and declared tagged ports. */
public interface StreamCollector {

    void collect(DataRow row);

    void collect(StreamOutputTag outputTag, DataRow row);
}
