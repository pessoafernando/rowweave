package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamJobId;

import java.util.List;
import java.util.function.UnaryOperator;

/** Linearizable storage seam beneath product job lifecycle semantics. */
public interface StreamProductJobStateStore {
    StreamProductJobSnapshot createIfAbsent(
            StreamProductJobSnapshot initial);

    StreamProductJobSnapshot current(StreamJobId jobId);

    List<StreamProductJobSnapshot> all();

    StreamProductJobSnapshot update(StreamJobId jobId,
            UnaryOperator<StreamProductJobSnapshot> mutation);
}
