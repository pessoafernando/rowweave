package io.rowweave.stream.cluster;

import io.rowweave.stream.runtime.StreamChannelOutput;

/** Physical route that crosses from the synchronous DAG into a mailbox task. */
final class StreamDagBoundaryRoute {
    final String producerUid;
    final String outputTagId;
    final StreamChannelOutput output;

    StreamDagBoundaryRoute(String producerUid, String outputTagId,
            StreamChannelOutput output) {
        if (producerUid == null || producerUid.isEmpty()
                || outputTagId != null && outputTagId.isEmpty()
                || output == null) {
            throw new IllegalArgumentException(
                    "Stream DAG boundary route is invalid");
        }
        this.producerUid = producerUid;
        this.outputTagId = outputTagId;
        this.output = output;
    }
}
