package io.rowweave.stream.cluster;

import io.rowweave.stream.runtime.StreamChannelOutput;

/** One physical output lane for an operator main or tagged port. */
final class StreamTaskOutput {
    final String outputTagId;
    final StreamChannelOutput output;

    StreamTaskOutput(String outputTagId, StreamChannelOutput output) {
        if (outputTagId != null && outputTagId.isEmpty() || output == null) {
            throw new IllegalArgumentException(
                    "Stream task output route is invalid");
        }
        this.outputTagId = outputTagId;
        this.output = output;
    }
}
