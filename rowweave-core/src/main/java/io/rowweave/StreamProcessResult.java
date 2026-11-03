package io.rowweave;

import io.rowweave.stream.api.StreamOutputTag;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Public graph views for the main and declared tagged outputs of a process. */
public final class StreamProcessResult {
    private final DataStream main;
    private final Map<String, DataStream> outputs;

    StreamProcessResult(DataStream main,
            java.util.List<StreamOutputTag> tags) {
        this.main = main;
        Map<String, DataStream> views = new LinkedHashMap<>();
        for (StreamOutputTag tag : tags) {
            views.put(tag.tagId(), DataStream.taggedView(main, tag));
        }
        this.outputs = Collections.unmodifiableMap(views);
    }

    public DataStream main() { return main; }

    public DataStream output(String tagId) {
        DataStream output = outputs.get(tagId);
        if (output == null) {
            throw new IllegalArgumentException("unknown process output tag");
        }
        return output;
    }
}
