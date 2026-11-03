package io.rowweave;

import java.io.Serializable;

/** Portable full-window callback over a bounded, spill-backed row cursor. */
public interface StreamWindowProcessFunction extends Serializable {

    void process(StreamWindowContext context,
            StreamWindowDataRowCursor elements, StreamWindowCollector output);
}
