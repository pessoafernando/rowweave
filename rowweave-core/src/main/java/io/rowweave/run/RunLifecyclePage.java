package io.rowweave.run;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** One terminal lifecycle page bounded by the backend work it costs. */
public final class RunLifecyclePage {
    private final List<RunLifecycleRef> runs;
    private final String nextCursor;
    private final boolean cycleComplete;

    public RunLifecyclePage(List<RunLifecycleRef> runs, String nextCursor,
                            boolean cycleComplete) {
        if (runs == null) throw new IllegalArgumentException("runs is required");
        if (nextCursor == null || nextCursor.isEmpty()) {
            throw new IllegalArgumentException("nextCursor is required");
        }
        this.runs = Collections.unmodifiableList(new ArrayList<>(runs));
        this.nextCursor = nextCursor;
        this.cycleComplete = cycleComplete;
    }

    public List<RunLifecycleRef> runs() { return runs; }
    public String nextCursor() { return nextCursor; }
    public boolean cycleComplete() { return cycleComplete; }
}
