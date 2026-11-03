package io.rowweave.run;

import java.util.List;

/** A snapshot and its event must become visible atomically at one apply linearization point. */
public interface RunStateStore {
    RunDecision apply(RunCommand command);
    RunSnapshot get(String runId);
    List<ExecutionEvent> events(String runId, long afterSequence, int limit);
}
