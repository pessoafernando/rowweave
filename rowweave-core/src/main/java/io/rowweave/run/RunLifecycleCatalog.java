package io.rowweave.run;

/** Bounded lifecycle authority over every Run, plus terminal discovery. */
public interface RunLifecycleCatalog {
    /** Point lookup; null means only that the identity is certainly absent. */
    RunLifecycleRef lifecycle(String runId);

    /** Returns TERMINAL and DELETING only; both the backend work and the result stay bounded per call. */
    RunLifecyclePage scanLifecycle(String cursor, int limit);
}
