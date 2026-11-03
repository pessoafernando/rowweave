package io.rowweave.incremental;

import io.rowweave.IncrementalSourceSplit;
import io.rowweave.SourceExecutor;
import io.rowweave.JdbcSnapshotReadRequest;

import java.util.List;

/** Worker-local JDBC access; implementations resolve references and secrets locally. */
public interface JdbcIncrementalSourceAccess {
    boolean supports(JdbcIncrementalSourceProfile profile);

    Capture capture(JdbcIncrementalSourceProfile profile,
                    IncrementalCaptureRequest request);

    List<JdbcIncrementalSourceProfile.Partition> materialize(
            JdbcIncrementalSourceProfile profile,
            CapturedTargetReceipt capturedTarget,
            JdbcIncrementalSourceProfile.Snapshot snapshot);

    SourceExecutor<IncrementalSourceSplit> open(
            JdbcIncrementalSourceProfile profile,
            IncrementalSourceSplit input,
            JdbcSnapshotReadRequest request);

    final class Capture {
        private final IncrementalFrontier target;
        private final JdbcIncrementalSourceProfile.Snapshot snapshot;

        public Capture(IncrementalFrontier target,
                       JdbcIncrementalSourceProfile.Snapshot snapshot) {
            if (target == null || snapshot == null) {
                throw new IllegalArgumentException(
                        "JDBC_PROFILE_CAPTURE_INVALID");
            }
            this.target = target;
            this.snapshot = snapshot;
        }

        public IncrementalFrontier target() { return target; }
        public JdbcIncrementalSourceProfile.Snapshot snapshot() {
            return snapshot;
        }
    }
}
