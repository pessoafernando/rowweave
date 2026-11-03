package io.rowweave.stream.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable exact-incarnation placement for one Stream attempt. */
public final class StreamWorkerPlacement {
    public static final class Assignment {
        private final int taskOrdinal;
        private final StreamTaskIdentity identity;
        private final String workerId;
        private final String workerIncarnation;

        Assignment(int taskOrdinal, String workerId,
                   String workerIncarnation) {
            this(taskOrdinal, "legacy-task-" + taskOrdinal, 0,
                    workerId, workerIncarnation);
        }

        Assignment(int taskOrdinal, String operatorUid, int subtaskIndex,
                   String workerId, String workerIncarnation) {
            this.taskOrdinal = taskOrdinal;
            this.identity = new StreamTaskIdentity(operatorUid, subtaskIndex);
            this.workerId = workerId;
            this.workerIncarnation = workerIncarnation;
        }

        public int taskOrdinal() { return taskOrdinal; }
        public StreamTaskIdentity identity() { return identity; }
        public String operatorUid() { return identity.operatorUid(); }
        public int subtaskIndex() { return identity.subtaskIndex(); }
        public String workerId() { return workerId; }
        public String workerIncarnation() { return workerIncarnation; }
    }

    private final List<Assignment> assignments;

    StreamWorkerPlacement(List<Assignment> assignments) {
        this.assignments = Collections.unmodifiableList(
                new ArrayList<>(assignments));
    }

    public List<Assignment> assignments() {
        return assignments;
    }

    public int reservedTasks(String workerId, String workerIncarnation) {
        int count = 0;
        for (Assignment assignment : assignments) {
            if (assignment.workerId.equals(workerId)
                    && assignment.workerIncarnation.equals(workerIncarnation)) {
                count++;
            }
        }
        return count;
    }
}
