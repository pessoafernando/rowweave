package io.rowweave.stream.contract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** Immutable checkpoint task closure frozen at trigger time. */
public final class CheckpointExpectation {
    private final CheckpointIdentity checkpointIdentity;
    private final List<CheckpointExpectedTask> tasks;
    private final Map<String, CheckpointExpectedTask> tasksById;

    public CheckpointExpectation(
            CheckpointIdentity checkpointIdentity,
            List<CheckpointExpectedTask> tasks) {
        if (checkpointIdentity == null || tasks == null || tasks.isEmpty()
                || tasks.size() > CheckpointManifestCodec.MAX_TASK_RECEIPTS) {
            throw StreamContractSupport.failure("INVALID_TASK_SET",
                    "checkpoint.expectation", "bounded task closure is required");
        }
        TreeSet<CheckpointExpectedTask> canonical = new TreeSet<>();
        for (CheckpointExpectedTask task : tasks) {
            if (task == null || !canonical.add(task)) {
                throw StreamContractSupport.failure("DUPLICATE_TASK",
                        "checkpoint.expectation.tasks", "task IDs must be unique");
            }
        }
        this.checkpointIdentity = checkpointIdentity;
        this.tasks = Collections.unmodifiableList(new ArrayList<>(canonical));
        Map<String, CheckpointExpectedTask> index = new LinkedHashMap<>();
        for (CheckpointExpectedTask task : this.tasks) index.put(task.taskId(), task);
        this.tasksById = Collections.unmodifiableMap(index);
    }

    public CheckpointIdentity checkpointIdentity() { return checkpointIdentity; }
    public List<CheckpointExpectedTask> tasks() { return tasks; }

    CheckpointExpectedTask task(String taskId) { return tasksById.get(taskId); }
}
