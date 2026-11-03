package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Deterministic bounded split ownership for source subtasks. */
final class StreamSourceSplitAssigner {
    private StreamSourceSplitAssigner() { }

    static List<List<StreamSourceSplit>> assign(
            StreamSourceBinding binding, int parallelism) {
        if (binding == null || parallelism <= 0 || parallelism > 4_096) {
            throw new IllegalArgumentException(
                    "Stream source binding and parallelism are required");
        }
        List<List<StreamSourceSplit>> result = new ArrayList<>(parallelism);
        for (int index = 0; index < parallelism; index++) {
            result.add(new ArrayList<StreamSourceSplit>());
        }
        List<StreamSourceSplit> splits = binding.fixedSplits();
        for (int index = 0; index < splits.size(); index++) {
            result.get(index % parallelism).add(splits.get(index));
        }
        List<List<StreamSourceSplit>> immutable = new ArrayList<>(parallelism);
        for (List<StreamSourceSplit> subtask : result) {
            immutable.add(Collections.unmodifiableList(subtask));
        }
        return Collections.unmodifiableList(immutable);
    }
}
