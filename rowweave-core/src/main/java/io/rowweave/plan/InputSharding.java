package io.rowweave.plan;

import io.rowweave.Source;
import io.rowweave.BatchSourceSplitBinding;

import java.util.ArrayList;
import java.util.List;

/**
 * Distribution of Source[] splits across read units, **shared by** the single-machine router parallelism and
 * the cluster source instance parallelism.
 *
 * <p>It takes the N caller-supplied Sources (each one split) and round-robins them into R = min(parallelism, N)
 * buckets, one bucket per parallel read unit. **Byte-deterministic distribution** is a hard requirement: single
 * machine and cluster call the same function, and the `i % r` assignment keeps results stable across
 * equivalence checks and reruns, since a different split mapping for the same job would change the intermediate
 * partitions and the comparison would fail.
 */
public final class InputSharding {

    private InputSharding() { }

    /** Bucket count R = max(1, min(parallelism, N)). */
    public static int shardCount(int inputCount, int parallelism) {
        return Math.max(1, Math.min(parallelism, inputCount));
    }

    /** Spread N Sources over R = min(parallelism, N) buckets round-robin, deterministically by i%r. */
    public static List<List<Source>> shard(List<Source> inputs, int parallelism) {
        int n = inputs.size();
        int r = shardCount(n, parallelism);
        List<List<Source>> buckets = new ArrayList<>(r);
        for (int i = 0; i < r; i++) {
            buckets.add(new ArrayList<>());
        }
        for (int i = 0; i < n; i++) {
            buckets.get(i % r).add(inputs.get(i));  // round-robin, deterministic
        }
        return buckets;
    }

    /** binding-aware variant with the identical deterministic {@code i % R} assignment. */
    public static List<List<BatchSourceSplitBinding>> shardSourceBindings(
            List<BatchSourceSplitBinding> bindings, int parallelism) {
        int r = shardCount(bindings.size(), parallelism);
        List<List<BatchSourceSplitBinding>> buckets = new ArrayList<>(r);
        for (int i = 0; i < r; i++) buckets.add(new ArrayList<>());
        for (int i = 0; i < bindings.size(); i++) {
            buckets.get(i % r).add(bindings.get(i));
        }
        return buckets;
    }
}
