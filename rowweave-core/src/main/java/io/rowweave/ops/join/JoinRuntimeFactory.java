package io.rowweave.ops.join;

import io.rowweave.DataSchema;
import io.rowweave.mem.ExecutionMemory;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.ResourceCloser;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.BoundedReplayStore;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.sched.PipelineExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/** One construction and resource-ownership path for local and cluster join consumers. */
public final class JoinRuntimeFactory {

    private static final Logger LOG = LoggerFactory.getLogger(JoinRuntimeFactory.class);

    private JoinRuntimeFactory() {
    }

    public static PipelineExecutor.BinaryStage managed(
            ExecutionMemory executionMemory,
            DataSchema leftMeta, DataSchema rightMeta,
            String[] leftKeys, String[] rightKeys,
            JoinType joinType, JoinPhysicalStrategy strategy,
            int probeRows, int equalKeyBlockThreshold) {
        Objects.requireNonNull(executionMemory, "executionMemory");
        Objects.requireNonNull(strategy, "strategy");
        if (strategy == JoinPhysicalStrategy.AUTO && (probeRows <= 0 || probeRows > 1_000_000)) {
            throw new IllegalArgumentException("probeRows must be in [1,1000000]: " + probeRows);
        }
        if (equalKeyBlockThreshold <= 0) {
            throw new IllegalArgumentException("equalKeyBlockThreshold must be > 0");
        }
        return PipelineExecutor.managedBinaryBreaker(executionMemory, operatorMemory -> {
            SpillStore candidate = executionMemory.newSpillStore();
            if (!(candidate instanceof BoundedReplayStore)) {
                candidate.close();
                throw new IllegalStateException("join runtime requires BoundedReplayStore");
            }
            MemorySegmentPool pool = executionMemory.segmentPool();
            return scoped(leftMeta, rightMeta, leftKeys, rightKeys, joinType, strategy,
                    probeRows, equalKeyBlockThreshold, operatorMemory,
                    (BoundedReplayStore) candidate, pool);
        });
    }

    static PipelineExecutor.BinaryStage scoped(
            DataSchema leftMeta, DataSchema rightMeta,
            String[] leftKeys, String[] rightKeys,
            JoinType joinType, JoinPhysicalStrategy strategy,
            int probeRows, int equalKeyBlockThreshold,
            MemoryManager operatorMemory, BoundedReplayStore store,
            MemorySegmentPool pool) {
        Objects.requireNonNull(strategy, "strategy");
        return (left, right, out) -> {
            Throwable primary = null;
            try {
                switch (strategy) {
                    case HASH: {
                        HashJoin hash = HashJoin.of(leftMeta, rightMeta, leftKeys, rightKeys, joinType)
                                .withSpill(operatorMemory, store, pool);
                        hash.join(left, right, out);
                        diagnostic(strategy, hash.fellBackToSortMerge()
                                ? "HASH_OVERFLOW_TO_SMJ" : "HASH_RIGHT");
                        break;
                    }
                    case SORT_MERGE: {
                        MemoryManager[] sides = operatorMemory.partitionEvenly(2);
                        SortMergeJoin.of(leftMeta, rightMeta, leftKeys, rightKeys, joinType)
                                .withBorrowedSpill(sides[0], sides[1], store, pool)
                                .withEqualKeyBlockThreshold(equalKeyBlockThreshold)
                                .join(left, right, out);
                        diagnostic(strategy, "SORT_MERGE");
                        break;
                    }
                    case AUTO: {
                        AutoJoinSelector selector = new AutoJoinSelector(
                                leftMeta, rightMeta, leftKeys, rightKeys, joinType,
                                probeRows, operatorMemory, store, pool);
                        selector.join(left, right, out);
                        String decision = selector.runtimeDecision();
                        if (decision != null) diagnostic(strategy, decision);
                        break;
                    }
                    default:
                        throw new IllegalStateException("unhandled join strategy: " + strategy);
                }
            } catch (RuntimeException | Error failure) {
                primary = failure;
                throw failure;
            } finally {
                ResourceCloser.close(primary,
                        store,
                        () -> {
                            if (pool.outstanding() != 0) {
                                throw new IllegalStateException("join runtime leaked segments: "
                                        + pool.outstanding());
                            }
                            pool.trim();
                        },
                        () -> {
                            if (operatorMemory.reservedBytes() != 0) {
                                throw new IllegalStateException("join runtime leaked managed memory: "
                                        + operatorMemory.reservedBytes());
                            }
                        });
            }
        };
    }

    private static void diagnostic(JoinPhysicalStrategy strategy, String decision) {
        LOG.info("ROWWEAVE_JOIN_RUNTIME strategy={} decision={}", strategy, decision);
    }
}
