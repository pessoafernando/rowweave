package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamJobProgress;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamTaskProgressAggregatorTest {

    @Test
    void aggregatesOnlyCurrentFencedPlacementWithoutDoubleCounting() {
        StreamClusterRunState state = state();
        StreamTaskProgressAggregator aggregator = aggregator(state);

        assertTrue(aggregator.offer(sample("source", 0, 1L, true,
                3L, 0L, 30L, 0L, 100L, false,
                2L, 20L, 3, 40L, 50L, 4, 5, 900_000L)));
        assertTrue(aggregator.offer(sample("sink", 0, 1L, false,
                0L, 2L, 0L, 22L, 120L, true,
                1L, 10L, 2, 30L, 20L, 1, 2, 600_000L)));

        StreamTaskProgressAggregate partial = aggregator.snapshot(
                2, 7L, 1, StreamCdcPhase.LOG);
        assertEquals(2, partial.placementTaskCount());
        assertEquals(1, partial.reportedTaskCount());
        assertFalse(partial.complete());
        StreamJobProgress first = partial.progress();
        assertEquals(3L, first.recordsRead());
        assertEquals(2L, first.recordsWritten());
        assertEquals(30L, first.bytesRead());
        assertEquals(22L, first.bytesWritten());
        assertEquals(100L, first.operatorWatermark());
        assertEquals(3L, first.backlogEvents());
        assertEquals(30L, first.backlogBytes());
        assertEquals(3, first.mailboxHighWaterEvents());
        assertEquals(40L, first.channelHighWaterBytes());
        assertEquals(50L, first.managedStateBytes());
        assertEquals(4, first.timerHighWaterCount());
        assertEquals(5, first.windowHighWaterCount());
        assertEquals(1L, first.backpressuredMillis());

        assertFalse(aggregator.offer(sample("source", 0, 1L, true,
                3L, 0L, 30L, 0L, 100L, false,
                2L, 20L, 3, 40L, 50L, 4, 5, 900_000L)));
        assertTrue(aggregator.offer(sample("sink", 0, 2L, true,
                0L, 4L, 0L, 44L, 130L, false,
                0L, 0L, 4, 60L, 25L, 2, 3, 1_100_000L)));

        StreamTaskProgressAggregate complete = aggregator.snapshot(
                2, 7L, 1, StreamCdcPhase.LOG);
        assertTrue(complete.complete());
        assertEquals(2, complete.reportedTaskCount());
        assertEquals(4L, complete.progress().recordsWritten());
        assertEquals(100L, complete.progress().operatorWatermark());
        assertEquals(2L, complete.progress().backlogEvents());
        assertEquals(60L, complete.progress().channelHighWaterBytes());
        assertEquals(2L, complete.progress().backpressuredMillis());
    }

    @Test
    void rejectsStaleUnknownRegressingOverCeilingAndOverflowingSamples() {
        StreamTaskProgressAggregator aggregator = aggregator(state());
        assertFalse(aggregator.offer(sample("source", 0, 1L, true,
                1L, 0L, 1L, 0L, 1L, false,
                0L, 0L, 0, 0L, 101L, 0, 0, 0L)));
        assertFalse(aggregator.offer(sample("other", 0, 1L, true,
                1L, 0L, 1L, 0L, 1L, false,
                0L, 0L, 0, 0L, 0L, 0, 0, 0L)));
        assertFalse(aggregator.offer(new StreamTaskProgress(
                "run", "attempt", 9L, 2L, 4L,
                new StreamTaskIdentity("source", 0), 1L, true,
                1L, 0L, 1L, 0L, 1L, false,
                0L, 0L, 0, 0L, 0L, 0, 0, 0L)));
        assertFalse(aggregator.offer(new StreamTaskProgress(
                "run", "old-attempt", 9L, 3L, 4L,
                new StreamTaskIdentity("source", 0), 1L, true,
                1L, 0L, 1L, 0L, 1L, false,
                0L, 0L, 0, 0L, 0L, 0, 0, 0L)));
        assertFalse(aggregator.offer(new StreamTaskProgress(
                "run", "attempt", 8L, 3L, 4L,
                new StreamTaskIdentity("source", 0), 1L, true,
                1L, 0L, 1L, 0L, 1L, false,
                0L, 0L, 0, 0L, 0L, 0, 0, 0L)));
        assertFalse(aggregator.offer(new StreamTaskProgress(
                "run", "attempt", 9L, 3L, 3L,
                new StreamTaskIdentity("source", 0), 1L, true,
                1L, 0L, 1L, 0L, 1L, false,
                0L, 0L, 0, 0L, 0L, 0, 0, 0L)));

        assertTrue(aggregator.offer(sample("source", 0, 2L, true,
                Long.MAX_VALUE, 0L, Long.MAX_VALUE, 0L, 2L, false,
                0L, 0L, 0, 0L, 0L, 0, 0, 0L)));
        assertFalse(aggregator.offer(sample("source", 0, 3L, true,
                Long.MAX_VALUE - 1L, 0L, Long.MAX_VALUE, 0L, 2L, false,
                0L, 0L, 0, 0L, 0L, 0, 0, 0L)));
        assertFalse(aggregator.offer(sample("sink", 0, 1L, true,
                1L, 0L, 1L, 0L, 2L, false,
                0L, 0L, 0, 0L, 0L, 0, 0, 0L)));
        assertEquals(Long.MAX_VALUE,
                aggregator.snapshot(0, 0L, 0,
                        StreamCdcPhase.NOT_APPLICABLE)
                        .progress().recordsRead());
    }

    private static StreamTaskProgress sample(String operatorUid,
            int subtask, long sequence, boolean complete,
            long recordsRead, long recordsWritten,
            long bytesRead, long bytesWritten,
            long watermark, boolean idle,
            long backlogEvents, long backlogBytes,
            int mailbox, long channelBytes, long stateBytes,
            int timers, int windows, long backpressuredNanos) {
        return new StreamTaskProgress("run", "attempt", 9L, 3L, 4L,
                new StreamTaskIdentity(operatorUid, subtask), sequence,
                complete, recordsRead, recordsWritten, bytesRead,
                bytesWritten, watermark, idle, backlogEvents, backlogBytes,
                mailbox, channelBytes, stateBytes, timers, windows,
                backpressuredNanos);
    }

    private static StreamClusterRunState state() {
        return StreamClusterRunState.initial(3L, "master", "run",
                "attempt", 9L, 4L, 5L,
                new StreamWorkerPlacement(Arrays.asList(
                        new StreamWorkerPlacement.Assignment(
                                0, "source", 0, "worker-a", "inc-a"),
                        new StreamWorkerPlacement.Assignment(
                                1, "sink", 0, "worker-b", "inc-b"))));
    }

    private static StreamTaskProgressAggregator aggregator(
            StreamClusterRunState state) {
        Map<StreamTaskIdentity, StreamTaskProgressAggregator.Limits> limits =
                new LinkedHashMap<>();
        for (StreamWorkerPlacement.Assignment assignment
                : state.placement().assignments()) {
            limits.put(assignment.identity(),
                    new StreamTaskProgressAggregator.Limits(
                            10, 20L, 2_000L, 100L, 10, 10));
        }
        return new StreamTaskProgressAggregator(state, limits);
    }
}
