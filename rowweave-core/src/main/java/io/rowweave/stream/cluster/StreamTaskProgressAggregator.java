package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamResourceProfile;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Fenced, sequence-aware aggregation of task-local cumulative samples. */
final class StreamTaskProgressAggregator {
    private final StreamClusterRunState state;
    private final Map<StreamTaskIdentity, StreamTaskProgress> latest =
            new LinkedHashMap<>();
    private final Map<StreamTaskIdentity, Limits> limits;

    StreamTaskProgressAggregator(StreamClusterRunState state,
            StreamExecutionGraph graph) {
        this(state, physicalLimits(state, graph));
    }

    StreamTaskProgressAggregator(StreamClusterRunState state,
            Map<StreamTaskIdentity, Limits> limits) {
        if (state == null || limits == null) {
            throw new IllegalArgumentException(
                    "Stream progress aggregation inputs are required");
        }
        this.state = state;
        Map<StreamTaskIdentity, Limits> copy = new LinkedHashMap<>();
        for (StreamWorkerPlacement.Assignment assignment
                : state.placement().assignments()) {
            Limits bound = limits.get(assignment.identity());
            if (bound == null) {
                throw new IllegalArgumentException(
                        "Stream progress limit misses a placed task");
            }
            copy.put(assignment.identity(), bound);
        }
        if (copy.size() != limits.size()) {
            throw new IllegalArgumentException(
                    "Stream progress limit contains an unplaced task");
        }
        this.limits = copy;
    }

    synchronized boolean offer(StreamTaskProgress sample) {
        if (!matchesCurrentPlacement(sample) || exceedsCeiling(sample)) {
            return false;
        }
        StreamTaskProgress prior = latest.get(sample.taskIdentity());
        if (prior != null && (sample.sampleSequence()
                <= prior.sampleSequence() || regresses(prior, sample))) {
            return false;
        }
        StreamTaskProgress replaced = latest.put(
                sample.taskIdentity(), sample);
        try {
            aggregate(0, 0L, 0, StreamCdcPhase.NOT_APPLICABLE);
            return true;
        } catch (ArithmeticException overflow) {
            if (replaced == null) latest.remove(sample.taskIdentity());
            else latest.put(sample.taskIdentity(), replaced);
            return false;
        }
    }

    synchronized StreamTaskProgressAggregate snapshot(
            int attempt, long completedCheckpoints, int restarts,
            StreamCdcPhase cdcPhase) {
        return aggregate(attempt, completedCheckpoints, restarts, cdcPhase);
    }

    synchronized Map<StreamTaskIdentity, StreamTaskProgress> samples() {
        return new LinkedHashMap<>(latest);
    }

    private StreamTaskProgressAggregate aggregate(
            int attempt, long completedCheckpoints, int restarts,
            StreamCdcPhase cdcPhase) {
        long recordsRead = 0L;
        long recordsWritten = 0L;
        long bytesRead = 0L;
        long bytesWritten = 0L;
        long backlogEvents = 0L;
        long backlogBytes = 0L;
        long backpressuredNanos = 0L;
        int mailboxHighWaterEvents = 0;
        long channelHighWaterBytes = 0L;
        long managedStateBytes = 0L;
        int timerHighWaterCount = 0;
        int windowHighWaterCount = 0;
        long watermark = Long.MAX_VALUE;
        boolean hasWatermark = false;
        int reported = 0;
        for (StreamTaskProgress sample : latest.values()) {
            Limits bound = limits.get(sample.taskIdentity());
            if (bound.sourceBoundary) {
                recordsRead = Math.addExact(recordsRead, sample.recordsRead());
                bytesRead = Math.addExact(bytesRead, sample.bytesRead());
            }
            if (bound.sinkBoundary) {
                recordsWritten = Math.addExact(recordsWritten, sample.recordsWritten());
                bytesWritten = Math.addExact(bytesWritten, sample.bytesWritten());
            }
            backlogEvents = Math.addExact(
                    backlogEvents, sample.backlogEvents());
            backlogBytes = Math.addExact(
                    backlogBytes, sample.backlogBytes());
            backpressuredNanos = Math.addExact(
                    backpressuredNanos, sample.backpressuredNanos());
            mailboxHighWaterEvents = Math.max(
                    mailboxHighWaterEvents,
                    sample.mailboxHighWaterEvents());
            channelHighWaterBytes = Math.max(
                    channelHighWaterBytes, sample.channelHighWaterBytes());
            managedStateBytes = Math.max(
                    managedStateBytes, sample.managedStateBytes());
            timerHighWaterCount = Math.max(
                    timerHighWaterCount, sample.timerHighWaterCount());
            windowHighWaterCount = Math.max(
                    windowHighWaterCount, sample.windowHighWaterCount());
            if (!sample.watermarkIdle()) {
                watermark = Math.min(watermark,
                        sample.operatorWatermark());
                hasWatermark = true;
            }
            if (sample.complete()) reported++;
        }
        StreamJobProgress progress = new StreamJobProgress(
                attempt, completedCheckpoints,
                hasWatermark ? watermark : Long.MIN_VALUE,
                cdcPhase, restarts, recordsRead, recordsWritten,
                bytesRead, bytesWritten, backlogEvents, backlogBytes,
                mailboxHighWaterEvents, channelHighWaterBytes,
                managedStateBytes, timerHighWaterCount,
                windowHighWaterCount,
                TimeUnit.NANOSECONDS.toMillis(backpressuredNanos));
        return new StreamTaskProgressAggregate(progress,
                state.placement().assignments().size(), reported);
    }

    private boolean matchesCurrentPlacement(StreamTaskProgress sample) {
        if (sample == null || !state.runId().equals(sample.runId())
                || !state.attemptId().equals(sample.attemptId())
                || state.fenceToken() != sample.fenceToken()
                || state.coordinatorTerm() != sample.coordinatorTerm()
                || state.assignmentGeneration()
                != sample.assignmentGeneration()) {
            return false;
        }
        for (StreamWorkerPlacement.Assignment assignment
                : state.placement().assignments()) {
            if (assignment.identity().equals(sample.taskIdentity())) {
                return true;
            }
        }
        return false;
    }

    private boolean exceedsCeiling(StreamTaskProgress sample) {
        Limits bound = limits.get(sample.taskIdentity());
        return bound == null
                || sample.mailboxHighWaterEvents() > bound.mailboxEvents
                || sample.managedStateBytes() > bound.managedStateBytes
                || sample.timerHighWaterCount() > bound.timerCount
                || sample.windowHighWaterCount() > bound.windowCount
                || sample.backlogEvents() > bound.laneEvents
                || sample.backlogBytes() > bound.laneBytes
                || sample.channelHighWaterBytes() > bound.laneBytes;
    }

    private static boolean regresses(StreamTaskProgress prior,
            StreamTaskProgress next) {
        return prior.complete() && !next.complete()
                || next.recordsRead() < prior.recordsRead()
                || next.recordsWritten() < prior.recordsWritten()
                || next.bytesRead() < prior.bytesRead()
                || next.bytesWritten() < prior.bytesWritten()
                || next.mailboxHighWaterEvents()
                < prior.mailboxHighWaterEvents()
                || next.channelHighWaterBytes()
                < prior.channelHighWaterBytes()
                || next.managedStateBytes() < prior.managedStateBytes()
                || next.timerHighWaterCount()
                < prior.timerHighWaterCount()
                || next.windowHighWaterCount()
                < prior.windowHighWaterCount()
                || next.backpressuredNanos() < prior.backpressuredNanos()
                || !prior.watermarkIdle() && !next.watermarkIdle()
                && next.operatorWatermark() < prior.operatorWatermark();
    }

    private static Map<StreamTaskIdentity, Limits> physicalLimits(
            StreamClusterRunState state, StreamExecutionGraph graph) {
        if (state == null || graph == null) {
            throw new IllegalArgumentException(
                    "Stream progress graph and placement are required");
        }
        StreamResourceProfile resources = graph.portableJob().resourceProfile();
        // Admission reserves credit per physical lane, not per placed task.
        // Derive observation ceilings from the same expansion as deployment.
        StreamPhysicalExecutionPlan physical =
                StreamPhysicalExecutionPlan.from(graph, state.placement());
        Map<StreamTaskIdentity, Limits> result = new LinkedHashMap<>();
        for (StreamWorkerPlacement.Assignment assignment
                : physical.tasks()) {
            StreamExecutionGraph.Node node = graph.node(assignment.operatorUid());
            StreamExecutionGraph.ResourceRequest request = node.resourceRequest();
            io.rowweave.stream.contract.StreamOperatorCode opcode = node.portableNode().opcode();
            result.put(assignment.identity(), new Limits(
                    resources.mailboxEvents(), 0L, 0L,
                    Math.max(request.maximumResidentBytes(),
                            request.checkpointBytes()),
                    request.timerCount(), request.windowCount(),
                    opcode == io.rowweave.stream.contract.StreamOperatorCode.SOURCE,
                    opcode == io.rowweave.stream.contract.StreamOperatorCode.SINK));
        }
        for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
            StreamTaskIdentity source = lane.source();
            result.put(source, result.get(source).withLane(
                    resources.mailboxEvents(),
                    lane.logicalEdge().initialCreditBytes()));
        }
        return result;
    }

    static final class Limits {
        final int mailboxEvents;
        final long laneEvents;
        final long laneBytes;
        final long managedStateBytes;
        final int timerCount;
        final int windowCount;
        final boolean sourceBoundary;
        final boolean sinkBoundary;

        Limits(int mailboxEvents, long laneEvents, long laneBytes,
                long managedStateBytes, int timerCount, int windowCount) {
            this(mailboxEvents, laneEvents, laneBytes, managedStateBytes,
                    timerCount, windowCount, true, true);
        }

        Limits(int mailboxEvents, long laneEvents, long laneBytes,
                long managedStateBytes, int timerCount, int windowCount,
                boolean sourceBoundary, boolean sinkBoundary) {
            if (mailboxEvents < 1 || laneEvents < 0L || laneBytes < 0L
                    || managedStateBytes < 0L || timerCount < 0
                    || windowCount < 0) {
                throw new IllegalArgumentException(
                        "Stream task progress limits are invalid");
            }
            this.mailboxEvents = mailboxEvents;
            this.laneEvents = laneEvents;
            this.laneBytes = laneBytes;
            this.managedStateBytes = managedStateBytes;
            this.timerCount = timerCount;
            this.windowCount = windowCount;
            this.sourceBoundary = sourceBoundary;
            this.sinkBoundary = sinkBoundary;
        }

        Limits withLane(int events, long bytes) {
            return new Limits(mailboxEvents,
                    Math.addExact(laneEvents, events),
                    Math.addExact(laneBytes, bytes), managedStateBytes,
                    timerCount, windowCount, sourceBoundary, sinkBoundary);
        }
    }
}
