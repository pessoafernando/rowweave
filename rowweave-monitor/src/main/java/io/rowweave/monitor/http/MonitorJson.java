package io.rowweave.monitor.http;

import io.rowweave.monitor.ArtifactRetentionView;
import io.rowweave.monitor.BatchSinkView;
import io.rowweave.monitor.JobView;
import io.rowweave.monitor.RunRetentionView;
import io.rowweave.monitor.RetryRecoveryView;
import io.rowweave.monitor.StageView;
import io.rowweave.monitor.StreamMonitorView;
import io.rowweave.monitor.WorkerView;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PartitionDistribution;
import io.rowweave.stream.api.StreamJobProgress;

import java.util.List;

/** Compact hand-written monitor JSON with fixed, contract-tested field names. */
final class MonitorJson {
    private MonitorJson() { }

    static String overview(List<JobView> jobs, List<WorkerView> workers) {
        int running = 0;
        int finished = 0;
        int failed = 0;
        int alive = 0;
        for (JobView job : jobs) {
            if ("RUNNING".equals(job.state())) running++;
            else if ("FINISHED".equals(job.state())) finished++;
            else if ("FAILED".equals(job.state())) failed++;
        }
        for (WorkerView worker : workers) {
            if (worker.alive()) alive++;
        }
        return "{\"jobsTotal\":" + jobs.size()
                + ",\"jobsRunning\":" + running
                + ",\"jobsFinished\":" + finished
                + ",\"jobsFailed\":" + failed
                + ",\"workersTotal\":" + workers.size()
                + ",\"workersAlive\":" + alive + "}";
    }

    static String health(List<JobView> jobs, List<WorkerView> workers) {
        return "{\"status\":\"UP\",\"jobs\":" + jobs.size()
                + ",\"workers\":" + workers.size() + "}";
    }

    static String exceptions(List<JobView> jobs) {
        StringBuilder result = new StringBuilder("[");
        boolean first = true;
        for (JobView job : jobs) {
            if (!"FAILED".equals(job.state()) || job.error() == null) continue;
            if (!first) result.append(',');
            first = false;
            result.append("{\"jobId\":\"").append(esc(job.jobId()))
                    .append("\",\"state\":\"FAILED\",\"endTime\":")
                    .append(job.endTime())
                    .append(",\"message\":\"").append(esc(job.error())).append("\"}");
        }
        return result.append(']').toString();
    }

    static String config(MonitorHttpSettings settings, int port) {
        StringBuilder origins = new StringBuilder("[");
        for (int index = 0; index < settings.allowedOrigins().size(); index++) {
            if (index > 0) origins.append(',');
            origins.append('"').append(esc(settings.allowedOrigins().get(index))).append('"');
        }
        origins.append(']');
        return "{\"bindAddress\":\"" + esc(settings.bindAddress())
                + "\",\"port\":" + port
                + ",\"history\":" + settings.history()
                + ",\"cors\":{\"allowCredentials\":false,\"allowedOrigins\":"
                + origins + "}}";
    }

    static String jobs(List<JobView> jobs) {
        StringBuilder result = new StringBuilder("[");
        for (int index = 0; index < jobs.size(); index++) {
            if (index > 0) result.append(',');
            jobSummary(result, jobs.get(index));
        }
        return result.append(']').toString();
    }

    private static void jobSummary(StringBuilder result, JobView job) {
        result.append('{')
                .append("\"jobId\":\"").append(esc(job.jobId())).append("\",")
                .append("\"name\":\"").append(esc(job.name())).append("\",")
                .append("\"state\":\"").append(esc(job.state())).append("\",")
                .append("\"submitTime\":").append(job.submitTime()).append(',')
                .append("\"endTime\":").append(job.endTime()).append(',')
                .append("\"stageCount\":").append(job.stages().size()).append(',')
                .append("\"desiredP\":").append(job.desiredParallelism()).append(',')
                .append("\"effectiveP\":").append(job.effectiveParallelism()).append(',')
                .append("\"downgraded\":").append(job.downgraded()).append(',')
                .append("\"error\":").append(jsonText(job.error()))
                .append('}');
    }

    static String jobDetail(JobView job) {
        StringBuilder result = new StringBuilder("{")
                .append("\"jobId\":\"").append(esc(job.jobId())).append("\",")
                .append("\"name\":\"").append(esc(job.name())).append("\",")
                .append("\"state\":\"").append(esc(job.state())).append("\",")
                .append("\"submitTime\":").append(job.submitTime()).append(',')
                .append("\"endTime\":").append(job.endTime()).append(',')
                .append("\"desiredP\":").append(job.desiredParallelism()).append(',')
                .append("\"effectiveP\":").append(job.effectiveParallelism()).append(',')
                .append("\"downgraded\":").append(job.downgraded()).append(',')
                .append("\"error\":").append(jsonText(job.error())).append(',')
                .append("\"batchSink\":");
        batchSink(result, job.batchSink());
        result.append(",\"executionTelemetry\":");
        executionTelemetry(result, job.executionReport());
        result.append(",\"stream\":");
        stream(result, job.stream());
        result.append(",\"dag\":").append(safeJson(job.dagJson()))
                .append(",\"stages\":[");
        for (int index = 0; index < job.stages().size(); index++) {
            if (index > 0) result.append(',');
            stage(result, job.stages().get(index));
        }
        return result.append("]}").toString();
    }

    private static void batchSink(StringBuilder result, BatchSinkView view) {
        if (view == null) {
            result.append("null");
            return;
        }
        result.append('{')
                .append("\"sinkCount\":").append(view.sinkCount()).append(',')
                .append("\"capabilityProfiles\":").append(stringArray(view.capabilityProfiles())).append(',')
                .append("\"commitDomainDisposition\":\"").append(esc(view.commitDomainDisposition())).append("\",")
                .append("\"descriptorDigestPrefixes\":").append(stringArray(view.descriptorDigestPrefixes())).append(',')
                .append("\"locatorClosureCount\":").append(view.locatorClosureCount()).append(',')
                .append("\"outcomeState\":\"").append(esc(view.outcomeState())).append("\"}")
        ;
    }

    private static void executionTelemetry(StringBuilder result, ExecutionReport report) {
        if (report == null) {
            result.append("null");
            return;
        }
        result.append('{')
                .append("\"resourceCoverage\":\"").append(report.resourceTelemetryCoverage()).append("\",")
                .append("\"managedPeakReservedBytes\":").append(report.peakReservedBytes()).append(',')
                .append("\"managedAcquiredBytes\":").append(report.acquiredBytes()).append(',')
                .append("\"spillCount\":").append(report.spillCount()).append(',')
                .append("\"spillRunCount\":").append(report.spillRunCount()).append(',')
                .append("\"spillLogicalBytesWritten\":").append(report.spillLogicalBytesWritten()).append(',')
                .append("\"spillPeakReservedBytes\":").append(report.spillPeakReservedBytes()).append(',')
                .append("\"rowsOut\":").append(report.rowsOut()).append(',')
                .append("\"partitionCoverage\":\"").append(report.partitionTelemetryCoverage()).append("\",")
                .append("\"budgetStatus\":\"").append(report.budgetStatus()).append("\",")
                .append("\"partitionDistributions\":").append(distributions(report.partitionDistributions()))
                .append("}");
    }

    private static String distributions(List<PartitionDistribution> distributions) {
        StringBuilder result = new StringBuilder("[");
        for (int index = 0; index < distributions.size(); index++) {
            PartitionDistribution distribution = distributions.get(index);
            if (index > 0) result.append(',');
            result.append("{\"logicalStage\":").append(distribution.logicalStage())
                    .append(",\"role\":\"").append(distribution.role()).append("\",")
                    .append("\"parallelism\":").append(distribution.parallelism())
                    .append(",\"coverage\":\"").append(distribution.coverage()).append("\",")
                    .append("\"presence\":").append(booleanArray(distribution.presenceBitmap()))
                    .append(",\"rowCounts\":").append(longArray(distribution.rowCounts()))
                    .append('}');
        }
        return result.append(']').toString();
    }

    private static void stream(StringBuilder result, StreamMonitorView view) {
        if (view == null) {
            result.append("null");
            return;
        }
        result.append('{')
                .append("\"available\":").append(view.available()).append(',')
                .append("\"complete\":").append(view.complete()).append(',')
                .append("\"runId\":").append(jsonText(view.runId())).append(',')
                .append("\"attemptId\":").append(jsonText(view.attemptId())).append(',')
                .append("\"attemptOrdinal\":").append(view.attemptOrdinal()).append(',')
                .append("\"latestCompletedCheckpointId\":").append(jsonText(view.latestCompletedCheckpointId())).append(',')
                .append("\"latestCheckpointDurationMillis\":").append(view.latestCheckpointDurationMillis()).append(',')
                .append("\"latestCheckpointBytes\":").append(view.latestCheckpointBytes()).append(',')
                .append("\"placementTaskCount\":").append(view.placementTaskCount()).append(',')
                .append("\"reportedTaskCount\":").append(view.reportedTaskCount()).append(',')
                .append("\"observedAtEpochMillis\":").append(view.observedAtEpochMillis()).append(',')
                .append("\"attempt\":");
        progress(result, view.attemptProgress());
        result.append(",\"lifetime\":");
        progress(result, view.lifetimeProgress());
        result.append('}');
    }

    private static void progress(StringBuilder result, StreamJobProgress progress) {
        result.append('{')
                .append("\"attempt\":").append(progress.attempt()).append(',')
                .append("\"completedCheckpoints\":").append(progress.completedCheckpoints()).append(',')
                .append("\"operatorWatermark\":").append(progress.operatorWatermark()).append(',')
                .append("\"cdcPhase\":\"").append(progress.cdcPhase()).append("\",")
                .append("\"restarts\":").append(progress.restarts()).append(',')
                .append("\"recordsRead\":").append(progress.recordsRead()).append(',')
                .append("\"recordsWritten\":").append(progress.recordsWritten()).append(',')
                .append("\"bytesRead\":").append(progress.bytesRead()).append(',')
                .append("\"bytesWritten\":").append(progress.bytesWritten()).append(',')
                .append("\"backlogEvents\":").append(progress.backlogEvents()).append(',')
                .append("\"backlogBytes\":").append(progress.backlogBytes()).append(',')
                .append("\"mailboxHighWaterEvents\":").append(progress.mailboxHighWaterEvents()).append(',')
                .append("\"channelHighWaterBytes\":").append(progress.channelHighWaterBytes()).append(',')
                .append("\"managedStateBytes\":").append(progress.managedStateBytes()).append(',')
                .append("\"timerHighWaterCount\":").append(progress.timerHighWaterCount()).append(',')
                .append("\"windowHighWaterCount\":").append(progress.windowHighWaterCount()).append(',')
                .append("\"backpressuredMillis\":").append(progress.backpressuredMillis())
                .append('}');
    }

    private static void stage(StringBuilder result, StageView stage) {
        result.append('{')
                .append("\"index\":").append(stage.index()).append(',')
                .append("\"role\":\"").append(esc(stage.role())).append("\",")
                .append("\"state\":\"").append(esc(stage.state())).append("\",")
                .append("\"opChain\":\"").append(esc(stage.opChain())).append("\",")
                .append("\"inputRows\":").append(stage.inputRows()).append(',')
                .append("\"outputRows\":").append(stage.outputRows()).append(',')
                .append("\"workerId\":").append(jsonText(stage.workerId())).append(',')
                .append("\"error\":").append(jsonText(stage.error())).append(',')
                .append("\"startTime\":").append(stage.startTime()).append(',')
                .append("\"endTime\":").append(stage.endTime()).append(',')
                .append("\"parallelism\":").append(stage.parallelism()).append(',')
                .append("\"sourceSplitCount\":").append(stage.sourceSplitCount()).append(',')
                .append("\"sourceReplayMode\":").append(jsonText(stage.sourceReplayMode())).append(',')
                .append("\"sourceDescriptorDigestPrefix\":").append(jsonText(stage.sourceDescriptorDigestPrefix())).append(',')
                .append("\"inputs\":").append(intArray(stage.inputs())).append(',')
                .append("\"inputChannels\":").append(stringArray(stage.inputChannels())).append(',')
                .append("\"instances\":[");
        for (int index = 0; index < stage.instances().size(); index++) {
            StageView.InstanceView instance = stage.instances().get(index);
            if (index > 0) result.append(',');
            result.append("{\"i\":").append(instance.instanceIndex)
                    .append(",\"worker\":\"").append(esc(instance.workerId)).append("\",")
                    .append("\"state\":\"").append(esc(instance.state)).append("\",")
                    .append("\"inputRows\":").append(instance.inputRows).append(',')
                    .append("\"outputRows\":").append(instance.outputRows).append(',')
                    .append("\"startTime\":").append(instance.startTime).append(',')
                    .append("\"endTime\":").append(instance.endTime).append('}');
        }
        result.append("]}");
    }

    static String workers(List<WorkerView> workers) {
        StringBuilder result = new StringBuilder("[");
        for (int index = 0; index < workers.size(); index++) {
            WorkerView worker = workers.get(index);
            if (index > 0) result.append(',');
            result.append("{\"workerId\":\"").append(esc(worker.workerId()))
                    .append("\",\"host\":\"").append(esc(worker.host()))
                    .append("\",\"port\":").append(worker.port())
                    .append(",\"slots\":").append(worker.slots())
                    .append(",\"freeSlots\":").append(worker.freeSlots())
                    .append(",\"managedMemoryUsedBytes\":").append(worker.managedMemoryUsedBytes())
                    .append(",\"managedMemoryMaxBytes\":").append(worker.managedMemoryMaxBytes())
                    .append(",\"startTimeMillis\":").append(worker.startTimeMillis())
                    .append(",\"alive\":").append(worker.alive()).append('}');
        }
        return result.append(']').toString();
    }

    static String retention(RunRetentionView view) {
        if (view == null) return "{}";
        return "{\"backlog\":" + view.backlog()
                + ",\"lifecycleCapacityPerShard\":" + view.lifecycleCapacityPerShard()
                + ",\"scanPageCapacity\":" + view.scanPageCapacity()
                + ",\"oldestTerminalAgeMillis\":" + view.oldestTerminalAgeMillis()
                + ",\"claimedRuns\":" + view.claimedRuns()
                + ",\"purgedRuns\":" + view.purgedRuns()
                + ",\"errorRuns\":" + view.errorRuns()
                + ",\"cycleComplete\":" + view.cycleComplete()
                + ",\"diagnosticSamples\":" + stringArray(view.diagnosticSamples()) + "}";
    }

    static String artifactRetention(ArtifactRetentionView view) {
        if (view == null) return "{}";
        return "{\"backlog\":" + view.backlog()
                + ",\"scanPageCapacity\":" + view.scanPageCapacity()
                + ",\"inventoryScope\":\"" + esc(view.inventoryScope()) + "\","
                + "\"inventoryObservedShard\":" + view.inventoryObservedShard()
                + ",\"inventoryEntries\":" + view.inventoryEntries()
                + ",\"inventoryBytes\":" + view.inventoryBytes()
                + ",\"inventoryEntryCapacity\":" + view.inventoryEntryCapacity()
                + ",\"inventoryByteCapacity\":" + view.inventoryByteCapacity()
                + ",\"oldestCandidateAgeMillis\":" + view.oldestCandidateAgeMillis()
                + ",\"deletedArtifacts\":" + view.deletedArtifacts()
                + ",\"retainedReferenced\":" + view.retainedReferenced()
                + ",\"staleOwners\":" + view.staleOwners()
                + ",\"errorArtifacts\":" + view.errorArtifacts()
                + ",\"notReadyPasses\":" + view.notReadyPasses()
                + ",\"cycleComplete\":" + view.cycleComplete()
                + ",\"referenceStatus\":\"" + esc(view.referenceStatus()) + "\","
                + "\"inventoryStatus\":\"" + esc(view.inventoryStatus()) + "\","
                + "\"diagnosticSamples\":" + stringArray(view.diagnosticSamples()) + "}";
    }

    static String retryRecovery(RetryRecoveryView view) {
        if (view == null) return "{}";
        return "{\"state\":\"" + esc(view.state()) + "\","
                + "\"activeRuns\":" + view.activeRuns()
                + ",\"waitingRetries\":" + view.waitingRetries()
                + ",\"nextDueDelayBucket\":\"" + esc(view.nextDueDelayBucket()) + "\","
                + "\"scheduledRetries\":" + view.scheduledRetries()
                + ",\"exhaustedRetries\":" + view.exhaustedRetries()
                + ",\"notRetryableClosures\":" + view.notRetryableClosures()
                + ",\"maintenanceCursorHealth\":\"" + esc(view.maintenanceCursorHealth()) + "\","
                + "\"scanPageCapacity\":" + view.scanPageCapacity()
                + ",\"diagnosticSamples\":" + stringArray(view.diagnosticSamples()) + "}";
    }

    private static String safeJson(String value) {
        return value == null || value.trim().isEmpty() ? "{}" : value;
    }

    private static String stringArray(List<String> values) {
        StringBuilder result = new StringBuilder("[");
        for (int index = 0; index < values.size(); index++) {
            if (index > 0) result.append(',');
            result.append('"').append(esc(values.get(index))).append('"');
        }
        return result.append(']').toString();
    }

    private static String intArray(List<Integer> values) {
        return values.toString();
    }

    private static String booleanArray(boolean[] values) {
        StringBuilder result = new StringBuilder("[");
        for (int index = 0; index < values.length; index++) {
            if (index > 0) result.append(',');
            result.append(values[index]);
        }
        return result.append(']').toString();
    }

    private static String longArray(long[] values) {
        StringBuilder result = new StringBuilder("[");
        for (int index = 0; index < values.length; index++) {
            if (index > 0) result.append(',');
            result.append(values[index]);
        }
        return result.append(']').toString();
    }

    private static String jsonText(String value) {
        return value == null ? "null" : "\"" + esc(value) + "\"";
    }

    static String esc(String value) {
        if (value == null) return "";
        StringBuilder result = new StringBuilder(value.length() + 8);
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            switch (current) {
                case '"': result.append("\\\""); break;
                case '\\': result.append("\\\\"); break;
                case '\n': result.append("\\n"); break;
                case '\r': result.append("\\r"); break;
                case '\t': result.append("\\t"); break;
                default:
                    if (current < 0x20) {
                        result.append(String.format("\\u%04x", (int) current));
                    } else {
                        result.append(current);
                    }
            }
        }
        return result.toString();
    }

}
