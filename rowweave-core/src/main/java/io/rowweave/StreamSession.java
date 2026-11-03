package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamTimeMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Public lazy builder for one complete unbounded Stream job graph. */
public final class StreamSession {
    static final class OutputRegistration {
        final DataStream input;
        final String uid;
        final StreamSinkSpec sink;

        OutputRegistration(DataStream input, String uid, StreamSinkSpec sink) {
            this.input = input;
            this.uid = uid;
            this.sink = sink;
        }
    }

    private final String jobName;
    private final String jobTitle;
    private final StreamJobClient jobClient;
    private final List<DataStream> nodes = new ArrayList<>();
    private final List<OutputRegistration> outputs = new ArrayList<>();
    private String definitionId;
    private int parallelism = 1;
    private int maxParallelism = 128;
    private StreamDeliveryGuarantee delivery = StreamDeliveryGuarantee.AT_LEAST_ONCE;
    private StreamChangelogMode changelog = StreamChangelogMode.APPEND;
    private StreamTimeMode time = StreamTimeMode.PROCESSING_TIME;
    private StreamCheckpointConfig checkpoint =
            StreamCheckpointConfig.manualOnly(30_000L, 1);
    private StreamRestartPolicy restart = StreamRestartPolicy.fixedDelay(1, 1_000L);
    private StreamResourceProfile resources = StreamResourceProfile.defaults();
    private String deploymentSetId;

    StreamSession(String jobName, String jobTitle) {
        this(jobName, jobTitle, null);
    }

    StreamSession(String jobName, String jobTitle, StreamJobClient jobClient) {
        if (jobName == null || jobName.isEmpty()) {
            throw new IllegalArgumentException("job name is required");
        }
        this.jobName = jobName;
        this.jobTitle = jobTitle == null ? jobName : jobTitle;
        this.jobClient = jobClient;
        this.definitionId = jobName;
    }

    public DataStream fromSource(StreamSourceSpec source) {
        if (source == null) throw new IllegalArgumentException("source is required");
        DataStream stream = DataStream.source(this, source);
        nodes.add(stream);
        return stream;
    }

    void registerNode(DataStream stream) { nodes.add(stream); }

    void registerOutput(DataStream input, String uid, StreamSinkSpec sink) {
        if (input == null || input.session() != this) {
            throw new IllegalArgumentException("output input belongs to another session");
        }
        if (uid == null || uid.isEmpty() || sink == null) {
            throw new IllegalArgumentException("output UID and sink are required");
        }
        outputs.add(new OutputRegistration(input, uid, sink));
    }

    public void setDefinitionId(String definitionId) {
        if (definitionId == null || definitionId.isEmpty()) {
            throw new IllegalArgumentException("definition ID is required");
        }
        this.definitionId = definitionId;
    }
    public void setParallelism(int parallelism) { this.parallelism = parallelism; }
    public void setMaxParallelism(int maxParallelism) { this.maxParallelism = maxParallelism; }
    public void setDeliveryGuarantee(StreamDeliveryGuarantee delivery) {
        if (delivery == null) throw new IllegalArgumentException("delivery is required");
        this.delivery = delivery;
    }
    public void setChangelogMode(StreamChangelogMode changelog) {
        if (changelog == null) throw new IllegalArgumentException("changelog is required");
        this.changelog = changelog;
    }
    public void setTimeMode(StreamTimeMode time) {
        if (time == null) throw new IllegalArgumentException("time mode is required");
        this.time = time;
    }
    public void setCheckpointConfig(StreamCheckpointConfig checkpoint) {
        if (checkpoint == null) throw new IllegalArgumentException("checkpoint is required");
        this.checkpoint = checkpoint;
    }
    public void setRestartPolicy(StreamRestartPolicy restart) {
        if (restart == null) throw new IllegalArgumentException("restart is required");
        this.restart = restart;
    }
    public void setResourceProfile(StreamResourceProfile resources) {
        if (resources == null) throw new IllegalArgumentException("resources are required");
        this.resources = resources;
    }

    /** Binds this complete Stream graph to one preinstalled signed set. */
    public void setDeploymentSetId(String deploymentSetId) {
        if (deploymentSetId == null
                || !deploymentSetId.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(
                    "deployment set ID must be 64 lowercase hex characters");
        }
        this.deploymentSetId = deploymentSetId;
    }

    public PlanArtifact toPlanArtifact() { return StreamPlanCompiler.compile(this); }

    public StreamJobHandle submit(
            io.rowweave.stream.api.StreamSubmissionId submissionId) {
        if (submissionId == null) {
            throw new IllegalArgumentException("submission ID is required");
        }
        PlanArtifact artifact = toPlanArtifact();
        if (jobClient == null) {
            throw new io.rowweave.stream.api.StreamApiException(
                    io.rowweave.stream.api.StreamApiException.Code.CLIENT_REQUIRED,
                    "StreamSession requires a StreamJobClient for submission");
        }
        return jobClient.submit(submissionId, artifact);
    }

    String definitionId() { return definitionId; }
    int parallelism() { return parallelism; }
    int maxParallelism() { return maxParallelism; }
    StreamDeliveryGuarantee delivery() { return delivery; }
    StreamChangelogMode changelog() { return changelog; }
    StreamTimeMode time() { return time; }
    StreamCheckpointConfig checkpoint() { return checkpoint; }
    StreamRestartPolicy restart() { return restart; }
    StreamResourceProfile resources() { return resources; }
    String deploymentSetId() { return deploymentSetId; }
    List<OutputRegistration> outputs() {
        return Collections.unmodifiableList(outputs);
    }
    List<DataStream> nodes() { return Collections.unmodifiableList(nodes); }
}
