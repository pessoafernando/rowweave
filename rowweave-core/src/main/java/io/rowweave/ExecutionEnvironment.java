package io.rowweave;

import io.rowweave.cluster.JobClient;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.OutputDestinationValidator;
import io.rowweave.plan.OutputCommitValidator;
import io.rowweave.internal.SourceInputSchemas;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.UncheckedIOException;

/**
 * The job session: {@link #fromSource} builds a source DataSet, transforms chain, {@link DataSet#output} registers a sink.
 * {@link #commit()} compiles the whole Job into <b>one complete PlanGraph DAG</b> ({@link #toPlanGraph}, at ExecutionEnvironment level, walked from every sink with
 * identity de-duplication), submits it through {@link JobClient}, and then waits for completion only - it <b>takes no data back</b> (a Sink is written by the Worker at the tail of execution).
 *
 * <p>A single commit always submits multiple sources, multiple SINK nodes and shared subgraphs as one complete graph; the execution
 * entry never splits the graph per sink, and a shared upstream is computed only once.
 */
public final class ExecutionEnvironment {

    private static final class PendingSink {
        final DataSet dataSet;
        final String sinkUid;
        final Sink sink;

        PendingSink(DataSet dataSet, String sinkUid, Sink sink) {
            this.dataSet = dataSet;
            this.sinkUid = sinkUid;
            this.sink = sink;
        }
    }

    private final String jobName;
    private final String jobTitle;
    private final List<PendingSink> sinks = new ArrayList<>();
    /** Advisory callback carrying the metrics of the whole Job execution. A listener exception is swallowed so it cannot pollute the job. */
    @FunctionalInterface
    public interface MetricsListener {
        void onSinkComplete(io.rowweave.plan.ExecutionReport report);
    }

    private final JobClient injectedClient; // explicitly injected client (when non-null, setParallelism does not override it)
    private int parallelism = 1;            // Job parallelism (default 1), compiled into the complete graph
    private boolean canonicalizeOutput = false; // default false: the output multiset is equivalent, and order is whatever OrderBy the developer writes
    private MetricsListener metricsListener;   // null = collect nothing (zero cost)
    private String definitionId;                // null = legacy deterministic derivation
    private boolean objectQualityClassificationClaimed;
    private boolean objectQualityPublicationRegistered;

    public ExecutionEnvironment(String jobName, String jobTitle) {
        this(jobName, jobTitle, null);
    }

    /** An injectable JobClient; by default a submission goes to the process-wide shared InProcess Batch Runtime. */
    public ExecutionEnvironment(String jobName, String jobTitle, JobClient jobClient) {
        this.jobName = jobName;
        this.jobTitle = jobTitle;
        this.injectedClient = jobClient;
    }

    /**
     * Sets Job parallelism (default 1). When P>1 and the graph contains a keyed breaker or a binary join, the keyed
     * shuffle runs in parallel (router to keyed-op x P to a merge). Every keyed stage uses this one P, a fixed value that never greedily expands (determinism).
 *
     * <p>P is a <b>Job property</b>: {@link #toPlanGraph()} compiles it into {@link PlanGraph#parallelism()}, and both
     * the default local path and a cluster <b>read it from the graph</b> ({@code MasterServer} reads
     * {@code graph.parallelism()} as the desired P). So this method <b>takes effect both locally and on a cluster</b>,
     * and P travels to the Workers with the serialized graph. (When the cluster has too few slots, the effective P may be lowered via {@code rowweave.cluster.downgrade}; the result multiset never changes.)
     */
    public void setParallelism(int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be > 0, got " + parallelism);
        }
        this.parallelism = parallelism;
    }

    public int getParallelism() {
        return parallelism;
    }

    /**
     * Sets whether a parallel shuffle canonicalizes at the tail (default false; true makes the result bit-identical to P=1).
     * false is valid <b>only</b> when the downstream ignores order (a pure write to storage): it skips the trailing full-order OrderBy, so the result
     * multiset stays equivalent while rows come out unordered across partitions, saving one budget slice. Note that under the current parallel-shuffle
     * shape constraint (a keyed or binary operator feeding a SINK directly), the tail is the Sink itself and no order-sensitive downstream
     * operator exists (a global Top or OrderBy at the tail would break the shape and take the serial path instead), so false cannot corrupt an order-sensitive result. Applies to the default in-process Runtime only; an explicitly injected client manages this itself.
     */
    public void setCanonicalizeOutput(boolean canonicalize) {
        this.canonicalizeOutput = canonicalize;
    }

    public boolean isCanonicalizeOutput() {
        return canonicalizeOutput;
    }

    /**
     * Sets the advisory metrics callback for the whole Job (optional). The interface method name {@code onSinkComplete} is kept for compatibility,
     * and today it fires exactly once per completed Job graph. null = collect nothing (zero cost).
     */
    public void setMetricsListener(MetricsListener listener) {
        this.metricsListener = listener;
    }

    public String getJobName() {
        return jobName;
    }

    public String getJobTitle() {
        return jobTitle;
    }

    /** Sets the job-definition identity that stays stable across publications; its format is validated once, at the PlanArtifact model boundary. */
    public void setDefinitionId(String definitionId) {
        this.definitionId = definitionId;
    }

    /** Builds a source DataSet from a Source (a Source is a re-traversable Iterable<DataRow>); the node is bound to this session so output/commit work. */
    public DataSet fromSource(Source input) {
        List<Source> validated = SourceInputSchemas.validateAgainstFirst(
                java.util.Collections.singletonList(input), "fromSource");
        return DataSet.source(this, validated.get(0).getSchema(), validated);
    }

    /**
     * Builds <b>one</b> source from several homogeneous Sources: a single logical SOURCE holds the several homogeneous Sources as <b>splits</b>,
     * and the runtime (the physical DAG) assigns those splits to several physical source instances by parallelism (the same rule
     * locally and on a cluster). The API layer does <b>not</b> union-fold them into multiple sources. Every Source must be homogeneous (identical DataSchema). A single Source reduces to {@link #fromSource(Source)}.
     */
    public DataSet fromSource(Source... inputs) {
        List<Source> validated = SourceInputSchemas.validateAgainstFirst(
                inputs == null ? null : java.util.Arrays.asList(inputs.clone()), "fromSource");
        return DataSet.source(this, validated.get(0).getSchema(), validated);
    }

    /**
     * build an Object-only row-quality source. The returned facade can only register
     * the complete valid/rejected/summary Manifest publication.
     */
    public ObjectStorageRowClassification classifyObjectStorageRows(
            String sourceOperatorUid, ObjectStorageSnapshotSource... inputs) {
        try {
            if (inputs == null || inputs.length == 0) {
                throw ObjectStorageQualitySource.invalid(
                        "at least one Object Storage input is required");
            }
            List<ObjectStorageSnapshotSource> originals = new ArrayList<>(inputs.length);
            for (int i = 0; i < inputs.length; i++) {
                if (inputs[i] == null) {
                    throw ObjectStorageQualitySource.invalid(
                            "input[" + i + "] must not be null");
                }
                originals.add(inputs[i]);
            }
            List<Source> schemaInputs = new ArrayList<>(originals.size());
            schemaInputs.addAll(originals);
            List<Source> validated = SourceInputSchemas.validateAgainstFirst(
                    schemaInputs, "classifyObjectStorageRows");
            ObjectStorageSnapshotSource first = originals.get(0);
            ObjectStorageReadRequest group = first.readRequest();
            String providerType = first.providerType();
            int providerVersion = first.providerVersion();
            java.util.Set<String> splitIds = new java.util.HashSet<>();
            List<Source> wrapped = new ArrayList<>(validated.size());
            for (int i = 0; i < originals.size(); i++) {
                ObjectStorageSnapshotSource current = originals.get(i);
                BatchSourceSplitDescriptor descriptor = current.batchSourceSplit();
                ObjectStorageReadRequest request = current.readRequest();
                if (!providerType.equals(current.providerType())
                        || providerVersion != current.providerVersion()
                        || !group.endpointRef().equals(request.endpointRef())
                        || !group.credentialRef().equals(request.credentialRef())
                        || !group.namespace().equals(request.namespace())) {
                    throw ObjectStorageQualitySource.invalid(
                            "Object Storage input group is incoherent");
                }
                if (!splitIds.add(descriptor.splitId())) {
                    throw ObjectStorageQualitySource.invalid(
                            "Object Storage split identity is duplicated");
                }
                wrapped.add(new ObjectStorageQualitySource(
                        sourceOperatorUid, current));
            }
            DataSchema inputMeta = first.getSchema();
            DataSchema carrierMeta = wrapped.get(0).getSchema();
            DataSet carrier = DataSet.source(this, carrierMeta, wrapped)
                    .uid(sourceOperatorUid);
            DataSet valid = carrier.objectQualityRouteSkeleton(
                    inputMeta, ObjectStorageQualitySource.VALID_ROUTE);
            DataSet rejected = carrier.objectQualityRouteSkeleton(
                    ObjectStorageQualitySource.rejectedSchema(),
                    ObjectStorageQualitySource.REJECTED_ROUTE);
            claimObjectStorageQualityClassification();
            return new ObjectStorageRowClassification(valid, rejected);
        } catch (RuntimeException invalid) {
            if (invalid instanceof IllegalArgumentException
                    && invalid.getMessage() != null
                    && invalid.getMessage().startsWith("OBJECT_QUALITY_INVALID:")) {
                throw invalid;
            }
            throw ObjectStorageQualitySource.invalid(
                    "Object Storage classification descriptor is invalid");
        }
    }

    /** Called by DataSet.sinkTo(Sink) to register a sink (package level). */
    void registerSink(DataSet dataSet, Sink sink) {
        registerSink(dataSet, null, sink);
    }

    void registerSink(DataSet dataSet, String sinkUid, Sink sink) {
        if (objectQualityClassificationClaimed) {
            throw ObjectStorageQualitySource.invalid(
                    "Object Storage quality job cannot register an ordinary sink");
        }
        sinks.add(new PendingSink(dataSet, sinkUid, sink));
    }

    /** publish the three prevalidated bindings with one list mutation. */
    void registerTypedQualityPublication(
            DataSet validRows, ManifestFileSink validOutput,
            DataSet rejectedRows, ManifestFileSink rejectedOutput,
            DataSet summaryRows, ManifestFileSink summaryOutput) {
        if (objectQualityClassificationClaimed) {
            throw new IllegalArgumentException(
                    "QUALITY_PUBLICATION_INVALID: Object Storage quality classification "
                            + "reserves the whole Job graph");
        }
        List<PendingSink> publication = new ArrayList<>(3);
        publication.add(new PendingSink(validRows, TypedQualityManifest.VALID_SINK_UID, validOutput));
        publication.add(new PendingSink(rejectedRows, TypedQualityManifest.REJECTED_SINK_UID, rejectedOutput));
        publication.add(new PendingSink(summaryRows, TypedQualityManifest.SUMMARY_SINK_UID, summaryOutput));
        sinks.addAll(publication);
    }

    /** install all three exclusive Object quality sinks with one list mutation. */
    void registerObjectStorageQualityPublication(
            DataSet validRows, ManifestFileSink validOutput,
            DataSet rejectedRows, ManifestFileSink rejectedOutput,
            DataSet summaryRows, ManifestFileSink summaryOutput) {
        if (!objectQualityClassificationClaimed
                || objectQualityPublicationRegistered || !sinks.isEmpty()) {
            throw new IllegalArgumentException(
                    "QUALITY_PUBLICATION_INVALID: Object Storage quality publication "
                            + "must be the only publication in its ExecutionEnvironment");
        }
        List<PendingSink> publication = new ArrayList<>(3);
        publication.add(new PendingSink(
                validRows, TypedQualityManifest.VALID_SINK_UID, validOutput));
        publication.add(new PendingSink(
                rejectedRows, TypedQualityManifest.REJECTED_SINK_UID, rejectedOutput));
        publication.add(new PendingSink(
                summaryRows, TypedQualityManifest.SUMMARY_SINK_UID, summaryOutput));
        sinks.addAll(publication);
        objectQualityPublicationRegistered = true;
    }

    private void claimObjectStorageQualityClassification() {
        if (objectQualityClassificationClaimed || !sinks.isEmpty()) {
            throw ObjectStorageQualitySource.invalid(
                    "Object Storage quality classification must be the only Job graph");
        }
        objectQualityClassificationClaimed = true;
    }

    /**
     * Compiles the whole Job into the <b>complete PlanGraph DAG</b>: it walks the upstream DataSet subtree of every registered sink,
     * de-duplicating by <b>identity</b> (one DataSet instance yields exactly one PlanNode), and wraps a SINK node holding the Sink at the tail of each sink.
     * Several SINK nodes are ordered topologically into <b>one</b> PlanGraph. Fan-out (a node referenced by multiple downstreams) makes a visited hit return the same index, which forms a multi-parent DAG;
     * the executor broadcasts after the shared node, so one submission computes a shared upstream only once.
     */
    public PlanGraph toPlanGraph() {
        if (sinks.isEmpty()) {
            throw new IllegalStateException("no sink registered; call DataSet.sinkTo(..) before commit");
        }
        // Every operator is describable - UNARY and BINARY both become typeId + params + userFunction serializable nodes (there is no in-memory Stage fallback).
        List<PlanNode> nodes = new ArrayList<>();
        IdentityHashMap<DataSet, Integer> visited = new IdentityHashMap<>();
        for (PendingSink registration : sinks) {
            int headIdx = registration.dataSet.buildPlanInto(nodes, visited);
            // Wrap a SINK node holding the Sink at the tail (outMeta passes through the head's meta)
            PlanNode sinkNode = PlanNode.sink(registration.dataSet.dataSchema(), headIdx, registration.sink).build();
            if (registration.sinkUid != null) {
                sinkNode = sinkNode.withParam(PlanNode.OPERATOR_UID_PARAM, registration.sinkUid);
            }
            nodes.add(sinkNode);
        }
        // P is a job property, compiled into the whole graph (both the local path and a cluster read P from the graph).
        return new PlanGraph(nodes, parallelism);
    }

    /** Compiles the complete Job into a versioned Artifact; it opens no Source and executes no Sink. */
    public PlanArtifact toPlanArtifact() {
        return LegacyPlanArtifactAdapter.wrap(effectiveDefinitionId(), toPlanGraph());
    }

    private String effectiveDefinitionId() {
        if (definitionId != null) {
            return definitionId;
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(jobName.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            byte[] hash = digest.digest(jobTitle.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte value : hash) {
                hex.append(Character.forDigit((value >>> 4) & 0x0f, 16));
                hex.append(Character.forDigit(value & 0x0f, 16));
            }
            return "legacy-definition-" + hex;
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    /**
     * <b>One submission for the whole graph</b>: compile the entire Job into one PlanGraph (multiple sources and multiple outputs are all nodes),
     * then call `jobClient.submit(whole graph)` <b>once</b> and wait for completion only - it <b>takes no data back</b> (a Sink is written by the Worker at the tail of execution). A shared subgraph is computed once: both the local and the cluster
     * executor broadcasts at the fan-out point or uses managed materialization as the plan dictates;
     * one commit never re-executes a shared upstream once per sink. For a single-sink graph, submit delegates internally to submitToSink.
     * The Sink is written at the tail of execution. A failure throws.
     */
    public void execute() {
        // P is already compiled into the graph; the default path and a cluster reuse the same Master/Worker execution model.
        PlanGraph full = toPlanGraph();
        // Reject a duplicated exclusive destination once the complete graph exists, before any client, Source or Sink side effect.
        OutputDestinationValidator.validate(full);
        OutputCommitValidator.validate(full);
        PlanArtifact artifact;
        try {
            artifact = LegacyPlanArtifactAdapter.wrap(effectiveDefinitionId(), full);
        } catch (UncheckedIOException nonPortablePlan) {
            // Local compatibility: the Real/Master validator rejects the local-attached format, so a cluster submission is never silently downgraded.
            artifact = LegacyPlanArtifactAdapter.wrapLocal(effectiveDefinitionId(), full);
        }
        // One submission for the whole graph (a shared subgraph is computed once). metricsListener moved from per-sink to per-job (one report per job).
        io.rowweave.plan.ExecutionReport rep = injectedClient != null
                ? injectedClient.submit(artifact)
                : InProcessBatchRuntime.processWide().submit(
                        artifact, canonicalizeOutput);
        if (metricsListener != null && rep != null) {
            try {
                metricsListener.onSinkComplete(rep); // listener exceptions are isolated so they cannot pollute the job
            } catch (RuntimeException ignored) {
                // an advisory listener exception is swallowed: metrics are observation, and must never affect job execution or results
            }
        }
    }
}
