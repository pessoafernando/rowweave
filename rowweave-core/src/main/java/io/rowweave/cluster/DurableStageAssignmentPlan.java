package io.rowweave.cluster;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.QualityGateAssignmentBinding;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageAssignmentToken;
import io.rowweave.run.SourceSplitAssignment;
import io.rowweave.run.SinkAttemptIdentity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongSupplier;

/** immutable bridge from a PhysicalPlan to durable routing identities. */
final class DurableStageAssignmentPlan {
    static final long MANIFEST_GENERATION = 1L;

    private final StageAssignmentManifest manifest;
    private final Map<Long, StageAssignmentRecord> byLogicalInstance;
    private final Map<Long, StageAssignmentRecord> byRuntimeId;
    private final Map<Long, SourceSplitAssignment> sourceAssignmentsByRuntimeId;
    private final Map<Long, SinkAttemptIdentity> sinkIdentitiesByRuntimeId;
    private final StageAssignmentSnapshot publishedSnapshot;

    private DurableStageAssignmentPlan(StageAssignmentManifest manifest,
                                       Map<Long, StageAssignmentRecord> records,
                                       Map<Long, StageAssignmentRecord> runtimeRecords,
                                       Map<Long, SourceSplitAssignment> sourceAssignments,
                                       Map<Long, SinkAttemptIdentity> sinkIdentities,
                                       StageAssignmentSnapshot publishedSnapshot) {
        this.manifest = manifest;
        this.byLogicalInstance = records;
        this.byRuntimeId = runtimeRecords;
        this.sourceAssignmentsByRuntimeId = sourceAssignments;
        this.sinkIdentitiesByRuntimeId = sinkIdentities;
        this.publishedSnapshot = publishedSnapshot;
    }

    static DurableStageAssignmentPlan create(
            RunAttemptFence fence, String artifactId, PlanGraph graph,
            ExecutionGraph executionGraph, PhysicalPlan physicalPlan,
            List<RegistryWorker> workers, LongSupplier runtimeIds) {
        return create(fence, artifactId, graph, executionGraph, physicalPlan,
                workers, runtimeIds, false);
    }

    static DurableStageAssignmentPlan createIncremental(
            RunAttemptFence fence, String artifactId, PlanGraph graph,
            ExecutionGraph executionGraph, PhysicalPlan physicalPlan,
            List<RegistryWorker> workers, LongSupplier runtimeIds) {
        return create(fence, artifactId, graph, executionGraph, physicalPlan,
                workers, runtimeIds, true);
    }

    private static DurableStageAssignmentPlan create(
            RunAttemptFence fence, String artifactId, PlanGraph graph,
            ExecutionGraph executionGraph, PhysicalPlan physicalPlan,
            List<RegistryWorker> workers, LongSupplier runtimeIds,
            boolean incremental) {
        if (runtimeIds == null) throw new IllegalArgumentException("runtimeIds is required");
        if (incremental) {
            requireIncrementalCapacity(physicalPlan);
        } else {
            requireCapacity(physicalPlan);
        }
        Map<String, RegistryWorker> endpoints = new LinkedHashMap<>();
        for (RegistryWorker worker : workers) {
            RegistryWorker previous = endpoints.put(worker.workerId(), worker);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "duplicate planned workerId: " + worker.workerId());
            }
        }
        Map<Integer, Stage> stages = new HashMap<>();
        for (Stage stage : executionGraph.stages()) stages.put(stage.index(), stage);

        List<StageAssignmentRecord> records = new ArrayList<>(
                physicalPlan.instances().size());
        Map<Long, StageAssignmentRecord> index = new HashMap<>();
        Map<Long, StageAssignmentRecord> runtimeIndex = new HashMap<>();
        Map<Long, SourceSplitAssignment> sourceAssignments = new HashMap<>();
        for (int ordinal = 0; ordinal < physicalPlan.instances().size(); ordinal++) {
            PhysicalPlan.Instance instance = physicalPlan.instances().get(ordinal);
            RegistryWorker endpoint = endpoints.get(instance.workerId());
            if (endpoint == null) {
                throw new IllegalStateException(
                        "physical assignment references an unknown worker: "
                                + instance.workerId());
            }
            Stage stage = stages.get(instance.logicalStage());
            if (stage == null) {
                throw new IllegalStateException(
                        "physical assignment references an unknown logical stage: "
                                + instance.logicalStage());
            }
            StageAssignmentRole role = roleOf(graph, physicalPlan, stage);
            SourceSplitAssignment sourceAssignment = sourceAssignment(
                    artifactId, graph, physicalPlan, stage, instance, role);
            long runtimeId = runtimeIds.getAsLong();
            StageAssignmentRecord record = sourceAssignment == null
                    ? StageAssignmentRecord.create(
                    fence, MANIFEST_GENERATION, ordinal, runtimeId,
                    instance.logicalStage(), instance.instanceIndex(), role,
                    endpoint.workerId(), endpoint.host(), endpoint.port(),
                    endpoint.dataPort(), endpoint.incarnationId())
                    : StageAssignmentRecord.create(
                    fence, MANIFEST_GENERATION, ordinal, runtimeId,
                    instance.logicalStage(), instance.instanceIndex(), role,
                    endpoint.workerId(), endpoint.host(), endpoint.port(),
                    endpoint.dataPort(), endpoint.incarnationId(), sourceAssignment);
            records.add(record);
            index.put(key(instance.logicalStage(), instance.instanceIndex()), record);
            runtimeIndex.put(record.runtimeStageId(), record);
            if (sourceAssignment != null) {
                sourceAssignments.put(record.runtimeStageId(), sourceAssignment);
            }
        }
        if (incremental) {
            if (workers.isEmpty()) {
                throw new IllegalArgumentException(
                        "incremental control assignment requires one Worker");
            }
            RegistryWorker endpoint = workers.get(0);
            int logicalStage = 0;
            for (Stage stage : executionGraph.stages()) {
                logicalStage = Math.max(logicalStage, stage.index() + 1);
            }
            StageAssignmentRecord control = StageAssignmentRecord.create(
                    fence, MANIFEST_GENERATION, records.size(),
                    runtimeIds.getAsLong(), logicalStage, 0,
                    StageAssignmentRole.SYSTEM_CONTROL,
                    endpoint.workerId(), endpoint.host(), endpoint.port(),
                    endpoint.dataPort(), endpoint.incarnationId());
            records.add(control);
            runtimeIndex.put(control.runtimeStageId(), control);
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, artifactId, MANIFEST_GENERATION,
                outputProfile(graph), records,
                qualityGateBinding(graph, executionGraph, records));
        Map<Long, SinkAttemptIdentity> sinkIdentities = new HashMap<>();
        for (StageAssignmentRecord record : records) {
            if (record.role() == StageAssignmentRole.SINK) {
                sinkIdentities.put(record.runtimeStageId(), sinkAttemptIdentity(
                        manifest, record, graph, executionGraph));
            }
        }
        return new DurableStageAssignmentPlan(manifest, index, runtimeIndex,
                sourceAssignments, sinkIdentities, null);
    }

    static void requireCapacity(PhysicalPlan physicalPlan) {
        int count = physicalPlan.instances().size();
        if (count < 1 || count > StageAssignmentManifest.MAX_ASSIGNMENTS) {
            throw new IllegalArgumentException("durable physical assignment count " + count
                    + " exceeds limit " + StageAssignmentManifest.MAX_ASSIGNMENTS);
        }
    }

    private static void requireIncrementalCapacity(PhysicalPlan physicalPlan) {
        int count = physicalPlan.instances().size();
        if (count < 1 || count >= StageAssignmentManifest.MAX_ASSIGNMENTS) {
            throw new IllegalArgumentException(
                    "incremental physical assignment count " + count
                            + " plus control member exceeds limit "
                            + StageAssignmentManifest.MAX_ASSIGNMENTS);
        }
    }

    StageAssignmentManifest manifest() { return manifest; }

    StageAssignmentSnapshot publishedSnapshot() {
        if (publishedSnapshot == null) {
            throw new IllegalStateException("assignment manifest is not published");
        }
        return publishedSnapshot;
    }

    DurableStageAssignmentPlan withPublishedSnapshot(StageAssignmentSnapshot snapshot) {
        if (snapshot == null || !java.security.MessageDigest.isEqual(
                io.rowweave.run.StageAssignmentManifestCodec.encodeRoot(manifest),
                io.rowweave.run.StageAssignmentManifestCodec.encodeRoot(snapshot.manifest()))) {
            throw new IllegalStateException("published assignment snapshot does not match plan");
        }
        return new DurableStageAssignmentPlan(
                manifest, byLogicalInstance, byRuntimeId,
                sourceAssignmentsByRuntimeId, sinkIdentitiesByRuntimeId, snapshot);
    }

    StageAssignmentToken token(long runtimeStageId) {
        if (publishedSnapshot == null) {
            throw new IllegalStateException("assignment manifest is not published");
        }
        StageAssignmentRecord record = byRuntimeId.get(runtimeStageId);
        if (record == null) {
            throw new IllegalStateException(
                    "unplanned durable runtime stage id: " + runtimeStageId);
        }
        return publishedSnapshot.token(record.physicalOrdinal());
    }

    long runtimeStageId(int logicalStage, int instanceIndex) {
        StageAssignmentRecord record = byLogicalInstance.get(key(logicalStage, instanceIndex));
        if (record == null) {
            throw new IllegalStateException("unplanned durable physical instance: "
                    + logicalStage + ":" + instanceIndex);
        }
        return record.runtimeStageId();
    }

    SourceSplitAssignment sourceAssignment(long runtimeStageId) {
        SourceSplitAssignment assignment = sourceAssignmentsByRuntimeId.get(runtimeStageId);
        if (assignment == null) {
            throw new IllegalStateException(
                    "durable stage does not own a source: " + runtimeStageId);
        }
        return assignment;
    }

    SinkAttemptIdentity sinkAttemptIdentity(long runtimeStageId) {
        SinkAttemptIdentity identity = sinkIdentitiesByRuntimeId.get(runtimeStageId);
        if (identity == null) {
            throw new IllegalStateException(
                    "durable stage does not own one Sink identity: " + runtimeStageId);
        }
        return identity;
    }

    byte[] sinkGroupIdentity() {
        try {
            java.security.MessageDigest digest =
                    java.security.MessageDigest.getInstance("SHA-256");
            boolean found = false;
            for (StageAssignmentRecord record : manifest.assignments()) {
                SinkAttemptIdentity identity =
                        sinkIdentitiesByRuntimeId.get(record.runtimeStageId());
                if (identity != null) {
                    digest.update(identity.identityDigest());
                    found = true;
                }
            }
            if (!found) {
                throw new IllegalStateException("durable assignment has no Sink identity");
            }
            return digest.digest();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    static SinkAttemptIdentity sinkAttemptIdentity(
            StageAssignmentManifest manifest, StageAssignmentRecord record,
            PlanGraph graph) {
        return sinkAttemptIdentity(manifest, record, graph,
                ExecutionGraphBuilder.buildWithSinkBoundary(graph));
    }

    private static SinkAttemptIdentity sinkAttemptIdentity(
            StageAssignmentManifest manifest, StageAssignmentRecord record,
            PlanGraph graph, ExecutionGraph executionGraph) {
        if (record.role() != StageAssignmentRole.SINK
                && record.role() != StageAssignmentRole.WHOLE_GRAPH) {
            throw new IllegalArgumentException(
                    "Sink attempt identity requires an output assignment");
        }
        int sinkNodeIndex = sinkNodeIndex(record, graph, executionGraph);
        PlanNode sink = graph.node(sinkNodeIndex);
        return SinkAttemptIdentity.forAssignment(
                manifest, record, operatorId(sink, sinkNodeIndex),
                sink.batchSinkDescriptor());
    }

    private static int sinkNodeIndex(
            StageAssignmentRecord record, PlanGraph graph,
            ExecutionGraph executionGraph) {
        int sinkNodeIndex = -1;
        if (record.role() == StageAssignmentRole.WHOLE_GRAPH) {
            for (int i = 0; i < graph.nodeCount(); i++) {
                if (graph.node(i).kind() == PlanNode.Kind.SINK) {
                    if (sinkNodeIndex >= 0) {
                        throw new IllegalArgumentException(
                                "whole-graph output assignment contains multiple Sink nodes");
                    }
                    sinkNodeIndex = i;
                }
            }
        } else {
            Stage selected = null;
            for (Stage stage : executionGraph.stages()) {
                if (stage.index() == record.logicalStage()) {
                    selected = stage;
                    break;
                }
            }
            if (selected == null || !selected.isSink()) {
                throw new IllegalArgumentException(
                        "Sink assignment logical stage is not a SINK stage");
            }
            for (int member : selected.members()) {
                if (graph.node(member).kind() == PlanNode.Kind.SINK) {
                    if (sinkNodeIndex >= 0) {
                        throw new IllegalArgumentException(
                                "physical SINK stage contains multiple Sink nodes");
                    }
                    sinkNodeIndex = member;
                }
            }
        }
        if (sinkNodeIndex < 0) {
            throw new IllegalArgumentException(
                    "physical SINK stage has no Sink node");
        }
        return sinkNodeIndex;
    }

    private static QualityGateAssignmentBinding qualityGateBinding(
            PlanGraph graph, ExecutionGraph executionGraph,
            List<StageAssignmentRecord> records) {
        Map<String, QualityGateAssignmentBinding.MemberBinding> byRole =
                new HashMap<>();
        byte[] policyDigest = null;
        boolean sawOrdinarySink = false;
        for (StageAssignmentRecord record : records) {
            if (record.role() != StageAssignmentRole.SINK
                    && record.role() != StageAssignmentRole.WHOLE_GRAPH) continue;
            int sinkIndex = sinkNodeIndex(record, graph, executionGraph);
            BatchSinkDescriptor descriptor =
                    graph.node(sinkIndex).batchSinkDescriptor();
            String binding = descriptor.bindingIdentity();
            if (!binding.startsWith("quality-gate-v1:")) {
                sawOrdinarySink = true;
                continue;
            }
            String[] fields = binding.split(":", -1);
            if (fields.length != 3 || fields[2].length() != 64) {
                throw new IllegalArgumentException(
                        "quality Sink binding identity is malformed");
            }
            byte[] currentPolicy = decodeHexDigest(fields[2]);
            if (policyDigest == null) {
                policyDigest = currentPolicy;
            } else if (!java.security.MessageDigest.isEqual(
                    policyDigest, currentPolicy)) {
                throw new IllegalArgumentException(
                        "quality Sink policies differ after plan validation");
            }
            QualityGateAssignmentBinding.MemberBinding previous = byRole.put(
                    fields[1], new QualityGateAssignmentBinding.MemberBinding(
                            record.physicalOrdinal(), fields[1],
                            BatchSinkDescriptorCodec.digest(descriptor)));
            if (previous != null) {
                throw new IllegalArgumentException(
                        "quality Sink role has multiple physical assignments");
            }
        }
        if (policyDigest == null) return null;
        if (sawOrdinarySink) {
            throw new IllegalArgumentException(
                    "quality assignment cannot mix ordinary Sink bindings");
        }
        List<QualityGateAssignmentBinding.MemberBinding> canonical =
                new ArrayList<>(QualityGateAssignmentBinding.MEMBER_COUNT);
        for (String role : QualityGateAssignmentBinding.canonicalRoles()) {
            canonical.add(byRole.get(role));
        }
        return new QualityGateAssignmentBinding(policyDigest, canonical);
    }

    private static byte[] decodeHexDigest(String value) {
        byte[] out = new byte[32];
        for (int i = 0; i < out.length; i++) {
            int high = Character.digit(value.charAt(i * 2), 16);
            int low = Character.digit(value.charAt(i * 2 + 1), 16);
            if (high < 0 || low < 0
                    || Character.toLowerCase(value.charAt(i * 2))
                    != value.charAt(i * 2)
                    || Character.toLowerCase(value.charAt(i * 2 + 1))
                    != value.charAt(i * 2 + 1)) {
                throw new IllegalArgumentException(
                        "quality policy digest must be lowercase SHA-256 hex");
            }
            out[i] = (byte) ((high << 4) | low);
        }
        return out;
    }

    private static SourceSplitAssignment sourceAssignment(
            String artifactId, PlanGraph graph, PhysicalPlan physicalPlan,
            Stage stage, PhysicalPlan.Instance instance, StageAssignmentRole role) {
        if (role == StageAssignmentRole.WHOLE_GRAPH) {
            List<SourceSplitAssignment.SourceGroup> groups = new ArrayList<>();
            for (int nodeIndex = 0; nodeIndex < graph.nodeCount(); nodeIndex++) {
                PlanNode node = graph.node(nodeIndex);
                if (node.kind() == PlanNode.Kind.SOURCE && !node.sourceSplitBindings().isEmpty()) {
                    groups.add(SourceSplitAssignment.SourceGroup.create(
                            operatorId(node, nodeIndex), node.sourceSplitBindings()));
                }
            }
            return SourceSplitAssignment.create(artifactId, instance.logicalStage(),
                    instance.instanceIndex(), groups);
        }
        if (role != StageAssignmentRole.SOURCE_STAGE) return null;
        PlanNode source = graph.node(stage.members().get(0));
        List<List<io.rowweave.BatchSourceSplitBinding>> buckets =
                io.rowweave.plan.InputSharding.shardSourceBindings(
                        source.sourceSplitBindings(),
                        physicalPlan.instancesOf(stage.index()).size());
        List<io.rowweave.BatchSourceSplitBinding> bucket =
                buckets.get(instance.instanceIndex());
        return SourceSplitAssignment.create(artifactId, instance.logicalStage(),
                instance.instanceIndex(), java.util.Collections.singletonList(
                        SourceSplitAssignment.SourceGroup.create(
                                operatorId(source, stage.members().get(0)), bucket)));
    }

    private static String operatorId(PlanNode node, int nodeIndex) {
        String explicit = node.param(PlanNode.OPERATOR_UID_PARAM);
        return explicit == null
                ? String.format(Locale.ROOT, "legacy-op-%06d", nodeIndex) : explicit;
    }

    private static StageAssignmentRole roleOf(
            PlanGraph graph, PhysicalPlan physicalPlan, Stage stage) {
        if (physicalPlan.isWholeGraph()) return StageAssignmentRole.WHOLE_GRAPH;
        if (stage.isSink()) return StageAssignmentRole.SINK;
        if (stage.inputStages().isEmpty()) return StageAssignmentRole.SOURCE_STAGE;
        PlanNode head = graph.node(stage.members().get(0));
        if (head.kind() == PlanNode.Kind.BINARY
                && ("join".equals(head.typeId()) || "coGroup".equals(head.typeId()))) {
            return StageAssignmentRole.JOIN_CONSUMER;
        }
        return StageAssignmentRole.KEYED_OP;
    }

    private static AttemptOutputProfile outputProfile(PlanGraph graph) {
        AttemptOutputProfile selected = null;
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.SINK) continue;
            BatchSinkDescriptor.CapabilityProfile capability =
                    node.batchSinkDescriptor().capabilityProfile();
            AttemptOutputProfile current;
            if (capability
                    == BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1) {
                current = AttemptOutputProfile.DISCARD;
            } else if (capability == BatchSinkDescriptor.CapabilityProfile
                    .MANIFEST_ATOMIC_GROUP_V1) {
                current = AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1;
            } else {
                throw new IllegalArgumentException(
                        "durable assignment has unsupported output profile: "
                                + capability);
            }
            if (selected != null && selected != current) {
                throw new IllegalArgumentException(
                        "durable assignment cannot mix output profiles");
            }
            selected = current;
        }
        if (selected == null) {
            throw new IllegalArgumentException(
                    "durable assignment requires at least one output profile");
        }
        return selected;
    }

    private static long key(int logicalStage, int instanceIndex) {
        return ((long) logicalStage << 32) | (instanceIndex & 0xffffffffL);
    }
}
