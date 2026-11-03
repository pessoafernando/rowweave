package io.rowweave.plan;

import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.CollectionSource;
import io.rowweave.JdbcSnapshotSource;
import io.rowweave.JdbcSnapshotReadRequest;
import io.rowweave.JdbcSnapshotSourceProvider;
import io.rowweave.ManifestFileSource;
import io.rowweave.ObjectStorageQualitySource;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;

import java.io.InvalidClassException;
import java.util.HashSet;
import java.util.Set;

/** Fail-closed admission for durable bounded-batch source replay. */
public final class BatchSourcePlanValidator {

    public static final String UNSUPPORTED_SOURCE_REPLAY = "UNSUPPORTED_SOURCE_REPLAY";

    private BatchSourcePlanValidator() {
    }

    /** Placeholder sources are stage-internal; every external source split must be a supported exact built-in. */
    public static void validateDurable(PlanGraph graph) {
        validateDurable(graph, null);
    }

    /** Trusted materialization may additionally admit an exact connector from the active deployment set. */
    public static void validateDurable(PlanGraph graph, ObjectTrustContext trustContext) {
        if (graph == null) {
            throw new IllegalArgumentException("batch source plan is required");
        }
        ObjectStorageQualityPlanValidator.validateFullGraph(graph);
        for (int nodeIndex = 0; nodeIndex < graph.nodeCount(); nodeIndex++) {
            PlanNode source = graph.node(nodeIndex);
            if (source.kind() != PlanNode.Kind.SOURCE || source.inputs().isEmpty()) {
                continue;
            }
            source.validateCurrentSourceBindings();
            validateJdbcGroup(source, nodeIndex);
            for (int splitIndex = 0;
                 splitIndex < source.sourceSplitBindings().size(); splitIndex++) {
                BatchSourceSplitBinding binding =
                        source.sourceSplitBindings().get(splitIndex);
                BatchSourceSplitDescriptor descriptor = binding.descriptor();
                if (descriptor.replayGuarantee()
                        != BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START) {
                    throw unsupported(source, nodeIndex, splitIndex,
                            "source is single-attempt legacy");
                }
                if (binding.input().getClass() == ObjectStorageQualitySource.class) {
                    validateObjectStorageQuality(source, nodeIndex, splitIndex,
                            (ObjectStorageQualitySource) binding.input(),
                            descriptor, trustContext);
                    continue;
                }
                if (binding.input() instanceof ObjectStorageSnapshotSource) {
                    validateObjectStorage(source, nodeIndex, splitIndex,
                            (ObjectStorageSnapshotSource) binding.input(),
                            descriptor, trustContext);
                    continue;
                }
                if (binding.input() instanceof JdbcSnapshotSource) {
                    validateJdbcSnapshot(source, nodeIndex, splitIndex,
                            (JdbcSnapshotSource) binding.input(),
                            descriptor, trustContext);
                    continue;
                }
                if (descriptor.connectorType().startsWith("rowweave.object-storage/")) {
                    throw unsupportedCode(source, nodeIndex, splitIndex,
                            "RESERVED_OBJECT_STORAGE_SOURCE",
                            "reserved object storage capability requires the built-in input");
                }
                if (descriptor.connectorType().startsWith(
                        "rowweave.object-storage-row-quality/")) {
                    throw unsupportedCode(source, nodeIndex, splitIndex,
                            "RESERVED_OBJECT_QUALITY_SOURCE",
                            "reserved Object quality capability requires the built-in wrapper");
                }
                if (descriptor.connectorType().startsWith("rowweave.jdbc-snapshot/")) {
                    throw unsupportedCode(source, nodeIndex, splitIndex,
                            "RESERVED_JDBC_SNAPSHOT_SOURCE",
                            "reserved JDBC snapshot capability requires the built-in input");
                }
                if (!(binding.input() instanceof CollectionSource)
                        && !(binding.input() instanceof ManifestFileSource)) {
                    if (trustContext == null
                            || !trustedDeploymentInput(trustContext, binding.input())) {
                        throw unsupported(source, nodeIndex, splitIndex,
                                "exact connector is not in the active deployment set");
                    }
                }
            }
        }
    }

    private static void validateJdbcGroup(PlanNode source, int nodeIndex) {
        JdbcGroupIdentity group = null;
        Set<String> partitionDigests = new HashSet<>();
        for (int splitIndex = 0;
             splitIndex < source.sourceSplitBindings().size(); splitIndex++) {
            BatchSourceSplitBinding binding =
                    source.sourceSplitBindings().get(splitIndex);
            if (!(binding.input() instanceof JdbcSnapshotSource)) continue;
            JdbcSnapshotSource input = (JdbcSnapshotSource) binding.input();
            JdbcSnapshotReadRequest request = input.readRequest();
            JdbcGroupIdentity current = new JdbcGroupIdentity(
                    binding.descriptor().connectorType(),
                    binding.descriptor().connectorVersion(),
                    request.endpointRef(), request.datasetRef(),
                    request.queryDigest(), request.snapshotId());
            if (group == null) {
                group = current;
            } else if (!group.equals(current)) {
                throw unsupportedCode(source, nodeIndex, splitIndex,
                        "INCOHERENT_JDBC_SNAPSHOT_GROUP",
                        "JDBC splits in one SOURCE must share provider, endpoint, "
                                + "dataset, query, and database snapshot identity");
            }
            if (!partitionDigests.add(request.partitionDigest())) {
                throw unsupportedCode(source, nodeIndex, splitIndex,
                        "DUPLICATE_JDBC_SNAPSHOT_PARTITION",
                        "JDBC partition identity is duplicated in one SOURCE");
            }
        }
    }

    private static void validateObjectStorage(
            PlanNode source, int nodeIndex, int splitIndex,
            ObjectStorageSnapshotSource input,
            BatchSourceSplitDescriptor descriptor,
            ObjectTrustContext trustContext) {
        ObjectStorageSourceProvider provider = input.sourceProvider();
        String expectedType = "rowweave.object-storage/" + provider.providerType();
        if (!expectedType.equals(descriptor.connectorType())
                || provider.providerVersion() != descriptor.connectorVersion()) {
            throw unsupportedCode(source, nodeIndex, splitIndex,
                    "INVALID_OBJECT_STORAGE_SOURCE",
                    "provider identity does not match the frozen descriptor");
        }
        if (trustContext == null) {
            throw unsupportedCode(source, nodeIndex, splitIndex,
                    "UNTRUSTED_OBJECT_STORAGE_PROVIDER",
                    "provider requires an exact trust context");
        }
        try {
            trustContext.verifyRootForWrite(provider, ObjectStorageSourceProvider.class);
        } catch (InvalidClassException rejected) {
            throw unsupportedCode(source, nodeIndex, splitIndex,
                    "UNTRUSTED_OBJECT_STORAGE_PROVIDER",
                    "provider is not in the active deployment set");
        }
    }

    private static void validateObjectStorageQuality(
            PlanNode source, int nodeIndex, int splitIndex,
            ObjectStorageQualitySource input,
            BatchSourceSplitDescriptor descriptor,
            ObjectTrustContext trustContext) {
        ObjectStorageSnapshotSource original = input.originalSource();
        ObjectStorageSourceProvider provider = original.sourceProvider();
        String expectedType = "rowweave.object-storage-row-quality/"
                + provider.providerType();
        if (!expectedType.equals(descriptor.connectorType())
                || descriptor.connectorVersion() != 1) {
            throw unsupportedCode(source, nodeIndex, splitIndex,
                    "INVALID_OBJECT_QUALITY_SOURCE",
                    "quality wrapper identity does not match its underlying provider");
        }
        validateObjectStorage(source, nodeIndex, splitIndex, original,
                original.batchSourceSplit(), trustContext);
    }

    private static void validateJdbcSnapshot(
            PlanNode source, int nodeIndex, int splitIndex,
            JdbcSnapshotSource input,
            BatchSourceSplitDescriptor descriptor,
            ObjectTrustContext trustContext) {
        JdbcSnapshotSourceProvider provider = input.sourceProvider();
        String expectedType = "rowweave.jdbc-snapshot/" + provider.providerType();
        if (!expectedType.equals(descriptor.connectorType())
                || provider.providerVersion() != descriptor.connectorVersion()) {
            throw unsupportedCode(source, nodeIndex, splitIndex,
                    "INVALID_JDBC_SNAPSHOT_SOURCE",
                    "provider identity does not match the frozen descriptor");
        }
        if (trustContext == null) {
            throw unsupportedCode(source, nodeIndex, splitIndex,
                    "UNTRUSTED_JDBC_SNAPSHOT_PROVIDER",
                    "provider requires an exact trust context");
        }
        try {
            trustContext.verifyRootForWrite(
                    provider, JdbcSnapshotSourceProvider.class);
        } catch (InvalidClassException rejected) {
            throw unsupportedCode(source, nodeIndex, splitIndex,
                    "UNTRUSTED_JDBC_SNAPSHOT_PROVIDER",
                    "provider is not in the active deployment set");
        }
    }

    private static boolean trustedDeploymentInput(
            ObjectTrustContext trustContext, io.rowweave.Source input) {
        try {
            trustContext.verifyRootForWrite(input, io.rowweave.Source.class);
            return true;
        } catch (java.io.InvalidClassException rejected) {
            return false;
        }
    }

    private static IllegalArgumentException unsupported(
            PlanNode source, int nodeIndex, int splitIndex, String detail) {
        return unsupportedCode(source, nodeIndex, splitIndex,
                UNSUPPORTED_SOURCE_REPLAY, detail);
    }

    private static IllegalArgumentException unsupportedCode(
            PlanNode source, int nodeIndex, int splitIndex,
            String code, String detail) {
        String operatorId = source.param(PlanNode.OPERATOR_UID_PARAM);
        if (operatorId == null || operatorId.isEmpty()) {
            operatorId = "source-node-" + nodeIndex;
        }
        return new IllegalArgumentException(
                code + " source=" + operatorId
                        + " splitIndex=" + splitIndex + ": " + detail);
    }

    private static final class JdbcGroupIdentity {
        private final String connectorType;
        private final int connectorVersion;
        private final String endpointRef;
        private final String datasetRef;
        private final String queryDigest;
        private final String snapshotId;

        private JdbcGroupIdentity(
                String connectorType, int connectorVersion, String endpointRef,
                String datasetRef, String queryDigest, String snapshotId) {
            this.connectorType = connectorType;
            this.connectorVersion = connectorVersion;
            this.endpointRef = endpointRef;
            this.datasetRef = datasetRef;
            this.queryDigest = queryDigest;
            this.snapshotId = snapshotId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof JdbcGroupIdentity)) return false;
            JdbcGroupIdentity that = (JdbcGroupIdentity) other;
            return connectorVersion == that.connectorVersion
                    && connectorType.equals(that.connectorType)
                    && endpointRef.equals(that.endpointRef)
                    && datasetRef.equals(that.datasetRef)
                    && queryDigest.equals(that.queryDigest)
                    && snapshotId.equals(that.snapshotId);
        }

        @Override
        public int hashCode() {
            int result = connectorType.hashCode();
            result = 31 * result + connectorVersion;
            result = 31 * result + endpointRef.hashCode();
            result = 31 * result + datasetRef.hashCode();
            result = 31 * result + queryDigest.hashCode();
            return 31 * result + snapshotId.hashCode();
        }
    }
}
