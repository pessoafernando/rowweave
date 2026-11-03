package io.rowweave.artifact;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** The first immutable model of the published plan envelope. */
public final class PlanArtifact {

    public static final int CURRENT_PLAN_SCHEMA_VERSION = 1;
    public static final int CURRENT_READER_VERSION = 1;
    public static final String FEATURE_TRUSTED_LEGACY_PLAN_GRAPH =
            "rowweave.payload.trusted-legacy-plan-graph.v1";
    public static final String FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS =
            "rowweave.operator-id.legacy-positional.v1";
    public static final String FEATURE_EXPLICIT_OPERATOR_IDS =
            "rowweave.operator-id.explicit.v1";
    public static final String FEATURE_LOCAL_ATTACHED_PLAN_GRAPH =
            "rowweave.payload.local-attached-plan-graph.v1";
    public static final String FEATURE_PORTABLE_BATCH_PLAN_V1 =
            "rowweave.payload.portable-batch-plan.v1";
    /** independently versioned Stream plan carrier; not executable by Batch runtimes. */
    public static final String FEATURE_PORTABLE_STREAM_PLAN_V1 =
            "rowweave.payload.portable-stream-plan.v1";
    /** product Stream envelope containing plan plus operational policy. */
    public static final String FEATURE_PORTABLE_STREAM_JOB_V1 =
            "rowweave.payload.portable-stream-job.v1";
    /** dynamic Source enumerator and checkpointed assignment contract. */
    public static final String FEATURE_STREAM_DYNAMIC_SPLIT_DISCOVERY_V2 =
            "rowweave.runtime.stream.dynamic-split-discovery.v2";
    /** complete INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE changelog. */
    public static final String FEATURE_STREAM_RETRACT_CHANGELOG_V2 =
            "rowweave.runtime.stream.retract-changelog.v2";
    /** bounded physical-lane state in unaligned checkpoint cuts. */
    public static final String FEATURE_STREAM_UNALIGNED_CHECKPOINT_V2 =
            "rowweave.runtime.stream.unaligned-checkpoint.v2";
    /** required independently from the portable payload syntax. */
    public static final String FEATURE_RUNTIME_STREAM_V1 =
            "rowweave.runtime.stream.v1";
    /** explicit stable field bindings for product windows. */
    public static final String FEATURE_STREAM_WINDOW_FIELD_BINDING_V1 =
            "rowweave.runtime.stream.window-field-binding.v1";
    /** explicit provider event-time progress contract. */
    public static final String FEATURE_STREAM_SOURCE_TIME_POLICY_V1 =
            "rowweave.runtime.stream.source-time-policy.v1";
    /** explicitly bounded public source reader completion. */
    public static final String FEATURE_STREAM_SOURCE_END_OF_INPUT_V1 =
            "rowweave.runtime.stream.source-end-of-input.v1";
    /** exact resident snapshot-to-log CDC source semantics. */
    public static final String FEATURE_STREAM_RESIDENT_CDC_V1 =
            "rowweave.runtime.stream.resident-cdc.v1";
    /** opaque CDC position and authoritative source acknowledgement. */
    public static final String FEATURE_STREAM_RESIDENT_CDC_V2 =
            "rowweave.runtime.stream.resident-cdc-external-position.v2";
    /** typed portable projection and predicate evaluation. */
    public static final String FEATURE_STREAM_CALC_V1 =
            "rowweave.runtime.stream.calc.v1";
    public static final String FEATURE_STREAM_FLAT_MAP_V1 =
            "rowweave.runtime.stream.flat-map.v1";
    public static final String FEATURE_STREAM_UNION_V1 =
            "rowweave.runtime.stream.union.v1";
    public static final String FEATURE_STREAM_EVENT_TIME_ASSIGNMENT_V1 =
            "rowweave.runtime.stream.event-time-assignment.v1";
    public static final String FEATURE_STREAM_KEYED_PROCESS_V1 =
            "rowweave.runtime.stream.keyed-process.v1";
    public static final String FEATURE_STREAM_KEYED_CO_PROCESS_V1 =
            "rowweave.runtime.stream.keyed-co-process.v1";
    public static final String FEATURE_STREAM_BROADCAST_STATE_V1 =
            "rowweave.runtime.stream.broadcast-state.v1";
    public static final String FEATURE_STREAM_GENERIC_WINDOW_V1 =
            "rowweave.runtime.stream.generic-window.v1";
    public static final String FEATURE_STREAM_TAGGED_OUTPUT_V1 =
            "rowweave.runtime.stream.tagged-output.v1";
    public static final String FEATURE_STREAM_ASYNC_WAIT_V1 =
            "rowweave.runtime.stream.async-wait.v1";
    public static final String FEATURE_STREAM_NODE_EXECUTION_CONTRACT_V1 =
            "rowweave.runtime.stream.node-execution-contract.v1";
    public static final String FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1 =
            "rowweave.runtime.stream.connector-deployment.v1";
    public static final String FEATURE_SINK_PROVIDER_CONFIG_ENVELOPE =
            "rowweave.sink-provider-config-envelope.v1";
    public static final String FEATURE_BOUNDED_INCREMENTAL_FRONTIER =
            "rowweave.bounded-incremental-frontier.v1";
    public static final String DEPLOYMENT_FEATURE_PREFIX = "rowweave.deployment-set-v1:";

    public enum RuntimeTarget {
        BATCH(1), STREAM(2);
        private final int wireId;
        RuntimeTarget(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static RuntimeTarget fromWire(int id, String path) {
            for (RuntimeTarget value : values()) if (value.wireId == id) return value;
            throw ArtifactText.failure("INVALID_SEMANTICS", path, "unknown runtime target: " + id);
        }
    }

    public enum Boundedness {
        BOUNDED(1), UNBOUNDED(2);
        private final int wireId;
        Boundedness(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static Boundedness fromWire(int id, String path) {
            for (Boundedness value : values()) if (value.wireId == id) return value;
            throw ArtifactText.failure("INVALID_SEMANTICS", path, "unknown boundedness: " + id);
        }
    }

    public enum ChangelogMode {
        APPEND(1), UPSERT(2), RETRACT(3);
        private final int wireId;
        ChangelogMode(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static ChangelogMode fromWire(int id, String path) {
            for (ChangelogMode value : values()) if (value.wireId == id) return value;
            throw ArtifactText.failure("INVALID_SEMANTICS", path, "unknown changelog mode: " + id);
        }
    }

    public enum TimeMode {
        NONE(1), PROCESSING_TIME(2), EVENT_TIME(3);
        private final int wireId;
        TimeMode(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static TimeMode fromWire(int id, String path) {
            for (TimeMode value : values()) if (value.wireId == id) return value;
            throw ArtifactText.failure("INVALID_SEMANTICS", path, "unknown time mode: " + id);
        }
    }

    public enum PayloadFormat {
        TRUSTED_LEGACY_PLAN_GRAPH(1),
        /** Only an original-JVM transient reference may execute; after a codec or RPC round trip this must fail closed. */
        LOCAL_ATTACHED_PLAN_GRAPH(2),
        /** Public v1 closed IR; contains no serialized Source/Sink/function object. */
        PORTABLE_BATCH_PLAN_V1(3),
        /** v1 closed IR carrier; payload semantics are admitted by Stream runtime only. */
        PORTABLE_STREAM_PLAN_V1(4),
        /** product Stream job envelope; existing wire IDs remain unchanged. */
        PORTABLE_STREAM_JOB_V1(5);
        private final int wireId;
        PayloadFormat(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static PayloadFormat fromWire(int id, String path) {
            for (PayloadFormat value : values()) if (value.wireId == id) return value;
            throw ArtifactText.failure("INVALID_SEMANTICS", path, "unknown payload format: " + id);
        }
    }

    private final String definitionId;
    private final int planSchemaVersion;
    private final int minimumReaderVersion;
    private final RuntimeTarget runtimeTarget;
    private final Boundedness boundedness;
    private final ChangelogMode changelogMode;
    private final TimeMode timeMode;
    private final List<String> requiredFeatures;
    private final List<String> optionalFeatures;
    private final List<OperatorDescriptor> operators;
    private final PayloadFormat payloadFormat;
    private final byte[] payload;
    private final String artifactId;
    /** Same-JVM compatibility reference, excluded from codec and fingerprint; read only by the adapter once payload and descriptor agree. */
    private final transient Object localPayloadReference;

    public PlanArtifact(
            String definitionId, int planSchemaVersion, int minimumReaderVersion,
            RuntimeTarget runtimeTarget, Boundedness boundedness, ChangelogMode changelogMode,
            TimeMode timeMode, Collection<String> requiredFeatures, Collection<String> optionalFeatures,
            List<OperatorDescriptor> operators, PayloadFormat payloadFormat, byte[] payload) {
        this(definitionId, planSchemaVersion, minimumReaderVersion,
                runtimeTarget, boundedness, changelogMode, timeMode,
                requiredFeatures, optionalFeatures, operators, payloadFormat, payload, false, null, null);
    }

    private PlanArtifact(
            String definitionId, int planSchemaVersion, int minimumReaderVersion,
            RuntimeTarget runtimeTarget, Boundedness boundedness, ChangelogMode changelogMode,
            TimeMode timeMode, Collection<String> requiredFeatures, Collection<String> optionalFeatures,
            List<OperatorDescriptor> operators, PayloadFormat payloadFormat, byte[] payload,
            boolean takePayloadOwnership, String verifiedArtifactId, Object localPayloadReference) {
        ArtifactText.encode(definitionId, "plan.definitionId", PlanArtifactLimits.MAX_TEXT_BYTES);
        if (planSchemaVersion <= 0 || minimumReaderVersion <= 0) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "plan.version", "versions must be positive");
        }
        if (runtimeTarget == null || boundedness == null || changelogMode == null || timeMode == null) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "plan.semantics", "plan semantics are required");
        }
        if (payloadFormat == null || payload == null) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "payload", "payload format and bytes are required");
        }
        if (payload.length > PlanArtifactLimits.MAX_PAYLOAD_BYTES) {
            throw ArtifactText.failure("LIMIT_EXCEEDED", "payload.bytes", "payload exceeds limit");
        }
        List<String> required = canonicalFeatures(requiredFeatures, "plan.requiredFeatures");
        List<String> optional = canonicalFeatures(optionalFeatures, "plan.optionalFeatures");
        Set<String> overlap = new HashSet<>(required);
        overlap.retainAll(optional);
        if (!overlap.isEmpty()) {
            throw ArtifactText.failure("DUPLICATE_FEATURE", "plan.features",
                    "required and optional features overlap: " + overlap.iterator().next());
        }
        if (operators == null || operators.isEmpty()) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "operators", "artifact requires operators");
        }
        if (operators.size() > PlanArtifactLimits.MAX_OPERATORS) {
            throw ArtifactText.failure("LIMIT_EXCEEDED", "operators", "operator count exceeds limit");
        }
        List<OperatorDescriptor> copiedOperators = new ArrayList<>(operators.size());
        Set<String> allIds = new HashSet<>();
        for (int i = 0; i < operators.size(); i++) {
            OperatorDescriptor operator = operators.get(i);
            if (operator == null) {
                throw ArtifactText.failure("INVALID_SEMANTICS", "operators[" + i + "]", "operator is null");
            }
            if (!allIds.add(operator.operatorId())) {
                throw ArtifactText.failure("DUPLICATE_OPERATOR_ID", "operators[" + i + "].operatorId",
                        "duplicate operator id: " + operator.operatorId());
            }
            copiedOperators.add(operator);
        }
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < copiedOperators.size(); i++) {
            OperatorDescriptor operator = copiedOperators.get(i);
            for (int input = 0; input < operator.inputOperatorIds().size(); input++) {
                String inputId = operator.inputOperatorIds().get(input);
                if (!allIds.contains(inputId)) {
                    throw ArtifactText.failure("UNKNOWN_OPERATOR_INPUT",
                            "operators[" + i + "].inputOperatorIds[" + input + "]",
                            "unknown input operator: " + inputId);
                }
                if (!seen.contains(inputId)) {
                    throw ArtifactText.failure("NON_TOPOLOGICAL_OPERATOR_INPUT",
                            "operators[" + i + "].inputOperatorIds[" + input + "]",
                            "input operator must precede consumer: " + inputId);
                }
            }
            seen.add(operator.operatorId());
        }
        this.definitionId = definitionId;
        this.planSchemaVersion = planSchemaVersion;
        this.minimumReaderVersion = minimumReaderVersion;
        this.runtimeTarget = runtimeTarget;
        this.boundedness = boundedness;
        this.changelogMode = changelogMode;
        this.timeMode = timeMode;
        this.requiredFeatures = Collections.unmodifiableList(required);
        this.optionalFeatures = Collections.unmodifiableList(optional);
        this.operators = Collections.unmodifiableList(copiedOperators);
        this.payloadFormat = payloadFormat;
        this.payload = takePayloadOwnership ? payload : payload.clone();
        this.artifactId = verifiedArtifactId == null
                ? PlanArtifactCodec.hex(PlanArtifactCodec.sha256(PlanArtifactCodec.encodeBody(this)))
                : verifiedArtifactId;
        this.localPayloadReference = localPayloadReference;
    }

    static PlanArtifact decoded(
            String definitionId, int planSchemaVersion, int minimumReaderVersion,
            RuntimeTarget runtimeTarget, Boundedness boundedness, ChangelogMode changelogMode,
            TimeMode timeMode, Collection<String> requiredFeatures, Collection<String> optionalFeatures,
            List<OperatorDescriptor> operators, PayloadFormat payloadFormat, byte[] ownedPayload,
            String verifiedArtifactId) {
        return new PlanArtifact(definitionId, planSchemaVersion, minimumReaderVersion,
                runtimeTarget, boundedness, changelogMode, timeMode,
                requiredFeatures, optionalFeatures, operators, payloadFormat, ownedPayload,
                true, verifiedArtifactId, null);
    }

    static PlanArtifact withLocalPayloadReference(PlanArtifact artifact, Object reference) {
        return new PlanArtifact(artifact.definitionId, artifact.planSchemaVersion, artifact.minimumReaderVersion,
                artifact.runtimeTarget, artifact.boundedness, artifact.changelogMode, artifact.timeMode,
                artifact.requiredFeatures, artifact.optionalFeatures, artifact.operators,
                artifact.payloadFormat, artifact.payload, true, artifact.artifactId, reference);
    }

    Object localPayloadReference() {
        return localPayloadReference;
    }

    private static List<String> canonicalFeatures(Collection<String> features, String path) {
        if (features == null || features.size() > PlanArtifactLimits.MAX_FEATURES) {
            throw ArtifactText.failure("LIMIT_EXCEEDED", path, "feature count exceeds limit");
        }
        TreeSet<String> sorted = new TreeSet<>();
        int index = 0;
        for (String feature : features) {
            ArtifactText.encode(feature, path + "[" + index + "]", PlanArtifactLimits.MAX_TEXT_BYTES);
            if (!sorted.add(feature)) {
                throw ArtifactText.failure("DUPLICATE_FEATURE", path + "[" + index + "]",
                        "duplicate feature: " + feature);
            }
            index++;
        }
        return new ArrayList<>(sorted);
    }

    public String definitionId() { return definitionId; }
    public int planSchemaVersion() { return planSchemaVersion; }
    public int minimumReaderVersion() { return minimumReaderVersion; }
    public RuntimeTarget runtimeTarget() { return runtimeTarget; }
    public Boundedness boundedness() { return boundedness; }
    public ChangelogMode changelogMode() { return changelogMode; }
    public TimeMode timeMode() { return timeMode; }
    public List<String> requiredFeatures() { return requiredFeatures; }
    public List<String> optionalFeatures() { return optionalFeatures; }
    public List<OperatorDescriptor> operators() { return operators; }
    public PayloadFormat payloadFormat() { return payloadFormat; }
    public byte[] payload() { return payload.clone(); }
    public int payloadSize() { return payload.length; }
    byte[] payloadUnsafe() { return payload; }
    public String artifactId() { return artifactId; }
}
