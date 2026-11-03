package io.rowweave.stream.contract;

import java.util.*;

/** Canonical Stream DAG contract; contains no live Java runtime object. */
public final class PortableStreamPlan {
    public static final int FIXED_SOURCE_VERSION=1;
    public static final int DYNAMIC_SOURCE_VERSION=2;
    public static final int RETRACT_CHANGELOG_VERSION=3;
    public static final int CURRENT_VERSION=FIXED_SOURCE_VERSION;
    public static final int MAX_PARALLELISM=32768, MAX_NODES=4096;
    public static final String STREAM_CAPABILITY="rowweave.runtime.stream.v1";
    public static final String TEMPORAL_JOIN_CAPABILITY =
            "rowweave.runtime.stream.temporal-join.v1";
    public static final String UPSERT_CAPABILITY =
            "rowweave.runtime.stream.upsert.v1";
    public static final String RESIDENT_CDC_CAPABILITY =
            "rowweave.runtime.stream.resident-cdc.v1";
    public static final String RESIDENT_CDC_V2_CAPABILITY =
            "rowweave.runtime.stream.resident-cdc-external-position.v2";
    public static final String WINDOW_FIELD_BINDING_CAPABILITY =
            "rowweave.runtime.stream.window-field-binding.v1";
    public static final String SOURCE_TIME_POLICY_CAPABILITY =
            "rowweave.runtime.stream.source-time-policy.v1";
    public static final String SOURCE_END_OF_INPUT_CAPABILITY =
            "rowweave.runtime.stream.source-end-of-input.v1";
    public static final String CALC_CAPABILITY =
            "rowweave.runtime.stream.calc.v1";
    public static final String FLAT_MAP_CAPABILITY =
            "rowweave.runtime.stream.flat-map.v1";
    public static final String UNION_CAPABILITY =
            "rowweave.runtime.stream.union.v1";
    public static final String EVENT_TIME_ASSIGNMENT_CAPABILITY =
            "rowweave.runtime.stream.event-time-assignment.v1";
    public static final String KEYED_PROCESS_CAPABILITY =
            "rowweave.runtime.stream.keyed-process.v1";
    public static final String KEYED_CO_PROCESS_CAPABILITY =
            "rowweave.runtime.stream.keyed-co-process.v1";
    public static final String BROADCAST_STATE_CAPABILITY =
            "rowweave.runtime.stream.broadcast-state.v1";
    public static final String GENERIC_WINDOW_CAPABILITY =
            "rowweave.runtime.stream.generic-window.v1";
    public static final String TAGGED_OUTPUT_CAPABILITY =
            "rowweave.runtime.stream.tagged-output.v1";
    public static final String ASYNC_WAIT_CAPABILITY =
            "rowweave.runtime.stream.async-wait.v1";
    public static final String NODE_EXECUTION_CONTRACT_CAPABILITY =
            "rowweave.runtime.stream.node-execution-contract.v1";
    public static final String UNALIGNED_CHECKPOINT_CAPABILITY =
            "rowweave.runtime.stream.unaligned-checkpoint.v2";
    public static final String DYNAMIC_SPLIT_DISCOVERY_CAPABILITY =
            "rowweave.runtime.stream.dynamic-split-discovery.v2";
    public static final String RETRACT_CHANGELOG_CAPABILITY =
            "rowweave.runtime.stream.retract-changelog.v2";
    public enum DeliveryGuarantee { EXACTLY_ONCE(1), AT_LEAST_ONCE(2); private final int id;DeliveryGuarantee(int id){this.id=id;}int wireId(){return id;}static DeliveryGuarantee fromWire(int id){for(DeliveryGuarantee v:values())if(v.id==id)return v;throw StreamContractSupport.failure("UNKNOWN_DELIVERY_GUARANTEE","plan.delivery","unknown delivery guarantee");}}
    public enum ChangelogMode { APPEND(1), UPSERT(2), RETRACT(3);private final int id;ChangelogMode(int id){this.id=id;}int wireId(){return id;}static ChangelogMode fromWire(int id){for(ChangelogMode v:values())if(v.id==id)return v;throw StreamContractSupport.failure("UNKNOWN_CHANGELOG_MODE","plan.changelog","unknown changelog mode");}}
    public enum TimeMode { PROCESSING_TIME(1), EVENT_TIME(2);private final int id;TimeMode(int id){this.id=id;}int wireId(){return id;}static TimeMode fromWire(int id){for(TimeMode v:values())if(v.id==id)return v;throw StreamContractSupport.failure("UNKNOWN_TIME_MODE","plan.time","unknown time mode");}}
    private final int version,parallelism,maxParallelism;private final DeliveryGuarantee delivery;private final ChangelogMode changelog;private final TimeMode time;
    private final List<String> roots;private final List<PortableStreamNode> nodes;private final List<StreamEdge> edges;private final List<String> capabilities;
    private final Map<String, ChangelogMode> outputChangelogs;
    public PortableStreamPlan(int version,DeliveryGuarantee delivery,int parallelism,int maxParallelism,ChangelogMode changelog,TimeMode time,List<String> roots,List<PortableStreamNode> nodes,List<StreamEdge> edges,List<String> capabilities){
        if(version!=FIXED_SOURCE_VERSION&&version!=DYNAMIC_SOURCE_VERSION&&version!=RETRACT_CHANGELOG_VERSION)throw StreamContractSupport.failure("UNSUPPORTED_PLAN_VERSION","plan.version","unsupported Stream plan version");
        if(parallelism<1||maxParallelism<parallelism||maxParallelism>MAX_PARALLELISM)throw StreamContractSupport.failure("INVALID_PARALLELISM","plan.parallelism","invalid parallelism bounds");
        if(delivery==null||changelog==null||time==null)throw StreamContractSupport.failure("INVALID_SEMANTICS","plan.semantics","plan semantics are required");
        this.version=version;this.delivery=delivery;this.parallelism=parallelism;this.maxParallelism=maxParallelism;this.changelog=changelog;this.time=time;
        this.roots=canonicalTexts(roots,"plan.roots",64);this.capabilities=canonicalTexts(capabilities,"plan.capabilities",256);
        if(!this.capabilities.contains(STREAM_CAPABILITY))throw StreamContractSupport.failure("MISSING_CAPABILITY","plan.capabilities","Stream capability is required");
        if(nodes==null||nodes.isEmpty()||nodes.size()>MAX_NODES)throw StreamContractSupport.failure("LIMIT_EXCEEDED","plan.nodes","invalid node count");
        Map<String, PortableStreamNode> seen = new HashMap<>();
        Set<String> sinks = new HashSet<>();
        Set<String> deploymentSets = new HashSet<>();
        List<PortableStreamNode> copy = new ArrayList<>();
        boolean eager = false;
        boolean dynamicSource = false;
        boolean boundedSource = false;
        for (PortableStreamNode node : nodes) {
            if (node == null || seen.put(node.operatorUid(), node) != null) {
                throw StreamContractSupport.failure("DUPLICATE_OPERATOR_UID",
                        "plan.nodes", "operator UIDs must be unique");
            }
            validateNodeExecutionContract(node, this.capabilities,
                    maxParallelism, time);
            for (int i = 0; i < node.inputOperatorUids().size(); i++) {
                PortableStreamNode upstream = seen.get(node.inputOperatorUids().get(i));
                if (upstream == null) {
                    throw StreamContractSupport.failure("INVALID_TOPOLOGY",
                            "plan.nodes", "inputs must reference earlier nodes");
                }
                if (!matchesInputSchema(upstream,
                        node.inputSchemaFingerprints().get(i))) {
                    throw StreamContractSupport.failure("SCHEMA_MISMATCH",
                            "plan.nodes", "input schema fingerprint drift");
                }
            }
            validateStableKeys(node);
            requireAtomicCapability(node, this.capabilities);
            if (node.opcode() == StreamOperatorCode.FLAT_MAP) {
                validateFlatMap(node);
            }
            if (node.opcode() == StreamOperatorCode.SOURCE
                    && node.scalarParameters().containsKey("source.mode")) {
                validateResidentCdcSource(node, changelog, this.capabilities);
            }
            if (node.opcode() == StreamOperatorCode.SOURCE
                    && this.capabilities.contains(SOURCE_TIME_POLICY_CAPABILITY)) {
                validateSourceTimePolicy(node, time);
            }
            if (node.opcode() == StreamOperatorCode.SOURCE
                    && node.scalarParameters().containsKey(
                    "source.termination")) {
                validateBoundedSource(node);
                boundedSource = true;
            }
            if (node.opcode() == StreamOperatorCode.KEY_BY
                    && node.stableKeyFields().isEmpty()) {
                throw StreamContractSupport.failure("MISSING_STABLE_KEY",
                        "plan.nodes.stableKeys",
                        "keyBy requires at least one stable non-null key");
            }
            if (node.opcode() == StreamOperatorCode.ASYNC_WAIT) {
                validateAsync(node, delivery);
            }
            if (node.binding() instanceof StreamFunctionBinding) {
                deploymentSets.add(((StreamFunctionBinding) node.binding())
                        .deploymentReference().deploymentSetId());
            } else if (node.binding() instanceof StreamProcessBinding) {
                deploymentSets.add(((StreamProcessBinding) node.binding())
                        .deploymentReference().deploymentSetId());
            } else if (node.binding() instanceof StreamWindowBinding) {
                deploymentSets.add(((StreamWindowBinding) node.binding())
                        .deploymentReference().deploymentSetId());
            } else if (node.binding() instanceof StreamAsyncBinding) {
                deploymentSets.add(((StreamAsyncBinding) node.binding())
                        .deploymentReference().deploymentSetId());
            }
            if (node.opcode() == StreamOperatorCode.SINK) {
                sinks.add(node.operatorUid());
                eager |= ((StreamSinkBinding) node.binding()).visibilityProfile()
                        == StreamSinkBinding.VisibilityProfile.EAGER_AT_LEAST_ONCE;
            }
            if (node.opcode() == StreamOperatorCode.SOURCE
                    && ((StreamSourceBinding) node.binding())
                    .dynamicDiscovery()) {
                dynamicSource = true;
            }
            copy.add(node);
        }
        boolean dynamicCapability = this.capabilities.contains(
                DYNAMIC_SPLIT_DISCOVERY_CAPABILITY);
        if (dynamicSource != dynamicCapability) {
            throw StreamContractSupport.failure(
                    "DYNAMIC_SOURCE_CAPABILITY_MISMATCH",
                    "plan.capabilities",
                    "dynamic Source binding and capability must agree");
        }
        if (boundedSource != this.capabilities.contains(
                SOURCE_END_OF_INPUT_CAPABILITY)) {
            throw StreamContractSupport.failure(
                    "SOURCE_TERMINATION_CAPABILITY_MISMATCH",
                    "plan.capabilities",
                    "bounded Source declaration and capability must agree");
        }
        if (dynamicCapability && version < DYNAMIC_SOURCE_VERSION) {
            throw StreamContractSupport.failure(
                    "UNSUPPORTED_PLAN_VERSION", "plan.version",
                    "dynamic split discovery requires Stream plan v2");
        }
        if (deploymentSets.size() > 1) {
            throw StreamContractSupport.failure("MULTIPLE_DEPLOYMENT_SETS",
                    "plan.nodes.function", "Stream requires one deployment set");
        }
        if(!sinks.equals(new HashSet<>(this.roots)))throw StreamContractSupport.failure("INVALID_ROOT_SET","plan.roots","roots must equal sink set");
        Set<String> reachable = new HashSet<>(this.roots);
        for (int i = copy.size() - 1; i >= 0; i--) {
            PortableStreamNode node = copy.get(i);
            if (reachable.contains(node.operatorUid())) {
                reachable.addAll(node.inputOperatorUids());
            }
        }
        if (reachable.size() != copy.size()) {
            throw StreamContractSupport.failure("UNREACHABLE_OPERATOR",
                    "plan.nodes", "every operator must contribute to a root Sink");
        }
        if(delivery==DeliveryGuarantee.EXACTLY_ONCE&&eager)throw StreamContractSupport.failure("DELIVERY_GUARANTEE_MISMATCH","plan.delivery","eager sink cannot provide exactly once");
        this.nodes=Collections.unmodifiableList(copy);
        this.edges=validateEdges(edges, seen, this.capabilities);
        validateNodeExecutionFrameCapacity(this.nodes, this.edges);
        this.outputChangelogs = validateEdgeBoundSemantics(
                this.nodes, this.edges, seen, time,
                changelog, this.capabilities);
        validateUpsertRedistribution(this.nodes, this.edges, changelog);
        validateRetractContracts(this.nodes, this.edges,
                this.outputChangelogs, this.capabilities);
        if (changelog == ChangelogMode.UPSERT
                && !this.capabilities.contains(UPSERT_CAPABILITY)) {
            throw StreamContractSupport.failure("MISSING_CAPABILITY",
                    "plan.capabilities", "Stream UPSERT capability is required");
        }
        boolean retractCapability = this.capabilities.contains(
                RETRACT_CHANGELOG_CAPABILITY);
        if ((changelog == ChangelogMode.RETRACT) != retractCapability) {
            throw StreamContractSupport.failure("MISSING_CAPABILITY",
                    "plan.capabilities",
                    "Stream RETRACT mode and capability must agree");
        }
        if (retractCapability && version < RETRACT_CHANGELOG_VERSION) {
            throw StreamContractSupport.failure("UNSUPPORTED_PLAN_VERSION",
                    "plan.version",
                    "RETRACT changelog requires its versioned Stream plan");
        }
    }

    private static void validateNodeExecutionContract(
            PortableStreamNode node, List<String> capabilities,
            int planMaximumParallelism, TimeMode planTimeMode) {
        boolean required = capabilities.contains(
                NODE_EXECUTION_CONTRACT_CAPABILITY);
        StreamNodeExecutionDescriptor execution =
                node.executionDescriptor();
        if (required && execution == null) {
            throw StreamContractSupport.failure(
                    "MISSING_NODE_EXECUTION_CONTRACT",
                    "plan.nodes.execution",
                    "every node requires explicit execution semantics");
        }
        if (!required && execution != null) {
            throw StreamContractSupport.failure("MISSING_CAPABILITY",
                    "plan.capabilities",
                    "node execution contract capability is required");
        }
        if (execution != null
                && execution.maximumParallelism()
                > planMaximumParallelism) {
            throw StreamContractSupport.failure(
                    "INVALID_NODE_EXECUTION_CONTRACT",
                    "plan.nodes.execution.maximumParallelism",
                    "node maximum parallelism exceeds the plan ceiling");
        }
        if (execution != null && execution.timeMode() != planTimeMode) {
            throw StreamContractSupport.failure(
                    "INVALID_NODE_EXECUTION_CONTRACT",
                    "plan.nodes.execution.timeMode",
                    "node execution time mode differs from the plan authority");
        }
        if (node.opcode() == StreamOperatorCode.SOURCE) {
            StreamSourceBinding source = (StreamSourceBinding) node.binding();
            if (source.fixedSplits().size() > planMaximumParallelism) {
                throw StreamContractSupport.failure(
                        "INVALID_NODE_EXECUTION_CONTRACT",
                        "plan.nodes.source.splits",
                        "source split count exceeds the plan parallelism ceiling");
            }
            if (execution != null && !source.dynamicDiscovery()
                    && execution.parallelism()
                    != source.fixedSplits().size()) {
                throw StreamContractSupport.failure(
                        "INVALID_NODE_EXECUTION_CONTRACT",
                        "plan.nodes.execution.parallelism",
                        "source parallelism must equal its fixed split count");
            }
            if (execution != null && source.dynamicDiscovery()
                    && execution.parallelism() < 1) {
                throw StreamContractSupport.failure(
                        "INVALID_NODE_EXECUTION_CONTRACT",
                        "plan.nodes.execution.parallelism",
                        "dynamic source parallelism must be positive");
            }
            if (execution != null
                    && node.scalarParameters().containsKey(
                    "source.time.idleTimeoutMillis")
                    && execution.maximumThreads() < 2) {
                throw StreamContractSupport.failure(
                        "INVALID_NODE_EXECUTION_CONTRACT",
                        "plan.nodes.execution.maximumThreads",
                        "event-time source requires reader and watchdog threads");
            }
        }
    }

    static boolean requiresCheckpointPayload(
            StreamOperatorCode opcode) {
        switch (opcode) {
            case SOURCE:
            case KEYED_AGGREGATE:
            case EVENT_TIME_WINDOW:
            case TEMPORAL_JOIN:
            case ASSIGN_EVENT_TIME:
            case KEYED_PROCESS:
            case KEYED_CO_PROCESS:
            case GENERIC_WINDOW:
            case ASYNC_WAIT:
                return true;
            default:
                return false;
        }
    }

    private static void validateNodeExecutionFrameCapacity(
            List<PortableStreamNode> nodes, List<StreamEdge> edges) {
        Map<String, Integer> maximumIncidentFrame = new HashMap<>();
        for (StreamEdge edge : edges) {
            mergeMaximum(maximumIncidentFrame, edge.sourceOperatorUid(),
                    edge.maximumFrameBytes());
            mergeMaximum(maximumIncidentFrame, edge.targetOperatorUid(),
                    edge.maximumFrameBytes());
        }
        for (PortableStreamNode node : nodes) {
            StreamNodeExecutionDescriptor execution =
                    node.executionDescriptor();
            int required = maximumIncidentFrame.containsKey(
                    node.operatorUid())
                    ? maximumIncidentFrame.get(node.operatorUid()) : 0;
            if (execution != null
                    && execution.maximumResidentBytes() < required) {
                throw StreamContractSupport.failure(
                        "INVALID_NODE_EXECUTION_CONTRACT",
                        "plan.nodes.execution.maximumResidentBytes",
                        "node resident lease must cover every incident edge frame");
            }
        }
    }

    private static void mergeMaximum(Map<String, Integer> values,
            String key, int candidate) {
        Integer current = values.get(key);
        if (current == null || candidate > current) {
            values.put(key, candidate);
        }
    }

    private static boolean matchesInputSchema(PortableStreamNode upstream,
            String fingerprint) {
        return matchingInputSchema(upstream, fingerprint) != null;
    }

    private static io.rowweave.portable.PortableRowSchema matchingInputSchema(
            PortableStreamNode upstream, String fingerprint) {
        if (upstream.outputSchema().fingerprint().equals(fingerprint)) {
            return upstream.outputSchema();
        }
        for (StreamOutputTagDescriptor tag : outputTags(upstream)) {
            if (tag.schema().fingerprint().equals(fingerprint)) {
                return tag.schema();
            }
        }
        return null;
    }

    private static void requireAtomicCapability(
            PortableStreamNode node, List<String> capabilities) {
        String required = null;
        switch (node.opcode()) {
            case CALC:
                required = CALC_CAPABILITY;
                break;
            case FLAT_MAP:
                required = FLAT_MAP_CAPABILITY;
                break;
            case UNION:
                required = UNION_CAPABILITY;
                break;
            case ASSIGN_EVENT_TIME:
                required = EVENT_TIME_ASSIGNMENT_CAPABILITY;
                break;
            case KEYED_PROCESS:
                required = KEYED_PROCESS_CAPABILITY;
                break;
            case KEYED_CO_PROCESS:
                required = KEYED_CO_PROCESS_CAPABILITY;
                break;
            case GENERIC_WINDOW:
                required = GENERIC_WINDOW_CAPABILITY;
                break;
            case ASYNC_WAIT:
                required = ASYNC_WAIT_CAPABILITY;
                break;
            default:
                return;
        }
        if (!capabilities.contains(required)) {
            throw StreamContractSupport.failure("MISSING_CAPABILITY",
                    "plan.capabilities",
                    "Stream atomic operator capability is required");
        }
    }

    private static Map<String, ChangelogMode> validateEdgeBoundSemantics(
            List<PortableStreamNode> nodes, List<StreamEdge> edges,
            Map<String, PortableStreamNode> byUid, TimeMode time,
            ChangelogMode changelog, List<String> capabilities) {
        Map<String, StreamEdge> inputsByTarget = new HashMap<>();
        for (StreamEdge edge : edges) {
            inputsByTarget.put(edge.targetOperatorUid() + '\0'
                    + edge.targetInputIndex(), edge);
        }
        Map<String, ChangelogMode> mainModes = new LinkedHashMap<>();
        for (PortableStreamNode node : nodes) {
            List<InputContract> inputs = inputContracts(
                    node, inputsByTarget, byUid, mainModes);
            if (node.opcode() == StreamOperatorCode.UNION) {
                validateUnion(node, inputs);
            } else if (node.opcode() == StreamOperatorCode.FILTER
                    || node.opcode() == StreamOperatorCode.KEY_BY) {
                validatePassThrough(node, inputs);
            } else if (node.opcode() == StreamOperatorCode.SINK) {
                validateSinkInputContract(node, inputs);
            } else if (node.opcode() == StreamOperatorCode.CALC) {
                validateCalc(node, inputs);
            } else if (node.opcode()
                    == StreamOperatorCode.ASSIGN_EVENT_TIME) {
                validateTimeAssignment(node, time, inputs);
            } else if (node.opcode() == StreamOperatorCode.KEYED_PROCESS
                    || node.opcode()
                    == StreamOperatorCode.KEYED_CO_PROCESS) {
                validateProcess(node, time, changelog, inputs, capabilities);
            } else if (node.opcode()
                    == StreamOperatorCode.EVENT_TIME_WINDOW) {
                validateWindow(node, time, inputs, capabilities);
            } else if (node.opcode()
                    == StreamOperatorCode.GENERIC_WINDOW) {
                validateGenericWindow(node, time, changelog, inputs,
                        capabilities);
            } else if (node.opcode()
                    == StreamOperatorCode.TEMPORAL_JOIN) {
                validateTemporalJoin(node, time, changelog, inputs,
                        capabilities);
            }
            validateStateTimeDomain(node, time);
            List<ChangelogMode> inputModes = new ArrayList<>(inputs.size());
            for (InputContract input : inputs) {
                inputModes.add(input.changelogMode);
            }
            ChangelogMode outputMode = producesChangelog(
                    node, inputModes, changelog);
            StreamNodeExecutionDescriptor execution =
                    node.executionDescriptor();
            if (execution != null
                    && execution.changelogMode() != outputMode) {
                throw StreamContractSupport.failure(
                        "INVALID_NODE_EXECUTION_CONTRACT",
                        "plan.nodes.execution.changelogMode",
                        "node execution changelog differs from its derived output contract");
            }
            mainModes.put(node.operatorUid(), outputMode);
        }
        Map<String, ChangelogMode> result = new LinkedHashMap<>();
        result.putAll(mainModes);
        return Collections.unmodifiableMap(result);
    }

    private static void validatePassThrough(PortableStreamNode node,
            List<InputContract> inputs) {
        InputContract input = inputs.get(0);
        if (!node.outputSchema().equals(input.schema)) {
            throw StreamContractSupport.failure("SCHEMA_MISMATCH",
                    "plan.nodes.outputSchema",
                    "pass-through operator must preserve the actual input schema");
        }
        if (node.opcode() == StreamOperatorCode.FILTER
                && !sameStableKeyCodec(input.schema, input.stableKeys,
                node.outputSchema(), node.stableKeyFields())) {
            throw StreamContractSupport.failure("INVALID_STABLE_KEY",
                    "plan.nodes.stableKeys",
                    "filter must preserve the actual input stable key contract");
        }
    }

    private static void validateSinkInputContract(PortableStreamNode node,
            List<InputContract> inputs) {
        InputContract input = inputs.get(0);
        StreamSinkBinding sink = (StreamSinkBinding) node.binding();
        if (!sink.accepts(input.changelogMode)) {
            throw StreamContractSupport.failure("CHANGELOG_MODE_MISMATCH",
                    "plan.nodes.sink.changelogModes",
                    "sink does not accept its input changelog mode");
        }
        if (!node.outputSchema().equals(input.schema)) {
            throw StreamContractSupport.failure("SCHEMA_MISMATCH",
                    "plan.nodes.outputSchema",
                    "sink metadata must preserve its actual input schema");
        }
        if (input.changelogMode == ChangelogMode.UPSERT
                && node.stableKeyFields().isEmpty()) {
            return;
        }
        if (!sameStableKeyCodec(input.schema, input.stableKeys,
                node.outputSchema(), node.stableKeyFields())) {
            throw StreamContractSupport.failure("INVALID_STABLE_KEY",
                    "plan.nodes.stableKeys",
                    "sink metadata must preserve its actual input stable key contract");
        }
    }

    private static void validateStateTimeDomain(PortableStreamNode node,
            TimeMode time) {
        for (StreamStateDescriptor state : node.stateDescriptors()) {
            if (state.ttlMode() == StreamStateDescriptor.TtlMode.EVENT_TIME
                    && time != TimeMode.EVENT_TIME) {
                throw StreamContractSupport.failure(
                        "STATE_TIME_MODE_MISMATCH",
                        "plan.nodes.states.ttlMode",
                        "event-time state TTL requires event-time plan semantics");
            }
        }
    }

    private static List<InputContract> inputContracts(
            PortableStreamNode node, Map<String, StreamEdge> inputsByTarget,
            Map<String, PortableStreamNode> byUid,
            Map<String, ChangelogMode> mainModes) {
        List<InputContract> result = new ArrayList<>();
        for (int index = 0; index < node.inputOperatorUids().size(); index++) {
            StreamEdge edge = inputsByTarget.get(node.operatorUid() + '\0'
                    + index);
            PortableStreamNode source = edge == null ? null
                    : byUid.get(edge.sourceOperatorUid());
            if (source == null) {
                throw StreamContractSupport.failure("INVALID_EDGE_SET",
                        "plan.edges", "node input edge is unavailable");
            }
            if (edge.sourceOutputTagId() == null) {
                ChangelogMode mode = mainModes.get(source.operatorUid());
                if (mode == null) {
                    throw StreamContractSupport.failure("INVALID_EDGE_SET",
                            "plan.edges",
                            "node input changelog is unavailable");
                }
                result.add(new InputContract(source.outputSchema(),
                        source.stableKeyFields(), mode));
            } else {
                StreamOutputTagDescriptor tag = outputTag(source,
                        edge.sourceOutputTagId());
                if (tag == null) {
                    throw StreamContractSupport.failure("INVALID_EDGE_SET",
                            "plan.edges.sourceOutputTagId",
                            "tagged input port is unavailable");
                }
                result.add(new InputContract(tag.schema(),
                        tag.stableKeyFields(), fromTagMode(
                        tag.changelogMode())));
            }
        }
        return result;
    }

    private static final class InputContract {
        private final io.rowweave.portable.PortableRowSchema schema;
        private final List<String> stableKeys;
        private final ChangelogMode changelogMode;

        private InputContract(
                io.rowweave.portable.PortableRowSchema schema,
                List<String> stableKeys, ChangelogMode changelogMode) {
            this.schema = schema;
            this.stableKeys = stableKeys;
            this.changelogMode = changelogMode;
        }
    }

    private static void validateUnion(PortableStreamNode node,
            List<InputContract> inputs) {
        if (!node.stateDescriptors().isEmpty()
                || !node.scalarParameters().isEmpty()
                || node.binding() != null) {
            throw StreamContractSupport.failure("INVALID_UNION",
                    "plan.nodes.union",
                    "union cannot carry state, parameters or a binding");
        }
        List<String> expectedKeys = null;
        ChangelogMode expectedChangelog = null;
        for (InputContract input : inputs) {
            if (!node.outputSchema().equals(input.schema)) {
                throw StreamContractSupport.failure("SCHEMA_MISMATCH",
                        "plan.nodes.union",
                        "union input and output schemas must be identical");
            }
            if (expectedKeys == null) {
                expectedKeys = input.stableKeys;
            } else if (!expectedKeys.equals(input.stableKeys)) {
                throw StreamContractSupport.failure("INVALID_UNION_KEY",
                        "plan.nodes.union",
                        "union inputs must have identical stable keys");
            }
            if (expectedChangelog == null) {
                expectedChangelog = input.changelogMode;
            } else if (expectedChangelog != input.changelogMode) {
                throw StreamContractSupport.failure(
                        "CHANGELOG_MODE_MISMATCH",
                        "plan.nodes.union",
                        "union inputs must have identical changelog modes");
            }
        }
        if (expectedKeys == null || !expectedKeys.equals(
                node.stableKeyFields())) {
            throw StreamContractSupport.failure("INVALID_UNION_KEY",
                    "plan.nodes.union",
                    "union must preserve the identical input stable key");
        }
    }

    private static boolean compatibleProcessKeys(InputContract left,
            InputContract right) {
        if (left.stableKeys.size() != right.stableKeys.size()) return false;
        for (int index = 0; index < left.stableKeys.size(); index++) {
            io.rowweave.portable.PortableField leftField = processKeyField(
                    left.schema, left.stableKeys.get(index));
            io.rowweave.portable.PortableField rightField = processKeyField(
                    right.schema, right.stableKeys.get(index));
            if (leftField == null || rightField == null
                    || leftField.dataType() != rightField.dataType()
                    || leftField.nullable() != rightField.nullable()) {
                return false;
            }
        }
        return true;
    }

    private static io.rowweave.portable.PortableField processKeyField(
            io.rowweave.portable.PortableRowSchema schema, String fieldId) {
        for (io.rowweave.portable.PortableField field :
                schema.fields()) {
            if (field.fieldId().equals(fieldId)) return field;
        }
        return null;
    }

    private static void validateCalc(PortableStreamNode node,
            List<InputContract> inputs) {
        if (!node.stateDescriptors().isEmpty()
                || !node.scalarParameters().isEmpty()) {
            throw StreamContractSupport.failure("INVALID_CALC",
                    "plan.nodes.calc",
                    "CALC cannot carry state or scalar parameters");
        }
        ((StreamCalcBinding) node.binding()).validateInputSchema(
                inputs.get(0).schema);
    }

    private static void validateTimeAssignment(PortableStreamNode node,
            TimeMode time, List<InputContract> inputs) {
        if (time != TimeMode.EVENT_TIME) {
            throw StreamContractSupport.failure("TIME_MODE_MISMATCH",
                    "plan.nodes.time",
                    "event-time assignment requires EVENT_TIME semantics");
        }
        InputContract input = inputs.get(0);
        if (!node.outputSchema().equals(input.schema)
                || !node.stableKeyFields().equals(input.stableKeys)
                || !node.stateDescriptors().isEmpty()
                || !node.scalarParameters().isEmpty()) {
            throw StreamContractSupport.failure("INVALID_TIME_ASSIGNMENT",
                    "plan.nodes.time",
                    "time assignment must preserve rows, schema and keys");
        }
        ((StreamTimeBinding) node.binding()).validateInputSchema(
                input.schema);
    }

    private static void validateProcess(PortableStreamNode node,
            TimeMode time, ChangelogMode changelog,
            List<InputContract> inputs,
            List<String> capabilities) {
        StreamProcessBinding binding = (StreamProcessBinding) node.binding();
        boolean broadcast = binding.broadcastState() != null;
        if (broadcast && !capabilities.contains(
                BROADCAST_STATE_CAPABILITY)) {
            throw StreamContractSupport.failure("MISSING_CAPABILITY",
                    "plan.capabilities",
                    "broadcast process requires broadcast-state capability");
        }
        if (!binding.outputTags().isEmpty()
                && !capabilities.contains(TAGGED_OUTPUT_CAPABILITY)) {
            throw StreamContractSupport.failure("MISSING_CAPABILITY",
                    "plan.capabilities",
                    "tagged process output requires tagged-output capability");
        }
        if (!node.scalarParameters().isEmpty()
                || binding.maximumElementOutputs()
                > binding.deploymentReference().resourceProfile()
                .maximumOutputRowsPerInput()
                || binding.maximumRightOutputs()
                > binding.deploymentReference().resourceProfile()
                .maximumOutputRowsPerInput()) {
            throw StreamContractSupport.failure(
                    "INVALID_PROCESS_RESOURCE_LIMIT", "plan.nodes.process",
                    "process output contract exceeds its function profile");
        }
        List<StreamStateDescriptor> declared = new ArrayList<>();
        for (StreamManagedStateDescriptor state : binding.managedStates()) {
            declared.add(state.state());
        }
        if (!declared.equals(node.stateDescriptors())) {
            throw StreamContractSupport.failure("STATE_BINDING_MISMATCH",
                    "plan.nodes.process.states",
                    "process state resources must match node state descriptors");
        }
        for (int index = 0; index < inputs.size(); index++) {
            InputContract input = inputs.get(index);
            if ((!broadcast || index == 0) && input.stableKeys.isEmpty()) {
                throw StreamContractSupport.failure("MISSING_STABLE_KEY",
                        "plan.nodes.process",
                        "keyed process requires keyed inputs");
            }
        }
        if (node.opcode() == StreamOperatorCode.KEYED_CO_PROCESS
                && !broadcast
                && !compatibleProcessKeys(inputs.get(0), inputs.get(1))) {
            throw StreamContractSupport.failure("INCOMPATIBLE_KEY_CODEC",
                    "plan.nodes.process",
                    "co-process keyed input codecs must be identical");
        }
        for (StreamTimerDescriptor timer : binding.timerFamilies()) {
            if (timer.timeDomain()
                    == StreamTimerDescriptor.TimeDomain.EVENT_TIME
                    && time != TimeMode.EVENT_TIME) {
                throw StreamContractSupport.failure("TIME_MODE_MISMATCH",
                        "plan.nodes.process.timers",
                        "event-time timers require EVENT_TIME semantics");
            }
        }
        for (StreamOutputTagDescriptor tag : binding.outputTags()) {
            int functionLimit = binding.deploymentReference().resourceProfile()
                    .maximumOutputRowsPerInput();
            if (tag.maximumLeftOutputs() > functionLimit
                    || tag.maximumRightOutputs() > functionLimit
                    || tag.maximumTimerOutputs() > functionLimit) {
                throw StreamContractSupport.failure(
                        "INVALID_PROCESS_RESOURCE_LIMIT",
                        "plan.nodes.process.outputTags",
                        "tagged output semantics exceed the process contract");
            }
        }
    }

    private static void validateFlatMap(PortableStreamNode node) {
        if (node.scalarParameters().size() != 2) invalidFlatMap();
        long rows = positiveLong(node.scalarParameters().get(
                "flatMap.maxOutputsPerInput"));
        long bytes = positiveLong(node.scalarParameters().get(
                "flatMap.maxOutputBytesPerInput"));
        if (rows > io.rowweave.stream.api.StreamEmissionLimit
                .MAX_OUTPUTS_PER_INPUT
                || bytes > io.rowweave.stream.api.StreamEmissionLimit
                .MAX_OUTPUT_BYTES_PER_INPUT) {
            invalidFlatMap();
        }
    }

    private static void validateGenericWindow(PortableStreamNode node,
            TimeMode time, ChangelogMode changelog,
            List<InputContract> inputs,
            List<String> capabilities) {
        if (!(node.binding() instanceof StreamWindowBinding)
                || node.stableKeyFields().isEmpty()
                || node.stateDescriptors().size() != 1
                || !node.scalarParameters().isEmpty()) {
            throw StreamContractSupport.failure("INVALID_WINDOW_BINDING",
                    "plan.nodes.window",
                    "generic window requires key, state and window binding");
        }
        StreamWindowBinding binding = (StreamWindowBinding) node.binding();
        StreamOutputTagDescriptor lateOutput = binding.lateOutputTag();
        if (lateOutput != null
                && !capabilities.contains(TAGGED_OUTPUT_CAPABILITY)) {
            throw StreamContractSupport.failure("MISSING_CAPABILITY",
                    "plan.capabilities",
                    "late output requires tagged-output capability");
        }
        boolean eventTime = binding.assignerKind()
                == StreamWindowBinding.AssignerKind.EVENT_TIME_TUMBLING
                || binding.assignerKind()
                == StreamWindowBinding.AssignerKind.EVENT_TIME_SLIDING
                || binding.assignerKind()
                == StreamWindowBinding.AssignerKind.EVENT_TIME_SESSION;
        boolean count = binding.assignerKind()
                == StreamWindowBinding.AssignerKind.COUNT_TUMBLING
                || binding.assignerKind()
                == StreamWindowBinding.AssignerKind.COUNT_SLIDING;
        if (eventTime != (time == TimeMode.EVENT_TIME)
                || eventTime && binding.triggerKind()
                != StreamWindowBinding.TriggerKind.EVENT_TIME_FINAL
                || count && binding.triggerKind()
                != StreamWindowBinding.TriggerKind.COUNT
                || !eventTime && !count && binding.triggerKind()
                != StreamWindowBinding.TriggerKind.PROCESSING_TIME
                || !eventTime && binding.allowedLatenessMillis() != 0L) {
            throw StreamContractSupport.failure("TIME_MODE_MISMATCH",
                    "plan.nodes.window",
                    "window assigner, trigger and plan time modes differ");
        }
        if (binding.accumulationMode()
                == StreamWindowBinding.AccumulationMode.ACCUMULATING
                && changelog != ChangelogMode.UPSERT) {
            throw StreamContractSupport.failure("CHANGELOG_MODE_MISMATCH",
                    "plan.nodes.window",
                    "accumulating windows require UPSERT changelog");
        }
        if (changelog == ChangelogMode.RETRACT
                && !retractWindowSupported(binding)) {
            throw StreamContractSupport.failure(
                    "CHANGELOG_MODE_MISMATCH", "plan.nodes.window",
                    "RETRACT window requires a reversible final event-time aggregate");
        }
        if (binding.resources().maximumTriggerOutputs()
                > binding.deploymentReference().resourceProfile()
                .maximumOutputRowsPerInput()) {
            throw StreamContractSupport.failure(
                    "INVALID_WINDOW_RESOURCE_LIMIT", "plan.nodes.window",
                    "window output quota exceeds function resource profile");
        }
        if (lateOutput != null) {
            int functionLimit = binding.deploymentReference().resourceProfile()
                    .maximumOutputRowsPerInput();
            if (lateOutput.maximumLeftOutputs() > functionLimit) {
                throw StreamContractSupport.failure(
                        "INVALID_WINDOW_RESOURCE_LIMIT",
                        "plan.nodes.window.lateOutput",
                        "late output exceeds window semantics or resources");
            }
        }
        if (eventTime) {
            io.rowweave.portable.PortableField field = null;
            for (io.rowweave.portable.PortableField candidate :
                    inputs.get(0).schema.fields()) {
                if (candidate.fieldId().equals(
                        binding.eventTimeFieldId())) field = candidate;
            }
            if (field == null || field.nullable()
                    || field.dataType()
                    != io.rowweave.portable.PortableDataType.TIMESTAMP) {
                throw StreamContractSupport.failure(
                        "INVALID_WINDOW_FIELD_BINDING", "plan.nodes.window",
                        "event-time field must be a non-null timestamp");
            }
        }
    }

    private static void validateAsync(PortableStreamNode node,
            DeliveryGuarantee delivery) {
        if (!(node.binding() instanceof StreamAsyncBinding)
                || !node.stateDescriptors().isEmpty()
                || !node.scalarParameters().isEmpty()) {
            throw StreamContractSupport.failure("INVALID_ASYNC_BINDING",
                    "plan.nodes.async", "async wait binding is invalid");
        }
        StreamAsyncBinding binding = (StreamAsyncBinding) node.binding();
        if (delivery == DeliveryGuarantee.EXACTLY_ONCE
                && binding.replayMode()
                == StreamAsyncBinding.ReplayMode.AT_LEAST_ONCE) {
            throw StreamContractSupport.failure(
                    "DELIVERY_GUARANTEE_MISMATCH", "plan.nodes.async",
                    "EXACTLY_ONCE async wait requires replay authority");
        }
        if (binding.resources().maximumResults()
                > binding.deploymentReference().resourceProfile()
                .maximumOutputRowsPerInput()) {
            throw StreamContractSupport.failure(
                    "INVALID_ASYNC_RESOURCE_LIMIT", "plan.nodes.async",
                    "async result quota exceeds function resource profile");
        }
    }

    private static void invalidFlatMap() {
        throw StreamContractSupport.failure("INVALID_FLAT_MAP_LIMIT",
                "plan.nodes.flatMap",
                "flatMap requires positive bounded row and byte limits");
    }

    private static List<StreamEdge> validateEdges(
            List<StreamEdge> values, Map<String, PortableStreamNode> nodes,
            List<String> capabilities) {
        if (values == null || values.size() > MAX_NODES * 2) {
            throw StreamContractSupport.failure("INVALID_EDGE_SET", "plan.edges",
                    "edge set exceeds the Stream topology bound");
        }
        Map<String, String> expected = new HashMap<>();
        for (PortableStreamNode node : nodes.values()) {
            for (int input = 0; input < node.inputOperatorUids().size(); input++) {
                expected.put(node.operatorUid() + '\0' + input,
                        node.inputOperatorUids().get(input));
            }
        }
        Set<String> channelIds = new HashSet<>();
        Set<String> consumedTags = new HashSet<>();
        List<StreamEdge> copy = new ArrayList<>(values.size());
        for (StreamEdge edge : values) {
            if (edge == null || !channelIds.add(edge.logicalChannelId())) {
                throw StreamContractSupport.failure("INVALID_EDGE_SET", "plan.edges",
                        "edge and logical channel identities must be unique");
            }
            if (edge.partitioning() == StreamEdge.Partitioning.REBALANCE) {
                throw StreamContractSupport.failure(
                        "UNSUPPORTED_PARTITIONING",
                        "plan.edges.partitioning",
                        "Stream supports only FORWARD, HASH and BROADCAST");
            }
            String key = edge.targetOperatorUid() + '\0' + edge.targetInputIndex();
            String source = expected.remove(key);
            if (source == null || !source.equals(edge.sourceOperatorUid())) {
                throw StreamContractSupport.failure("INVALID_EDGE_SET", "plan.edges",
                        "each node input requires one exact logical edge");
            }
            PortableStreamNode target = nodes.get(edge.targetOperatorUid());
            PortableStreamNode sourceNode = nodes.get(
                    edge.sourceOperatorUid());
            String inputFingerprint = target == null ? null
                    : target.inputSchemaFingerprints().get(
                    edge.targetInputIndex());
            if (edge.sourceOutputTagId() == null) {
                if (sourceNode == null || !sourceNode.outputSchema()
                        .fingerprint().equals(inputFingerprint)) {
                    throw StreamContractSupport.failure("SCHEMA_MISMATCH",
                            "plan.edges.sourceOutputTagId",
                            "main output edge schema does not match target input");
                }
            } else {
                if (!capabilities.contains(TAGGED_OUTPUT_CAPABILITY)) {
                    throw StreamContractSupport.failure("MISSING_CAPABILITY",
                            "plan.capabilities",
                            "tagged edge requires tagged-output capability");
                }
                StreamOutputTagDescriptor tag = outputTag(sourceNode,
                        edge.sourceOutputTagId());
                if (tag == null || !tag.schema().fingerprint().equals(
                        inputFingerprint)) {
                    throw StreamContractSupport.failure("SCHEMA_MISMATCH",
                            "plan.edges.sourceOutputTagId",
                            "tagged edge schema does not match its declared port");
                }
                if (tag.changelogMode()
                        == StreamOutputTagDescriptor.ChangelogMode.UPSERT
                        && !capabilities.contains(UPSERT_CAPABILITY)) {
                    throw StreamContractSupport.failure("MISSING_CAPABILITY",
                            "plan.capabilities",
                            "UPSERT tagged edge requires UPSERT capability");
                }
                if (tag.changelogMode()
                        == StreamOutputTagDescriptor.ChangelogMode.RETRACT
                        && !capabilities.contains(
                        RETRACT_CHANGELOG_CAPABILITY)) {
                    throw StreamContractSupport.failure("MISSING_CAPABILITY",
                            "plan.capabilities",
                            "RETRACT tagged edge requires RETRACT capability");
                }
                consumedTags.add(edge.sourceOperatorUid() + '\0'
                        + edge.sourceOutputTagId());
            }
            if (edge.partitioning() == StreamEdge.Partitioning.HASH
                    && (target == null || target.stableKeyFields().isEmpty())) {
                throw StreamContractSupport.failure("MISSING_PARTITION_KEY",
                        "plan.edges.partitioning",
                        "HASH partitioning requires a stable target key");
            }
            if (target != null
                    && target.binding() instanceof StreamProcessBinding
                    && ((StreamProcessBinding) target.binding())
                    .broadcastState() != null) {
                StreamEdge.Partitioning required = edge.targetInputIndex() == 1
                        ? StreamEdge.Partitioning.BROADCAST
                        : StreamEdge.Partitioning.FORWARD;
                if (edge.partitioning() != required) {
                    throw StreamContractSupport.failure(
                            "INVALID_BROADCAST_EDGE",
                            "plan.edges.partitioning",
                            "broadcast process edge partitioning is invalid");
                }
            } else if (edge.partitioning()
                    == StreamEdge.Partitioning.BROADCAST) {
                throw StreamContractSupport.failure("INVALID_BROADCAST_EDGE",
                        "plan.edges.partitioning",
                        "BROADCAST requires a declared broadcast state");
            }
            copy.add(edge);
        }
        if (!expected.isEmpty()) {
            throw StreamContractSupport.failure("INVALID_EDGE_SET", "plan.edges",
                    "each node input requires one exact logical edge");
        }
        for (PortableStreamNode node : nodes.values()) {
            for (StreamOutputTagDescriptor tag : outputTags(node)) {
                if (!consumedTags.contains(node.operatorUid() + '\0'
                        + tag.tagId())) {
                    throw StreamContractSupport.failure("UNUSED_OUTPUT_TAG",
                            "plan.edges.sourceOutputTagId",
                            "declared output tag must be consumed");
                }
            }
        }
        copy.sort(Comparator
                .comparing(StreamEdge::targetOperatorUid)
                .thenComparingInt(StreamEdge::targetInputIndex)
                .thenComparing(StreamEdge::logicalChannelId));
        return Collections.unmodifiableList(copy);
    }

    private static void validateUpsertRedistribution(
            List<PortableStreamNode> nodes, List<StreamEdge> edges,
            ChangelogMode planChangelog) {
        Map<String, StreamEdge> inputs = new HashMap<>();
        for (StreamEdge edge : edges) {
            inputs.put(edge.targetOperatorUid() + '\0'
                    + edge.targetInputIndex(), edge);
        }
        Map<String, Boolean> mainUpsert = new HashMap<>();
        Map<String, Boolean> mainTombstones = new HashMap<>();
        Map<String, PortableStreamNode> byUid = new HashMap<>();
        for (PortableStreamNode node : nodes) {
            byUid.put(node.operatorUid(), node);
            List<Boolean> inputModes = new ArrayList<>();
            List<Boolean> inputTombstones = new ArrayList<>();
            for (int index = 0; index < node.inputOperatorUids().size(); index++) {
                StreamEdge edge = inputs.get(node.operatorUid() + '\0' + index);
                if (edge == null) {
                    throw StreamContractSupport.failure("INVALID_EDGE_SET",
                            "plan.edges", "node input edge is unavailable");
                }
                PortableStreamNode source = byUid.get(
                        edge.sourceOperatorUid());
                boolean upsert = edge.sourceOutputTagId() == null
                        ? Boolean.TRUE.equals(mainUpsert.get(
                        edge.sourceOperatorUid()))
                        : outputTag(source, edge.sourceOutputTagId())
                        .changelogMode()
                        == StreamOutputTagDescriptor.ChangelogMode.UPSERT;
                boolean tombstones = edge.sourceOutputTagId() == null
                        ? Boolean.TRUE.equals(mainTombstones.get(
                        edge.sourceOperatorUid()))
                        : outputTag(source, edge.sourceOutputTagId())
                        .changelogMode()
                        == StreamOutputTagDescriptor.ChangelogMode.UPSERT;
                inputModes.add(upsert);
                inputTombstones.add(tombstones);
                if (tombstones && isStatefulMailbox(node.opcode())) {
                    throw StreamContractSupport.failure(
                            "CHANGELOG_MODE_MISMATCH",
                            "plan.nodes.changelog",
                            "UPSERT tombstones require an explicit delete-aware stateful ABI");
                }
                if (upsert && node.inputOperatorUids().size() == 1
                        && node.opcode() != StreamOperatorCode.SINK
                        && !isStatefulMailbox(node.opcode())) {
                    List<String> sourceKeys = edge.sourceOutputTagId() == null
                            ? source.stableKeyFields()
                            : outputTag(source, edge.sourceOutputTagId())
                            .stableKeyFields();
                    io.rowweave.portable.PortableRowSchema sourceSchema =
                            edge.sourceOutputTagId() == null
                                    ? source.outputSchema()
                                    : outputTag(source,
                                    edge.sourceOutputTagId()).schema();
                    if (!sameStableKeyCodec(sourceSchema, sourceKeys,
                            node.outputSchema(), node.stableKeyFields())) {
                        throw StreamContractSupport.failure(
                                "CHANGELOG_MODE_MISMATCH",
                                "plan.nodes.stableKeys",
                                "UPSERT-preserving unary operators must preserve the exact input stable key codec");
                    }
                }
            }
            if (node.opcode() == StreamOperatorCode.SINK
                    && inputModes.size() == 1
                    && Boolean.TRUE.equals(inputModes.get(0))) {
                validateUpsertKey(node);
            }
            List<ChangelogMode> legacyModes = new ArrayList<>(
                    inputModes.size());
            for (Boolean inputMode : inputModes) {
                legacyModes.add(Boolean.TRUE.equals(inputMode)
                        ? ChangelogMode.UPSERT : ChangelogMode.APPEND);
            }
            mainUpsert.put(node.operatorUid(), producesChangelog(
                    node, legacyModes, planChangelog)
                    == ChangelogMode.UPSERT);
            mainTombstones.put(node.operatorUid(), producesTombstones(
                    node, inputTombstones));
        }
    }

    private static void validateRetractContracts(
            List<PortableStreamNode> nodes, List<StreamEdge> edges,
            Map<String, ChangelogMode> mainModes,
            List<String> capabilities) {
        Map<String, StreamEdge> inputs = new HashMap<>();
        Map<String, PortableStreamNode> byUid = new HashMap<>();
        for (PortableStreamNode node : nodes) {
            byUid.put(node.operatorUid(), node);
        }
        for (StreamEdge edge : edges) {
            inputs.put(edge.targetOperatorUid() + '\0'
                    + edge.targetInputIndex(), edge);
        }
        for (PortableStreamNode node : nodes) {
            boolean retractInput = false;
            for (int index = 0;
                    index < node.inputOperatorUids().size(); index++) {
                StreamEdge edge = inputs.get(node.operatorUid() + '\0'
                        + index);
                PortableStreamNode source = edge == null ? null
                        : byUid.get(edge.sourceOperatorUid());
                if (source == null) {
                    throw StreamContractSupport.failure("INVALID_EDGE_SET",
                            "plan.edges",
                            "node input edge is unavailable");
                }
                ChangelogMode mode = edge.sourceOutputTagId() == null
                        ? mainModes.get(source.operatorUid())
                        : fromTagMode(outputTag(source,
                                edge.sourceOutputTagId()).changelogMode());
                retractInput |= mode == ChangelogMode.RETRACT;
            }
            boolean retractOutput = mainModes.get(node.operatorUid())
                    == ChangelogMode.RETRACT;
            if (!retractInput && !retractOutput) continue;
            if (!capabilities.contains(RETRACT_CHANGELOG_CAPABILITY)) {
                throw StreamContractSupport.failure("MISSING_CAPABILITY",
                        "plan.capabilities",
                        "RETRACT edge requires RETRACT capability");
            }
            if (node.opcode() == StreamOperatorCode.SOURCE
                    || node.opcode() == StreamOperatorCode.SINK
                    || node.opcode() == StreamOperatorCode.MAP
                    || node.opcode() == StreamOperatorCode.FILTER
                    || node.opcode() == StreamOperatorCode.CALC
                    || node.opcode() == StreamOperatorCode.FLAT_MAP
                    || node.opcode() == StreamOperatorCode.KEY_BY
                    || node.opcode() == StreamOperatorCode.UNION
                    || node.opcode()
                    == StreamOperatorCode.ASSIGN_EVENT_TIME) {
                if (node.stableKeyFields().isEmpty()) {
                    throw StreamContractSupport.failure(
                            "CHANGELOG_MODE_MISMATCH",
                            "plan.nodes.stableKeys",
                            "RETRACT operator requires a stable output identity");
                }
                continue;
            }
            if (node.opcode() == StreamOperatorCode.ASYNC_WAIT) {
                throw StreamContractSupport.failure(
                        "CHANGELOG_MODE_MISMATCH", "plan.nodes.async",
                        "RETRACT async wait requires a retract-aware result identity contract");
            }
            if (node.opcode() == StreamOperatorCode.KEYED_AGGREGATE
                    && ((StreamFunctionBinding) node.binding())
                    .deploymentReference().functionAbi()
                    == io.rowweave.portable.FunctionAbi
                    .STREAM_RETRACT_AGGREGATE_V2) {
                continue;
            }
            if (node.opcode() == StreamOperatorCode.GENERIC_WINDOW
                    && retractWindowSupported(
                    (StreamWindowBinding) node.binding())) {
                continue;
            }
            if (node.opcode() == StreamOperatorCode.TEMPORAL_JOIN) {
                continue;
            }
            if (isStatefulMailbox(node.opcode())) {
                throw StreamContractSupport.failure(
                        "CHANGELOG_MODE_MISMATCH", "plan.nodes.changelog",
                        "RETRACT stateful operator requires an explicit delete-aware ABI");
            }
        }
    }

    private static boolean retractWindowSupported(
            StreamWindowBinding binding) {
        return binding.functionKind()
                == StreamWindowBinding.FunctionKind.AGGREGATE
                && binding.deploymentReference().functionAbi()
                == io.rowweave.portable.FunctionAbi
                .STREAM_RETRACT_WINDOW_AGGREGATE_V2
                && (binding.assignerKind()
                == StreamWindowBinding.AssignerKind.EVENT_TIME_TUMBLING
                || binding.assignerKind()
                == StreamWindowBinding.AssignerKind.EVENT_TIME_SLIDING)
                && binding.triggerKind()
                == StreamWindowBinding.TriggerKind.EVENT_TIME_FINAL
                && binding.purging()
                && binding.accumulationMode()
                == StreamWindowBinding.AccumulationMode.DISCARDING
                && binding.allowedLatenessMillis() == 0L
                && binding.lateOutputTag() == null;
    }

    private static boolean sameStableKeyCodec(
            io.rowweave.portable.PortableRowSchema inputSchema,
            List<String> inputKeys,
            io.rowweave.portable.PortableRowSchema outputSchema,
            List<String> outputKeys) {
        if (!inputKeys.equals(outputKeys)) return false;
        for (String key : inputKeys) {
            io.rowweave.portable.PortableField input = processKeyField(
                    inputSchema, key);
            io.rowweave.portable.PortableField output = processKeyField(
                    outputSchema, key);
            if (input == null || !input.equals(output)) return false;
        }
        return true;
    }

    private static boolean producesTombstones(PortableStreamNode node,
            List<Boolean> inputTombstones) {
        if (node.opcode() == StreamOperatorCode.SOURCE) {
            return "RESIDENT_CDC_SNAPSHOT_LOG".equals(
                    node.scalarParameters().get("source.mode"));
        }
        if (isStatefulMailbox(node.opcode())
                || node.opcode() == StreamOperatorCode.SINK) {
            return false;
        }
        for (Boolean input : inputTombstones) {
            if (Boolean.TRUE.equals(input)) return true;
        }
        return false;
    }

    private static ChangelogMode producesChangelog(PortableStreamNode node,
            List<ChangelogMode> inputModes, ChangelogMode planChangelog) {
        if (node.opcode() == StreamOperatorCode.SOURCE) {
            if ("RETRACT".equals(
                    node.scalarParameters().get("source.changelog"))) {
                return ChangelogMode.RETRACT;
            }
            return "RESIDENT_CDC_SNAPSHOT_LOG".equals(
                    node.scalarParameters().get("source.mode"))
                    ? ChangelogMode.UPSERT : ChangelogMode.APPEND;
        }
        ChangelogMode output = node.opcode()
                == StreamOperatorCode.TEMPORAL_JOIN
                ? planChangelog == ChangelogMode.RETRACT
                ? ChangelogMode.RETRACT : ChangelogMode.UPSERT
                : ChangelogMode.APPEND;
        if (node.opcode() == StreamOperatorCode.KEYED_AGGREGATE) {
            output = planChangelog == ChangelogMode.RETRACT
                    ? ChangelogMode.RETRACT : ChangelogMode.UPSERT;
        }
        if (node.opcode() == StreamOperatorCode.GENERIC_WINDOW) {
            output = planChangelog == ChangelogMode.RETRACT
                    ? ChangelogMode.RETRACT
                    : ((StreamWindowBinding) node.binding()).accumulationMode()
                    == StreamWindowBinding.AccumulationMode.ACCUMULATING
                    ? ChangelogMode.UPSERT : ChangelogMode.APPEND;
        }
        if ((node.opcode() == StreamOperatorCode.KEYED_PROCESS
                || node.opcode() == StreamOperatorCode.KEYED_CO_PROCESS)
                && planChangelog != ChangelogMode.APPEND) {
            output = planChangelog;
        }
        if (node.opcode() == StreamOperatorCode.UNION
                && !inputModes.isEmpty()) {
            output = inputModes.get(0);
        }
        if (output == ChangelogMode.APPEND && inputModes.size() == 1) {
            output = inputModes.get(0);
        }
        return output;
    }

    private static ChangelogMode fromTagMode(
            StreamOutputTagDescriptor.ChangelogMode mode) {
        switch (mode) {
            case APPEND: return ChangelogMode.APPEND;
            case UPSERT: return ChangelogMode.UPSERT;
            case RETRACT: return ChangelogMode.RETRACT;
            default: throw new IllegalStateException(
                    "unknown tagged changelog mode");
        }
    }

    private static StreamOutputTagDescriptor outputTag(
            PortableStreamNode source, String tagId) {
        if (source == null) return null;
        for (StreamOutputTagDescriptor tag : outputTags(source)) {
            if (tag.tagId().equals(tagId)) return tag;
        }
        return null;
    }

    private static List<StreamOutputTagDescriptor> outputTags(
            PortableStreamNode node) {
        if (node.binding() instanceof StreamProcessBinding) {
            return ((StreamProcessBinding) node.binding()).outputTags();
        }
        if (node.binding() instanceof StreamWindowBinding
                && ((StreamWindowBinding) node.binding())
                .lateOutputTag() != null) {
            return Collections.singletonList(
                    ((StreamWindowBinding) node.binding()).lateOutputTag());
        }
        return Collections.emptyList();
    }
    private static void validateStableKeys(PortableStreamNode node) {
        if (node.stableKeyFields().isEmpty()) return;
        Map<String, Boolean> fields = new HashMap<>();
        for (io.rowweave.portable.PortableField field :
                node.outputSchema().fields()) {
            fields.put(field.fieldId(), field.nullable());
        }
        Set<String> seen = new HashSet<>();
        for (String key : node.stableKeyFields()) {
            Boolean nullable = fields.get(key);
            if (!seen.add(key) || nullable == null || nullable) {
                throw StreamContractSupport.failure("INVALID_STABLE_KEY",
                        "plan.nodes.stableKeys",
                        "stable keys must be unique non-null output fields");
            }
        }
    }

    private static void validateWindow(
            PortableStreamNode node, TimeMode timeMode,
            List<InputContract> inputs, List<String> capabilities) {
        if (timeMode != TimeMode.EVENT_TIME) {
            throw StreamContractSupport.failure("TIME_MODE_MISMATCH",
                    "plan.nodes.window",
                    "event-time window requires event-time plan semantics");
        }
        boolean explicitFields = capabilities.contains(
                WINDOW_FIELD_BINDING_CAPABILITY);
        int expectedParameters = explicitFields ? 9 : 4;
        if (node.stableKeyFields().isEmpty()
                || node.stateDescriptors().isEmpty()
                || node.scalarParameters().size() != expectedParameters
                || !"FINAL".equals(node.scalarParameters().get("window.trigger"))) {
            invalidWindow();
        }
        String type = node.scalarParameters().get("window.type");
        long size = positiveLong(
                node.scalarParameters().get("window.sizeMillis"));
        long slide = positiveLong(
                node.scalarParameters().get("window.slideMillis"));
        if ((!"TUMBLING".equals(type) && !"SLIDING".equals(type))
                || slide > size
                || ("TUMBLING".equals(type) && slide != size)) {
            invalidWindow();
        }
        if (explicitFields) validateWindowFields(node, inputs.get(0));
    }

    private static void validateWindowFields(PortableStreamNode node,
            InputContract upstream) {
        if (!upstream.stableKeys.containsAll(node.stableKeyFields())
                || node.stateDescriptors().size() != 1
                || node.stateDescriptors().get(0).kind()
                != StreamStateDescriptor.Kind.KEYED_VALUE
                || node.stateDescriptors().get(0).ttlMode()
                != StreamStateDescriptor.TtlMode.NONE) {
            invalidWindowFields();
        }
        java.util.Map<String, io.rowweave.portable.PortableField> input =
                fields(upstream.schema);
        java.util.Map<String, io.rowweave.portable.PortableField> output =
                fields(node.outputSchema());
        io.rowweave.portable.PortableField eventTime = input.get(
                node.scalarParameters().get("window.eventTimeField"));
        io.rowweave.portable.PortableField value = input.get(
                node.scalarParameters().get("window.valueField"));
        io.rowweave.portable.PortableField start = output.get(
                node.scalarParameters().get("window.startOutputField"));
        io.rowweave.portable.PortableField end = output.get(
                node.scalarParameters().get("window.endOutputField"));
        io.rowweave.portable.PortableField result = output.get(
                node.scalarParameters().get("window.valueOutputField"));
        if (eventTime == null || eventTime.nullable()
                || eventTime.dataType()
                != io.rowweave.portable.PortableDataType.TIMESTAMP
                || value == null || value.dataType()
                != io.rowweave.portable.PortableDataType.BIG_DECIMAL
                || start == null || start.nullable() || start.dataType()
                != io.rowweave.portable.PortableDataType.LONG
                || end == null || end.nullable() || end.dataType()
                != io.rowweave.portable.PortableDataType.LONG
                || result == null || result.nullable() || result.dataType()
                != io.rowweave.portable.PortableDataType.BIG_DECIMAL) {
            invalidWindowFields();
        }
    }

    private static java.util.Map<String, io.rowweave.portable.PortableField> fields(
            io.rowweave.portable.PortableRowSchema schema) {
        java.util.Map<String, io.rowweave.portable.PortableField> fields =
                new java.util.HashMap<>();
        for (io.rowweave.portable.PortableField field : schema.fields()) {
            fields.put(field.fieldId(), field);
        }
        return fields;
    }

    private static void invalidWindowFields() {
        throw StreamContractSupport.failure("INVALID_WINDOW_FIELD_BINDING",
                "plan.nodes.window.fields",
                "product window fields and state must be explicit and type-safe");
    }

    private static long positiveLong(String value) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed > 0L) return parsed;
        } catch (RuntimeException ignored) {
            // Rejected below with the stable Stream contract error.
        }
        invalidWindow();
        return 0L;
    }

    private static void invalidWindow() {
        throw StreamContractSupport.failure("INVALID_WINDOW",
                "plan.nodes.window",
                "window requires a finite tumbling/sliding final-trigger contract");
    }

    private static void validateTemporalJoin(
            PortableStreamNode node, TimeMode timeMode,
            ChangelogMode changelogMode,
            List<InputContract> inputs,
            List<String> capabilities) {
        if (timeMode != TimeMode.EVENT_TIME) {
            throw StreamContractSupport.failure("TIME_MODE_MISMATCH",
                    "plan.nodes.temporalJoin",
                    "temporal join requires event-time plan semantics");
        }
        if (!capabilities.contains(TEMPORAL_JOIN_CAPABILITY)) {
            throw StreamContractSupport.failure("MISSING_CAPABILITY",
                    "plan.capabilities",
                    "temporal join capability is required");
        }
        if (changelogMode != ChangelogMode.UPSERT
                && changelogMode != ChangelogMode.RETRACT) {
            throw StreamContractSupport.failure("CHANGELOG_MODE_MISMATCH",
                    "plan.changelog",
                    "temporal join requires UPSERT or RETRACT output");
        }
        SortedMap<String, String> parameters = node.scalarParameters();
        if (parameters.size() != 4) invalidTemporalJoin();
        String leftKey = parameters.get("join.leftKeyField");
        String rightKey = parameters.get("join.rightKeyField");
        Long lower = signedLong(parameters.get("join.lowerBoundMillis"));
        Long upper = signedLong(parameters.get("join.upperBoundMillis"));
        if (leftKey == null || leftKey.isEmpty()
                || rightKey == null || rightKey.isEmpty()
                || lower == null || upper == null
                || lower.longValue() > upper.longValue()) {
            invalidTemporalJoin();
        }
        InputContract left = inputs.get(0);
        InputContract right = inputs.get(1);
        if (changelogMode == ChangelogMode.RETRACT
                && (left.changelogMode == ChangelogMode.UPSERT
                || right.changelogMode == ChangelogMode.UPSERT)) {
            throw StreamContractSupport.failure("CHANGELOG_MODE_MISMATCH",
                    "plan.nodes.temporalJoin",
                    "RETRACT temporal join requires before-images or append-only input");
        }
        if (!hasStableKey(left, leftKey) || !hasStableKey(right, rightKey)) {
            throw StreamContractSupport.failure("INVALID_TEMPORAL_JOIN_KEY",
                    "plan.nodes.temporalJoin.key",
                    "both temporal join inputs require declared non-null stable keys");
        }
        long horizon = cleanupHorizon(lower.longValue(), upper.longValue());
        if (node.stateDescriptors().size() != 2) invalidTemporalJoinState();
        for (StreamStateDescriptor state : node.stateDescriptors()) {
            if (state.kind() != StreamStateDescriptor.Kind.KEYED_MAP
                    || state.ttlMode()
                    != StreamStateDescriptor.TtlMode.EVENT_TIME
                    || state.ttlMillis() < horizon) {
                invalidTemporalJoinState();
            }
        }
    }

    private static boolean hasStableKey(
            InputContract input, String fieldId) {
        return input.stableKeys.contains(fieldId);
    }

    private static Long signedLong(String value) {
        if (value == null) return null;
        try {
            return Long.valueOf(Long.parseLong(value));
        } catch (NumberFormatException invalid) {
            return null;
        }
    }

    private static long cleanupHorizon(long lower, long upper) {
        if (lower == Long.MIN_VALUE) invalidTemporalJoin();
        return Math.max(0L, Math.max(upper, -lower));
    }

    private static void invalidTemporalJoin() {
        throw StreamContractSupport.failure("INVALID_TEMPORAL_JOIN",
                "plan.nodes.temporalJoin",
                "temporal join requires finite ordered lower/upper bounds and keys");
    }

    private static void invalidTemporalJoinState() {
        throw StreamContractSupport.failure("INVALID_TEMPORAL_JOIN_STATE",
                "plan.nodes.temporalJoin.state",
                "temporal join requires two bounded event-time keyed-map states");
    }

    private static void validateSourceTimePolicy(
            PortableStreamNode node, TimeMode timeMode) {
        SortedMap<String, String> parameters = node.scalarParameters();
        boolean retract = "RETRACT".equals(
                parameters.get("source.changelog"));
        int expectedParameters = (retract ? 5 : 4)
                + (parameters.containsKey("source.termination") ? 1 : 0);
        if (timeMode != TimeMode.EVENT_TIME
                || parameters.size() != expectedParameters
                || !"PROVIDER_RECORD".equals(
                parameters.get("source.time.timestamp"))
                || !"PROVIDER_EVENT".equals(
                parameters.get("source.time.watermark"))
                || !"GENERATION_FENCED_ACTIVE_IDLE".equals(
                parameters.get("source.time.status"))) {
            throw StreamContractSupport.failure("INVALID_SOURCE_TIME_POLICY",
                    "plan.nodes.source.time",
                    "event-time source policy is incomplete or mismatched");
        }
        long idleTimeout = sourceIdleTimeout(
                parameters.get("source.time.idleTimeoutMillis"));
        if (idleTimeout > 7L * 24L * 60L * 60L * 1000L) {
            throw StreamContractSupport.failure("INVALID_SOURCE_TIME_POLICY",
                    "plan.nodes.source.time.idleTimeoutMillis",
                    "source idle timeout exceeds its bound");
        }
    }

    private static void validateBoundedSource(PortableStreamNode node) {
        StreamSourceBinding source = (StreamSourceBinding) node.binding();
        SortedMap<String, String> parameters = node.scalarParameters();
        if (source.dynamicDiscovery()
                || parameters.containsKey("source.mode")
                || !"EXPLICIT_END_OF_INPUT".equals(
                parameters.get("source.termination"))
                || parameters.size() != 1
                + (parameters.containsKey("source.changelog") ? 1 : 0)
                + (parameters.containsKey("source.time.timestamp") ? 4 : 0)) {
            throw StreamContractSupport.failure(
                    "INVALID_SOURCE_TERMINATION",
                    "plan.nodes.source.termination",
                    "bounded completion requires one fixed Source termination declaration");
        }
    }

    private static long sourceIdleTimeout(String value) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed > 0L) return parsed;
        } catch (RuntimeException ignored) {
            // Rejected below with the stable source policy error.
        }
        throw StreamContractSupport.failure("INVALID_SOURCE_TIME_POLICY",
                "plan.nodes.source.time.idleTimeoutMillis",
                "source idle timeout must be positive");
    }

    private static void validateResidentCdcSource(
            PortableStreamNode node, ChangelogMode changelogMode,
            List<String> capabilities) {
        if (!capabilities.contains(RESIDENT_CDC_CAPABILITY)) {
            throw StreamContractSupport.failure("MISSING_CAPABILITY",
                    "plan.capabilities",
                    "resident CDC capability is required");
        }
        if (changelogMode != ChangelogMode.UPSERT) {
            throw StreamContractSupport.failure("CHANGELOG_MODE_MISMATCH",
                    "plan.changelog",
                    "resident CDC requires UPSERT output");
        }
        StreamSourceBinding source = (StreamSourceBinding) node.binding();
        SortedMap<String, String> parameters = node.scalarParameters();
        boolean v2 = "STREAM_CDC_ENUMERATOR_V2".equals(
                parameters.get("source.enumeratorStateFormat"));
        if (v2 && !capabilities.contains(RESIDENT_CDC_V2_CAPABILITY)) {
            throw StreamContractSupport.failure("MISSING_CAPABILITY",
                    "plan.capabilities",
                    "resident CDC v2 capability is required");
        }
        if (parameters.size() != 2
                || !"RESIDENT_CDC_SNAPSHOT_LOG".equals(
                parameters.get("source.mode"))
                || (!("STREAM_CDC_ENUMERATOR_V1".equals(
                parameters.get("source.enumeratorStateFormat")))
                && !("STREAM_CDC_ENUMERATOR_V2".equals(
                parameters.get("source.enumeratorStateFormat"))))
                || source.fixedSplits().size() > 1
                || node.stableKeyFields().isEmpty()) {
            throw StreamContractSupport.failure("INVALID_RESIDENT_CDC_SOURCE",
                    "plan.nodes.source",
                    "resident CDC requires a supported snapshot-log enumerator and stable keys");
        }
    }

    private static boolean isStatefulMailbox(StreamOperatorCode opcode) {
        return opcode == StreamOperatorCode.KEYED_AGGREGATE
                || opcode == StreamOperatorCode.EVENT_TIME_WINDOW
                || opcode == StreamOperatorCode.TEMPORAL_JOIN
                || opcode == StreamOperatorCode.KEYED_PROCESS
                || opcode == StreamOperatorCode.KEYED_CO_PROCESS
                || opcode == StreamOperatorCode.GENERIC_WINDOW;
    }
    private static void validateUpsertKey(PortableStreamNode sink){if(sink.stableKeyFields().isEmpty())throw StreamContractSupport.failure("MISSING_UPSERT_KEY","plan.nodes.stableKeys","UPSERT sink requires a stable key");Map<String,Boolean> fields=new HashMap<>();for(io.rowweave.portable.PortableField field:sink.outputSchema().fields())fields.put(field.fieldId(),field.nullable());for(String key:sink.stableKeyFields()){Boolean nullable=fields.get(key);if(nullable==null||nullable)throw StreamContractSupport.failure("INVALID_UPSERT_KEY","plan.nodes.stableKeys","UPSERT key must reference a non-null field");}}
    private static List<String> canonicalTexts(List<String> values,String path,int max){if(values==null||values.isEmpty()||values.size()>max)throw StreamContractSupport.failure("LIMIT_EXCEEDED",path,"invalid list size");TreeSet<String> out=new TreeSet<>();for(String v:values)if(!out.add(StreamContractSupport.text(v,path,256)))throw StreamContractSupport.failure("DUPLICATE_VALUE",path,"values must be unique");return Collections.unmodifiableList(new ArrayList<>(out));}
    public int planVersion(){return version;}public DeliveryGuarantee deliveryGuarantee(){return delivery;}public int parallelism(){return parallelism;}public int maxParallelism(){return maxParallelism;}public ChangelogMode changelogMode(){return changelog;}public TimeMode timeMode(){return time;}public List<String> rootSinkOperatorUids(){return roots;}public List<PortableStreamNode> nodes(){return nodes;}public List<StreamEdge> edges(){return edges;}public List<String> requiredCapabilities(){return capabilities;}
    public ChangelogMode outputChangelogMode(String operatorUid) {
        ChangelogMode mode = outputChangelogs.get(operatorUid);
        if (mode == null) {
            throw StreamContractSupport.failure("UNKNOWN_OPERATOR_UID",
                    "plan.nodes.operatorUid",
                    "operator output changelog is unavailable");
        }
        return mode;
    }
    @Override public boolean equals(Object o){if(!(o instanceof PortableStreamPlan))return false;PortableStreamPlan p=(PortableStreamPlan)o;return version==p.version&&parallelism==p.parallelism&&maxParallelism==p.maxParallelism&&delivery==p.delivery&&changelog==p.changelog&&time==p.time&&roots.equals(p.roots)&&nodes.equals(p.nodes)&&edges.equals(p.edges)&&capabilities.equals(p.capabilities);}
    @Override public int hashCode(){return Objects.hash(version,delivery,parallelism,maxParallelism,changelog,time,roots,nodes,edges,capabilities);}
}
