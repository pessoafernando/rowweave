package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCheckpointMode;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamContractException;
import io.rowweave.stream.contract.StreamCalcBinding;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamFunctionBinding;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceDiscoveryDescriptor;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.contract.StreamStateDescriptor;
import io.rowweave.stream.contract.StreamTimeBinding;
import io.rowweave.stream.contract.StreamManagedStateDescriptor;
import io.rowweave.stream.contract.StreamProcessBinding;
import io.rowweave.stream.contract.StreamTimerDescriptor;
import io.rowweave.stream.contract.StreamBroadcastStateDescriptor;
import io.rowweave.stream.contract.StreamOutputTagDescriptor;
import io.rowweave.stream.contract.StreamWindowBinding;
import io.rowweave.stream.contract.StreamWindowResourceDescriptor;
import io.rowweave.stream.contract.StreamAsyncBinding;
import io.rowweave.stream.contract.StreamAsyncResourceDescriptor;
import io.rowweave.stream.contract.StreamNodeExecutionDescriptor;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

/** Bounded-metadata compiler from the public immutable DSL to Stream IR. */
final class StreamPlanCompiler {
    private final StreamSession session;
    private final List<PortableStreamNode> nodes = new ArrayList<>();
    private final List<StreamEdge> edges = new ArrayList<>();
    private final Map<DataStream, PortableStreamNode> compiled =
            new IdentityHashMap<>();
    private final Map<DataStream, StreamChangelogMode> outputChangelogs =
            new IdentityHashMap<>();
    private final Map<String, StreamChangelogMode> nodeOutputChangelogs =
            new HashMap<>();
    private final Map<String, DataStream> identities = new LinkedHashMap<>();

    private StreamPlanCompiler(StreamSession session) { this.session = session; }

    static PlanArtifact compile(StreamSession session) {
        for (DataStream node : session.nodes()) {
            if (node.kind() == DataStream.Kind.MAP
                    && node.function().functionAbi() != FunctionAbi.MAP_V1) {
                throw failure(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                        "map requires MAP_V1 function ABI");
            }
            if (node.kind() == DataStream.Kind.FILTER
                    && node.function().functionAbi() != FunctionAbi.FILTER_V1) {
                throw failure(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                        "filter requires FILTER_V1 function ABI");
            }
            if (node.kind() == DataStream.Kind.FLAT_MAP
                    && node.function().functionAbi() != FunctionAbi.FLAT_MAP_V1) {
                throw failure(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                        "flatMap requires FLAT_MAP_V1 function ABI");
            }
            if (node.kind() == DataStream.Kind.ASSIGN_EVENT_TIME
                    && session.time()
                    != io.rowweave.stream.api.StreamTimeMode.EVENT_TIME) {
                throw failure(StreamApiException.Code.TIME_MODE_MISMATCH,
                        "event-time assignment requires EVENT_TIME session semantics");
            }
            if (node.kind() == DataStream.Kind.KEYED_AGGREGATE
                    && node.function().functionAbi()
                    != FunctionAbi.STREAM_AGGREGATE_V1
                    && node.function().functionAbi()
                    != FunctionAbi.STREAM_RETRACT_AGGREGATE_V2) {
                throw failure(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                        "keyed aggregate requires a supported aggregate ABI");
            }
            if (node.kind() == DataStream.Kind.KEYED_PROCESS
                    && node.function().functionAbi()
                    != FunctionAbi.KEYED_PROCESS_V1) {
                throw failure(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                        "keyed process requires KEYED_PROCESS_V1 function ABI");
            }
            if (node.kind() == DataStream.Kind.KEYED_CO_PROCESS
                    && node.function().functionAbi()
                    != FunctionAbi.KEYED_CO_PROCESS_V1) {
                throw failure(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                        "keyed co-process requires KEYED_CO_PROCESS_V1 function ABI");
            }
            if (node.kind() == DataStream.Kind.KEYED_AGGREGATE
                    && session.changelog()
                    != io.rowweave.stream.api.StreamChangelogMode.UPSERT
                    && session.changelog()
                    != io.rowweave.stream.api.StreamChangelogMode.RETRACT) {
                throw failure(StreamApiException.Code.CHANGELOG_MODE_MISMATCH,
                        "keyed aggregate requires UPSERT or RETRACT changelog mode");
            }
            if (node.kind() == DataStream.Kind.KEYED_AGGREGATE
                    && session.changelog()
                    == io.rowweave.stream.api.StreamChangelogMode.RETRACT
                    && node.function().functionAbi()
                    != FunctionAbi.STREAM_RETRACT_AGGREGATE_V2) {
                throw failure(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                        "RETRACT keyed aggregate requires its reversible v2 ABI");
            }
            if (node.kind() == DataStream.Kind.TEMPORAL_JOIN
                    && node.function().functionAbi() != FunctionAbi.CO_GROUP_V1) {
                throw failure(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                        "temporal join requires CO_GROUP_V1 function ABI");
            }
        }
        if (session.outputs().isEmpty()) {
            throw failure(StreamApiException.Code.MISSING_SINK,
                    "Stream job requires at least one sink");
        }
        for (DataStream node : session.nodes()) {
            if (node.kind() != DataStream.Kind.SOURCE
                    || !node.sourceSpec().residentCdc()) continue;
            if (session.changelog()
                    != io.rowweave.stream.api.StreamChangelogMode.UPSERT) {
                throw failure(StreamApiException.Code.CHANGELOG_MODE_MISMATCH,
                        "resident CDC requires UPSERT changelog mode");
            }
            if (session.time()
                    != io.rowweave.stream.api.StreamTimeMode.PROCESSING_TIME) {
                throw failure(StreamApiException.Code.TIME_MODE_MISMATCH,
                        "resident CDC v1 requires processing-time semantics");
            }
        }
        try {
            return new StreamPlanCompiler(session).compileGraph();
        } catch (StreamApiException failure) {
            throw failure;
        } catch (StreamContractException failure) {
            throw translate(failure);
        }
    }

    private PlanArtifact compileGraph() {
        List<String> roots = new ArrayList<>();
        List<StreamSession.OutputRegistration> outputs =
                new ArrayList<>(session.outputs());
        outputs.sort(Comparator.comparing(output -> output.uid));
        for (StreamSession.OutputRegistration output : outputs) {
            PortableStreamNode upstream = compileNode(output.input);
            checkUnique(output.uid, null);
            StreamSinkSpec sink = output.sink;
            io.rowweave.portable.PortableRowSchema inputSchema =
                    output.input.schema().toPortableSchema();
            PortableStreamNode sinkNode = new PortableStreamNode(
                    output.uid, StreamOperatorCode.SINK,
                    Collections.singletonList(upstream.operatorUid()),
                    Collections.singletonList(inputSchema.fingerprint()),
                    inputSchema, outputPortStableKeys(output.input, upstream),
                    Collections.emptyList(), Collections.emptyMap(),
                    new StreamSinkBinding(sink.providerKey(),
                            sink.providerContractDigest(), sha256(sink.configurationFrame()),
                            sink.configurationFrame(),
                            sink.visibility() == StreamSinkSpec.Visibility.CHECKPOINT_TRANSACTIONAL
                                    ? StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL
                                    : StreamSinkBinding.VisibilityProfile.EAGER_AT_LEAST_ONCE,
                            contractModes(sink.acceptedChangelogModes())));
            nodes.add(sinkNode);
            nodeOutputChangelogs.put(sinkNode.operatorUid(),
                    outputPortChangelog(output.input));
            addEdge(upstream, sinkNode, 0, outputTagId(output.input));
            roots.add(output.uid);
        }
        List<String> capabilities = new ArrayList<>();
        capabilities.add(PortableStreamPlan.STREAM_CAPABILITY);
        if (session.changelog()
                == io.rowweave.stream.api.StreamChangelogMode.UPSERT) {
            capabilities.add(PortableStreamPlan.UPSERT_CAPABILITY);
        } else if (session.changelog()
                == io.rowweave.stream.api.StreamChangelogMode.RETRACT) {
            capabilities.add(PortableStreamPlan.RETRACT_CHANGELOG_CAPABILITY);
        }
        if (session.checkpoint().mode() == StreamCheckpointMode.UNALIGNED) {
            capabilities.add(
                    PortableStreamPlan.UNALIGNED_CHECKPOINT_CAPABILITY);
        }
        boolean temporalJoin = false;
        boolean explicitWindow = false;
        boolean sourceTimePolicy = false;
        boolean residentCdc = false;
        boolean residentCdcV2 = false;
        boolean dynamicSource = false;
        boolean boundedSource = false;
        boolean flatMap = false;
        boolean union = false;
        boolean calc = false;
        boolean eventTimeAssignment = false;
        boolean keyedProcess = false;
        boolean keyedCoProcess = false;
        boolean broadcastState = false;
        boolean taggedOutput = false;
        boolean taggedUpsert = false;
        boolean genericWindow = false;
        boolean asyncWait = false;
        for (PortableStreamNode node : nodes) {
            if (node.opcode() == StreamOperatorCode.TEMPORAL_JOIN) {
                temporalJoin = true;
            } else if (node.opcode() == StreamOperatorCode.EVENT_TIME_WINDOW) {
                explicitWindow = true;
            }
            if (node.opcode() == StreamOperatorCode.FLAT_MAP) flatMap = true;
            if (node.opcode() == StreamOperatorCode.UNION) union = true;
            if (node.opcode() == StreamOperatorCode.CALC) calc = true;
            if (node.opcode() == StreamOperatorCode.ASSIGN_EVENT_TIME) {
                eventTimeAssignment = true;
            }
            if (node.opcode() == StreamOperatorCode.KEYED_PROCESS) {
                keyedProcess = true;
            }
            if (node.opcode() == StreamOperatorCode.KEYED_CO_PROCESS) {
                keyedCoProcess = true;
                broadcastState |= ((StreamProcessBinding) node.binding())
                        .broadcastState() != null;
            }
            List<StreamOutputTagDescriptor> outputTags =
                    declaredOutputTags(node);
            taggedOutput |= !outputTags.isEmpty();
            for (StreamOutputTagDescriptor tag : outputTags) {
                taggedUpsert |= tag.changelogMode()
                        == StreamOutputTagDescriptor.ChangelogMode.UPSERT;
            }
            if (node.opcode() == StreamOperatorCode.GENERIC_WINDOW) {
                genericWindow = true;
            }
            if (node.opcode() == StreamOperatorCode.ASYNC_WAIT) {
                asyncWait = true;
            }
            if (node.opcode() == StreamOperatorCode.SOURCE
                    && node.scalarParameters().containsKey(
                    "source.time.watermark")) {
                sourceTimePolicy = true;
            }
            if (node.opcode() == StreamOperatorCode.SOURCE
                    && "RESIDENT_CDC_SNAPSHOT_LOG".equals(
                    node.scalarParameters().get("source.mode"))) {
                residentCdc = true;
                residentCdcV2 |= "STREAM_CDC_ENUMERATOR_V2".equals(
                        node.scalarParameters().get(
                                "source.enumeratorStateFormat"));
            }
            if (node.opcode() == StreamOperatorCode.SOURCE
                    && ((StreamSourceBinding) node.binding())
                    .dynamicDiscovery()) {
                dynamicSource = true;
            }
            if (node.opcode() == StreamOperatorCode.SOURCE
                    && "EXPLICIT_END_OF_INPUT".equals(
                    node.scalarParameters().get("source.termination"))) {
                boundedSource = true;
            }
        }
        if (temporalJoin) {
            capabilities.add(PortableStreamPlan.TEMPORAL_JOIN_CAPABILITY);
        }
        if (explicitWindow) {
            capabilities.add(PortableStreamPlan.WINDOW_FIELD_BINDING_CAPABILITY);
        }
        if (sourceTimePolicy) {
            capabilities.add(PortableStreamPlan.SOURCE_TIME_POLICY_CAPABILITY);
        }
        if (residentCdc) {
            capabilities.add(PortableStreamPlan.RESIDENT_CDC_CAPABILITY);
        }
        if (residentCdcV2) {
            capabilities.add(PortableStreamPlan.RESIDENT_CDC_V2_CAPABILITY);
        }
        if (dynamicSource) {
            capabilities.add(
                    PortableStreamPlan.DYNAMIC_SPLIT_DISCOVERY_CAPABILITY);
        }
        if (boundedSource) {
            capabilities.add(
                    PortableStreamPlan.SOURCE_END_OF_INPUT_CAPABILITY);
        }
        if (flatMap) capabilities.add(PortableStreamPlan.FLAT_MAP_CAPABILITY);
        if (union) capabilities.add(PortableStreamPlan.UNION_CAPABILITY);
        if (calc) capabilities.add(PortableStreamPlan.CALC_CAPABILITY);
        if (eventTimeAssignment) capabilities.add(
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY);
        if (keyedProcess) capabilities.add(
                PortableStreamPlan.KEYED_PROCESS_CAPABILITY);
        if (keyedCoProcess) capabilities.add(
                PortableStreamPlan.KEYED_CO_PROCESS_CAPABILITY);
        if (broadcastState) capabilities.add(
                PortableStreamPlan.BROADCAST_STATE_CAPABILITY);
        if (taggedOutput) capabilities.add(
                PortableStreamPlan.TAGGED_OUTPUT_CAPABILITY);
        if (taggedUpsert && !capabilities.contains(
                PortableStreamPlan.UPSERT_CAPABILITY)) {
            capabilities.add(PortableStreamPlan.UPSERT_CAPABILITY);
        }
        if (genericWindow) capabilities.add(
                PortableStreamPlan.GENERIC_WINDOW_CAPABILITY);
        if (asyncWait) capabilities.add(
                PortableStreamPlan.ASYNC_WAIT_CAPABILITY);
        capabilities.add(
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY);
        validateConsumedOutputTags();
        List<PortableStreamNode> executionNodes =
                withNodeExecutionContracts(nodes);
        PortableStreamPlan plan = new PortableStreamPlan(
                session.changelog()
                        == io.rowweave.stream.api.StreamChangelogMode.RETRACT
                        ? PortableStreamPlan.RETRACT_CHANGELOG_VERSION
                        : dynamicSource
                        ? PortableStreamPlan.DYNAMIC_SOURCE_VERSION
                        : PortableStreamPlan.FIXED_SOURCE_VERSION,
                session.delivery() == io.rowweave.stream.api.StreamDeliveryGuarantee.EXACTLY_ONCE
                        ? PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE
                        : PortableStreamPlan.DeliveryGuarantee.AT_LEAST_ONCE,
                session.parallelism(), session.maxParallelism(),
                contractMode(session.changelog()),
                session.time() == io.rowweave.stream.api.StreamTimeMode.EVENT_TIME
                        ? PortableStreamPlan.TimeMode.EVENT_TIME
                        : PortableStreamPlan.TimeMode.PROCESSING_TIME,
                roots, executionNodes, edges, capabilities);
        PlanArtifact artifact = PortableStreamJobArtifact.wrap(session.definitionId(),
                new PortableStreamJob(plan, session.checkpoint(),
                        session.restart(), session.resources()));
        return session.deploymentSetId() == null ? artifact
                : PlanArtifactDeploymentBinding.bindStreamConnector(
                        artifact, session.deploymentSetId());
    }

    private List<PortableStreamNode> withNodeExecutionContracts(
            List<PortableStreamNode> compiledNodes) {
        List<PortableStreamNode> result =
                new ArrayList<>(compiledNodes.size());
        for (PortableStreamNode node : compiledNodes) {
            result.add(new PortableStreamNode(node.operatorUid(),
                    node.opcode(), node.inputOperatorUids(),
                    node.inputSchemaFingerprints(), node.outputSchema(),
                    node.stableKeyFields(), node.stateDescriptors(),
                    node.scalarParameters(), node.binding(),
                    nodeExecutionDescriptor(node)));
        }
        return result;
    }

    private StreamNodeExecutionDescriptor nodeExecutionDescriptor(
            PortableStreamNode node) {
        io.rowweave.stream.api.StreamResourceProfile resources =
                session.resources();
        int nodeParallelism = session.parallelism();
        if (node.opcode() == StreamOperatorCode.SOURCE) {
            StreamSourceBinding source = (StreamSourceBinding) node.binding();
            nodeParallelism = source.dynamicDiscovery()
                    ? session.parallelism() : source.fixedSplits().size();
            if (nodeParallelism > session.maxParallelism()) {
                throw failure(StreamApiException.Code.INVALID_CONFIGURATION,
                        "fixed source splits exceed maximum parallelism");
            }
        }
        boolean stateful = node.opcode() == StreamOperatorCode.KEYED_AGGREGATE
                || node.opcode() == StreamOperatorCode.EVENT_TIME_WINDOW
                || node.opcode() == StreamOperatorCode.TEMPORAL_JOIN
                || node.opcode() == StreamOperatorCode.KEYED_PROCESS
                || node.opcode() == StreamOperatorCode.KEYED_CO_PROCESS
                || node.opcode() == StreamOperatorCode.GENERIC_WINDOW;
        boolean timed = node.opcode() == StreamOperatorCode.EVENT_TIME_WINDOW
                || node.opcode() == StreamOperatorCode.TEMPORAL_JOIN
                || node.opcode() == StreamOperatorCode.KEYED_PROCESS
                || node.opcode() == StreamOperatorCode.KEYED_CO_PROCESS
                || node.opcode() == StreamOperatorCode.GENERIC_WINDOW;
        boolean windowed = node.opcode()
                == StreamOperatorCode.EVENT_TIME_WINDOW
                || node.opcode() == StreamOperatorCode.GENERIC_WINDOW;
        int threads = 1;
        if (node.opcode() == StreamOperatorCode.SOURCE
                && node.scalarParameters().containsKey(
                "source.time.idleTimeoutMillis")) {
            threads = Math.addExact(threads, 1);
        }
        long residentBytes = resources.maximumFrameBytes();
        if (stateful) {
            residentBytes = Math.max(residentBytes,
                    resources.managedStateBytes());
        }
        if (node.binding() instanceof StreamAsyncBinding) {
            StreamAsyncBinding async = (StreamAsyncBinding) node.binding();
            residentBytes = Math.addExact(residentBytes,
                    async.resources().maximumPendingBytes());
            threads = Math.addExact(threads,
                    Math.addExact(async.resources().maximumConcurrency(), 1));
        }
        return new StreamNodeExecutionDescriptor(
                contractMode(nodeOutputChangelog(node)),
                session.time()
                        == io.rowweave.stream.api.StreamTimeMode.EVENT_TIME
                        ? PortableStreamPlan.TimeMode.EVENT_TIME
                        : PortableStreamPlan.TimeMode.PROCESSING_TIME,
                nodeParallelism, session.maxParallelism(),
                residentBytes, timed ? resources.timerCount() : 0,
                windowed ? resources.windowCount() : 0,
                resources.checkpointBytes(),
                stateful ? resources.spillDiskBytes() : 0L, threads);
    }

    private PortableStreamNode compileNode(DataStream stream) {
        PortableStreamNode existing = compiled.get(stream);
        if (existing != null) return existing;
        if (stream.session() != session) {
            throw failure(StreamApiException.Code.INVALID_TOPOLOGY,
                    "Stream graph crosses session boundary");
        }
        if (stream.kind() == DataStream.Kind.TAGGED_VIEW) {
            PortableStreamNode producer = compileNode(stream.input());
            compiled.put(stream, producer);
            outputChangelogs.put(stream, stream.outputTag().changelogMode());
            return producer;
        }
        if (stream.kind() == DataStream.Kind.FILTER
                && stream.function().functionAbi() != FunctionAbi.FILTER_V1) {
            throw failure(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                    "filter requires FILTER_V1 function ABI");
        }
        String uid = stream.operatorUid();
        if (uid == null || uid.isEmpty()) {
            throw failure(StreamApiException.Code.MISSING_OPERATOR_UID,
                    "every Stream operator requires an explicit UID");
        }
        checkUnique(uid, stream);
        PortableStreamNode node;
        if (stream.kind() == DataStream.Kind.SOURCE) {
            StreamSourceSpec source = stream.sourceSpec();
            boolean eventTime = session.time()
                    == io.rowweave.stream.api.StreamTimeMode.EVENT_TIME;
            if (eventTime && source.watermarkPolicy() == null
                    && hasSourcePath(stream, false)) {
                throw failure(StreamApiException.Code.MISSING_WATERMARK_POLICY,
                        "event-time source requires a watermark/idle policy");
            }
            if (eventTime && source.watermarkPolicy() != null
                    && hasSourcePath(stream, true)) {
                throw failure(StreamApiException.Code.TIME_AUTHORITY_CONFLICT,
                        "provider and ASSIGN_EVENT_TIME authorities overlap");
            }
            if (!eventTime && source.watermarkPolicy() != null) {
                throw failure(StreamApiException.Code.TIME_MODE_MISMATCH,
                        "watermark policy requires EVENT_TIME session semantics");
            }
            List<StreamSourceSplit> splits = new ArrayList<>();
            for (StreamSplitSpec split : source.splits()) {
                splits.add(new StreamSourceSplit(split.splitId(),
                        split.positionCodecId(), split.descriptorFrame()));
            }
            Map<String, String> parameters = new LinkedHashMap<>();
            if (source.residentCdc()) {
                parameters.put("source.mode", "RESIDENT_CDC_SNAPSHOT_LOG");
                parameters.put("source.enumeratorStateFormat",
                        source.residentCdcStateVersion() == 1
                                ? "STREAM_CDC_ENUMERATOR_V1"
                                : "STREAM_CDC_ENUMERATOR_V2");
            }
            if (source.boundedTermination()) {
                parameters.put("source.termination", "EXPLICIT_END_OF_INPUT");
            }
            if (source.changelogMode() == StreamChangelogMode.RETRACT) {
                parameters.put("source.changelog", "RETRACT");
            }
            if (source.watermarkPolicy() != null) {
                parameters.put("source.time.timestamp", "PROVIDER_RECORD");
                parameters.put("source.time.watermark", "PROVIDER_EVENT");
                parameters.put("source.time.status",
                        "GENERATION_FENCED_ACTIVE_IDLE");
                parameters.put("source.time.idleTimeoutMillis", Long.toString(
                        source.watermarkPolicy().idleTimeoutMillis()));
            }
            StreamSourceBinding sourceBinding;
            if (source.dynamicDiscovery()) {
                io.rowweave.stream.api.StreamSourceDiscoverySpec discovery =
                        source.discoverySpec();
                sourceBinding = new StreamSourceBinding(source.providerKey(),
                        source.providerContractDigest(),
                        sha256(source.configurationFrame()),
                        source.configurationFrame(),
                        new StreamSourceDiscoveryDescriptor(
                                discovery.discoveryIntervalMillis(),
                                discovery.discoveryTimeoutMillis(),
                                discovery.maximumKnownSplits(),
                                discovery.maximumPendingSplits(),
                                discovery.maximumChangesPerDiscovery(),
                                discovery.maximumCursorBytes(),
                                discovery.maximumDescriptorBytes()),
                        StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED);
            } else {
                sourceBinding = new StreamSourceBinding(source.providerKey(),
                        source.providerContractDigest(),
                        sha256(source.configurationFrame()),
                        source.configurationFrame(), splits,
                        StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED);
            }
            node = new PortableStreamNode(uid, StreamOperatorCode.SOURCE,
                    Collections.emptyList(), Collections.emptyList(),
                    stream.schema().toPortableSchema(),
                    source.stableKeyFieldIds(),
                    Collections.emptyList(), parameters,
                    sourceBinding);
        } else if (stream.kind() == DataStream.Kind.UNION) {
            PortableStreamNode left = compileNode(stream.input());
            PortableStreamNode right = compileNode(stream.secondInput());
            io.rowweave.portable.PortableRowSchema leftSchema =
                    outputPortSchema(stream.input(), left);
            io.rowweave.portable.PortableRowSchema rightSchema =
                    outputPortSchema(stream.secondInput(), right);
            List<String> leftKeys = outputPortStableKeys(stream.input(), left);
            List<String> rightKeys = outputPortStableKeys(
                    stream.secondInput(), right);
            StreamChangelogMode leftChangelog = outputPortChangelog(
                    stream.input());
            StreamChangelogMode rightChangelog = outputPortChangelog(
                    stream.secondInput());
            if (!leftSchema.equals(rightSchema)
                    || !leftSchema.equals(
                    stream.schema().toPortableSchema())) {
                throw failure(StreamApiException.Code.SCHEMA_MISMATCH,
                        "union input and output schemas must be identical");
            }
            if (!leftKeys.equals(rightKeys)
                    || !leftKeys.equals(stream.keyFields())) {
                throw failure(StreamApiException.Code.INVALID_STABLE_KEY,
                        "union inputs must have identical stable keys");
            }
            if (leftChangelog != rightChangelog) {
                throw failure(StreamApiException.Code.CHANGELOG_MODE_MISMATCH,
                        "union inputs must have identical changelog modes");
            }
            node = new PortableStreamNode(uid, StreamOperatorCode.UNION,
                    Arrays.asList(left.operatorUid(), right.operatorUid()),
                    Arrays.asList(leftSchema.fingerprint(),
                            rightSchema.fingerprint()),
                    stream.schema().toPortableSchema(), stream.keyFields(),
                    Collections.emptyList(), Collections.emptyMap(), null);
            addEdge(left, node, 0, outputTagId(stream.input()));
            addEdge(right, node, 1, outputTagId(stream.secondInput()));
        } else if (stream.kind() == DataStream.Kind.TEMPORAL_JOIN) {
            PortableStreamNode left = compileNode(stream.input());
            PortableStreamNode right = compileNode(stream.secondInput());
            List<String> leftKeys = outputPortStableKeys(stream.input(), left);
            List<String> rightKeys = outputPortStableKeys(
                    stream.secondInput(), right);
            validateJoinInputKey(leftKeys,
                    stream.temporalJoinSpec().leftKeyFieldId());
            validateJoinInputKey(rightKeys,
                    stream.temporalJoinSpec().rightKeyFieldId());
            io.rowweave.portable.PortableRowSchema leftSchema =
                    outputPortSchema(stream.input(), left);
            io.rowweave.portable.PortableRowSchema rightSchema =
                    outputPortSchema(stream.secondInput(), right);
            List<String> outputKeys = validateStableKeys(stream);
            Map<String, String> parameters = new LinkedHashMap<>();
            parameters.put("join.leftKeyField",
                    stream.temporalJoinSpec().leftKeyFieldId());
            parameters.put("join.rightKeyField",
                    stream.temporalJoinSpec().rightKeyFieldId());
            parameters.put("join.lowerBoundMillis", Long.toString(
                    stream.temporalJoinSpec().lowerBoundMillis()));
            parameters.put("join.upperBoundMillis", Long.toString(
                    stream.temporalJoinSpec().upperBoundMillis()));
            node = new PortableStreamNode(uid, StreamOperatorCode.TEMPORAL_JOIN,
                    Arrays.asList(left.operatorUid(), right.operatorUid()),
                    Arrays.asList(leftSchema.fingerprint(),
                            rightSchema.fingerprint()),
                    stream.schema().toPortableSchema(), outputKeys,
                    Arrays.asList(state(stream.stateSpec()),
                            state(stream.secondStateSpec())),
                    parameters, new StreamFunctionBinding(
                            StreamFunctionBinding.FunctionKind.TEMPORAL_JOIN,
                            stream.function()));
            addEdge(left, node, 0, outputTagId(stream.input()));
            addEdge(right, node, 1, outputTagId(stream.secondInput()));
        } else if (stream.kind() == DataStream.Kind.EVENT_TIME_WINDOW) {
            PortableStreamNode upstream = compileNode(stream.input());
            io.rowweave.portable.PortableRowSchema upstreamSchema =
                    outputPortSchema(stream.input(), upstream);
            List<String> upstreamKeys = outputPortStableKeys(
                    stream.input(), upstream);
            validateWindowBinding(stream, upstreamKeys);
            Map<String, String> parameters = new LinkedHashMap<>();
            parameters.put("window.type", stream.windowSpec().type().name());
            parameters.put("window.sizeMillis",
                    Long.toString(stream.windowSpec().sizeMillis()));
            parameters.put("window.slideMillis",
                    Long.toString(stream.windowSpec().slideMillis()));
            parameters.put("window.trigger", "FINAL");
            parameters.put("window.eventTimeField",
                    stream.windowSpec().eventTimeFieldId());
            parameters.put("window.valueField",
                    stream.windowSpec().valueFieldId());
            parameters.put("window.startOutputField",
                    stream.windowSpec().startOutputFieldId());
            parameters.put("window.endOutputField",
                    stream.windowSpec().endOutputFieldId());
            parameters.put("window.valueOutputField",
                    stream.windowSpec().valueOutputFieldId());
            node = new PortableStreamNode(uid,
                    StreamOperatorCode.EVENT_TIME_WINDOW,
                    Collections.singletonList(upstream.operatorUid()),
                    Collections.singletonList(upstreamSchema.fingerprint()),
                    stream.schema().toPortableSchema(),
                    validateStableKeys(stream),
                    Collections.singletonList(state(stream.stateSpec())),
                    parameters, null);
            addEdge(upstream, node, 0, outputTagId(stream.input()));
        } else {
            PortableStreamNode upstream = compileNode(stream.input());
            io.rowweave.portable.PortableRowSchema upstreamSchema =
                    outputPortSchema(stream.input(), upstream);
            List<String> upstreamKeys = outputPortStableKeys(
                    stream.input(), upstream);
            StreamOperatorCode opcode;
            List<String> stableKeys;
            io.rowweave.stream.contract.StreamNodeBinding binding;
            Map<String, String> parameters = Collections.emptyMap();
            if (stream.kind() == DataStream.Kind.CALC) {
                List<StreamCalcBinding.Projection> projections =
                        new ArrayList<>();
                for (io.rowweave.stream.api.StreamCalcSpec.Projection value :
                        stream.calcSpec().projections()) {
                    projections.add(value.decimalOutputBoundary()
                            ? StreamCalcBinding.decimalProjection(
                            value.outputFieldId(), value.expression())
                            : StreamCalcBinding.projection(
                            value.outputFieldId(), value.expression()));
                }
                StreamCalcBinding calcBinding = new StreamCalcBinding(
                        stream.calcSpec().predicate(),
                        projections);
                calcBinding.validateInputSchema(upstreamSchema);
                opcode = StreamOperatorCode.CALC;
                stableKeys = calcStableKeys(upstreamKeys,
                        calcBinding);
                binding = calcBinding;
            } else if (stream.kind() == DataStream.Kind.ASSIGN_EVENT_TIME) {
                io.rowweave.stream.api.StreamEventTimeSpec specification =
                        stream.eventTimeSpec();
                StreamTimeBinding timeBinding = new StreamTimeBinding(
                        specification.timestampExpression(),
                        specification.watermarkMode()
                                == io.rowweave.stream.api.StreamEventTimeSpec
                                .WatermarkMode.MONOTONIC
                                ? StreamTimeBinding.WatermarkMode.MONOTONIC
                                : StreamTimeBinding.WatermarkMode
                                .BOUNDED_OUT_OF_ORDERNESS,
                        specification.maximumOutOfOrderMillis(),
                        specification.idleTimeoutMillis(),
                        specification.invalidTimestampPolicy()
                                == io.rowweave.stream.api.StreamEventTimeSpec
                                .InvalidTimestampPolicy.FAIL
                                ? StreamTimeBinding.InvalidTimestampPolicy.FAIL
                                : StreamTimeBinding.InvalidTimestampPolicy.DROP);
                timeBinding.validateInputSchema(upstreamSchema);
                opcode = StreamOperatorCode.ASSIGN_EVENT_TIME;
                stableKeys = upstreamKeys;
                binding = timeBinding;
            } else if (stream.kind() == DataStream.Kind.KEYED_PROCESS) {
                if (upstreamKeys.isEmpty()) {
                    throw failure(StreamApiException.Code.MISSING_STABLE_KEY,
                            "keyed process requires a prior keyBy");
                }
                List<StreamManagedStateDescriptor> managedStates =
                        managedStates(stream.processSpec().managedStates());
                List<StreamTimerDescriptor> timers = timers(
                        stream.processSpec().timerFamilies());
                opcode = StreamOperatorCode.KEYED_PROCESS;
                stableKeys = validateStableKeys(stream);
                binding = new StreamProcessBinding(
                        StreamProcessBinding.FunctionKind.KEYED_PROCESS,
                        stream.function(), managedStates, timers,
                        stream.processSpec().elementEmissionLimit()
                                .maximumOutputsPerInput(),
                        stream.processSpec().elementEmissionLimit()
                                .maximumOutputBytesPerInput(),
                        stream.processSpec().elementEmissionLimit()
                                .maximumOutputsPerInput(),
                        stream.processSpec().elementEmissionLimit()
                                .maximumOutputBytesPerInput(),
                        stream.processSpec().timerEmissionLimit()
                                .maximumOutputsPerInput(),
                        stream.processSpec().timerEmissionLimit()
                                .maximumOutputBytesPerInput(), null,
                        outputTags(stream.processSpec().outputTags()));
            } else if (stream.kind() == DataStream.Kind.KEYED_CO_PROCESS) {
                PortableStreamNode right = compileNode(stream.secondInput());
                List<String> rightKeys = outputPortStableKeys(
                        stream.secondInput(), right);
                io.rowweave.stream.api.StreamCoProcessSpec specification =
                        stream.coProcessSpec();
                boolean broadcast = specification.broadcastState() != null;
                if (upstreamKeys.isEmpty()
                        || !broadcast && rightKeys.isEmpty()) {
                    throw failure(StreamApiException.Code.MISSING_STABLE_KEY,
                            "keyed co-process requires keyBy on both inputs");
                }
                if (!broadcast && !compatibleKeyCodec(
                        upstreamSchema, upstreamKeys,
                        outputPortSchema(stream.secondInput(), right),
                        rightKeys)) {
                    throw failure(StreamApiException.Code.INCOMPATIBLE_KEY_CODEC,
                            "co-process input key codecs differ");
                }
                opcode = StreamOperatorCode.KEYED_CO_PROCESS;
                stableKeys = validateStableKeys(stream);
                binding = new StreamProcessBinding(
                        StreamProcessBinding.FunctionKind.KEYED_CO_PROCESS,
                        stream.function(),
                        managedStates(specification.managedStates()),
                        timers(specification.timerFamilies()),
                        specification.leftEmissionLimit()
                                .maximumOutputsPerInput(),
                        specification.leftEmissionLimit()
                                .maximumOutputBytesPerInput(),
                        specification.rightEmissionLimit()
                                .maximumOutputsPerInput(),
                        specification.rightEmissionLimit()
                                .maximumOutputBytesPerInput(),
                        specification.timerEmissionLimit()
                                .maximumOutputsPerInput(),
                        specification.timerEmissionLimit()
                                .maximumOutputBytesPerInput(),
                        broadcast(specification.broadcastState()),
                        outputTags(specification.outputTags()));
            } else if (stream.kind() == DataStream.Kind.GENERIC_WINDOW) {
                io.rowweave.stream.api.StreamGenericWindowSpec specification =
                        stream.genericWindowSpec();
                if (upstreamKeys.isEmpty()) {
                    throw failure(StreamApiException.Code.MISSING_STABLE_KEY,
                            "generic window requires a stable keyed input");
                }
                if (specification.assigner().eventTime()
                        && session.time()
                        != io.rowweave.stream.api.StreamTimeMode.EVENT_TIME
                        || !specification.assigner().eventTime()
                        && session.time()
                        == io.rowweave.stream.api.StreamTimeMode.EVENT_TIME) {
                    throw failure(StreamApiException.Code.TIME_MODE_MISMATCH,
                            "window assigner and session time modes differ");
                }
                validateGenericWindowTimeField(stream.input(), specification);
                opcode = StreamOperatorCode.GENERIC_WINDOW;
                stableKeys = validateStableKeys(stream);
                binding = windowBinding(specification);
            } else if (stream.kind() == DataStream.Kind.ASYNC_WAIT) {
                io.rowweave.stream.api.StreamAsyncWaitSpec specification =
                        stream.asyncWaitSpec();
                if (session.delivery()
                        == io.rowweave.stream.api.StreamDeliveryGuarantee
                        .EXACTLY_ONCE && !specification.replay().replayStable()) {
                    throw failure(
                            StreamApiException.Code.DELIVERY_GUARANTEE_MISMATCH,
                            "EXACTLY_ONCE async wait requires replay authority");
                }
                opcode = StreamOperatorCode.ASYNC_WAIT;
                stableKeys = validateStableKeys(stream);
                binding = asyncBinding(specification);
            } else if (stream.kind() == DataStream.Kind.MAP) {
                opcode = StreamOperatorCode.MAP;
                stableKeys = validateStableKeys(stream);
                binding = new StreamFunctionBinding(
                        StreamFunctionBinding.FunctionKind.MAP, stream.function());
            } else if (stream.kind() == DataStream.Kind.FLAT_MAP) {
                if (stream.emissionLimit().maximumOutputsPerInput()
                        > stream.function().resourceProfile()
                        .maximumOutputRowsPerInput()) {
                    throw failure(StreamApiException.Code.INVALID_CONFIGURATION,
                            "flatMap output quota exceeds function resource profile");
                }
                opcode = StreamOperatorCode.FLAT_MAP;
                stableKeys = validateStableKeys(stream);
                binding = new StreamFunctionBinding(
                        StreamFunctionBinding.FunctionKind.FLAT_MAP,
                        stream.function());
                parameters = new LinkedHashMap<>();
                parameters.put("flatMap.maxOutputsPerInput", Integer.toString(
                        stream.emissionLimit().maximumOutputsPerInput()));
                parameters.put("flatMap.maxOutputBytesPerInput", Long.toString(
                        stream.emissionLimit().maximumOutputBytesPerInput()));
            } else if (stream.kind() == DataStream.Kind.KEY_BY) {
                opcode = StreamOperatorCode.KEY_BY;
                stableKeys = validateStableKeys(stream);
                binding = null;
            } else if (stream.kind() == DataStream.Kind.KEYED_AGGREGATE) {
                if (upstreamKeys.isEmpty()) {
                    throw failure(StreamApiException.Code.MISSING_STABLE_KEY,
                            "keyed aggregate requires a prior keyBy");
                }
                if (stream.stateSpec().kind()
                        != StreamStateSpec.Kind.KEYED_AGGREGATING
                        || stream.stateSpec().ttlMode()
                        == StreamStateSpec.TtlMode.NONE) {
                    throw failure(
                            StreamApiException.Code.INVALID_STATE_DESCRIPTOR,
                            "keyed aggregate requires aggregating state with finite TTL");
                }
                opcode = StreamOperatorCode.KEYED_AGGREGATE;
                stableKeys = validateStableKeys(stream);
                binding = new StreamFunctionBinding(
                        StreamFunctionBinding.FunctionKind.KEYED_AGGREGATE,
                        stream.function());
            } else {
                opcode = StreamOperatorCode.FILTER;
                stableKeys = upstreamKeys;
                binding = new StreamFunctionBinding(
                        StreamFunctionBinding.FunctionKind.FILTER,
                        stream.function());
            }
            List<StreamStateDescriptor> states =
                    stream.kind() == DataStream.Kind.KEYED_AGGREGATE
                            ? Collections.singletonList(state(stream.stateSpec()))
                            : stream.kind() == DataStream.Kind.GENERIC_WINDOW
                            ? Collections.singletonList(state(stream.stateSpec()))
                            : processStates(binding);
            if (stream.kind() == DataStream.Kind.KEYED_CO_PROCESS) {
                PortableStreamNode right = compiled.get(stream.secondInput());
                node = new PortableStreamNode(uid, opcode,
                        Arrays.asList(upstream.operatorUid(),
                                right.operatorUid()),
                        Arrays.asList(upstreamSchema.fingerprint(),
                                outputPortSchema(stream.secondInput(), right)
                                        .fingerprint()),
                        stream.schema().toPortableSchema(), stableKeys,
                        states, parameters, binding);
                addEdge(upstream, node, 0, outputTagId(stream.input()));
                addEdge(right, node, 1, outputTagId(stream.secondInput()));
            } else {
                node = new PortableStreamNode(uid, opcode,
                        Collections.singletonList(upstream.operatorUid()),
                        Collections.singletonList(upstreamSchema.fingerprint()),
                        stream.schema().toPortableSchema(), stableKeys,
                        states, parameters, binding);
                addEdge(upstream, node, 0, outputTagId(stream.input()));
            }
        }
        nodes.add(node);
        compiled.put(stream, node);
        StreamChangelogMode outputChangelog = deriveOutputChangelog(stream);
        outputChangelogs.put(stream, outputChangelog);
        nodeOutputChangelogs.put(node.operatorUid(), outputChangelog);
        return node;
    }

    private StreamChangelogMode nodeOutputChangelog(
            PortableStreamNode node) {
        StreamChangelogMode mode = nodeOutputChangelogs.get(
                node.operatorUid());
        if (mode == null) {
            throw failure(StreamApiException.Code.INVALID_TOPOLOGY,
                    "Stream node output changelog contract is unavailable");
        }
        return mode;
    }

    private void checkUnique(String uid, DataStream stream) {
        if (identities.containsKey(uid)) {
            throw failure(StreamApiException.Code.DUPLICATE_OPERATOR_UID,
                    "duplicate Stream operator UID: " + uid);
        }
        identities.put(uid, stream);
    }

    private void addEdge(PortableStreamNode source, PortableStreamNode target,
                         int inputIndex) {
        addEdge(source, target, inputIndex, null);
    }

    private void addEdge(PortableStreamNode source, PortableStreamNode target,
            int inputIndex, String sourceOutputTagId) {
        String channel = sha256((source.operatorUid() + '\0'
                + (sourceOutputTagId == null ? "" : sourceOutputTagId) + '\0'
                + target.operatorUid() + '\0' + inputIndex)
                .getBytes(StandardCharsets.UTF_8));
        StreamEdge.Partitioning partitioning;
        if (target.opcode() == StreamOperatorCode.KEYED_CO_PROCESS
                && ((StreamProcessBinding) target.binding())
                .broadcastState() != null) {
            partitioning = inputIndex == 1
                    ? StreamEdge.Partitioning.BROADCAST
                    : StreamEdge.Partitioning.FORWARD;
        } else {
            partitioning = target.opcode() == StreamOperatorCode.KEY_BY
                        || target.opcode() == StreamOperatorCode.TEMPORAL_JOIN
                        || target.opcode()
                        == StreamOperatorCode.KEYED_CO_PROCESS
                        ? StreamEdge.Partitioning.HASH
                        : StreamEdge.Partitioning.FORWARD;
        }
        edges.add(new StreamEdge(source.operatorUid(), target.operatorUid(),
                inputIndex, channel, partitioning,
                session.resources().maximumFrameBytes(),
                session.resources().channelCreditBytes(), sourceOutputTagId));
    }

    private void validateConsumedOutputTags() {
        Set<String> consumed = new HashSet<>();
        for (StreamEdge edge : edges) {
            if (edge.sourceOutputTagId() != null) {
                consumed.add(edge.sourceOperatorUid() + '\0'
                        + edge.sourceOutputTagId());
            }
        }
        for (PortableStreamNode node : nodes) {
            for (StreamOutputTagDescriptor tag : declaredOutputTags(node)) {
                if (!consumed.contains(node.operatorUid() + '\0'
                        + tag.tagId())) {
                    throw failure(StreamApiException.Code.UNUSED_OUTPUT_TAG,
                            "declared output tag is not consumed: "
                                    + tag.tagId());
                }
            }
        }
    }

    private static List<StreamOutputTagDescriptor> declaredOutputTags(
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

    private static String outputTagId(DataStream stream) {
        return stream.kind() == DataStream.Kind.TAGGED_VIEW
                ? stream.outputTag().tagId() : null;
    }

    private static List<String> outputPortStableKeys(DataStream stream,
            PortableStreamNode producer) {
        return stream.kind() == DataStream.Kind.TAGGED_VIEW
                ? stream.keyFields() : producer.stableKeyFields();
    }

    private static io.rowweave.portable.PortableRowSchema outputPortSchema(
            DataStream stream, PortableStreamNode producer) {
        return stream.kind() == DataStream.Kind.TAGGED_VIEW
                ? stream.schema().toPortableSchema() : producer.outputSchema();
    }

    private StreamChangelogMode outputPortChangelog(DataStream stream) {
        StreamChangelogMode mode = outputChangelogs.get(stream);
        if (mode == null) {
            throw failure(StreamApiException.Code.INVALID_TOPOLOGY,
                    "Stream output port changelog contract is unavailable");
        }
        return mode;
    }

    private static PortableStreamPlan.ChangelogMode contractMode(
            StreamChangelogMode mode) {
        switch (mode) {
            case APPEND: return PortableStreamPlan.ChangelogMode.APPEND;
            case UPSERT: return PortableStreamPlan.ChangelogMode.UPSERT;
            case RETRACT: return PortableStreamPlan.ChangelogMode.RETRACT;
            default: throw new IllegalStateException(
                    "unknown Stream changelog mode");
        }
    }

    private static List<PortableStreamPlan.ChangelogMode> contractModes(
            List<StreamChangelogMode> modes) {
        List<PortableStreamPlan.ChangelogMode> result =
                new ArrayList<>(modes.size());
        for (StreamChangelogMode mode : modes) {
            result.add(contractMode(mode));
        }
        return result;
    }

    private StreamChangelogMode deriveOutputChangelog(DataStream stream) {
        if (stream.kind() == DataStream.Kind.SOURCE) {
            return stream.sourceSpec().changelogMode();
        }
        if (stream.kind() == DataStream.Kind.TEMPORAL_JOIN) {
            return session.changelog() == StreamChangelogMode.RETRACT
                    ? StreamChangelogMode.RETRACT
                    : StreamChangelogMode.UPSERT;
        }
        if (stream.kind() == DataStream.Kind.KEYED_AGGREGATE) {
            return session.changelog() == StreamChangelogMode.RETRACT
                    ? StreamChangelogMode.RETRACT
                    : StreamChangelogMode.UPSERT;
        }
        if (stream.kind() == DataStream.Kind.GENERIC_WINDOW
                && session.changelog() == StreamChangelogMode.RETRACT) {
            return StreamChangelogMode.RETRACT;
        }
        if (stream.kind() == DataStream.Kind.GENERIC_WINDOW
                && stream.genericWindowSpec().accumulationMode()
                == io.rowweave.stream.api.StreamAccumulationMode
                .ACCUMULATING) {
            return StreamChangelogMode.UPSERT;
        }
        if ((stream.kind() == DataStream.Kind.KEYED_PROCESS
                || stream.kind() == DataStream.Kind.KEYED_CO_PROCESS)
                && session.changelog() == StreamChangelogMode.UPSERT) {
            return StreamChangelogMode.UPSERT;
        }
        return outputPortChangelog(stream.input());
    }

    private static List<String> validateStableKeys(DataStream stream) {
        Set<String> seen = new HashSet<>();
        for (String key : stream.keyFields()) {
            if (key == null || !seen.add(key)) {
                throw failure(StreamApiException.Code.INVALID_STABLE_KEY,
                        "stable key field IDs must be non-null and unique");
            }
            boolean valid = false;
            for (io.rowweave.stream.api.StreamField field :
                    stream.schema().fields()) {
                if (field.fieldId().equals(key) && !field.nullable()) {
                    valid = true;
                    break;
                }
            }
            if (!valid) {
                throw failure(StreamApiException.Code.INVALID_STABLE_KEY,
                        "stable key must reference a non-null field ID: " + key);
            }
        }
        return stream.keyFields();
    }

    private static List<String> calcStableKeys(List<String> upstreamKeys,
            StreamCalcBinding binding) {
        if (upstreamKeys.isEmpty()) return Collections.emptyList();
        Set<String> identities = new HashSet<>();
        for (StreamCalcBinding.Projection projection :
                binding.projections()) {
            StreamExpression expression = projection.expression();
            if (expression.kind() == StreamExpression.Kind.INPUT
                    && projection.outputFieldId().equals(
                    expression.inputFieldId())) {
                identities.add(projection.outputFieldId());
            }
        }
        return identities.containsAll(upstreamKeys)
                ? new ArrayList<>(upstreamKeys)
                : Collections.<String>emptyList();
    }

    private static List<StreamStateDescriptor> processStates(
            io.rowweave.stream.contract.StreamNodeBinding binding) {
        if (!(binding instanceof StreamProcessBinding)) {
            return Collections.emptyList();
        }
        List<StreamStateDescriptor> states = new ArrayList<>();
        for (StreamManagedStateDescriptor value :
                ((StreamProcessBinding) binding).managedStates()) {
            states.add(value.state());
        }
        return states;
    }

    private static List<StreamManagedStateDescriptor> managedStates(
            List<io.rowweave.stream.api.StreamManagedStateSpec> values) {
        List<StreamManagedStateDescriptor> result = new ArrayList<>();
        for (io.rowweave.stream.api.StreamManagedStateSpec value : values) {
            result.add(new StreamManagedStateDescriptor(state(value.state()),
                    value.maximumEntriesPerKey(), value.maximumTotalEntries(),
                    value.maximumTotalBytes(), value.maximumEntryBytes()));
        }
        return result;
    }

    private static List<StreamTimerDescriptor> timers(
            List<io.rowweave.stream.api.StreamTimerSpec> values) {
        List<StreamTimerDescriptor> result = new ArrayList<>();
        for (io.rowweave.stream.api.StreamTimerSpec value : values) {
            result.add(new StreamTimerDescriptor(value.timerFamilyId(),
                    value.timeDomain()
                            == io.rowweave.stream.api.StreamTimerSpec
                            .TimeDomain.EVENT_TIME
                            ? StreamTimerDescriptor.TimeDomain.EVENT_TIME
                            : StreamTimerDescriptor.TimeDomain.PROCESSING_TIME,
                    value.maximumTimersPerKey(), value.maximumTotalTimers(),
                    value.maximumTotalBytes()));
        }
        return result;
    }

    private static StreamBroadcastStateDescriptor broadcast(
            io.rowweave.stream.api.StreamBroadcastStateSpec value) {
        if (value == null) return null;
        return new StreamBroadcastStateDescriptor(value.stateName(),
                value.authorityKind()
                        == io.rowweave.stream.api.StreamBroadcastStateSpec
                        .AuthorityKind.IMMUTABLE_SNAPSHOT
                        ? StreamBroadcastStateDescriptor.AuthorityKind
                        .IMMUTABLE_SNAPSHOT
                        : StreamBroadcastStateDescriptor.AuthorityKind
                        .REPLAYABLE_CHANGELOG,
                value.snapshotOrVersionId(), value.authorityDigest(),
                value.serializerContractId(), value.stateSchemaVersion(),
                value.maximumEntries(), value.maximumTotalBytes(),
                value.maximumUpdateBytes(), value.maximumRetainedVersions());
    }

    private static List<StreamOutputTagDescriptor> outputTags(
            List<io.rowweave.stream.api.StreamOutputTag> values) {
        List<StreamOutputTagDescriptor> result = new ArrayList<>();
        for (io.rowweave.stream.api.StreamOutputTag value : values) {
            result.add(new StreamOutputTagDescriptor(value.tagId(),
                    value.schema().toPortableSchema(),
                    outputTagMode(value.changelogMode()),
                    value.stableKeyFieldIds(),
                    value.leftEmissionLimit().maximumOutputsPerInput(),
                    value.leftEmissionLimit().maximumOutputBytesPerInput(),
                    value.rightEmissionLimit().maximumOutputsPerInput(),
                    value.rightEmissionLimit().maximumOutputBytesPerInput(),
                    value.timerEmissionLimit().maximumOutputsPerInput(),
                    value.timerEmissionLimit().maximumOutputBytesPerInput()));
        }
        return result;
    }

    private static StreamOutputTagDescriptor.ChangelogMode outputTagMode(
            io.rowweave.stream.api.StreamChangelogMode mode) {
        switch (mode) {
            case APPEND:
                return StreamOutputTagDescriptor.ChangelogMode.APPEND;
            case UPSERT:
                return StreamOutputTagDescriptor.ChangelogMode.UPSERT;
            case RETRACT:
                return StreamOutputTagDescriptor.ChangelogMode.RETRACT;
            default:
                throw new IllegalStateException(
                        "unknown Stream output tag changelog mode");
        }
    }

    private static StreamWindowBinding windowBinding(
            io.rowweave.stream.api.StreamGenericWindowSpec value) {
        io.rowweave.stream.api.StreamWindowAssignerSpec assigner =
                value.assigner();
        io.rowweave.stream.api.StreamWindowTriggerSpec trigger =
                value.trigger();
        io.rowweave.stream.api.StreamWindowResourceSpec resources =
                value.resources();
        return new StreamWindowBinding(
                StreamWindowBinding.AssignerKind.valueOf(
                        assigner.kind().name()), assigner.eventTimeFieldId(),
                assigner.sizeOrGap(), assigner.slide(),
                StreamWindowBinding.TriggerKind.valueOf(
                        trigger.kind().name()), trigger.intervalOrCount(),
                trigger.isPurging(),
                StreamWindowBinding.AccumulationMode.valueOf(
                        value.accumulationMode().name()),
                value.allowedLatenessMillis(),
                StreamWindowBinding.FunctionKind.valueOf(
                        value.functionKind().name()), value.function(),
                new StreamWindowResourceDescriptor(
                        resources.maximumActiveWindowsPerKey(),
                        resources.maximumRowsPerWindow(),
                        resources.maximumWindowBytes(),
                        resources.maximumTotalBytes(),
                        resources.maximumSessionMergeFanIn(),
                        resources.triggerEmissionLimit()
                                .maximumOutputsPerInput(),
                        resources.triggerEmissionLimit()
                                .maximumOutputBytesPerInput()),
                value.lateOutput() == null ? null
                        : outputTag(value.lateOutput()));
    }

    private static StreamOutputTagDescriptor outputTag(
            io.rowweave.stream.api.StreamOutputTag value) {
        return outputTags(Collections.singletonList(value)).get(0);
    }

    private static StreamAsyncBinding asyncBinding(
            io.rowweave.stream.api.StreamAsyncWaitSpec value) {
        io.rowweave.stream.api.StreamAsyncResourceSpec resources =
                value.resources();
        return new StreamAsyncBinding(value.function(),
                StreamAsyncBinding.Ordering.valueOf(value.ordering().name()),
                StreamAsyncBinding.ReplayMode.valueOf(
                        value.replay().mode().name()),
                value.replay().authorityId(),
                value.replay().authorityDigest(),
                new StreamAsyncResourceDescriptor(
                        resources.maximumConcurrency(),
                        resources.maximumPendingBytes(),
                        resources.timeoutMillis(), resources.maximumAttempts(),
                        resources.initialBackoffMillis(),
                        resources.maximumBackoffMillis(),
                        resources.resultEmissionLimit()
                                .maximumOutputsPerInput(),
                        resources.resultEmissionLimit()
                                .maximumOutputBytesPerInput()));
    }

    private static void validateGenericWindowTimeField(DataStream input,
            io.rowweave.stream.api.StreamGenericWindowSpec specification) {
        if (!specification.assigner().eventTime()) return;
        io.rowweave.stream.api.StreamField field = field(input.schema(),
                specification.assigner().eventTimeFieldId());
        if (field == null || field.nullable()
                || field.dataType() != DataType.TimestampType) {
            throw failure(StreamApiException.Code.INVALID_WINDOW_FIELD_BINDING,
                    "event-time window requires a non-null timestamp field");
        }
    }

    private static boolean compatibleKeyCodec(
            io.rowweave.portable.PortableRowSchema leftSchema,
            List<String> leftKeys,
            io.rowweave.portable.PortableRowSchema rightSchema,
            List<String> rightKeys) {
        if (leftKeys.size() != rightKeys.size()) return false;
        for (int index = 0; index < leftKeys.size(); index++) {
            io.rowweave.portable.PortableField leftField = field(
                    leftSchema, leftKeys.get(index));
            io.rowweave.portable.PortableField rightField = field(
                    rightSchema, rightKeys.get(index));
            if (leftField == null || rightField == null
                    || leftField.dataType() != rightField.dataType()
                    || leftField.nullable() != rightField.nullable()) {
                return false;
            }
        }
        return true;
    }

    private static io.rowweave.portable.PortableField field(
            io.rowweave.portable.PortableRowSchema schema, String fieldId) {
        for (io.rowweave.portable.PortableField value : schema.fields()) {
            if (value.fieldId().equals(fieldId)) return value;
        }
        return null;
    }

    private boolean hasSourcePath(DataStream source,
            boolean assignmentRequired) {
        for (StreamSession.OutputRegistration output : session.outputs()) {
            if (pathMatches(output.input, source, false,
                    assignmentRequired)) return true;
        }
        return false;
    }

    private static boolean pathMatches(DataStream current,
            DataStream source, boolean assignmentSeen,
            boolean assignmentRequired) {
        boolean assigned = assignmentSeen
                || current.kind() == DataStream.Kind.ASSIGN_EVENT_TIME;
        if (current == source) return assigned == assignmentRequired;
        return current.input() != null && pathMatches(current.input(), source,
                assigned, assignmentRequired)
                || current.secondInput() != null && pathMatches(
                current.secondInput(), source, assigned, assignmentRequired);
    }

    private static void validateJoinInputKey(List<String> stableKeys,
                                             String requiredFieldId) {
        if (!stableKeys.contains(requiredFieldId)) {
            throw failure(StreamApiException.Code.INVALID_STABLE_KEY,
                    "temporal join input requires prior keyBy for: "
                            + requiredFieldId);
        }
    }

    private static void validateWindowBinding(DataStream stream,
                                              List<String> upstreamKeys) {
        if (upstreamKeys.isEmpty()) {
            throw failure(StreamApiException.Code.MISSING_STABLE_KEY,
                    "event-time window requires a prior keyBy");
        }
        if (!upstreamKeys.containsAll(stream.keyFields())
                || stream.stateSpec().kind() != StreamStateSpec.Kind.KEYED_VALUE
                || stream.stateSpec().ttlMode() != StreamStateSpec.TtlMode.NONE) {
            invalidWindowBinding();
        }
        io.rowweave.stream.api.StreamField eventTime = field(
                stream.input().schema(), stream.windowSpec().eventTimeFieldId());
        io.rowweave.stream.api.StreamField value = field(
                stream.input().schema(), stream.windowSpec().valueFieldId());
        io.rowweave.stream.api.StreamField start = field(
                stream.schema(), stream.windowSpec().startOutputFieldId());
        io.rowweave.stream.api.StreamField end = field(
                stream.schema(), stream.windowSpec().endOutputFieldId());
        io.rowweave.stream.api.StreamField result = field(
                stream.schema(), stream.windowSpec().valueOutputFieldId());
        if (eventTime == null || eventTime.nullable()
                || eventTime.dataType() != DataType.TimestampType
                || value == null || value.dataType() != DataType.BigDecimalType
                || start == null || start.nullable()
                || start.dataType() != DataType.LongType
                || end == null || end.nullable()
                || end.dataType() != DataType.LongType
                || result == null || result.nullable()
                || result.dataType() != DataType.BigDecimalType) {
            invalidWindowBinding();
        }
    }

    private static io.rowweave.stream.api.StreamField field(
            io.rowweave.stream.api.StreamSchema schema, String fieldId) {
        for (io.rowweave.stream.api.StreamField field : schema.fields()) {
            if (field.fieldId().equals(fieldId)) return field;
        }
        return null;
    }

    private static void invalidWindowBinding() {
        throw failure(StreamApiException.Code.INVALID_WINDOW_FIELD_BINDING,
                "window fields and state must be explicit and type-safe");
    }

    private static StreamStateDescriptor state(StreamStateSpec state) {
        StreamStateDescriptor.Kind kind;
        switch (state.kind()) {
            case KEYED_VALUE: kind = StreamStateDescriptor.Kind.KEYED_VALUE; break;
            case KEYED_MAP: kind = StreamStateDescriptor.Kind.KEYED_MAP; break;
            case KEYED_AGGREGATING:
                kind = StreamStateDescriptor.Kind.KEYED_AGGREGATING; break;
            case OPERATOR_LIST: kind = StreamStateDescriptor.Kind.OPERATOR_LIST; break;
            case KEYED_LIST: kind = StreamStateDescriptor.Kind.KEYED_LIST; break;
            default: throw new IllegalArgumentException("unknown state kind");
        }
        StreamStateDescriptor.TtlMode ttl;
        switch (state.ttlMode()) {
            case NONE: ttl = StreamStateDescriptor.TtlMode.NONE; break;
            case PROCESSING_TIME:
                ttl = StreamStateDescriptor.TtlMode.PROCESSING_TIME; break;
            case EVENT_TIME: ttl = StreamStateDescriptor.TtlMode.EVENT_TIME; break;
            default: throw new IllegalArgumentException("unknown state TTL mode");
        }
        return new StreamStateDescriptor(state.stateName(), kind,
                state.serializerContractId(), state.stateSchemaVersion(),
                ttl, state.ttlMillis());
    }

    private static StreamApiException translate(StreamContractException failure) {
        if ("CALC_SCHEMA_MISMATCH".equals(failure.code())) {
            return new StreamApiException(StreamApiException.Code.SCHEMA_MISMATCH,
                    failure.getMessage());
        }
        if ("INVALID_EXPRESSION".equals(failure.code())
                || failure.code().startsWith("EXPRESSION_")
                || "UNKNOWN_EXPRESSION_FUNCTION".equals(failure.code())) {
            return new StreamApiException(
                    StreamApiException.Code.INVALID_EXPRESSION,
                    failure.getMessage());
        }
        StreamApiException.Code code;
        try {
            code = StreamApiException.Code.valueOf(failure.code());
        } catch (IllegalArgumentException unknown) {
            code = StreamApiException.Code.INVALID_CONFIGURATION;
        }
        return new StreamApiException(code, failure.getMessage());
    }

    private static StreamApiException failure(StreamApiException.Code code,
                                              String message) {
        return new StreamApiException(code, message);
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
