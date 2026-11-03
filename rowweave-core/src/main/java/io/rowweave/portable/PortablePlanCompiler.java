package io.rowweave.portable;

import io.rowweave.CoGroupFunction;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.BatchSink;
import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.Collector;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FilterFunction;
import io.rowweave.FlatMapFunction;
import io.rowweave.GroupCombineFunction;
import io.rowweave.GroupCombineReduceFunction;
import io.rowweave.GroupReduceFunction;
import io.rowweave.Source;
import io.rowweave.MapFunction;
import io.rowweave.MapPartitionEmissionLimit;
import io.rowweave.MapPartitionFunction;
import io.rowweave.Sink;
import io.rowweave.PartitionDataRowCursor;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Validates and projects portable batch IR into worker-local internal execution IR. */
public final class PortablePlanCompiler {
    public static final String OPERATOR_ID_PARAM = "__portable_operator_id_v1__";
    public static final String OPCODE_PARAM = "__portable_opcode_v1__";
    public static final String OUTPUT_SCHEMA_PARAM = "__portable_output_schema_v1__";

    private PortablePlanCompiler() { }

    /**
     * Complete validation and metadata admission precede every runtime resolution.
     * Resolvers may create descriptors but must not open their data-plane resources.
     */
    public static PlanGraph compile(
            PortableBatchPlan plan, PortableBindingVerifier verifier,
            PortableRuntimeResolver resolver) {
        if (plan == null || verifier == null || resolver == null) {
            throw PortablePlanText.failure("INVALID_COMPILER_INPUT", "compiler",
                    "plan, verifier, and resolver are required");
        }

        preflight(plan, verifier);

        Map<String, List<Source>> sources = new HashMap<>();
        Map<String, Serializable> functions = new HashMap<>();
        Map<String, Sink> sinks = new HashMap<>();
        resolveSources(plan, resolver, sources);
        resolveFunctions(plan, resolver, functions);
        resolveSinks(plan, resolver, sinks);
        PlanGraph projected = project(plan, sources, functions, sinks);
        requireExactProjection(plan, projected);
        return projected;
    }

    /** Complete static admission without creating any runtime binding object. */
    public static void preflight(
            PortableBatchPlan plan, PortableBindingVerifier verifier) {
        if (plan == null || verifier == null) {
            throw PortablePlanText.failure("INVALID_COMPILER_INPUT", "compiler",
                    "plan and verifier are required");
        }
        preflightContracts(plan);
        verifyBindings(plan, verifier);
    }

    /**
     * Builds an inert core-only graph for Master planning. No provider or
     * function implementation is loaded, and every executable callback fails
     * closed if this projection is accidentally handed to an executor.
     */
    public static PlanGraph projectMetadata(
            PortableBatchPlan plan, PortableBindingVerifier verifier) {
        preflight(plan, verifier);
        Map<String, List<Source>> sources = new HashMap<>();
        Map<String, Serializable> functions = new HashMap<>();
        Map<String, Sink> sinks = new HashMap<>();
        for (PortablePlanNode node : plan.nodes()) {
            if (node.opcode() == PortableOperatorCode.SOURCE) {
                PortableSourceBinding binding =
                        (PortableSourceBinding) node.binding();
                DataSchema dataSchema = toDataSchema(node.outputSchema());
                List<Source> inputs = new ArrayList<>(
                        binding.splitFramesUnsafe().size());
                for (byte[] frame : binding.splitFramesUnsafe()) {
                    inputs.add(new MetadataInput(dataSchema,
                            BatchSourceSplitDescriptorCodec.decode(frame)));
                }
                sources.put(node.operatorId(),
                        Collections.unmodifiableList(inputs));
            } else if (node.opcode() == PortableOperatorCode.SINK) {
                PortableSinkBinding binding =
                        (PortableSinkBinding) node.binding();
                sinks.put(node.operatorId(), new MetadataOutput(
                        toDataSchema(node.outputSchema()),
                        BatchSinkDescriptorCodec.decode(
                                binding.descriptorFrameUnsafe())));
            } else if (node.opcode().bindingKind()
                    == PortableOperatorCode.BindingKind.FUNCTION) {
                functions.put(node.operatorId(), metadataFunction(
                        ((FunctionDeploymentReference) node.binding())
                                .functionAbi(),
                        toDataSchema(node.outputSchema())));
            }
        }
        PlanGraph projected = project(plan, sources, functions, sinks);
        requireExactProjection(plan, projected);
        return projected;
    }

    /** Durable public-v1 subset with exact replay and no external side effects. */
    public static void preflightDurable(
            PortableBatchPlan plan, PortableBindingVerifier verifier) {
        preflight(plan, verifier);
        for (PortablePlanNode node : plan.nodes()) {
            if (node.opcode() == PortableOperatorCode.SOURCE) {
                PortableSourceBinding source =
                        (PortableSourceBinding) node.binding();
                for (byte[] frame : source.splitFramesUnsafe()) {
                    BatchSourceSplitDescriptor descriptor =
                            BatchSourceSplitDescriptorCodec.decode(frame);
                    if (descriptor.replayGuarantee()
                            != BatchSourceSplitDescriptor.ReplayGuarantee
                            .EXACT_ORDERED_FROM_START) {
                        invalid("DURABLE_SOURCE_REPLAY_UNSUPPORTED",
                                "nodes.binding.source.replayGuarantee");
                    }
                }
            } else if (node.opcode() == PortableOperatorCode.SINK) {
                BatchSinkDescriptor descriptor =
                        BatchSinkDescriptorCodec.decode(
                                ((PortableSinkBinding) node.binding())
                                .descriptorFrameUnsafe());
                if (descriptor.capabilityProfile()
                        != BatchSinkDescriptor.CapabilityProfile
                        .SIDE_EFFECT_FREE_V1) {
                    invalid("DURABLE_SINK_PROFILE_UNSUPPORTED",
                            "nodes.binding.sink.capabilityProfile");
                }
            }
        }
    }

    /** Rejects any internal compiler drift from the already admitted portable model. */
    public static void requireExactProjection(
            PortableBatchPlan plan, PlanGraph graph) {
        if (plan == null || graph == null
                || plan.desiredParallelism() != graph.parallelism()
                || plan.nodes().size() != graph.nodeCount()) {
            invalid("PROJECTION_MISMATCH", "compiler.projection");
        }
        Map<String, Integer> indices = new HashMap<>();
        for (int index = 0; index < plan.nodes().size(); index++) {
            PortablePlanNode portable = plan.nodes().get(index);
            PlanNode runtime = graph.node(index);
            requireNodeProjection(portable, runtime, indices);
            indices.put(portable.operatorId(), index);
        }
    }

    private static void requireNodeProjection(
            PortablePlanNode portable, PlanNode runtime,
            Map<String, Integer> indices) {
        PlanNode.Kind expectedKind = portable.opcode()
                == PortableOperatorCode.SOURCE ? PlanNode.Kind.SOURCE
                : portable.opcode() == PortableOperatorCode.SINK
                ? PlanNode.Kind.SINK
                : portable.opcode().inputCount() == 2
                ? PlanNode.Kind.BINARY : PlanNode.Kind.UNARY;
        String expectedType = portable.opcode() == PortableOperatorCode.SOURCE
                ? "source" : portable.opcode() == PortableOperatorCode.SINK
                ? "sink" : internalTypeId(portable.opcode());
        if (runtime.kind() != expectedKind
                || !expectedType.equals(runtime.typeId())
                || !sameSchema(toDataSchema(portable.outputSchema()), runtime.outMeta())) {
            invalid("PROJECTION_MISMATCH", "compiler.projection.node");
        }

        int[] children = runtime.childIndices();
        if (children.length != portable.inputOperatorIds().size()) {
            invalid("PROJECTION_MISMATCH", "compiler.projection.inputs");
        }
        for (int child = 0; child < children.length; child++) {
            Integer expected = indices.get(
                    portable.inputOperatorIds().get(child));
            if (expected == null || children[child] != expected) {
                invalid("PROJECTION_MISMATCH", "compiler.projection.inputs");
            }
        }

        Map<String, String> expectedParams = new HashMap<>(
                portable.scalarParameters());
        expectedParams.put(PlanNode.NAME_PARAM, portable.operatorId());
        expectedParams.put(OPERATOR_ID_PARAM, portable.operatorId());
        expectedParams.put(OPCODE_PARAM,
                Integer.toString(portable.opcode().wireId()));
        expectedParams.put(OUTPUT_SCHEMA_PARAM,
                portable.outputSchema().fingerprint());
        if (portable.opcode() == PortableOperatorCode.SOURCE) {
            expectedParams.put(PlanNode.OPERATOR_UID_PARAM,
                    portable.operatorId());
        }
        if (!expectedParams.equals(runtime.params())) {
            invalid("PROJECTION_MISMATCH", "compiler.projection.parameters");
        }

        Map<String, List<String>> actualLists = new HashMap<>(
                runtime.listParams());
        if (portable.opcode() == PortableOperatorCode.SOURCE) {
            List<String> descriptors = actualLists.remove(
                    PlanNode.SOURCE_SPLIT_DESCRIPTORS_PARAM);
            PortableSourceBinding binding =
                    (PortableSourceBinding) portable.binding();
            if (descriptors == null
                    || descriptors.size()
                    != binding.splitFramesUnsafe().size()) {
                invalid("PROJECTION_MISMATCH", "compiler.projection.source");
            }
        } else if (portable.opcode() == PortableOperatorCode.SINK) {
            List<String> descriptors = actualLists.remove(
                    PlanNode.SINK_DESCRIPTOR_PARAM);
            if (descriptors == null || descriptors.size() != 1) {
                invalid("PROJECTION_MISMATCH", "compiler.projection.sink");
            }
        }
        if (!portable.listParameters().equals(actualLists)) {
            invalid("PROJECTION_MISMATCH", "compiler.projection.parameters");
        }

        boolean function = portable.opcode().bindingKind()
                == PortableOperatorCode.BindingKind.FUNCTION;
        boolean sourceMismatch = portable.opcode()
                == PortableOperatorCode.SOURCE
                && (runtime.inputs() == null || runtime.inputs().size()
                != ((PortableSourceBinding) portable.binding())
                .splitFramesUnsafe().size());
        boolean sinkMismatch = portable.opcode()
                == PortableOperatorCode.SINK && runtime.output() == null;
        if (function != (runtime.userFunction() != null)
                || sourceMismatch || sinkMismatch) {
            invalid("PROJECTION_MISMATCH", "compiler.projection.binding");
        }
    }

    public static DataSchema toDataSchema(PortableRowSchema schema) {
        if (schema == null) {
            throw PortablePlanText.failure("INVALID_SCHEMA", "schema",
                    "portable schema is required");
        }
        Field[] fields = new Field[schema.fieldCount()];
        for (int index = 0; index < fields.length; index++) {
            PortableField field = schema.field(index);
            fields[index] = new Field(field.name(), field.alias(),
                    toDataType(field.dataType()));
        }
        return new DataSchema(fields);
    }

    private static DataType toDataType(PortableDataType type) {
        switch (type) {
            case BOOLEAN: return DataType.BooleanType;
            case STRING: return DataType.StringType;
            case INTEGER: return DataType.IntegerType;
            case LONG: return DataType.LongType;
            case BIG_DECIMAL: return DataType.BigDecimalType;
            case DATE: return DataType.DateType;
            case TIMESTAMP: return DataType.TimestampType;
            case NULL: return DataType.NullType;
            default:
                throw PortablePlanText.failure("UNKNOWN_DATA_TYPE", "schema.type",
                        "portable data type is not supported");
        }
    }

    private static void preflightContracts(PortableBatchPlan plan) {
        Map<String, PortablePlanNode> nodes = new HashMap<>();
        for (PortablePlanNode node : plan.nodes()) {
            validateParameters(node);
            if (node.opcode() == PortableOperatorCode.SOURCE) {
                validateSourceDescriptors(node);
            }
            for (String inputId : node.inputOperatorIds()) {
                PortablePlanNode input = nodes.get(inputId);
                if (input == null) invalid("INVALID_TOPOLOGY", "nodes.inputs");
                validateSchemaContract(node, input);
            }
            if (node.opcode() == PortableOperatorCode.SINK) {
                validateSinkDescriptor(
                        node, nodes.get(node.inputOperatorIds().get(0)));
            }
            nodes.put(node.operatorId(), node);
        }
    }

    private static void validateSourceDescriptors(PortablePlanNode node) {
        PortableSourceBinding binding =
                (PortableSourceBinding) node.binding();
        String expectedSchema = SchemaFingerprint.of(
                toDataSchema(node.outputSchema()));
        Set<String> splitIds = new HashSet<>();
        for (byte[] frame : binding.splitFramesUnsafe()) {
            BatchSourceSplitDescriptor descriptor;
            try {
                descriptor = BatchSourceSplitDescriptorCodec.decode(frame);
            } catch (RuntimeException malformed) {
                invalid("SOURCE_DESCRIPTOR_INVALID", "nodes.binding.source");
                return;
            }
            if (!Arrays.equals(frame,
                    BatchSourceSplitDescriptorCodec.encode(descriptor))) {
                invalid("SOURCE_DESCRIPTOR_NONCANONICAL",
                        "nodes.binding.source");
            }
            if (!expectedSchema.equals(descriptor.schemaFingerprint())) {
                invalid("SOURCE_DESCRIPTOR_SCHEMA_MISMATCH",
                        "nodes.binding.source");
            }
            if (!splitIds.add(descriptor.splitId())) {
                invalid("SOURCE_DESCRIPTOR_DUPLICATE",
                        "nodes.binding.source");
            }
        }
    }

    private static void validateSinkDescriptor(
            PortablePlanNode node, PortablePlanNode input) {
        PortableSinkBinding binding = (PortableSinkBinding) node.binding();
        byte[] frame = binding.descriptorFrameUnsafe();
        BatchSinkDescriptor descriptor;
        try {
            descriptor = BatchSinkDescriptorCodec.decode(frame);
        } catch (RuntimeException malformed) {
            invalid("SINK_DESCRIPTOR_INVALID", "nodes.binding.sink");
            return;
        }
        if (!Arrays.equals(frame, BatchSinkDescriptorCodec.encode(descriptor))) {
            invalid("SINK_DESCRIPTOR_NONCANONICAL", "nodes.binding.sink");
        }
        String expectedSchema = SchemaFingerprint.of(
                toDataSchema(input.outputSchema()));
        if (!expectedSchema.equals(descriptor.schemaFingerprint())) {
            invalid("SINK_DESCRIPTOR_SCHEMA_MISMATCH", "nodes.binding.sink");
        }
        if (descriptor.capabilityProfile()
                == BatchSinkDescriptor.CapabilityProfile.LEGACY_PROCESS_LOCAL) {
            invalid("SINK_PROFILE_UNSUPPORTED", "nodes.binding.sink");
        }
    }

    private static void verifyBindings(
            PortableBatchPlan plan, PortableBindingVerifier verifier) {
        for (PortablePlanNode node : plan.nodes()) {
            switch (node.opcode().bindingKind()) {
                case SOURCE:
                    verifier.verifySource((PortableSourceBinding) node.binding());
                    break;
                case FUNCTION:
                    verifier.verifyFunction(
                            (FunctionDeploymentReference) node.binding());
                    break;
                case SINK:
                    verifier.verifySink((PortableSinkBinding) node.binding());
                    break;
                default:
                    break;
            }
        }
    }

    private static void resolveSources(
            PortableBatchPlan plan, PortableRuntimeResolver resolver,
            Map<String, List<Source>> resolved) {
        for (PortablePlanNode node : plan.nodes()) {
            if (node.opcode() != PortableOperatorCode.SOURCE) continue;
            PortableSourceBinding binding = (PortableSourceBinding) node.binding();
            List<Source> inputs = resolver.resolveSource(binding);
            if (inputs == null || inputs.size() != binding.splitFrames().size()) {
                invalid("RESOLVED_SOURCE_SPLIT_MISMATCH", "nodes.binding.source");
            }
            DataSchema expected = toDataSchema(node.outputSchema());
            List<Source> copied = new ArrayList<>(inputs.size());
            for (Source input : inputs) {
                if (input == null || !sameSchema(expected, input.getSchema())) {
                    invalid("RESOLVED_SCHEMA_MISMATCH", "nodes.binding.source");
                }
                copied.add(input);
            }
            resolved.put(node.operatorId(),
                    Collections.unmodifiableList(copied));
        }
    }

    private static void resolveFunctions(
            PortableBatchPlan plan, PortableRuntimeResolver resolver,
            Map<String, Serializable> resolved) {
        for (PortablePlanNode node : plan.nodes()) {
            if (node.opcode().bindingKind()
                    != PortableOperatorCode.BindingKind.FUNCTION) continue;
            FunctionDeploymentReference reference =
                    (FunctionDeploymentReference) node.binding();
            Serializable function = resolver.resolveFunction(reference);
            if (!matchesAbi(reference.functionAbi(), function)) {
                invalid("RESOLVED_FUNCTION_ABI_MISMATCH",
                        "nodes.binding.function");
            }
            if (reference.functionAbi() == FunctionAbi.MAP_PARTITION_V1
                    && !sameSchema(toDataSchema(node.outputSchema()),
                    ((MapPartitionFunction) function).getResultSchema())) {
                invalid("RESOLVED_SCHEMA_MISMATCH",
                        "nodes.binding.function.resultSchema");
            }
            resolved.put(node.operatorId(), function);
        }
    }

    private static void resolveSinks(
            PortableBatchPlan plan, PortableRuntimeResolver resolver,
            Map<String, Sink> resolved) {
        Map<String, PortablePlanNode> nodes = index(plan);
        for (PortablePlanNode node : plan.nodes()) {
            if (node.opcode() != PortableOperatorCode.SINK) continue;
            PortablePlanNode input = nodes.get(node.inputOperatorIds().get(0));
            DataSchema expected = toDataSchema(input.outputSchema());
            Sink output = resolver.resolveSink(
                    (PortableSinkBinding) node.binding(), expected);
            if (output == null || !sameSchema(expected, output.getSchema())) {
                invalid("RESOLVED_SCHEMA_MISMATCH", "nodes.binding.sink");
            }
            resolved.put(node.operatorId(), output);
        }
    }

    private static PlanGraph project(
            PortableBatchPlan plan, Map<String, List<Source>> sources,
            Map<String, Serializable> functions, Map<String, Sink> sinks) {
        List<PlanNode> projected = new ArrayList<>(plan.nodes().size());
        Map<String, Integer> indices = new HashMap<>();
        try {
            for (PortablePlanNode node : plan.nodes()) {
                int[] children = new int[node.inputOperatorIds().size()];
                for (int index = 0; index < children.length; index++) {
                    children[index] = indices.get(
                            node.inputOperatorIds().get(index));
                }
                PlanNode result = projectNode(node, children, sources,
                        functions, sinks);
                result = result.withName(node.operatorId())
                        .withParam(OPERATOR_ID_PARAM, node.operatorId())
                        .withParam(OPCODE_PARAM,
                                Integer.toString(node.opcode().wireId()))
                        .withParam(OUTPUT_SCHEMA_PARAM,
                                node.outputSchema().fingerprint());
                if (node.opcode() == PortableOperatorCode.SOURCE) {
                    result = result.withParam(
                            PlanNode.OPERATOR_UID_PARAM, node.operatorId());
                }
                projected.add(result);
                indices.put(node.operatorId(), projected.size() - 1);
            }
            return new PlanGraph(projected, plan.desiredParallelism());
        } catch (PortablePlanException failure) {
            throw failure;
        } catch (RuntimeException invalidRuntimeObject) {
            throw PortablePlanText.failure("RESOLVED_BINDING_INVALID",
                    "compiler.projection",
                    "resolved runtime object does not match portable metadata");
        }
    }

    private static PlanNode projectNode(
            PortablePlanNode node, int[] children,
            Map<String, List<Source>> sources,
            Map<String, Serializable> functions,
            Map<String, Sink> sinks) {
        DataSchema output = toDataSchema(node.outputSchema());
        if (node.opcode() == PortableOperatorCode.SOURCE) {
            return PlanNode.sourceFromInputs(
                    output, sources.get(node.operatorId()));
        }
        if (node.opcode() == PortableOperatorCode.SINK) {
            return PlanNode.sink(output, children[0],
                    sinks.get(node.operatorId())).build();
        }
        String typeId = internalTypeId(node.opcode());
        Serializable function = functions.get(node.operatorId());
        if (function != null) {
            if (children.length == 1) {
                return node.scalarParameters().isEmpty()
                        && node.listParameters().isEmpty()
                        ? PlanNode.unaryFn(typeId, output, children[0], function)
                        : PlanNode.unaryFnWithParams(typeId, output, children[0],
                                node.scalarParameters(), node.listParameters(),
                                function);
            }
            return PlanNode.binaryFn(typeId, output, children[0], children[1],
                    node.scalarParameters(), node.listParameters(), function);
        }
        PlanNode.Builder builder = children.length == 1
                ? PlanNode.unary(typeId, output, children[0])
                : PlanNode.binary(typeId, output, children[0], children[1]);
        copyParameters(node, builder);
        return builder.build();
    }

    private static void copyParameters(
            PortablePlanNode node, PlanNode.Builder builder) {
        for (Map.Entry<String, String> entry
                : node.scalarParameters().entrySet()) {
            builder.param(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, List<String>> entry
                : node.listParameters().entrySet()) {
            builder.listParam(entry.getKey(), entry.getValue());
        }
    }

    private static boolean matchesAbi(FunctionAbi abi, Serializable function) {
        if (function == null) return false;
        switch (abi) {
            case FILTER_V1: return function instanceof FilterFunction;
            case MAP_V1: return function instanceof MapFunction;
            case FLAT_MAP_V1: return function instanceof FlatMapFunction;
            case REDUCE_GROUP_V1: return function instanceof GroupReduceFunction;
            case COMBINE_GROUP_V1: return function instanceof GroupCombineFunction;
            case COMBINE_REDUCE_GROUP_V1:
                return function instanceof GroupCombineReduceFunction;
            case CO_GROUP_V1: return function instanceof CoGroupFunction;
            case MAP_PARTITION_V1:
                return function instanceof MapPartitionFunction;
            default: return false;
        }
    }

    private static boolean sameSchema(DataSchema expected, DataSchema actual) {
        if (actual == null
                || expected.getFieldCount() != actual.getFieldCount()) return false;
        for (int index = 0; index < expected.getFieldCount(); index++) {
            Field left = expected.getField(index);
            Field right = actual.getField(index);
            if (right == null || !left.getName().equals(right.getName())
                    || !left.getAlias().equals(right.getAlias())
                    || left.getDataType() != right.getDataType()) return false;
        }
        return true;
    }

    private static Map<String, PortablePlanNode> index(PortableBatchPlan plan) {
        Map<String, PortablePlanNode> result = new HashMap<>();
        for (PortablePlanNode node : plan.nodes()) {
            result.put(node.operatorId(), node);
        }
        return result;
    }

    private static void validateSchemaContract(
            PortablePlanNode node, PortablePlanNode input) {
        switch (node.opcode()) {
            case SINK:
            case FILTER_EXPRESSION:
            case ROW_VALIDATE:
            case PARTITION_HASH:
            case DISTINCT:
            case ORDER_BY:
            case TOP_SORTED:
            case TOP_FIRST:
            case FUNCTION_FILTER:
                requireSameOutput(node, input);
                break;
            case UNION:
                requireSameOutput(node, input);
                break;
            default:
                break;
        }
    }

    private static void requireSameOutput(
            PortablePlanNode node, PortablePlanNode input) {
        if (!node.outputSchema().fingerprint().equals(
                input.outputSchema().fingerprint())) {
            invalid("OPERATOR_SCHEMA_MISMATCH", "nodes.outputSchema");
        }
    }

    private static void validateParameters(PortablePlanNode node) {
        switch (node.opcode()) {
            case SOURCE: case SINK: case UNION:
            case FUNCTION_FILTER: case FUNCTION_MAP: case FUNCTION_FLAT_MAP:
                exact(node);
                break;
            case FILTER_EXPRESSION:
                exact(node, strings("expr"), strings());
                requireText(node, "expr");
                break;
            case SELECT_EXPRESSIONS:
                exact(node, strings(), strings("exprs"));
                requireList(node, "exprs", true);
                break;
            case SELECT_INDICES:
                exact(node, strings(), strings("indices"));
                requireNonNegativeIntegers(node, "indices", true);
                break;
            case ADD_FIELDS:
                exact(node, strings(), strings(
                        "addNames", "addTypes", "addValues"));
                requireParallelLists(node, true,
                        "addNames", "addTypes", "addValues");
                break;
            case SCHEMA_CONVERT:
                exact(node, strings(), strings("sourceIndices", "casts",
                        "nullPolicies", "defaultKinds", "defaultValues"));
                requireParallelLists(node, true, "sourceIndices", "casts",
                        "nullPolicies", "defaultKinds", "defaultValues");
                break;
            case ROW_VALIDATE:
                exact(node, strings(), strings("ruleIds", "ruleKinds",
                        "fieldIndices", "lowerBounds", "upperBounds"));
                requireParallelLists(node, true, "ruleIds", "ruleKinds",
                        "fieldIndices", "lowerBounds", "upperBounds");
                break;
            case ROW_CLASSIFY:
                exact(node, strings("sourcePositionMode", "carrierVersion",
                                "routeContract"),
                        strings("ruleIds", "ruleKinds", "fieldIndices",
                                "lowerBounds", "upperBounds"));
                requireParallelLists(node, true, "ruleIds", "ruleKinds",
                        "fieldIndices", "lowerBounds", "upperBounds");
                break;
            case QUALITY_ROUTE: case OBJECT_QUALITY_ROUTE:
                exact(node, strings("carrierVersion", "route"), strings());
                requireText(node, "carrierVersion");
                requireText(node, "route");
                break;
            case PARTITION_HASH:
                exact(node, strings(), strings("keys"));
                requireList(node, "keys", true);
                break;
            case TYPED_QUALITY_ACCEPTED_SUMMARY:
                exact(node, strings("summaryVersion"), strings());
                requireText(node, "summaryVersion");
                break;
            case TYPED_QUALITY_REJECTED_SUMMARY:
                exact(node, strings("summaryVersion", "maxReasonBuckets"),
                        strings());
                requirePositive(node, "maxReasonBuckets");
                break;
            case GROUP_SUM: case GROUP_MAX: case GROUP_MIN:
                exact(node, strings("measure"), strings("keys"));
                requireText(node, "measure");
                requireList(node, "keys", false);
                break;
            case GROUP_COUNT:
                exactOptionalScalar(node, strings(), strings("measure"),
                        strings("keys"));
                requireList(node, "keys", false);
                break;
            case GROUP_MULTI:
                exact(node, strings(), strings("keys", "aggKinds",
                        "aggMeasures"));
                requireParallelLists(node, true, "aggKinds", "aggMeasures");
                requireList(node, "keys", false);
                break;
            case DISTINCT:
                exact(node, strings(), strings("keys"));
                requireList(node, "keys", false);
                break;
            case ORDER_BY:
                exact(node, strings(), strings("sort"));
                requireList(node, "sort", true);
                break;
            case TOP_SORTED:
                exact(node, strings("n"), strings("sort"));
                requirePositive(node, "n");
                requireList(node, "sort", true);
                break;
            case TOP_FIRST:
                exact(node, strings("n"), strings());
                requirePositive(node, "n");
                break;
            case WHOLE_AGG:
                exactOptionalScalar(node, strings("kind"), strings("field"),
                        strings());
                requireText(node, "kind");
                if (!"COUNT_ALL".equals(node.scalarParameters().get("kind"))) {
                    requireText(node, "field");
                } else if (node.scalarParameters().containsKey("field")) {
                    invalid("INVALID_PARAMETER", "nodes.scalarParameters.field");
                }
                break;
            case JOIN:
                exactOptionalScalar(node, strings("joinType"),
                        strings("joinHint"), strings("leftKeys", "rightKeys"));
                requireText(node, "joinType");
                requireParallelLists(node, true, "leftKeys", "rightKeys");
                break;
            case FUNCTION_REDUCE_GROUP:
                exact(node, strings(), strings("keys"));
                requireList(node, "keys", false);
                break;
            case FUNCTION_COMBINE_GROUP:
            case FUNCTION_COMBINE_REDUCE_GROUP:
                exact(node, strings("shards"), strings());
                requirePositive(node, "shards");
                break;
            case FUNCTION_CO_GROUP:
                exact(node, strings(), strings("leftKeys", "rightKeys"));
                requireParallelLists(node, true, "leftKeys", "rightKeys");
                break;
            case FUNCTION_MAP_PARTITION:
                exact(node, strings("maxOutputRowsPerPartition",
                        "maxOutputBytesPerPartition"), strings());
                requireMapPartitionLimits(node);
                break;
            default:
                invalid("UNKNOWN_OPCODE", "nodes.opcode");
        }
    }

    private static void exact(PortablePlanNode node) {
        exact(node, strings(), strings());
    }

    private static void exact(
            PortablePlanNode node, Set<String> scalar, Set<String> lists) {
        if (!node.scalarParameters().keySet().equals(scalar)
                || !node.listParameters().keySet().equals(lists)) {
            invalid("UNKNOWN_PARAMETER", "nodes.parameters");
        }
    }

    private static void exactOptionalScalar(
            PortablePlanNode node, Set<String> required,
            Set<String> optional, Set<String> lists) {
        Set<String> actual = new HashSet<>(node.scalarParameters().keySet());
        if (!actual.containsAll(required)) {
            invalid("MISSING_PARAMETER", "nodes.scalarParameters");
        }
        actual.removeAll(required);
        if (!optional.containsAll(actual)
                || !node.listParameters().keySet().equals(lists)) {
            invalid("UNKNOWN_PARAMETER", "nodes.parameters");
        }
    }

    private static void requireText(PortablePlanNode node, String key) {
        String value = node.scalarParameters().get(key);
        if (value == null || value.isEmpty()) {
            invalid("INVALID_PARAMETER", "nodes.scalarParameters." + key);
        }
    }

    private static void requirePositive(PortablePlanNode node, String key) {
        String value = node.scalarParameters().get(key);
        try {
            if (value == null || Integer.parseInt(value) <= 0) {
                invalid("INVALID_PARAMETER", "nodes.scalarParameters." + key);
            }
        } catch (NumberFormatException invalid) {
            invalid("INVALID_PARAMETER", "nodes.scalarParameters." + key);
        }
    }

    private static void requirePositiveLong(
            PortablePlanNode node, String key) {
        String value = node.scalarParameters().get(key);
        try {
            if (value == null || Long.parseLong(value) <= 0L) {
                invalid("INVALID_PARAMETER",
                        "nodes.scalarParameters." + key);
            }
        } catch (NumberFormatException invalid) {
            invalid("INVALID_PARAMETER", "nodes.scalarParameters." + key);
        }
    }

    private static void requireMapPartitionLimits(PortablePlanNode node) {
        requirePositiveLong(node, "maxOutputRowsPerPartition");
        requirePositiveLong(node, "maxOutputBytesPerPartition");
        try {
            new MapPartitionEmissionLimit(
                    Long.parseLong(node.scalarParameters().get(
                            "maxOutputRowsPerPartition")),
                    Long.parseLong(node.scalarParameters().get(
                            "maxOutputBytesPerPartition")));
        } catch (IllegalArgumentException invalidLimit) {
            invalid("LIMIT_EXCEEDED", "nodes.scalarParameters");
        }
    }

    private static void requireNonNegativeIntegers(
            PortablePlanNode node, String key, boolean nonEmpty) {
        requireList(node, key, nonEmpty);
        for (String value : node.listParameters().get(key)) {
            try {
                if (Integer.parseInt(value) < 0) {
                    invalid("INVALID_PARAMETER", "nodes.listParameters." + key);
                }
            } catch (NumberFormatException invalid) {
                invalid("INVALID_PARAMETER", "nodes.listParameters." + key);
            }
        }
    }

    private static void requireList(
            PortablePlanNode node, String key, boolean nonEmpty) {
        List<String> values = node.listParameters().get(key);
        if (values == null || (nonEmpty && values.isEmpty())) {
            invalid("INVALID_PARAMETER", "nodes.listParameters." + key);
        }
    }

    private static void requireParallelLists(
            PortablePlanNode node, boolean nonEmpty, String... keys) {
        int length = -1;
        for (String key : keys) {
            List<String> values = node.listParameters().get(key);
            if (values == null || (nonEmpty && values.isEmpty())
                    || (length >= 0 && values.size() != length)) {
                invalid("INVALID_PARAMETER", "nodes.listParameters." + key);
            }
            length = values.size();
        }
    }

    private static Set<String> strings(String... values) {
        Set<String> result = new HashSet<>();
        Collections.addAll(result, values);
        return result;
    }

    private static Serializable metadataFunction(
            FunctionAbi abi, DataSchema outputSchema) {
        switch (abi) {
            case FILTER_V1:
                return new FilterFunction() {
                    private static final long serialVersionUID = 1L;
                    @Override public boolean filter(DataRow row) {
                        throw metadataExecution();
                    }
                };
            case MAP_V1:
                return new MapFunction() {
                    private static final long serialVersionUID = 1L;
                    @Override public DataRow map(DataRow row) {
                        throw metadataExecution();
                    }
                };
            case FLAT_MAP_V1:
                return new FlatMapFunction() {
                    private static final long serialVersionUID = 1L;
                    @Override public void flatMap(DataRow row, Collector collector) {
                        throw metadataExecution();
                    }
                };
            case REDUCE_GROUP_V1:
                return new GroupReduceFunction() {
                    private static final long serialVersionUID = 1L;
                    @Override public void reduce(
                            Iterable<DataRow> rows, Collector collector) {
                        throw metadataExecution();
                    }
                    @Override public DataSchema getResultSchema() {
                        return outputSchema;
                    }
                };
            case COMBINE_GROUP_V1:
                return new GroupCombineFunction() {
                    private static final long serialVersionUID = 1L;
                    @Override public void combine(
                            Iterable<DataRow> rows, Collector collector) {
                        throw metadataExecution();
                    }
                    @Override public DataSchema getResultSchema() {
                        return outputSchema;
                    }
                };
            case COMBINE_REDUCE_GROUP_V1:
                return new GroupCombineReduceFunction() {
                    private static final long serialVersionUID = 1L;
                    @Override public void combine(
                            Iterable<DataRow> rows, Collector collector) {
                        throw metadataExecution();
                    }
                    @Override public void reduce(
                            Iterable<DataRow> rows, Collector collector) {
                        throw metadataExecution();
                    }
                    @Override public DataSchema getResultSchema() {
                        return outputSchema;
                    }
                };
            case CO_GROUP_V1:
                return new CoGroupFunction() {
                    private static final long serialVersionUID = 1L;
                    @Override public void coGroup(
                            Iterable<DataRow> left, Iterable<DataRow> right,
                            Collector collector) {
                        throw metadataExecution();
                    }
                    @Override public DataSchema getResultSchema() {
                        return outputSchema;
                    }
                };
            case MAP_PARTITION_V1:
                return new MapPartitionFunction() {
                    private static final long serialVersionUID = 1L;
                    @Override public void mapPartition(
                            PartitionDataRowCursor rows, Collector collector) {
                        throw metadataExecution();
                    }
                    @Override public DataSchema getResultSchema() {
                        return outputSchema;
                    }
                };
            default:
                throw PortablePlanText.failure("UNKNOWN_FUNCTION_ABI",
                        "function.reference.abi",
                        "function ABI cannot be projected");
        }
    }

    private static IllegalStateException metadataExecution() {
        return new IllegalStateException(
                "metadata-only portable projection is not executable");
    }

    private static final class MetadataInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final DataSchema dataSchema;
        private final BatchSourceSplitDescriptor descriptor;

        private MetadataInput(
                DataSchema dataSchema, BatchSourceSplitDescriptor descriptor) {
            this.dataSchema = dataSchema;
            this.descriptor = descriptor;
        }

        @Override public DataSchema getSchema() { return dataSchema; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() {
            return descriptor;
        }
        @Override public Iterator<DataRow> iterator() {
            throw metadataExecution();
        }
    }

    private static final class MetadataOutput implements BatchSink {
        private static final long serialVersionUID = 1L;
        private final DataSchema dataSchema;
        private final BatchSinkDescriptor descriptor;

        private MetadataOutput(
                DataSchema dataSchema, BatchSinkDescriptor descriptor) {
            this.dataSchema = dataSchema;
            this.descriptor = descriptor;
        }

        @Override public DataSchema getSchema() { return dataSchema; }
        @Override public void collect(DataRow row) { throw metadataExecution(); }
        @Override public BatchSinkDescriptor batchSinkDescriptor() {
            return descriptor;
        }
        @Override public boolean isWorkerWritable() { return true; }
        @Override public String exclusiveDestinationId() {
            return descriptor.capabilityProfile()
                    == BatchSinkDescriptor.CapabilityProfile
                    .MANIFEST_ATOMIC_GROUP_V1
                    ? descriptor.destinationId() : null;
        }
        @Override public boolean supportsCoordinatedCommit() {
            return descriptor.capabilityProfile()
                    == BatchSinkDescriptor.CapabilityProfile
                    .MANIFEST_ATOMIC_GROUP_V1;
        }
        @Override public void prepareCommit() { throw metadataExecution(); }
        @Override public void commitPrepared() { throw metadataExecution(); }
        @Override public void rollbackCommit() { throw metadataExecution(); }
        @Override public void finalizeCommit() { throw metadataExecution(); }
    }

    private static String internalTypeId(PortableOperatorCode code) {
        switch (code) {
            case FILTER_EXPRESSION: return "filter";
            case SELECT_EXPRESSIONS: return "select";
            case SELECT_INDICES: return "selectByIndex";
            case ADD_FIELDS: return "addFields";
            case SCHEMA_CONVERT: return "schemaConvert";
            case ROW_VALIDATE: return "rowValidate";
            case ROW_CLASSIFY: return "rowClassify";
            case QUALITY_ROUTE: return "qualityRoute";
            case OBJECT_QUALITY_ROUTE: return "objectQualityRoute";
            case PARTITION_HASH: return "partitionByHash";
            case TYPED_QUALITY_ACCEPTED_SUMMARY:
                return "typedQualityAcceptedSummary";
            case TYPED_QUALITY_REJECTED_SUMMARY:
                return "typedQualityRejectedSummary";
            case GROUP_SUM: return "groupBySum";
            case GROUP_COUNT: return "groupByCount";
            case GROUP_MAX: return "groupByMax";
            case GROUP_MIN: return "groupByMin";
            case GROUP_MULTI: return "groupBy";
            case DISTINCT: return "distinct";
            case ORDER_BY: return "orderBy";
            case TOP_SORTED: return "top";
            case TOP_FIRST: return "topN";
            case WHOLE_AGG: return "wholeAgg";
            case UNION: return "union";
            case JOIN: return "join";
            case FUNCTION_FILTER: return "filterFn";
            case FUNCTION_MAP: return "map";
            case FUNCTION_FLAT_MAP: return "flatMap";
            case FUNCTION_REDUCE_GROUP: return "reduceGroup";
            case FUNCTION_COMBINE_GROUP: return "combineGroup";
            case FUNCTION_COMBINE_REDUCE_GROUP: return "combineReduceGroup";
            case FUNCTION_CO_GROUP: return "coGroup";
            case FUNCTION_MAP_PARTITION: return "mapPartition";
            default:
                throw PortablePlanText.failure("UNKNOWN_OPCODE", "nodes.opcode",
                        "portable opcode cannot be projected");
        }
    }

    private static void invalid(String code, String path) {
        throw PortablePlanText.failure(code, path,
                "portable plan contract is invalid");
    }
}
