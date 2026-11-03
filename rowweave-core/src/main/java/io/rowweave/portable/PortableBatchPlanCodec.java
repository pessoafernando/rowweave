package io.rowweave.portable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;

/** Canonical, checksummed and allocation-bounded portable batch plan v1 frame. */
public final class PortableBatchPlanCodec {
    private static final byte[] MAGIC =
            "RWXXPBP1".getBytes(StandardCharsets.US_ASCII);
    private static final int ENVELOPE_VERSION = 1;
    private static final int HEADER_BYTES = 16;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_BODY_BYTES = 32 * 1024 * 1024;
    public static final int MAX_FRAME_BYTES =
            HEADER_BYTES + MAX_BODY_BYTES + DIGEST_BYTES;

    private PortableBatchPlanCodec() { }

    public static byte[] magic() { return MAGIC.clone(); }

    public static byte[] encode(PortableBatchPlan plan) {
        return encode(plan, ignored -> { });
    }

    public static byte[] encode(PortableBatchPlan plan, LongConsumer reservation) {
        if (plan == null || reservation == null) {
            throw new IllegalArgumentException("plan and reservation are required");
        }
        long upperBound = encodedBodyUpperBound(plan);
        if (upperBound > MAX_BODY_BYTES) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", "envelope.body",
                    "portable plan body exceeds its limit");
        }
        reservation.accept(Math.addExact(Math.multiplyExact(6L, upperBound),
                HEADER_BYTES + DIGEST_BYTES));
        byte[] body = encodeBody(plan);
        if (body.length > MAX_BODY_BYTES) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", "envelope.body",
                    "portable plan body exceeds its limit");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    HEADER_BYTES + body.length + DIGEST_BYTES);
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.write(MAGIC);
                output.writeInt(ENVELOPE_VERSION);
                output.writeInt(body.length);
                output.write(body);
                output.write(PortablePlanText.sha256(body));
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory portable plan encoding failed",
                    impossible);
        }
    }

    private static byte[] encodeBody(PortableBatchPlan plan) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(plan.planVersion());
                output.writeInt(plan.desiredParallelism());
                writeTexts(output, plan.requiredCapabilities());
                writeTexts(output, plan.rootSinkOperatorIds());
                output.writeInt(plan.nodes().size());
                for (PortablePlanNode node : plan.nodes()) {
                    writeText(output, node.operatorId());
                    output.writeInt(node.opcode().wireId());
                    writeTexts(output, node.inputOperatorIds());
                    writeTexts(output, node.inputSchemaFingerprints());
                    writeSchema(output, node.outputSchema());
                    writeScalarParameters(output, node.scalarParameters());
                    writeListParameters(output, node.listParameters());
                    writeBinding(output, node.binding());
                }
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory portable plan encoding failed",
                    impossible);
        }
    }

    private static void writeSchema(
            DataOutputStream output, PortableRowSchema schema) throws IOException {
        output.writeInt(schema.fieldCount());
        for (PortableField field : schema.fields()) {
            writeText(output, field.fieldId());
            writeText(output, field.name());
            writeText(output, field.alias());
            output.writeInt(field.dataType().wireId());
            output.writeBoolean(field.nullable());
        }
    }

    private static void writeScalarParameters(
            DataOutputStream output, Map<String, String> values) throws IOException {
        output.writeInt(values.size());
        for (Map.Entry<String, String> entry : values.entrySet()) {
            writeText(output, entry.getKey());
            writeText(output, entry.getValue());
        }
    }

    private static void writeListParameters(
            DataOutputStream output, Map<String, List<String>> values)
            throws IOException {
        output.writeInt(values.size());
        for (Map.Entry<String, List<String>> entry : values.entrySet()) {
            writeText(output, entry.getKey());
            writeTexts(output, entry.getValue());
        }
    }

    private static void writeBinding(
            DataOutputStream output, PortableNodeBinding binding) throws IOException {
        if (binding == null) {
            output.writeInt(0);
        } else if (binding instanceof PortableSourceBinding) {
            PortableSourceBinding source = (PortableSourceBinding) binding;
            output.writeInt(1);
            writeText(output, source.providerKey());
            writeText(output, source.providerContractDigest());
            writeFrame(output, source.configurationFrameUnsafe());
            List<byte[]> splits = source.splitFramesUnsafe();
            output.writeInt(splits.size());
            for (byte[] split : splits) writeFrame(output, split);
        } else if (binding instanceof FunctionDeploymentReference) {
            FunctionDeploymentReference function =
                    (FunctionDeploymentReference) binding;
            output.writeInt(2);
            writeText(output, function.deploymentSetId());
            writeText(output, function.deploymentId());
            writeText(output, function.deploymentJarDigest());
            writeText(output, function.className());
            output.writeInt(function.functionAbi().wireId());
            writeText(output, function.configurationCodecId());
            writeText(output, function.configurationDigest());
            writeFrame(output, function.configurationFrameUnsafe());
            output.writeLong(function.resourceProfile().maximumHeapBytes());
            output.writeLong(function.resourceProfile().maximumDirectBytes());
            output.writeInt(function.resourceProfile().maximumOutputRowsPerInput());
        } else if (binding instanceof PortableSinkBinding) {
            PortableSinkBinding sink = (PortableSinkBinding) binding;
            output.writeInt(3);
            writeText(output, sink.providerKey());
            writeText(output, sink.providerContractDigest());
            writeFrame(output, sink.descriptorFrameUnsafe());
            writeFrame(output, sink.configurationFrameUnsafe());
        } else {
            throw PortablePlanText.failure("BINDING_KIND_MISMATCH", "nodes.binding",
                    "binding implementation is outside the closed v1 set");
        }
    }

    private static void writeTexts(DataOutputStream output, List<String> values)
            throws IOException {
        output.writeInt(values.size());
        for (String value : values) writeText(output, value);
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static void writeFrame(DataOutputStream output, byte[] value)
            throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    public static PortableBatchPlan decode(byte[] frame) {
        return decode(frame, ignored -> { });
    }

    public static PortableBatchPlan decode(
            byte[] frame, LongConsumer reservation) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES) {
            throw PortablePlanText.failure("FRAME_LENGTH_MISMATCH", "envelope",
                    "portable plan frame is shorter than its envelope");
        }
        if (frame.length > MAX_FRAME_BYTES) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", "envelope",
                    "portable plan frame exceeds its limit");
        }
        if (reservation == null) {
            throw new IllegalArgumentException("reservation is required");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            reservation.accept(MAGIC.length);
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!MessageDigest.isEqual(MAGIC, magic)) {
                throw PortablePlanText.failure("INVALID_MAGIC", "envelope.magic",
                        "portable plan magic is invalid");
            }
            int envelopeVersion = input.readInt();
            if (envelopeVersion != ENVELOPE_VERSION) {
                throw PortablePlanText.failure("UNSUPPORTED_ENVELOPE_VERSION",
                        "envelope.version",
                        "portable plan envelope version is not supported");
            }
            int bodyLength = input.readInt();
            if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES) {
                throw PortablePlanText.failure("LIMIT_EXCEEDED", "envelope.body",
                        "portable plan body length is outside its limit");
            }
            if (HEADER_BYTES + (long) bodyLength + DIGEST_BYTES != frame.length) {
                throw PortablePlanText.failure("FRAME_LENGTH_MISMATCH", "envelope.body",
                        "portable plan frame length does not match its body");
            }
            reservation.accept(DIGEST_BYTES * 2L);
            byte[] actual = sha256(frame, HEADER_BYTES, bodyLength);
            byte[] expected = new byte[DIGEST_BYTES];
            System.arraycopy(frame, HEADER_BYTES + bodyLength,
                    expected, 0, DIGEST_BYTES);
            if (!MessageDigest.isEqual(actual, expected)) {
                throw PortablePlanText.failure("CHECKSUM_MISMATCH", "envelope.digest",
                        "portable plan checksum does not match its body");
            }
            try (DataInputStream body = new DataInputStream(
                    new ByteArrayInputStream(frame, HEADER_BYTES, bodyLength))) {
                PortableBatchPlan plan = readPlan(body, reservation);
                if (body.available() != 0) {
                    throw PortablePlanText.failure("FRAME_LENGTH_MISMATCH",
                            "envelope.body", "portable plan body has trailing bytes");
                }
                return plan;
            }
        } catch (PortablePlanException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw PortablePlanText.failure("FRAME_LENGTH_MISMATCH", "envelope.body",
                    "portable plan body is truncated");
        } catch (IOException malformed) {
            throw PortablePlanText.failure("FRAME_LENGTH_MISMATCH", "envelope.body",
                    "portable plan body cannot be decoded");
        }
    }

    private static PortableBatchPlan readPlan(
            DataInputStream input, LongConsumer reservation) throws IOException {
        int planVersion = input.readInt();
        int parallelism = input.readInt();
        List<String> capabilities = readTexts(input, reservation,
                1, PortableBatchPlan.MAX_CAPABILITIES, 256,
                "plan.capabilities");
        List<String> roots = readTexts(input, reservation,
                1, PortableBatchPlan.MAX_ROOT_SINKS,
                PortablePlanNode.MAX_OPERATOR_ID_BYTES, "plan.rootSinks");
        int nodeCount = readCount(input, 1, PortableBatchPlan.MAX_NODES,
                "plan.nodes");
        reservation.accept(nodeCount * 256L);
        List<PortablePlanNode> nodes = new ArrayList<>(nodeCount);
        for (int index = 0; index < nodeCount; index++) {
            String path = "plan.nodes[" + index + "]";
            String operatorId = readText(input, reservation,
                    PortablePlanNode.MAX_OPERATOR_ID_BYTES,
                    path + ".operatorId", false);
            PortableOperatorCode opcode = PortableOperatorCode.fromWireId(
                    input.readInt());
            List<String> inputs = readTexts(input, reservation,
                    0, 2, PortablePlanNode.MAX_OPERATOR_ID_BYTES,
                    path + ".inputs");
            List<String> inputSchemas = readTexts(input, reservation,
                    0, 2, 64, path + ".inputSchemaFingerprints");
            PortableRowSchema outputSchema = readSchema(input, reservation, path);
            Map<String, String> scalar = readScalarParameters(
                    input, reservation, path);
            Map<String, List<String>> lists = readListParameters(
                    input, reservation, path);
            PortableNodeBinding binding = readBinding(input, reservation, path);
            nodes.add(new PortablePlanNode(operatorId, opcode, inputs,
                    inputSchemas, outputSchema, scalar, lists, binding));
        }
        return new PortableBatchPlan(planVersion, parallelism,
                roots, nodes, capabilities);
    }

    private static PortableRowSchema readSchema(
            DataInputStream input, LongConsumer reservation, String path)
            throws IOException {
        int count = readCount(input, 1, PortableRowSchema.MAX_FIELDS,
                path + ".outputSchema.fields");
        reservation.accept(count * 128L);
        List<PortableField> fields = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            fields.add(new PortableField(
                    readText(input, reservation, PortableField.MAX_FIELD_ID_BYTES,
                            path + ".outputSchema.fieldId", false),
                    readText(input, reservation, PortableField.MAX_NAME_BYTES,
                            path + ".outputSchema.name", false),
                    readText(input, reservation, PortableField.MAX_NAME_BYTES,
                            path + ".outputSchema.alias", false),
                    PortableDataType.fromWireId(input.readInt()),
                    input.readBoolean()));
        }
        return new PortableRowSchema(fields);
    }

    private static Map<String, String> readScalarParameters(
            DataInputStream input, LongConsumer reservation, String path)
            throws IOException {
        int count = readCount(input, 0, PortablePlanNode.MAX_PARAMETERS,
                path + ".scalarParameters");
        reservation.accept(count * 96L);
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < count; index++) {
            String key = readText(input, reservation,
                    PortablePlanNode.MAX_PARAMETER_KEY_BYTES,
                    path + ".scalarParameters.key", false);
            String value = readText(input, reservation,
                    PortablePlanNode.MAX_PARAMETER_VALUE_BYTES,
                    path + ".scalarParameters.value", true);
            if (values.put(key, value) != null) {
                throw PortablePlanText.failure("DUPLICATE_PARAMETER",
                        path + ".scalarParameters",
                        "scalar parameter keys must be unique");
            }
        }
        return values;
    }

    private static Map<String, List<String>> readListParameters(
            DataInputStream input, LongConsumer reservation, String path)
            throws IOException {
        int count = readCount(input, 0, PortablePlanNode.MAX_PARAMETERS,
                path + ".listParameters");
        reservation.accept(count * 96L);
        Map<String, List<String>> values = new LinkedHashMap<>();
        for (int index = 0; index < count; index++) {
            String key = readText(input, reservation,
                    PortablePlanNode.MAX_PARAMETER_KEY_BYTES,
                    path + ".listParameters.key", false);
            List<String> elements = readTexts(input, reservation,
                    0, PortablePlanNode.MAX_LIST_VALUES,
                    PortablePlanNode.MAX_PARAMETER_VALUE_BYTES,
                    path + ".listParameters.value", true);
            if (values.put(key, elements) != null) {
                throw PortablePlanText.failure("DUPLICATE_PARAMETER",
                        path + ".listParameters",
                        "list parameter keys must be unique");
            }
        }
        return values;
    }

    private static PortableNodeBinding readBinding(
            DataInputStream input, LongConsumer reservation, String path)
            throws IOException {
        int kind = input.readInt();
        switch (kind) {
            case 0:
                return null;
            case 1:
                String sourceKey = readText(input, reservation, 256,
                        path + ".source.providerKey", false);
                String sourceDigest = readText(input, reservation, 64,
                        path + ".source.providerContractDigest", false);
                byte[] sourceConfig = readFrame(input, reservation,
                        PortableBindingLimits.MAX_CONFIGURATION_BYTES,
                        path + ".source.configurationFrame");
                int splitCount = readCount(input, 1,
                        PortableBindingLimits.MAX_SPLITS,
                        path + ".source.splitFrames");
                reservation.accept(splitCount * 32L);
                List<byte[]> splits = new ArrayList<>(splitCount);
                long total = 0L;
                for (int index = 0; index < splitCount; index++) {
                    byte[] split = readFrame(input, reservation,
                            PortableBindingLimits.MAX_SPLIT_FRAME_BYTES,
                            path + ".source.splitFrames");
                    total += split.length;
                    if (total > PortableBindingLimits.MAX_TOTAL_SPLIT_BYTES) {
                        throw PortablePlanText.failure("LIMIT_EXCEEDED",
                                path + ".source.splitFrames",
                                "source split bytes exceed their aggregate limit");
                    }
                    splits.add(split);
                }
                return new PortableSourceBinding(
                        sourceKey, sourceDigest, sourceConfig, splits);
            case 2:
                return readFunctionBinding(input, reservation, path);
            case 3:
                return new PortableSinkBinding(
                        readText(input, reservation, 256,
                                path + ".sink.providerKey", false),
                        readText(input, reservation, 64,
                                path + ".sink.providerContractDigest", false),
                        readFrame(input, reservation,
                                PortableBindingLimits.MAX_DESCRIPTOR_BYTES,
                                path + ".sink.descriptorFrame"),
                        readFrame(input, reservation,
                                PortableBindingLimits.MAX_CONFIGURATION_BYTES,
                                path + ".sink.configurationFrame"));
            default:
                throw PortablePlanText.failure("UNKNOWN_BINDING_KIND",
                        path + ".binding",
                        "portable binding kind is not supported");
        }
    }

    private static FunctionDeploymentReference readFunctionBinding(
            DataInputStream input, LongConsumer reservation, String path)
            throws IOException {
        String deploymentSetId = readText(input, reservation, 64,
                path + ".function.deploymentSetId", false);
        String deploymentId = readText(input, reservation, 256,
                path + ".function.deploymentId", false);
        String jarDigest = readText(input, reservation, 64,
                path + ".function.deploymentJarDigest", false);
        String className = readText(input, reservation, 512,
                path + ".function.className", false);
        FunctionAbi abi = FunctionAbi.fromWireId(input.readInt());
        String codecId = readText(input, reservation, 256,
                path + ".function.configurationCodecId", false);
        String configDigest = readText(input, reservation, 64,
                path + ".function.configurationDigest", false);
        byte[] config = readFrame(input, reservation,
                PortableBindingLimits.MAX_CONFIGURATION_BYTES,
                path + ".function.configurationFrame");
        FunctionResourceProfile profile = new FunctionResourceProfile(
                input.readLong(), input.readLong(), input.readInt());
        return new FunctionDeploymentReference(
                deploymentSetId, deploymentId, jarDigest, className,
                abi, codecId, configDigest, config, profile);
    }

    private static byte[] readFrame(
            DataInputStream input, LongConsumer reservation,
            int maximum, String path) throws IOException {
        int length = readCount(input, 1, maximum, path);
        if (length > input.available()) {
            throw PortablePlanText.failure("FRAME_LENGTH_MISMATCH", path,
                    "binding frame is truncated");
        }
        reservation.accept(length * 2L);
        byte[] value = new byte[length];
        input.readFully(value);
        return value;
    }

    private static List<String> readTexts(
            DataInputStream input, LongConsumer reservation,
            int minimum, int maximum, int maximumBytes, String path)
            throws IOException {
        return readTexts(input, reservation, minimum, maximum,
                maximumBytes, path, false);
    }

    private static List<String> readTexts(
            DataInputStream input, LongConsumer reservation,
            int minimum, int maximum, int maximumBytes, String path,
            boolean allowEmpty) throws IOException {
        int count = readCount(input, minimum, maximum, path);
        reservation.accept(count * 32L);
        List<String> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            values.add(readText(input, reservation, maximumBytes,
                    path, allowEmpty));
        }
        return values;
    }

    private static String readText(
            DataInputStream input, LongConsumer reservation,
            int maximumBytes, String path, boolean allowEmpty)
            throws IOException {
        int minimum = allowEmpty ? 0 : 1;
        int length = readCount(input, minimum, maximumBytes, path);
        if (length > input.available()) {
            throw PortablePlanText.failure("FRAME_LENGTH_MISMATCH", path,
                    "portable text is truncated");
        }
        reservation.accept(length * 3L + 40L);
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            String value = decoded.toString();
            return allowEmpty
                    ? PortablePlanText.value(value, path, maximumBytes)
                    : PortablePlanText.require(value, path, maximumBytes);
        } catch (CharacterCodingException invalid) {
            throw PortablePlanText.failure("INVALID_FIELD", path,
                    "portable text is not canonical UTF-8");
        }
    }

    private static int readCount(
            DataInputStream input, int minimum, int maximum, String path)
            throws IOException {
        int value = input.readInt();
        if (value < minimum || value > maximum) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", path,
                    "portable count is outside its contract limit");
        }
        return value;
    }

    private static long encodedBodyUpperBound(PortableBatchPlan plan) {
        long size = 12L;
        size = addTexts(size, plan.requiredCapabilities());
        size = addTexts(size, plan.rootSinkOperatorIds());
        for (PortablePlanNode node : plan.nodes()) {
            size = add(size, textUpperBound(node.operatorId()) + 4L);
            size = addTexts(size, node.inputOperatorIds());
            size = addTexts(size, node.inputSchemaFingerprints());
            size = add(size, 4L);
            for (PortableField field : node.outputSchema().fields()) {
                size = add(size, textUpperBound(field.fieldId()));
                size = add(size, textUpperBound(field.name()));
                size = add(size, textUpperBound(field.alias()) + 5L);
            }
            size = add(size, 4L);
            for (Map.Entry<String, String> entry :
                    node.scalarParameters().entrySet()) {
                size = add(size, textUpperBound(entry.getKey()));
                size = add(size, textUpperBound(entry.getValue()));
            }
            size = add(size, 4L);
            for (Map.Entry<String, List<String>> entry :
                    node.listParameters().entrySet()) {
                size = add(size, textUpperBound(entry.getKey()));
                size = addTexts(size, entry.getValue());
            }
            size = add(size, bindingUpperBound(node.binding()));
        }
        return size;
    }

    private static long bindingUpperBound(PortableNodeBinding binding) {
        long size = 4L;
        if (binding == null) return size;
        if (binding instanceof PortableSourceBinding) {
            PortableSourceBinding source = (PortableSourceBinding) binding;
            size = add(size, textUpperBound(source.providerKey()));
            size = add(size, textUpperBound(source.providerContractDigest()));
            size = add(size, frameSize(source.configurationFrameUnsafe()) + 4L);
            for (byte[] split : source.splitFramesUnsafe()) {
                size = add(size, frameSize(split));
            }
            return size;
        }
        if (binding instanceof PortableSinkBinding) {
            PortableSinkBinding sink = (PortableSinkBinding) binding;
            size = add(size, textUpperBound(sink.providerKey()));
            size = add(size, textUpperBound(sink.providerContractDigest()));
            size = add(size, frameSize(sink.descriptorFrameUnsafe()));
            return add(size, frameSize(sink.configurationFrameUnsafe()));
        }
        FunctionDeploymentReference function =
                (FunctionDeploymentReference) binding;
        size = add(size, textUpperBound(function.deploymentSetId()));
        size = add(size, textUpperBound(function.deploymentId()));
        size = add(size, textUpperBound(function.deploymentJarDigest()));
        size = add(size, textUpperBound(function.className()));
        size = add(size, 4L + textUpperBound(function.configurationCodecId()));
        size = add(size, textUpperBound(function.configurationDigest()));
        size = add(size, frameSize(function.configurationFrameUnsafe()));
        return add(size, 20L);
    }

    private static long addTexts(long size, List<String> values) {
        size = add(size, 4L);
        for (String value : values) size = add(size, textUpperBound(value));
        return size;
    }

    private static long textUpperBound(String value) {
        return 4L + 3L * value.length();
    }

    private static long frameSize(byte[] value) {
        return 4L + value.length;
    }

    private static long add(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", "envelope.body",
                    "portable plan resident estimate overflowed");
        }
    }

    private static byte[] sha256(byte[] bytes, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, offset, length);
            return digest.digest();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
