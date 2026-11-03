package io.rowweave.plan;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.DataType;
import io.rowweave.DurableSinkConfigurationEnvelope;
import io.rowweave.DurableSinkConfigurationEnvelopeCodec;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.commit.SchemaFingerprint;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.LongConsumer;

/** Complete structural preflight of a PlanGraph v1 frame, without instantiating any user object. */
public final class PlanGraphEnvelopeScanner {
    public static final int MAX_PLAN_BYTES = 64 * 1024 * 1024;
    public static final int MAX_NODES = 100_000;
    public static final int MAX_PARALLELISM = 65_536;
    public static final int MAX_FIELDS = 4_096;
    public static final int MAX_PARAMS = 4_096;
    public static final int MAX_LIST_VALUES = 4_096;
    public static final int MAX_INPUTS = 4_096;
    public static final int MAX_UTF8_BYTES = 64 * 1024;
    public static final int MAX_OBJECT_BLOB_BYTES = 16 * 1024 * 1024;
    private static final PlanNode.Kind[] KINDS = PlanNode.Kind.values();
    private static final int DATA_TYPE_COUNT = DataType.values().length;

    private PlanGraphEnvelopeScanner() {
    }

    public static Scan scan(byte[] bytes) {
        return scan(bytes, ignored -> { });
    }

    /** The temporary indexes a scan allocates must be inside the request resident lease, reserved before allocation. */
    public static Scan scan(byte[] bytes, LongConsumer reservation) {
        return scan(bytes, 0, bytes == null ? 0 : bytes.length,
                reservation, false);
    }

    public static Scan scan(byte[] bytes, int offset, int length, LongConsumer reservation) {
        return scan(bytes, offset, length, reservation, false);
    }

    /** Durable preflight additionally restricts legacy -2 roots before OIS. */
    public static Scan scanDurable(byte[] bytes, LongConsumer reservation) {
        return scan(bytes, 0, bytes == null ? 0 : bytes.length,
                reservation, true);
    }

    public static Scan scanDurable(
            byte[] bytes, int offset, int length, LongConsumer reservation) {
        return scan(bytes, offset, length, reservation, true);
    }

    public static void requireExactDurableLegacyOutputRoot(
            byte[] bytes, int offset, int length) {
        SerializedRootClassScanner.requireExactOutputRoot(
                bytes, offset, length);
    }

    private static Scan scan(byte[] bytes, int offset, int length,
                             LongConsumer reservation, boolean durable) {
        if (reservation == null) {
            throw new IllegalArgumentException("reservation is required");
        }
        if (bytes == null || offset < 0 || length < 0 || offset > bytes.length - length
                || length > MAX_PLAN_BYTES) {
            throw new IllegalArgumentException("plan bytes length out of bounds");
        }
        Cursor in = new Cursor(bytes, offset, offset + length);
        int nodeCount = in.count("node count", 1, MAX_NODES);
        long structural = 0L;
        long utf8 = 0L;
        long objectBlobs = 0L;
        long providerFrames = 0L;
        List<ProviderSinkEnvelope> providerSinks = new ArrayList<>();
        List<DurableSinkEnvelope> durableSinks = new ArrayList<>();
        List<OperatorDescriptor> operatorDescriptors = new ArrayList<>(nodeCount);
        DataSchema rootSchema = null;
        for (int node = 0; node < nodeCount; node++) {
            int kindOrdinal = in.readInt("node kind");
            if (kindOrdinal < 0 || kindOrdinal >= KINDS.length) {
                throw new IllegalArgumentException("node kind ordinal out of bounds: " + kindOrdinal);
            }
            PlanNode.Kind kind = KINDS[kindOrdinal];
            long typeIdSlice = in.utf8Slice("node typeId");
            utf8 = add(utf8, sliceLength(typeIdSlice));
            String typeId = text(bytes, typeIdSlice);
            int fields = in.count("field count", 0, MAX_FIELDS);
            structural = add(structural, fields);
            Field[] rowFields = new Field[fields];
            for (int field = 0; field < fields; field++) {
                long nameSlice = in.utf8Slice("field name");
                long aliasSlice = in.utf8Slice("field alias");
                utf8 = add(utf8, sliceLength(nameSlice));
                utf8 = add(utf8, sliceLength(aliasSlice));
                int dataType = in.readInt("field data type");
                if (dataType < 0 || dataType >= DATA_TYPE_COUNT) {
                    throw new IllegalArgumentException(
                            "field data type ordinal out of bounds: " + dataType);
                }
                rowFields[field] = new Field(text(bytes, nameSlice),
                        text(bytes, aliasSlice), DataType.values()[dataType]);
            }
            int children = in.count("child count", 0, 2);
            int expectedChildren = kind == PlanNode.Kind.SOURCE ? 0
                    : kind == PlanNode.Kind.BINARY ? 2 : 1;
            if (children != expectedChildren) {
                throw new IllegalArgumentException("child count mismatch for " + kind
                        + ": expected=" + expectedChildren + " actual=" + children);
            }
            int[] childIndices = new int[children];
            for (int child = 0; child < children; child++) {
                int index = in.readInt("child index");
                if (index < 0 || index >= node) {
                    throw new IllegalArgumentException(
                        "child must topologically precede node " + node + ": " + index);
                }
                childIndices[child] = index;
            }

            int params = in.count("param count", 0, MAX_PARAMS);
            structural = add(structural, params);
            reserveLongArray(reservation, params);
            long[] paramKeys = new long[params];
            String operatorId = null;
            for (int param = 0; param < params; param++) {
                long key = in.utf8Slice("param key");
                rejectDuplicate(bytes, paramKeys, param, key, "duplicate param key");
                paramKeys[param] = key;
                utf8 = add(utf8, sliceLength(key));
                long value = in.utf8Slice("param value");
                utf8 = add(utf8, sliceLength(value));
                if (sliceEqualsAscii(bytes, key, PlanNode.OPERATOR_UID_PARAM)) {
                    operatorId = text(bytes, value);
                }
            }

            int listParams = in.count("list param count", 0, MAX_PARAMS);
            structural = add(structural, listParams);
            reserveLongArray(reservation, listParams);
            long[] listKeys = new long[listParams];
            long sinkDescriptorSlice = -1L;
            for (int param = 0; param < listParams; param++) {
                long key = in.utf8Slice("list param key");
                rejectDuplicate(bytes, listKeys, param, key, "duplicate list param key");
                listKeys[param] = key;
                utf8 = add(utf8, sliceLength(key));
                int values = in.count("list param value count", 0, MAX_LIST_VALUES);
                structural = add(structural, values);
                for (int value = 0; value < values; value++) {
                    long valueSlice = in.utf8Slice("list param value");
                    utf8 = add(utf8, sliceLength(valueSlice));
                    if (kind == PlanNode.Kind.SINK
                            && sliceEqualsAscii(bytes, key,
                            PlanNode.SINK_DESCRIPTOR_PARAM)) {
                        if (values != 1 || value != 0
                                || sinkDescriptorSlice != -1L) {
                            throw new IllegalArgumentException(
                                    "provider sink descriptor metadata is invalid");
                        }
                        sinkDescriptorSlice = valueSlice;
                    }
                }
            }

            int trailer = in.readInt("node payload tag");
            if (kind == PlanNode.Kind.SOURCE) {
                if (trailer < 0 || trailer > MAX_INPUTS) {
                    throw new IllegalArgumentException("input count out of bounds: " + trailer);
                }
                for (int input = 0; input < trailer; input++) {
                    objectBlobs = add(objectBlobs, in.blob("source object blob"));
                }
            } else if (trailer == -2) {
                if (kind != PlanNode.Kind.SINK) {
                    throw new IllegalArgumentException("output object blob requires SINK node");
                }
                long outputSlice = in.blobSlice(
                        "output object blob", MAX_OBJECT_BLOB_BYTES);
                objectBlobs = add(objectBlobs, sliceLength(outputSlice));
                if (durable) {
                    if (sinkDescriptorSlice == -1L) {
                        throw new IllegalArgumentException(
                                "durable legacy Sink descriptor is missing");
                    }
                    String root = SerializedRootClassScanner.exactOutputRoot(
                            bytes, sliceOffset(outputSlice),
                            sliceLength(outputSlice));
                    durableSinks.add(new DurableSinkEnvelope(
                            node, decodeDescriptor(bytes, sinkDescriptorSlice,
                            reservation), root.equals(
                            "io.rowweave.DiscardSink")
                            ? DurableSinkEnvelope.Kind.DISCARD
                            : DurableSinkEnvelope.Kind.MANIFEST, null));
                }
            } else if (trailer == -3) {
                if (kind == PlanNode.Kind.SINK) {
                    throw new IllegalArgumentException("function object blob is invalid for SINK");
                }
                objectBlobs = add(objectBlobs, in.blob("function object blob"));
            } else if (trailer == -4) {
                if (kind != PlanNode.Kind.SINK || sinkDescriptorSlice == -1L) {
                    throw new IllegalArgumentException(
                            "provider configuration requires described SINK node");
                }
                long frameSlice = in.blobSlice(
                        "provider configuration frame",
                        DurableSinkConfigurationEnvelopeCodec.MAX_FRAME_BYTES);
                int frameLength = sliceLength(frameSlice);
                providerFrames = add(providerFrames, frameLength);
                reservation.accept(multiply(6L, frameLength));
                DurableSinkConfigurationEnvelope envelope =
                        DurableSinkConfigurationEnvelopeCodec.decode(
                                bytes, sliceOffset(frameSlice), frameLength);
                BatchSinkDescriptor descriptor = decodeDescriptor(
                        bytes, sinkDescriptorSlice, reservation);
                providerSinks.add(new ProviderSinkEnvelope(
                        node, descriptor, envelope));
                if (durable) {
                    durableSinks.add(new DurableSinkEnvelope(
                            node, descriptor,
                            DurableSinkEnvelope.Kind.PROVIDER, envelope));
                }
            } else if (trailer != -1) {
                throw new IllegalArgumentException("unknown plan payload tag: " + trailer);
            } else if (durable && kind == PlanNode.Kind.SINK) {
                throw new IllegalArgumentException(
                        "UNSUPPORTED_DURABLE_SINK_PROVIDER: Sink payload is missing");
            }
            List<String> inputOperatorIds = new ArrayList<>(children);
            for (int child : childIndices) {
                inputOperatorIds.add(
                        operatorDescriptors.get(child).operatorId());
            }
            rootSchema = new DataSchema(rowFields);
            operatorDescriptors.add(new OperatorDescriptor(
                    operatorId == null
                            ? String.format(Locale.ROOT, "legacy-op-%06d", node)
                            : operatorId,
                    operatorKind(kind), typeId,
                    SchemaFingerprint.of(rootSchema),
                    inputOperatorIds));
        }
        int parallelism = in.remaining() == Integer.BYTES
                ? in.readInt("parallelism") : 1;
        if (parallelism < 1 || parallelism > MAX_PARALLELISM) {
            throw new IllegalArgumentException("parallelism out of bounds: " + parallelism);
        }
        if (in.remaining() != 0) {
            throw new IllegalArgumentException("plan graph has trailing bytes");
        }
        long resident = add(add(multiply(512L, nodeCount), multiply(256L, structural)),
                add(multiply(3L, utf8), multiply(3L, providerFrames)));
        return new Scan(nodeCount, parallelism, resident, objectBlobs,
                providerSinks, durableSinks, operatorDescriptors, rootSchema);
    }

    private static String text(byte[] bytes, long slice) {
        return new String(bytes, sliceOffset(slice), sliceLength(slice),
                StandardCharsets.UTF_8);
    }

    private static OperatorDescriptor.Kind operatorKind(PlanNode.Kind kind) {
        switch (kind) {
            case SOURCE: return OperatorDescriptor.Kind.SOURCE;
            case UNARY: return OperatorDescriptor.Kind.UNARY;
            case BINARY: return OperatorDescriptor.Kind.BINARY;
            case SINK: return OperatorDescriptor.Kind.SINK;
            default: throw new AssertionError("unknown PlanNode kind: " + kind);
        }
    }

    private static void reserveLongArray(LongConsumer reservation, int length) {
        reservation.accept(align8(add(16L, multiply(Long.BYTES, length))));
    }

    private static void rejectDuplicate(byte[] bytes, long[] prior, int count, long candidate,
                                        String message) {
        for (int i = 0; i < count; i++) {
            if (sliceEquals(bytes, prior[i], candidate)) {
                throw new IllegalArgumentException(message);
            }
        }
    }

    private static boolean sliceEquals(byte[] bytes, long left, long right) {
        int leftLength = sliceLength(left);
        if (leftLength != sliceLength(right)) return false;
        int leftOffset = (int) (left >>> 32);
        int rightOffset = (int) (right >>> 32);
        for (int i = 0; i < leftLength; i++) {
            if (bytes[leftOffset + i] != bytes[rightOffset + i]) return false;
        }
        return true;
    }

    private static boolean sliceEqualsAscii(
            byte[] bytes, long slice, String expected) {
        int length = sliceLength(slice);
        if (length != expected.length()) return false;
        int offset = sliceOffset(slice);
        for (int i = 0; i < length; i++) {
            if ((bytes[offset + i] & 0xff) != expected.charAt(i)) return false;
        }
        return true;
    }

    private static BatchSinkDescriptor decodeDescriptor(
            byte[] bytes, long slice, LongConsumer reservation) {
        int length = sliceLength(slice);
        reservation.accept(multiply(3L, length));
        String encoded = new String(
                bytes, sliceOffset(slice), length, StandardCharsets.UTF_8);
        if (encoded.isEmpty() || encoded.indexOf('=') >= 0) {
            throw new IllegalArgumentException(
                    "provider sink descriptor metadata is not canonical");
        }
        try {
            reservation.accept(add(16L, length));
            byte[] frame = Base64.getUrlDecoder().decode(encoded);
            if (!Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(frame).equals(encoded)) {
                throw new IllegalArgumentException(
                        "provider sink descriptor metadata is not canonical");
            }
            return BatchSinkDescriptorCodec.decode(frame);
        } catch (IllegalArgumentException malformed) {
            throw new IllegalArgumentException(
                    "provider sink descriptor metadata is not canonical", malformed);
        }
    }

    private static int sliceLength(long slice) { return (int) slice; }
    private static int sliceOffset(long slice) { return (int) (slice >>> 32); }

    private static long add(long left, long right) {
        if (right < 0L || left > Long.MAX_VALUE - right) {
            throw new IllegalArgumentException("plan scan size overflow");
        }
        return left + right;
    }

    private static long multiply(long left, long right) {
        if (left < 0L || right < 0L || (left != 0L && right > Long.MAX_VALUE / left)) {
            throw new IllegalArgumentException("plan scan size overflow");
        }
        return left * right;
    }

    private static long align8(long value) {
        return add(value, 7L) & ~7L;
    }

    public static final class Scan {
        private final int nodeCount;
        private final int parallelism;
        private final long residentBytes;
        private final long objectBlobBytes;
        private final List<ProviderSinkEnvelope> providerSinkEnvelopes;
        private final List<DurableSinkEnvelope> durableSinkEnvelopes;
        private final List<OperatorDescriptor> operatorDescriptors;
        private final DataSchema rootSchema;

        private Scan(int nodeCount, int parallelism, long residentBytes,
                     long objectBlobBytes,
                     List<ProviderSinkEnvelope> providerSinkEnvelopes,
                     List<DurableSinkEnvelope> durableSinkEnvelopes,
                     List<OperatorDescriptor> operatorDescriptors,
                     DataSchema rootSchema) {
            this.nodeCount = nodeCount;
            this.parallelism = parallelism;
            this.residentBytes = residentBytes;
            this.objectBlobBytes = objectBlobBytes;
            this.providerSinkEnvelopes = Collections.unmodifiableList(
                    providerSinkEnvelopes);
            this.durableSinkEnvelopes = Collections.unmodifiableList(
                    durableSinkEnvelopes);
            this.operatorDescriptors = Collections.unmodifiableList(
                    operatorDescriptors);
            this.rootSchema = rootSchema;
        }

        public int nodeCount() { return nodeCount; }
        public int parallelism() { return parallelism; }
        public long residentBytes() { return residentBytes; }
        public long objectBlobBytes() { return objectBlobBytes; }
        public List<ProviderSinkEnvelope> providerSinkEnvelopes() {
            return providerSinkEnvelopes;
        }
        public List<DurableSinkEnvelope> durableSinkEnvelopes() {
            return durableSinkEnvelopes;
        }
        public List<OperatorDescriptor> operatorDescriptors() {
            return operatorDescriptors;
        }
        public DataSchema rootSchema() { return rootSchema; }
    }

    public static final class DurableSinkEnvelope {
        public enum Kind { DISCARD, MANIFEST, PROVIDER }

        private final int nodeIndex;
        private final BatchSinkDescriptor descriptor;
        private final Kind kind;
        private final DurableSinkConfigurationEnvelope configuration;

        private DurableSinkEnvelope(
                int nodeIndex, BatchSinkDescriptor descriptor, Kind kind,
                DurableSinkConfigurationEnvelope configuration) {
            this.nodeIndex = nodeIndex;
            this.descriptor = descriptor;
            this.kind = kind;
            this.configuration = configuration;
        }

        public int nodeIndex() { return nodeIndex; }
        public BatchSinkDescriptor descriptor() { return descriptor; }
        public Kind kind() { return kind; }
        public io.rowweave.DurableSinkCodecDescriptor configurationCodec() {
            return configuration == null ? null
                    : configuration.configurationCodec();
        }
        public byte[] configurationBytes() {
            return configuration == null ? null
                    : configuration.configurationBytes();
        }
    }

    public static final class ProviderSinkEnvelope {
        private final int nodeIndex;
        private final BatchSinkDescriptor descriptor;
        private final DurableSinkConfigurationEnvelope configuration;

        private ProviderSinkEnvelope(
                int nodeIndex, BatchSinkDescriptor descriptor,
                DurableSinkConfigurationEnvelope configuration) {
            this.nodeIndex = nodeIndex;
            this.descriptor = descriptor;
            this.configuration = configuration;
        }

        public int nodeIndex() { return nodeIndex; }
        public BatchSinkDescriptor descriptor() { return descriptor; }
        public io.rowweave.DurableSinkCodecDescriptor configurationCodec() {
            return configuration.configurationCodec();
        }
        public byte[] configurationBytes() {
            return configuration.configurationBytes();
        }
    }

    private static final class Cursor {
        private final byte[] bytes;
        private final int limit;
        private int position;

        private Cursor(byte[] bytes, int position, int limit) {
            this.bytes = bytes;
            this.position = position;
            this.limit = limit;
        }

        private int readInt(String field) {
            require(Integer.BYTES, field);
            int value = ((bytes[position] & 0xff) << 24)
                    | ((bytes[position + 1] & 0xff) << 16)
                    | ((bytes[position + 2] & 0xff) << 8)
                    | (bytes[position + 3] & 0xff);
            position += Integer.BYTES;
            return value;
        }

        private int count(String field, int min, int max) {
            int value = readInt(field);
            if (value < min || value > max) {
                throw new IllegalArgumentException(field + " out of bounds: " + value);
            }
            return value;
        }

        private int utf8(String field) { return sliceLength(utf8Slice(field)); }

        private long utf8Slice(String field) {
            int length = count(field + " length", 0, MAX_UTF8_BYTES);
            require(length, field);
            int offset = position;
            validateUtf8(bytes, offset, length, field);
            position += length;
            return ((long) offset << 32) | (length & 0xffffffffL);
        }

        private int blob(String field) {
            return sliceLength(blobSlice(field, MAX_OBJECT_BLOB_BYTES));
        }

        private long blobSlice(String field, int maximumBytes) {
            int length = readInt(field + " length");
            if (length < 0 || length > maximumBytes) {
                throw new IllegalArgumentException(field + " length out of bounds: " + length);
            }
            require(length, field);
            int offset = position;
            position += length;
            return ((long) offset << 32) | (length & 0xffffffffL);
        }

        private void require(int length, String field) {
            if (length < 0 || limit - position < length) {
                throw new IllegalArgumentException(field + " is truncated");
            }
        }

        private int remaining() { return limit - position; }
    }

    private static void validateUtf8(byte[] bytes, int offset, int length, String field) {
        int end = offset + length;
        for (int i = offset; i < end; ) {
            int first = bytes[i++] & 0xff;
            if (first <= 0x7f) continue;
            int needed;
            int codePoint;
            int minimum;
            if (first >= 0xc2 && first <= 0xdf) {
                needed = 1; codePoint = first & 0x1f; minimum = 0x80;
            } else if (first >= 0xe0 && first <= 0xef) {
                needed = 2; codePoint = first & 0x0f; minimum = 0x800;
            } else if (first >= 0xf0 && first <= 0xf4) {
                needed = 3; codePoint = first & 0x07; minimum = 0x10000;
            } else {
                throw new IllegalArgumentException(field + " contains malformed UTF-8");
            }
            if (end - i < needed) {
                throw new IllegalArgumentException(field + " contains truncated UTF-8");
            }
            for (int n = 0; n < needed; n++) {
                int next = bytes[i++] & 0xff;
                if ((next & 0xc0) != 0x80) {
                    throw new IllegalArgumentException(field + " contains malformed UTF-8");
                }
                codePoint = (codePoint << 6) | (next & 0x3f);
            }
            if (codePoint < minimum || codePoint > 0x10ffff
                    || (codePoint >= 0xd800 && codePoint <= 0xdfff)) {
                throw new IllegalArgumentException(field + " contains malformed UTF-8");
            }
        }
    }
}
