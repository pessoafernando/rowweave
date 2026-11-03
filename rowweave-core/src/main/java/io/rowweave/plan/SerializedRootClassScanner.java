package io.rowweave.plan;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bounded Java serialization grammar scanner for the two legacy durable core
 * Outputs. It resolves no class and executes no serialization callback.
 */
final class SerializedRootClassScanner {

    private static final int STREAM_MAGIC = 0xaced;
    private static final int STREAM_VERSION = 5;
    private static final int BASE_HANDLE = 0x7e0000;
    private static final int MAX_HANDLES = 4096;
    private static final int MAX_DEPTH = 64;

    private static final int TC_NULL = 0x70;
    private static final int TC_REFERENCE = 0x71;
    private static final int TC_CLASSDESC = 0x72;
    private static final int TC_OBJECT = 0x73;
    private static final int TC_STRING = 0x74;
    private static final int TC_ARRAY = 0x75;
    private static final int TC_CLASS = 0x76;
    private static final int TC_BLOCKDATA = 0x77;
    private static final int TC_ENDBLOCKDATA = 0x78;
    private static final int TC_RESET = 0x79;
    private static final int TC_BLOCKDATALONG = 0x7a;
    private static final int TC_EXCEPTION = 0x7b;
    private static final int TC_LONGSTRING = 0x7c;
    private static final int TC_PROXYCLASSDESC = 0x7d;
    private static final int TC_ENUM = 0x7e;

    private static final int SC_WRITE_METHOD = 0x01;
    private static final int SC_SERIALIZABLE = 0x02;
    private static final int SC_EXTERNALIZABLE = 0x04;

    private SerializedRootClassScanner() { }

    static void requireExactOutputRoot(byte[] bytes, int offset, int length) {
        exactOutputRoot(bytes, offset, length);
    }

    static String exactOutputRoot(byte[] bytes, int offset, int length) {
        Parser parser = new Parser(bytes, offset, length);
        ObjectValue root = parser.readRoot();
        String name = root.descriptor.name;
        if (!("io.rowweave.ManifestFileSink".equals(name)
                || "io.rowweave.DiscardSink".equals(name))) {
            throw new IllegalArgumentException(
                    "durable run requires crash-reconcilable output: " + name);
        }
        if (root.descriptor.serialVersionUid != 1L) throw rejected();
        parser.requireEnd();
        return name;
    }

    private static boolean allowedDescriptor(String name) {
        return "io.rowweave.ManifestFileSink".equals(name)
                || "io.rowweave.DiscardSink".equals(name)
                || "io.rowweave.DataSchema".equals(name)
                || "io.rowweave.Field".equals(name)
                || "io.rowweave.DataType".equals(name)
                || "java.lang.Enum".equals(name)
                || "[Lio.rowweave.Field;".equals(name)
                || "[B".equals(name);
    }

    private static IllegalArgumentException rejected() {
        return new IllegalArgumentException(
                "UNSUPPORTED_DURABLE_SINK_PROVIDER: legacy output graph rejected");
    }

    private static final class Parser {
        private final byte[] bytes;
        private final int limit;
        private int position;
        private int nextHandle = BASE_HANDLE;
        private final Map<Integer, Object> handles = new HashMap<>();

        private Parser(byte[] bytes, int offset, int length) {
            if (bytes == null || offset < 0 || length < 9
                    || offset > bytes.length - length) throw rejected();
            this.bytes = bytes;
            this.position = offset;
            this.limit = offset + length;
            if (u16() != STREAM_MAGIC || u16() != STREAM_VERSION) {
                throw rejected();
            }
        }

        private ObjectValue readRoot() {
            Object value = content(0, false);
            if (!(value instanceof ObjectValue)) throw rejected();
            return (ObjectValue) value;
        }

        private Object content(int depth, boolean annotation) {
            if (depth > MAX_DEPTH) throw rejected();
            int token = u8();
            switch (token) {
                case TC_NULL:
                    return NullValue.INSTANCE;
                case TC_REFERENCE:
                    return reference();
                case TC_STRING:
                    skipShortString();
                    return assign(new StringValue());
                case TC_LONGSTRING:
                    skipLongString();
                    return assign(new StringValue());
                case TC_OBJECT: {
                    Descriptor descriptor = classDesc(depth + 1);
                    if (descriptor == null) throw rejected();
                    ObjectValue object = assign(new ObjectValue(descriptor));
                    classData(descriptor, depth + 1);
                    return object;
                }
                case TC_ARRAY:
                    return array(depth + 1);
                case TC_ENUM: {
                    Descriptor descriptor = classDesc(depth + 1);
                    ObjectValue value = assign(new ObjectValue(descriptor));
                    Object constant = content(depth + 1, false);
                    if (!(constant instanceof StringValue)) throw rejected();
                    return value;
                }
                case TC_CLASS: {
                    Descriptor descriptor = classDesc(depth + 1);
                    return assign(new ClassValue(descriptor));
                }
                case TC_BLOCKDATA:
                    if (!annotation) throw rejected();
                    skip(u8());
                    return BlockValue.INSTANCE;
                case TC_BLOCKDATALONG:
                    if (!annotation) throw rejected();
                    skip(nonNegativeInt());
                    return BlockValue.INSTANCE;
                case TC_RESET:
                case TC_EXCEPTION:
                case TC_PROXYCLASSDESC:
                case TC_CLASSDESC:
                case TC_ENDBLOCKDATA:
                default:
                    throw rejected();
            }
        }

        private Object array(int depth) {
            Descriptor descriptor = classDesc(depth + 1);
            if (descriptor == null || !descriptor.name.startsWith("[")) {
                throw rejected();
            }
            ArrayValue array = assign(new ArrayValue(descriptor));
            int length = nonNegativeInt();
            if (length > 1_000_000) throw rejected();
            if ("[B".equals(descriptor.name)) {
                skip(length);
                return array;
            }
            if ("[Lio.rowweave.Field;".equals(descriptor.name)) {
                for (int i = 0; i < length; i++) content(depth + 1, false);
                return array;
            }
            throw rejected();
        }

        private Descriptor classDesc(int depth) {
            if (depth > MAX_DEPTH) throw rejected();
            int token = u8();
            if (token == TC_NULL) return null;
            if (token == TC_REFERENCE) {
                Object value = reference();
                if (!(value instanceof Descriptor)) throw rejected();
                return (Descriptor) value;
            }
            if (token == TC_PROXYCLASSDESC || token != TC_CLASSDESC) {
                throw rejected();
            }
            String name = asciiUtf();
            long serialVersionUid = i64();
            boolean rootDescriptor = handles.isEmpty();
            if (!rootDescriptor && !allowedDescriptor(name)) throw rejected();
            Descriptor descriptor = assign(new Descriptor(name, serialVersionUid));
            descriptor.flags = u8();
            if ((descriptor.flags & SC_EXTERNALIZABLE) != 0
                    || (descriptor.flags & SC_SERIALIZABLE) == 0) {
                throw rejected();
            }
            int fieldCount = u16();
            if (fieldCount > 64) throw rejected();
            for (int i = 0; i < fieldCount; i++) {
                char type = (char) u8();
                asciiUtf();
                if (type == 'L' || type == '[') {
                    Object typeName = content(depth + 1, false);
                    if (!(typeName instanceof StringValue)) throw rejected();
                } else if ("BCDFIJSZ".indexOf(type) < 0) {
                    throw rejected();
                }
                descriptor.fields.add(type);
            }
            annotations(depth + 1);
            descriptor.parent = classDesc(depth + 1);
            return descriptor;
        }

        private void classData(Descriptor descriptor, int depth) {
            if (descriptor.parent != null) classData(descriptor.parent, depth + 1);
            for (char type : descriptor.fields) {
                switch (type) {
                    case 'B': case 'Z': skip(1); break;
                    case 'C': case 'S': skip(2); break;
                    case 'F': case 'I': skip(4); break;
                    case 'D': case 'J': skip(8); break;
                    case 'L': case '[': content(depth + 1, false); break;
                    default: throw rejected();
                }
            }
            if ((descriptor.flags & SC_WRITE_METHOD) != 0) {
                annotations(depth + 1);
            }
        }

        private void annotations(int depth) {
            while (true) {
                require(1);
                if ((bytes[position] & 0xff) == TC_ENDBLOCKDATA) {
                    position++;
                    return;
                }
                content(depth + 1, true);
            }
        }

        private Object reference() {
            Object value = handles.get(i32());
            if (value == null) throw rejected();
            return value;
        }

        private <T> T assign(T value) {
            if (handles.size() >= MAX_HANDLES) throw rejected();
            handles.put(nextHandle++, value);
            return value;
        }

        private void skipShortString() { skip(u16()); }

        private void skipLongString() {
            long length = i64();
            if (length < 0L || length > Integer.MAX_VALUE) throw rejected();
            skip((int) length);
        }

        private String asciiUtf() {
            int length = u16();
            if (length < 1 || length > 256) throw rejected();
            require(length);
            for (int i = 0; i < length; i++) {
                int value = bytes[position + i] & 0xff;
                if (value == 0 || value > 0x7f) throw rejected();
            }
            String value = new String(
                    bytes, position, length, StandardCharsets.US_ASCII);
            position += length;
            return value;
        }

        private int nonNegativeInt() {
            int value = i32();
            if (value < 0) throw rejected();
            return value;
        }

        private int u8() {
            require(1);
            return bytes[position++] & 0xff;
        }

        private int u16() {
            require(2);
            int value = ((bytes[position] & 0xff) << 8)
                    | (bytes[position + 1] & 0xff);
            position += 2;
            return value;
        }

        private int i32() {
            require(4);
            int value = ((bytes[position] & 0xff) << 24)
                    | ((bytes[position + 1] & 0xff) << 16)
                    | ((bytes[position + 2] & 0xff) << 8)
                    | (bytes[position + 3] & 0xff);
            position += 4;
            return value;
        }

        private long i64() {
            return ((long) i32() << 32) | (i32() & 0xffffffffL);
        }

        private void skip(int length) {
            require(length);
            position += length;
        }

        private void require(int length) {
            if (length < 0 || limit - position < length) throw rejected();
        }

        private void requireEnd() {
            if (position != limit) throw rejected();
        }
    }

    private static final class Descriptor {
        private final String name;
        private final long serialVersionUid;
        private int flags;
        private Descriptor parent;
        private final List<Character> fields = new ArrayList<>();

        private Descriptor(String name, long serialVersionUid) {
            this.name = name;
            this.serialVersionUid = serialVersionUid;
        }
    }

    private static final class ObjectValue {
        private final Descriptor descriptor;
        private ObjectValue(Descriptor descriptor) { this.descriptor = descriptor; }
    }
    private static final class ArrayValue {
        @SuppressWarnings("unused") private final Descriptor descriptor;
        private ArrayValue(Descriptor descriptor) { this.descriptor = descriptor; }
    }
    private static final class ClassValue {
        @SuppressWarnings("unused") private final Descriptor descriptor;
        private ClassValue(Descriptor descriptor) { this.descriptor = descriptor; }
    }
    private static final class StringValue { }
    private enum NullValue { INSTANCE }
    private enum BlockValue { INSTANCE }
}
