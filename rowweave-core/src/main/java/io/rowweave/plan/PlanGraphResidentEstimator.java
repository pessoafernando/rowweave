package io.rowweave.plan;

/**
 * Scans a PlanGraph v1 frame without allocating, and computes the conservative resident charge that must
 * be held before materialization. Meaning and whitelist checks stay with the trusted scanner; this class
 * supplies only the safe cursor and the accounting formula.
 */
public final class PlanGraphResidentEstimator {
    private static final int MAX_NODES = 100_000;
    private static final int MAX_COUNT = 1_000_000;

    private PlanGraphResidentEstimator() { }

    public static long estimate(byte[] bytes) {
        if (bytes == null) throw new IllegalArgumentException("plan bytes are required");
        Cursor in = new Cursor(bytes);
        int nodes = in.count("nodes", MAX_NODES);
        long fieldsAndParams = 0L;
        long utf8Bytes = 0L;
        long objectBlobBytes = 0L;
        for (int node = 0; node < nodes; node++) {
            in.readInt("node.kind");
            utf8Bytes = add(utf8Bytes, in.utf8("node.typeId"));
            int fields = in.count("node.fields", MAX_COUNT);
            fieldsAndParams = add(fieldsAndParams, fields);
            for (int field = 0; field < fields; field++) {
                utf8Bytes = add(utf8Bytes, in.utf8("field.name"));
                utf8Bytes = add(utf8Bytes, in.utf8("field.alias"));
                in.readInt("field.type");
            }
            int children = in.count("node.children", MAX_COUNT);
            in.skipInts(children, "node.children");
            int params = in.count("node.params", MAX_COUNT);
            fieldsAndParams = add(fieldsAndParams, params);
            for (int param = 0; param < params; param++) {
                utf8Bytes = add(utf8Bytes, in.utf8("param.key"));
                utf8Bytes = add(utf8Bytes, in.utf8("param.value"));
            }
            int listParams = in.count("node.listParams", MAX_COUNT);
            fieldsAndParams = add(fieldsAndParams, listParams);
            for (int param = 0; param < listParams; param++) {
                utf8Bytes = add(utf8Bytes, in.utf8("listParam.key"));
                int values = in.count("listParam.values", MAX_COUNT);
                fieldsAndParams = add(fieldsAndParams, values);
                for (int value = 0; value < values; value++) {
                    utf8Bytes = add(utf8Bytes, in.utf8("listParam.value"));
                }
            }
            int trailer = in.readInt("node.payloadTag");
            if (trailer >= 0) {
                if (trailer > MAX_COUNT) throw new IllegalArgumentException("input count out of bounds");
                for (int input = 0; input < trailer; input++) {
                    objectBlobBytes = add(objectBlobBytes, in.blob("source.input"));
                }
            } else if (trailer == -2 || trailer == -3) {
                objectBlobBytes = add(objectBlobBytes, in.blob("node.object"));
            } else if (trailer == -4) {
                utf8Bytes = add(utf8Bytes, in.blob("sink.providerConfig"));
            } else if (trailer != -1) {
                throw new IllegalArgumentException("unknown plan payload tag: " + trailer);
            }
        }
        if (in.remaining() == Integer.BYTES) in.readInt("parallelism");
        if (in.remaining() != 0) throw new IllegalArgumentException("plan graph has trailing bytes");
        return add(add(multiply(512L, nodes), multiply(256L, fieldsAndParams)),
                add(multiply(3L, utf8Bytes), multiply(3L, objectBlobBytes)));
    }

    private static long add(long left, long right) {
        if (right < 0L || left > Long.MAX_VALUE - right) {
            throw new IllegalArgumentException("plan resident estimate overflow");
        }
        return left + right;
    }

    private static long multiply(long left, long right) {
        if (left < 0L || right < 0L || (left != 0L && right > Long.MAX_VALUE / left)) {
            throw new IllegalArgumentException("plan resident estimate overflow");
        }
        return left * right;
    }

    private static final class Cursor {
        private final byte[] bytes;
        private int position;

        private Cursor(byte[] bytes) { this.bytes = bytes; }

        private int readInt(String field) {
            require(Integer.BYTES, field);
            int value = ((bytes[position] & 0xff) << 24)
                    | ((bytes[position + 1] & 0xff) << 16)
                    | ((bytes[position + 2] & 0xff) << 8)
                    | (bytes[position + 3] & 0xff);
            position += Integer.BYTES;
            return value;
        }

        private int count(String field, int max) {
            int value = readInt(field);
            if (value < 0 || value > max) {
                throw new IllegalArgumentException(field + " count out of bounds: " + value);
            }
            return value;
        }

        private int utf8(String field) {
            int length = count(field + ".length", bytes.length);
            require(length, field);
            position += length;
            return length;
        }

        private int blob(String field) {
            int length = count(field + ".length", bytes.length);
            require(length, field);
            position += length;
            return length;
        }

        private void skipInts(int count, String field) {
            long bytesToSkip = (long) count * Integer.BYTES;
            if (bytesToSkip > Integer.MAX_VALUE) throw new IllegalArgumentException(field + " is too large");
            require((int) bytesToSkip, field);
            position += (int) bytesToSkip;
        }

        private void require(int length, String field) {
            if (length < 0 || bytes.length - position < length) {
                throw new IllegalArgumentException(field + " is truncated");
            }
        }

        private int remaining() { return bytes.length - position; }
    }
}
