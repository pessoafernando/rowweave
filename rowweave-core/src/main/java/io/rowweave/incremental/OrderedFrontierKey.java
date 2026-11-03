package io.rowweave.incremental;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Core-owned order-preserving key used only by TIMESTAMP_KEY_V1 frontiers.
 * This is deliberately independent of RowSerializer identity encoding.
 */
public final class OrderedFrontierKey {
    public static final int MAX_COMPONENTS = 16;
    public static final int MAX_COMPONENT_BYTES = 1024;
    public static final int MAX_COMPONENT_CODE_UNITS = MAX_COMPONENT_BYTES;
    public static final int MAX_ORDER_BYTES = 4096;

    private static final int MAGIC_0 = 0x4f;
    private static final int MAGIC_1 = 0x4b;
    private static final int VERSION = 1;
    private static final int TYPE_INT64 = 0x10;
    private static final int TYPE_UTF8 = 0x20;
    private static final int TYPE_BYTES = 0x30;

    private final byte[] orderBytes;

    private OrderedFrontierKey(byte[] orderBytes) {
        this.orderBytes = orderBytes;
    }

    public static Component int64(long value) {
        return new Component(TYPE_INT64, value, null);
    }

    public static Component utf8(String value) {
        if (value == null) throw new IllegalArgumentException("ordered UTF-8 key is required");
        if (value.length() > MAX_COMPONENT_CODE_UNITS) {
            throw new IllegalArgumentException("ordered UTF-8 key exceeds code-unit limit");
        }
        if (!Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw new IllegalArgumentException("ordered UTF-8 key must be NFC text");
        }
        byte[] bytes = strictUtf8(value);
        requireComponentSize(bytes.length);
        return new Component(TYPE_UTF8, 0L, bytes);
    }

    public static Component bytes(byte[] value) {
        if (value == null) throw new IllegalArgumentException("ordered byte key is required");
        requireComponentSize(value.length);
        return new Component(TYPE_BYTES, 0L, value.clone());
    }

    public static OrderedFrontierKey of(Component... values) {
        if (values == null || values.length == 0 || values.length > MAX_COMPONENTS) {
            throw new IllegalArgumentException("ordered key component count is invalid");
        }
        List<Component> components = new ArrayList<>(values.length);
        for (Component value : values) {
            if (value == null) throw new IllegalArgumentException("ordered key component is required");
            components.add(value.copy());
        }
        byte[] encoded = encode(components);
        return new OrderedFrontierKey(encoded);
    }

    public static OrderedFrontierKey decode(byte[] encoded) {
        if (encoded == null || encoded.length < 5 || encoded.length > MAX_ORDER_BYTES) {
            throw new IllegalArgumentException("ordered key frame length is invalid");
        }
        Cursor cursor = new Cursor(encoded);
        if (cursor.u8() != MAGIC_0 || cursor.u8() != MAGIC_1 || cursor.u8() != VERSION) {
            throw new IllegalArgumentException("ordered key header is invalid");
        }
        List<Component> values = new ArrayList<>();
        while (true) {
            int type = cursor.u8();
            if (type == 0) break;
            if (values.size() >= MAX_COMPONENTS) {
                throw new IllegalArgumentException("ordered key has too many components");
            }
            if (type == TYPE_INT64) {
                long shifted = cursor.i64();
                values.add(int64(shifted ^ Long.MIN_VALUE));
            } else if (type == TYPE_UTF8 || type == TYPE_BYTES) {
                byte[] bytes = cursor.escapedBytes();
                requireComponentSize(bytes.length);
                if (type == TYPE_UTF8) {
                    String text = decodeStrictUtf8(bytes);
                    Component component = utf8(text);
                    if (!Arrays.equals(bytes, component.bytes)) {
                        throw new IllegalArgumentException("ordered UTF-8 key is not canonical");
                    }
                    values.add(component);
                } else {
                    values.add(bytes(bytes));
                }
            } else {
                throw new IllegalArgumentException("unknown ordered key component type");
            }
        }
        if (!cursor.exhausted() || values.isEmpty()) {
            throw new IllegalArgumentException("ordered key frame is malformed");
        }
        OrderedFrontierKey decoded = of(values.toArray(new Component[values.size()]));
        if (!Arrays.equals(encoded, decoded.orderBytes)) {
            throw new IllegalArgumentException("ordered key frame is not canonical");
        }
        return decoded;
    }

    public byte[] canonicalOrderBytes() {
        return orderBytes.clone();
    }

    public static int compare(OrderedFrontierKey left, OrderedFrontierKey right) {
        if (left == null || right == null) {
            throw new IllegalArgumentException("ordered keys are required");
        }
        return unsignedLexicographic(left.orderBytes, right.orderBytes);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof OrderedFrontierKey
                && Arrays.equals(orderBytes, ((OrderedFrontierKey) other).orderBytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(orderBytes);
    }

    private static byte[] encode(List<Component> values) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(MAGIC_0);
        out.write(MAGIC_1);
        out.write(VERSION);
        for (Component value : values) {
            out.write(value.type);
            if (value.type == TYPE_INT64) {
                long shifted = value.longValue ^ Long.MIN_VALUE;
                for (int shift = 56; shift >= 0; shift -= 8) {
                    out.write((int) (shifted >>> shift) & 0xff);
                }
            } else {
                for (byte b : value.bytes) {
                    int unsigned = b & 0xff;
                    if (unsigned == 0) {
                        out.write(0);
                        out.write(0xff);
                    } else {
                        out.write(unsigned);
                    }
                }
                out.write(0);
                out.write(0);
            }
            if (out.size() >= MAX_ORDER_BYTES) {
                throw new IllegalArgumentException("ordered key frame exceeds limit");
            }
        }
        out.write(0);
        byte[] encoded = out.toByteArray();
        if (encoded.length > MAX_ORDER_BYTES) {
            throw new IllegalArgumentException("ordered key frame exceeds limit");
        }
        return encoded;
    }

    private static int unsignedLexicographic(byte[] left, byte[] right) {
        int size = Math.min(left.length, right.length);
        for (int i = 0; i < size; i++) {
            int compared = Integer.compare(left[i] & 0xff, right[i] & 0xff);
            if (compared != 0) return compared;
        }
        return Integer.compare(left.length, right.length);
    }

    private static void requireComponentSize(int size) {
        if (size > MAX_COMPONENT_BYTES) {
            throw new IllegalArgumentException("ordered key component exceeds limit");
        }
    }

    private static byte[] strictUtf8(String value) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException("ordered key contains invalid UTF-16 text", invalid);
        }
    }

    private static String decodeStrictUtf8(byte[] value) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(value)).toString();
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException("ordered key contains invalid UTF-8", invalid);
        }
    }

    public static final class Component {
        private final int type;
        private final long longValue;
        private final byte[] bytes;

        private Component(int type, long longValue, byte[] bytes) {
            this.type = type;
            this.longValue = longValue;
            this.bytes = bytes;
        }

        private Component copy() {
            return new Component(type, longValue, bytes == null ? null : bytes.clone());
        }
    }

    private static final class Cursor {
        private final byte[] bytes;
        private int index;

        private Cursor(byte[] bytes) {
            this.bytes = bytes;
        }

        private int u8() {
            if (index >= bytes.length) throw new IllegalArgumentException("truncated ordered key");
            return bytes[index++] & 0xff;
        }

        private long i64() {
            if (bytes.length - index < 8) throw new IllegalArgumentException("truncated ordered key");
            long value = 0L;
            for (int i = 0; i < 8; i++) value = (value << 8) | u8();
            return value;
        }

        private byte[] escapedBytes() {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            while (true) {
                int value = u8();
                if (value != 0) {
                    out.write(value);
                } else {
                    int marker = u8();
                    if (marker == 0) return out.toByteArray();
                    if (marker != 0xff) {
                        throw new IllegalArgumentException("invalid ordered key escape");
                    }
                    out.write(0);
                }
                if (out.size() > MAX_COMPONENT_BYTES) {
                    throw new IllegalArgumentException("ordered key component exceeds limit");
                }
            }
        }

        private boolean exhausted() {
            return index == bytes.length;
        }
    }
}
