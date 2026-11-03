package io.rowweave.mem.seg;

/**
 * Heap implementation of MemorySegment: stored in a byte[] with big-endian byte order, matching RowSerializer so values compare across implementations.
 */
final class HeapSegment implements MemorySegment {

    private final byte[] data;

    HeapSegment(int size) {
        this.data = new byte[size];
    }

    @Override
    public int size() {
        return data.length;
    }

    private void checkRange(int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IndexOutOfBoundsException(
                    "offset=" + offset + " length=" + length + " size=" + data.length);
        }
    }

    @Override
    public void putByte(int offset, byte value) {
        // JVM array access itself performs the exact single-byte bounds check.
        data[offset] = value;
    }

    @Override
    public byte getByte(int offset) {
        // JVM array access itself performs the exact single-byte bounds check.
        return data[offset];
    }

    @Override
    public void putInt(int offset, int value) {
        checkRange(offset, 4);
        data[offset] = (byte) (value >>> 24);
        data[offset + 1] = (byte) (value >>> 16);
        data[offset + 2] = (byte) (value >>> 8);
        data[offset + 3] = (byte) value;
    }

    @Override
    public int getInt(int offset) {
        checkRange(offset, 4);
        return ((data[offset] & 0xFF) << 24)
                | ((data[offset + 1] & 0xFF) << 16)
                | ((data[offset + 2] & 0xFF) << 8)
                | (data[offset + 3] & 0xFF);
    }

    @Override
    public void putLong(int offset, long value) {
        checkRange(offset, 8);
        for (int i = 0; i < 8; i++) {
            data[offset + i] = (byte) (value >>> (56 - 8 * i));
        }
    }

    @Override
    public long getLong(int offset) {
        checkRange(offset, 8);
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (data[offset + i] & 0xFF);
        }
        return v;
    }

    @Override
    public void putBytes(int offset, byte[] src) {
        checkRange(offset, src.length);
        System.arraycopy(src, 0, data, offset, src.length);
    }

    @Override
    public byte[] getBytes(int offset, int length) {
        checkRange(offset, length);
        byte[] out = new byte[length];
        System.arraycopy(data, offset, out, 0, length);
        return out;
    }

    @Override
    public void copyTo(int offset, byte[] target, int targetOffset, int length) {
        checkRange(offset, length);
        if (targetOffset < 0 || length < 0 || targetOffset + length > target.length) {
            throw new IndexOutOfBoundsException("targetOffset=" + targetOffset
                    + " length=" + length + " targetSize=" + target.length);
        }
        System.arraycopy(data, offset, target, targetOffset, length);
    }

    @Override
    public void clear() {
        java.util.Arrays.fill(data, (byte) 0);
    }
}
