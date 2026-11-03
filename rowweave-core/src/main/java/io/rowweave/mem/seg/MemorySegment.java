package io.rowweave.mem.seg;

/**
 * Fixed-length memory segment: read and write primitives addressed by byte offset, the basic unit of segmented memory.
 * The heap implementation wraps a byte[]; the pooled implementation wraps an Unsafe or DirectByteBuffer address. Either substitutes behind this interface.
 * Every offset access is bounds-checked and throws {@link IndexOutOfBoundsException} when out of range.
 */
public interface MemorySegment {

    int size();

    void putByte(int offset, byte value);

    byte getByte(int offset);

    void putInt(int offset, int value);

    int getInt(int offset);

    void putLong(int offset, long value);

    long getLong(int offset);

    void putBytes(int offset, byte[] src);

    byte[] getBytes(int offset, int length);

    /** Copies the range into an array the caller already allocated; no intermediate byte[] may be created. */
    void copyTo(int offset, byte[] target, int targetOffset, int length);

    /** Zeroes the segment (called before a pooled segment is returned and reused, so an earlier payload cannot leak). */
    void clear();
}
