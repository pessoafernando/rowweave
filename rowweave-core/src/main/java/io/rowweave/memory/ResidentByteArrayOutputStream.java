package io.rowweave.memory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.function.LongConsumer;

/** Bounded byte-array writer that calls the reservation hook before every backing or copy allocation. */
public final class ResidentByteArrayOutputStream extends OutputStream {
    private final LongConsumer reservation;
    private final int maxBytes;
    private byte[] buffer;
    private int count;

    public ResidentByteArrayOutputStream(LongConsumer reservation, int initialBytes, int maxBytes) {
        if (reservation == null || initialBytes < 0 || maxBytes < 1 || initialBytes > maxBytes) {
            throw new IllegalArgumentException("invalid resident byte writer bounds");
        }
        this.reservation = reservation;
        this.maxBytes = maxBytes;
        reservation.accept(initialBytes);
        this.buffer = new byte[initialBytes];
    }

    @Override
    public void write(int value) throws IOException {
        ensureCapacity(1);
        buffer[count++] = (byte) value;
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        if (bytes == null) throw new NullPointerException("bytes");
        if ((offset | length) < 0 || length > bytes.length - offset) {
            throw new IndexOutOfBoundsException();
        }
        ensureCapacity(length);
        System.arraycopy(bytes, offset, buffer, count, length);
        count += length;
    }

    public byte[] toByteArray() {
        reservation.accept(count);
        return Arrays.copyOf(buffer, count);
    }

    public int size() { return count; }

    private void ensureCapacity(int additional) throws IOException {
        if (additional > maxBytes - count) {
            throw new IOException("resident byte writer exceeds limit: " + maxBytes);
        }
        int required = count + additional;
        if (required <= buffer.length) return;
        int doubled = buffer.length <= maxBytes / 2 ? buffer.length * 2 : maxBytes;
        int next = Math.max(required, Math.max(1, doubled));
        if (next > maxBytes) next = maxBytes;
        // old + new coexist during Arrays.copyOf; keep the conservative historical charge until lease close.
        reservation.accept(next);
        buffer = Arrays.copyOf(buffer, next);
    }
}
