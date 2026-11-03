package io.rowweave.mem;

/** Java UTF-16 ordering over a RowSerializer UTF-8 payload, compared without allocating, plus its prefix. */
final class SerializedStringOrder {

    private static final long UTF16_END = -1L;
    private static final long POSITION_MASK = 0xFFFFFFFFL;
    private static final long UTF16_STATE_MASK = 0x0000FFFFFFFFFFFFL;

    private SerializedStringOrder() {
    }

    static <T> int compareUtf8AsUtf16(T left, int leftOffset, int leftLength,
                                      T right, int rightOffset, int rightLength,
                                      ByteAccess<T> access) {
        requireRange(leftOffset, 4, leftLength);
        requireRange(rightOffset, 4, rightLength);
        int leftBytes = access.getInt(left, leftOffset);
        int rightBytes = access.getInt(right, rightOffset);
        if (leftBytes < 0) throw corrupt("negative string length", leftOffset);
        if (rightBytes < 0) throw corrupt("negative string length", rightOffset);
        int leftStart = advance(leftOffset, 4, leftLength);
        int rightStart = advance(rightOffset, 4, rightLength);
        int leftEnd = advance(leftStart, leftBytes, leftLength);
        int rightEnd = advance(rightStart, rightBytes, rightLength);

        int leftPosition = leftStart;
        int rightPosition = rightStart;
        while (leftEnd - leftPosition >= 8 && rightEnd - rightPosition >= 8) {
            long leftWord = access.getLong(left, leftPosition);
            long rightWord = access.getLong(right, rightPosition);
            if (((leftWord | rightWord) & 0x8080808080808080L) != 0) break;
            long different = leftWord ^ rightWord;
            if (different != 0) {
                int shift = 56 - (Long.numberOfLeadingZeros(different) / 8) * 8;
                return Integer.compare((int) ((leftWord >>> shift) & 0xFF),
                        (int) ((rightWord >>> shift) & 0xFF));
            }
            leftPosition += 8;
            rightPosition += 8;
        }
        while (leftPosition < leftEnd && rightPosition < rightEnd) {
            int leftByte = access.getByte(left, leftPosition) & 0xFF;
            int rightByte = access.getByte(right, rightPosition) & 0xFF;
            if (leftByte >= 0x80 || rightByte >= 0x80) {
                if (leftByte < 0x80) {
                    nextUtf16(right, rightEnd, rightPosition & POSITION_MASK, access);
                    return -1;
                }
                if (rightByte < 0x80) {
                    nextUtf16(left, leftEnd, leftPosition & POSITION_MASK, access);
                    return 1;
                }
                break;
            }
            if (leftByte != rightByte) return Integer.compare(leftByte, rightByte);
            leftPosition++;
            rightPosition++;
        }
        if (leftPosition == leftEnd || rightPosition == rightEnd) {
            if (leftPosition == leftEnd && rightPosition == rightEnd) return 0;
            return leftPosition == leftEnd ? -1 : 1;
        }

        long leftState = leftPosition & POSITION_MASK;
        long rightState = rightPosition & POSITION_MASK;
        while (true) {
            long leftNext = nextUtf16(left, leftEnd, leftState, access);
            long rightNext = nextUtf16(right, rightEnd, rightState, access);
            if (leftNext == UTF16_END || rightNext == UTF16_END) {
                if (leftNext == rightNext) return 0;
                return leftNext == UTF16_END ? -1 : 1;
            }
            int leftUnit = (int) ((leftNext >>> 48) & 0xFFFF);
            int rightUnit = (int) ((rightNext >>> 48) & 0xFFFF);
            if (leftUnit != rightUnit) return Integer.compare(leftUnit, rightUnit);
            leftState = leftNext & UTF16_STATE_MASK;
            rightState = rightNext & UTF16_STATE_MASK;
        }
    }

    static long prefix(byte[] source, int offset, ByteAccess<byte[]> access) {
        requireRange(offset, 4, source.length);
        int byteLength = access.getInt(source, offset);
        if (byteLength < 0) throw corrupt("negative string length", offset);
        int start = advance(offset, 4, source.length);
        int end = advance(start, byteLength, source.length);
        long state = start & POSITION_MASK;
        long prefix = 0L;
        int emitted = 0;
        while (true) {
            long next = nextUtf16(source, end, state, access);
            if (next == UTF16_END) break;
            if (emitted < 4) {
                prefix = (prefix << 16) | ((next >>> 48) & 0xFFFFL);
                emitted++;
            }
            state = next & UTF16_STATE_MASK;
        }
        while (emitted++ < 4) prefix <<= 16;
        return prefix;
    }

    /** state:low32=byte offset,bits32..47=pending low surrogate;result high16=current unit. */
    private static <T> long nextUtf16(T source, int end, long state, ByteAccess<T> access) {
        int position = (int) (state & POSITION_MASK);
        int pendingLow = (int) ((state >>> 32) & 0xFFFF);
        if (pendingLow != 0) {
            return ((long) pendingLow << 48) | (position & POSITION_MASK);
        }
        if (position == end) return UTF16_END;
        if (position < 0 || position > end) {
            throw corrupt("UTF-8 decoder position exceeds payload", position);
        }
        long decoded = decodeUtf8CodePoint(source, position, end, access);
        int codePoint = (int) (decoded >>> 32);
        int nextPosition = (int) decoded;
        if (codePoint <= 0xFFFF) {
            return ((long) codePoint << 48) | (nextPosition & POSITION_MASK);
        }
        int supplementary = codePoint - 0x10000;
        int high = 0xD800 | (supplementary >>> 10);
        int low = 0xDC00 | (supplementary & 0x3FF);
        return ((long) high << 48) | ((long) low << 32)
                | (nextPosition & POSITION_MASK);
    }

    private static <T> long decodeUtf8CodePoint(T source, int position, int end,
                                                 ByteAccess<T> access) {
        requireUtf8Range(position, 1, end);
        int first = access.getByte(source, position) & 0xFF;
        if (first < 0x80) return packCodePoint(first, position + 1);
        if (first >= 0xC2 && first <= 0xDF) {
            requireUtf8Range(position, 2, end);
            int second = continuation(source, position + 1, access);
            return packCodePoint(((first & 0x1F) << 6) | second, position + 2);
        }
        if (first >= 0xE0 && first <= 0xEF) {
            requireUtf8Range(position, 3, end);
            int secondByte = access.getByte(source, position + 1) & 0xFF;
            int second = continuation(secondByte, position + 1);
            int third = continuation(source, position + 2, access);
            if (first == 0xE0 && secondByte < 0xA0) {
                throw corrupt("overlong three-byte UTF-8 sequence", position);
            }
            if (first == 0xED && secondByte >= 0xA0) {
                throw corrupt("UTF-8 surrogate code point", position);
            }
            return packCodePoint(((first & 0x0F) << 12) | (second << 6) | third,
                    position + 3);
        }
        if (first >= 0xF0 && first <= 0xF4) {
            requireUtf8Range(position, 4, end);
            int secondByte = access.getByte(source, position + 1) & 0xFF;
            int second = continuation(secondByte, position + 1);
            int third = continuation(source, position + 2, access);
            int fourth = continuation(source, position + 3, access);
            if (first == 0xF0 && secondByte < 0x90) {
                throw corrupt("overlong four-byte UTF-8 sequence", position);
            }
            if (first == 0xF4 && secondByte > 0x8F) {
                throw corrupt("UTF-8 code point exceeds U+10FFFF", position);
            }
            return packCodePoint(((first & 0x07) << 18) | (second << 12)
                    | (third << 6) | fourth, position + 4);
        }
        throw corrupt("invalid UTF-8 leading byte " + first, position);
    }

    private static <T> int continuation(T source, int position, ByteAccess<T> access) {
        return continuation(access.getByte(source, position) & 0xFF, position);
    }

    private static int continuation(int value, int position) {
        if ((value & 0xC0) != 0x80) {
            throw corrupt("invalid UTF-8 continuation byte " + value, position);
        }
        return value & 0x3F;
    }

    private static long packCodePoint(int codePoint, int nextPosition) {
        return ((long) codePoint << 32) | (nextPosition & POSITION_MASK);
    }

    private static void requireUtf8Range(int position, int width, int end) {
        if (position < 0 || width < 0 || (long) position + width > end) {
            throw corrupt("truncated UTF-8 sequence", position);
        }
    }

    private static int advance(int offset, long amount, int length) {
        long end = offset + amount;
        if (amount < 0 || end < 0 || end > length) {
            throw corrupt("encoded field exceeds row length " + length, offset);
        }
        return (int) end;
    }

    private static void requireRange(int offset, int width, int length) {
        if (offset < 0 || width < 0 || (long) offset + width > length) {
            throw corrupt("encoded read exceeds row length " + length, offset);
        }
    }

    private static IllegalArgumentException corrupt(String message, int offset) {
        return new IllegalArgumentException(message + " at offset " + offset);
    }

    interface ByteAccess<T> {
        byte getByte(T source, int offset);
        int getInt(T source, int offset);
        long getLong(T source, int offset);
    }
}
