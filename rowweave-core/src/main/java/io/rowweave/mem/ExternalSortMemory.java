package io.rowweave.mem;

/** VM-layout credit formula shared by the synchronous and three-threaded external sorts. */
final class ExternalSortMemory {

    static final long BATCH_FIXED_CREDIT = 4L * 1024;
    static final long PER_ROW_METADATA_CREDIT = 256;
    static final long READER_BUFFER_CREDIT = ManagedMemoryLayout.arrayBytes(1024);
    static final long CURSOR_METADATA_CREDIT = 512;
    static final int FRAME_HEADER_BYTES = 12;

    private ExternalSortMemory() {
    }

    static long rowCredit(int serializedSize, long rowEstimate, long scratch) {
        long segment = ManagedMemoryLayout.arrayBytes(Math.max(1, serializedSize));
        long frame = frameCredit(serializedSize);
        long migrationOrFrame = Math.max(Math.addExact(rowEstimate, scratch), frame);
        return Math.addExact(Math.addExact(segment, migrationOrFrame), PER_ROW_METADATA_CREDIT);
    }

    static long finalHeadCredit(int serializedSize, long rowEstimate) {
        long frame = frameCredit(serializedSize);
        long payloadPeak = Math.addExact(Math.multiplyExact(2L, frame), Math.max(frame, rowEstimate));
        return Math.addExact(READER_BUFFER_CREDIT,
                Math.addExact(payloadPeak, CURSOR_METADATA_CREDIT));
    }

    static long frameCredit(int serializedSize) {
        return ManagedMemoryLayout.arrayBytes(Math.addExact(FRAME_HEADER_BYTES, serializedSize));
    }
}
