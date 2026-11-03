package io.rowweave.run;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongConsumer;

/** Canonical fixed-width checksummed frame for a public Run event page. */
public final class RunExecutionEventPageCodec {
    public static final int MAX_FRAME_BYTES = 64 * 1024;

    private static final int MAGIC = 0x52574550; // RWEP
    private static final int VERSION = 1;
    private static final int KIND_PAGE = 1;
    private static final int HEADER_BYTES = 12;
    private static final int PAGE_BODY_BYTES = 22;
    private static final int EVENT_BYTES = 30;
    private static final int CHECKSUM_BYTES = 32;
    private static final long EVENT_RESIDENT_BYTES = 128L;
    private static final LongConsumer NO_RESERVATION = value -> { };

    private RunExecutionEventPageCodec() {
    }

    public static byte[] encode(RunExecutionEventPage page) {
        return encode(page, NO_RESERVATION);
    }

    public static byte[] encode(RunExecutionEventPage page, LongConsumer reservation) {
        if (page == null) throw new IllegalArgumentException("event page is required");
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        int count = page.events().size();
        int bodyLength = exactLength(PAGE_BODY_BYTES, count, EVENT_BYTES);
        int frameLength = exactLength(HEADER_BYTES + CHECKSUM_BYTES, 1, bodyLength);
        if (frameLength > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("event page frame exceeds limit");
        }
        reservation.accept(frameLength);
        byte[] frame = new byte[frameLength];
        ByteBuffer out = ByteBuffer.wrap(frame);
        out.putInt(MAGIC);
        out.putShort((short) VERSION);
        out.put((byte) KIND_PAGE);
        out.put((byte) 0);
        out.putInt(bodyLength);
        out.put((byte) stateId(page.state()));
        out.putLong(page.afterSequence());
        out.putLong(page.nextAfterSequence());
        out.put((byte) (page.hasMore() ? 1 : 0));
        out.putInt(count);
        for (RunExecutionEvent event : page.events()) writeEvent(out, event);
        byte[] checksum = sha256(frame, 0, frameLength - CHECKSUM_BYTES);
        out.put(checksum);
        if (out.position() != frameLength) {
            throw new IllegalStateException("event page frame length mismatch");
        }
        return frame;
    }

    public static RunExecutionEventPage decode(byte[] encoded) {
        return decode(encoded, NO_RESERVATION);
    }

    public static RunExecutionEventPage decode(byte[] encoded, LongConsumer reservation) {
        if (encoded == null || encoded.length < HEADER_BYTES + PAGE_BODY_BYTES + CHECKSUM_BYTES
                || encoded.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("event page frame length out of bounds");
        }
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        reservation.accept(encoded.length);

        ByteBuffer in = ByteBuffer.wrap(encoded);
        if (in.getInt() != MAGIC) throw new IllegalArgumentException("invalid event page magic");
        if ((in.getShort() & 0xffff) != VERSION) {
            throw new IllegalArgumentException("unsupported event page version");
        }
        if ((in.get() & 0xff) != KIND_PAGE) {
            throw new IllegalArgumentException("unknown event page frame kind");
        }
        if (in.get() != 0) throw new IllegalArgumentException("non-zero event page reserved byte");
        int bodyLength = in.getInt();
        if (bodyLength < PAGE_BODY_BYTES
                || encoded.length != HEADER_BYTES + bodyLength + CHECKSUM_BYTES) {
            throw new IllegalArgumentException("event page body length mismatch");
        }

        int stateId = in.get() & 0xff;
        long afterSequence = in.getLong();
        long encodedNext = in.getLong();
        int hasMoreId = in.get() & 0xff;
        if (hasMoreId > 1) throw new IllegalArgumentException("invalid event page hasMore flag");
        int count = in.getInt();
        if (count < 0 || count > RunExecutionEventPage.MAX_EVENTS
                || bodyLength != exactLength(PAGE_BODY_BYTES, count, EVENT_BYTES)) {
            throw new IllegalArgumentException("invalid event page entry count");
        }
        reservation.accept(EVENT_RESIDENT_BYTES * count);
        if (!checksumMatches(encoded)) {
            throw new IllegalArgumentException("event page checksum mismatch");
        }

        List<RunExecutionEvent> events = count == 0
                ? Collections.emptyList() : new ArrayList<>(count);
        for (int i = 0; i < count; i++) events.add(readEvent(in));
        in.position(in.position() + CHECKSUM_BYTES);
        if (in.hasRemaining()) throw new IllegalArgumentException("event page has trailing bytes");

        RunExecutionEventPage.State state = state(stateId);
        RunExecutionEventPage page;
        if (state == RunExecutionEventPage.State.FOUND) {
            page = RunExecutionEventPage.found(afterSequence, events, hasMoreId == 1);
        } else if (state == RunExecutionEventPage.State.NOT_FOUND) {
            page = RunExecutionEventPage.notFound(afterSequence);
        } else {
            page = RunExecutionEventPage.retiring(afterSequence);
        }
        if (encodedNext != page.nextAfterSequence()
                || page.events().size() != count || page.hasMore() != (hasMoreId == 1)) {
            throw new IllegalArgumentException("event page scalar mismatch");
        }
        return page;
    }

    private static void writeEvent(ByteBuffer out, RunExecutionEvent event) {
        out.putLong(event.sequence());
        out.put((byte) typeId(event.type()));
        out.put((byte) RunWire.runStatusId(event.runFrom()));
        out.put((byte) RunWire.runStatusId(event.runTo()));
        out.put((byte) RunWire.attemptStatusId(event.attemptFrom()));
        out.put((byte) RunWire.attemptStatusId(event.attemptTo()));
        out.putLong(event.eventTimeMillis());
        out.put((byte) diagnosticCategoryId(event.diagnosticCategory()));
        writeDigestPrefix(out, event.mutationDigestPrefix());
    }

    private static RunExecutionEvent readEvent(ByteBuffer in) {
        long sequence = in.getLong();
        RunExecutionEvent.Type type = type(in.get() & 0xff);
        RunStatus runFrom = RunWire.runStatus(in.get() & 0xff, true);
        RunStatus runTo = RunWire.runStatus(in.get() & 0xff, false);
        AttemptStatus attemptFrom = RunWire.attemptStatus(in.get() & 0xff, true);
        AttemptStatus attemptTo = RunWire.attemptStatus(in.get() & 0xff, true);
        long eventTime = in.getLong();
        RunExecutionEvent.DiagnosticCategory category =
                diagnosticCategory(in.get() & 0xff);
        String digestPrefix = readDigestPrefix(in);
        return RunExecutionEvent.of(sequence, type, runFrom, runTo,
                attemptFrom, attemptTo, eventTime, category, digestPrefix);
    }

    private static int stateId(RunExecutionEventPage.State state) {
        switch (state) {
            case FOUND: return 1;
            case NOT_FOUND: return 2;
            case RETIRING: return 3;
            default: throw new IllegalArgumentException("unknown event page state");
        }
    }

    private static RunExecutionEventPage.State state(int id) {
        switch (id) {
            case 1: return RunExecutionEventPage.State.FOUND;
            case 2: return RunExecutionEventPage.State.NOT_FOUND;
            case 3: return RunExecutionEventPage.State.RETIRING;
            default: throw new IllegalArgumentException("unknown event page state: " + id);
        }
    }

    private static int typeId(RunExecutionEvent.Type type) {
        switch (type) {
            case RUN_CREATED: return 1;
            case ATTEMPT_CREATED: return 2;
            case ATTEMPT_ADVANCED: return 3;
            case CANCEL_REQUESTED: return 4;
            case TIMEOUT_REQUESTED: return 5;
            case FENCE_REQUESTED: return 6;
            default: throw new IllegalArgumentException("unknown public event type");
        }
    }

    private static RunExecutionEvent.Type type(int id) {
        switch (id) {
            case 1: return RunExecutionEvent.Type.RUN_CREATED;
            case 2: return RunExecutionEvent.Type.ATTEMPT_CREATED;
            case 3: return RunExecutionEvent.Type.ATTEMPT_ADVANCED;
            case 4: return RunExecutionEvent.Type.CANCEL_REQUESTED;
            case 5: return RunExecutionEvent.Type.TIMEOUT_REQUESTED;
            case 6: return RunExecutionEvent.Type.FENCE_REQUESTED;
            default: throw new IllegalArgumentException("unknown public event type: " + id);
        }
    }

    private static int diagnosticCategoryId(RunExecutionEvent.DiagnosticCategory category) {
        switch (category) {
            case NONE: return 1;
            case ADMISSION: return 2;
            case DISPATCH: return 3;
            case EXECUTION: return 4;
            case CANCELED: return 5;
            case TIMEOUT: return 6;
            case FENCED: return 7;
            case COMMIT: return 8;
            case RECOVERY: return 9;
            case RESOURCE: return 10;
            case OTHER: return 11;
            default: throw new IllegalArgumentException("unknown diagnostic category");
        }
    }

    private static RunExecutionEvent.DiagnosticCategory diagnosticCategory(int id) {
        switch (id) {
            case 1: return RunExecutionEvent.DiagnosticCategory.NONE;
            case 2: return RunExecutionEvent.DiagnosticCategory.ADMISSION;
            case 3: return RunExecutionEvent.DiagnosticCategory.DISPATCH;
            case 4: return RunExecutionEvent.DiagnosticCategory.EXECUTION;
            case 5: return RunExecutionEvent.DiagnosticCategory.CANCELED;
            case 6: return RunExecutionEvent.DiagnosticCategory.TIMEOUT;
            case 7: return RunExecutionEvent.DiagnosticCategory.FENCED;
            case 8: return RunExecutionEvent.DiagnosticCategory.COMMIT;
            case 9: return RunExecutionEvent.DiagnosticCategory.RECOVERY;
            case 10: return RunExecutionEvent.DiagnosticCategory.RESOURCE;
            case 11: return RunExecutionEvent.DiagnosticCategory.OTHER;
            default: throw new IllegalArgumentException("unknown diagnostic category: " + id);
        }
    }

    private static void writeDigestPrefix(ByteBuffer out, String prefix) {
        for (int i = 0; i < prefix.length(); i += 2) {
            int high = Character.digit(prefix.charAt(i), 16);
            int low = Character.digit(prefix.charAt(i + 1), 16);
            if (high < 0 || low < 0) throw new IllegalArgumentException("invalid digest prefix");
            out.put((byte) ((high << 4) | low));
        }
    }

    private static String readDigestPrefix(ByteBuffer in) {
        char[] chars = new char[16];
        for (int i = 0; i < 8; i++) {
            int value = in.get() & 0xff;
            chars[2 * i] = Character.forDigit(value >>> 4, 16);
            chars[2 * i + 1] = Character.forDigit(value & 0xf, 16);
        }
        return new String(chars);
    }

    private static int exactLength(int base, int count, int width) {
        try {
            return Math.addExact(base, Math.multiplyExact(count, width));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("event page length overflow", overflow);
        }
    }

    private static boolean checksumMatches(byte[] encoded) {
        byte[] expected = sha256(encoded, 0, encoded.length - CHECKSUM_BYTES);
        int difference = 0;
        int checksumOffset = encoded.length - CHECKSUM_BYTES;
        for (int i = 0; i < CHECKSUM_BYTES; i++) {
            difference |= expected[i] ^ encoded[checksumOffset + i];
        }
        return difference == 0;
    }

    private static byte[] sha256(byte[] bytes, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, offset, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
