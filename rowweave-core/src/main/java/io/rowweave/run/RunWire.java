package io.rowweave.run;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Numeric identifiers and bounded primitive readers for the Run control-plane wire. */
final class RunWire {
    private RunWire() {
    }

    static int kindId(RunCommand.Kind value) {
        switch (value) {
            case CREATE_RUN: return 1;
            case CREATE_ATTEMPT: return 2;
            case ADVANCE_ATTEMPT: return 3;
            case REQUEST_CANCELLATION: return 4;
            case CANCEL_READY_RUN: return 5;
            case TIMEOUT_READY_RUN: return 6;
            default: throw new IllegalArgumentException("unknown command kind");
        }
    }

    static RunCommand.Kind commandKind(int id) {
        switch (id) {
            case 1: return RunCommand.Kind.CREATE_RUN;
            case 2: return RunCommand.Kind.CREATE_ATTEMPT;
            case 3: return RunCommand.Kind.ADVANCE_ATTEMPT;
            case 4: return RunCommand.Kind.REQUEST_CANCELLATION;
            case 5: return RunCommand.Kind.CANCEL_READY_RUN;
            case 6: return RunCommand.Kind.TIMEOUT_READY_RUN;
            default: throw new IllegalArgumentException("unknown command kind: " + id);
        }
    }

    static int attemptStatusId(AttemptStatus value) {
        if (value == null) return 0;
        switch (value) {
            case CREATED: return 1;
            case DISPATCHING: return 2;
            case RUNNING: return 3;
            case COMMITTING: return 4;
            case RECONCILING: return 5;
            case CANCELING: return 6;
            case SUCCEEDED: return 7;
            case FAILED: return 8;
            case CANCELED: return 9;
            case TIMED_OUT: return 10;
            case FENCED: return 11;
            default: throw new IllegalArgumentException("unknown attempt status");
        }
    }

    static AttemptStatus attemptStatus(int id, boolean nullable) {
        if (id == 0 && nullable) return null;
        switch (id) {
            case 1: return AttemptStatus.CREATED;
            case 2: return AttemptStatus.DISPATCHING;
            case 3: return AttemptStatus.RUNNING;
            case 4: return AttemptStatus.COMMITTING;
            case 5: return AttemptStatus.RECONCILING;
            case 6: return AttemptStatus.CANCELING;
            case 7: return AttemptStatus.SUCCEEDED;
            case 8: return AttemptStatus.FAILED;
            case 9: return AttemptStatus.CANCELED;
            case 10: return AttemptStatus.TIMED_OUT;
            case 11: return AttemptStatus.FENCED;
            default: throw new IllegalArgumentException("unknown attempt status: " + id);
        }
    }

    static int cancellationCauseId(CancellationCause value) {
        if (value == null) return 0;
        switch (value) {
            case USER: return 1;
            case TIMEOUT: return 2;
            case FENCE: return 3;
            default: throw new IllegalArgumentException("unknown cancellation cause");
        }
    }

    static CancellationCause cancellationCause(int id, boolean nullable) {
        if (id == 0 && nullable) return null;
        switch (id) {
            case 1: return CancellationCause.USER;
            case 2: return CancellationCause.TIMEOUT;
            case 3: return CancellationCause.FENCE;
            default: throw new IllegalArgumentException("unknown cancellation cause: " + id);
        }
    }

    static int publicationKindId(RunResultSummary.PublicationKind value) {
        if (value == RunResultSummary.PublicationKind.DISCARD) return 1;
        if (value == RunResultSummary.PublicationKind.MANIFEST) return 2;
        throw new IllegalArgumentException("unknown publication kind");
    }

    static RunResultSummary.PublicationKind publicationKind(int id) {
        if (id == 1) return RunResultSummary.PublicationKind.DISCARD;
        if (id == 2) return RunResultSummary.PublicationKind.MANIFEST;
        throw new IllegalArgumentException("unknown publication kind: " + id);
    }

    static int runStatusId(RunStatus value) {
        if (value == null) return 0;
        switch (value) {
            case READY: return 1;
            case RUNNING: return 2;
            case CANCELING: return 3;
            case RECONCILING: return 4;
            case SUCCEEDED: return 5;
            case FAILED: return 6;
            case CANCELED: return 7;
            default: throw new IllegalArgumentException("unknown run status");
        }
    }

    static RunStatus runStatus(int id, boolean nullable) {
        if (id == 0 && nullable) return null;
        switch (id) {
            case 1: return RunStatus.READY;
            case 2: return RunStatus.RUNNING;
            case 3: return RunStatus.CANCELING;
            case 4: return RunStatus.RECONCILING;
            case 5: return RunStatus.SUCCEEDED;
            case 6: return RunStatus.FAILED;
            case 7: return RunStatus.CANCELED;
            default: throw new IllegalArgumentException("unknown run status: " + id);
        }
    }

    static int eventTypeId(ExecutionEvent.Type value) {
        switch (value) {
            case RUN_CREATED: return 1;
            case ATTEMPT_CREATED: return 2;
            case ATTEMPT_ADVANCED: return 3;
            case CANCEL_REQUESTED: return 4;
            case TIMEOUT_REQUESTED: return 5;
            case FENCE_REQUESTED: return 6;
            default: throw new IllegalArgumentException("unknown event type");
        }
    }

    static ExecutionEvent.Type eventType(int id) {
        switch (id) {
            case 1: return ExecutionEvent.Type.RUN_CREATED;
            case 2: return ExecutionEvent.Type.ATTEMPT_CREATED;
            case 3: return ExecutionEvent.Type.ATTEMPT_ADVANCED;
            case 4: return ExecutionEvent.Type.CANCEL_REQUESTED;
            case 5: return ExecutionEvent.Type.TIMEOUT_REQUESTED;
            case 6: return ExecutionEvent.Type.FENCE_REQUESTED;
            default: throw new IllegalArgumentException("unknown event type: " + id);
        }
    }

    static void writeText(DataOutputStream out, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    static void writeNullableText(DataOutputStream out, String value) throws IOException {
        if (value == null) out.writeInt(-1); else writeText(out, value);
    }

    static final class Reader {
        private final byte[] bytes;
        private final java.util.function.LongConsumer reservation;
        private int position;

        Reader(byte[] bytes) {
            this(bytes, value -> { });
        }

        Reader(byte[] bytes, java.util.function.LongConsumer reservation) {
            this.bytes = bytes;
            this.reservation = reservation;
        }

        int readUnsignedByte(String path) {
            require(1, path);
            return bytes[position++] & 0xff;
        }

        boolean readBoolean(String path) {
            int value = readUnsignedByte(path);
            if (value > 1) throw new IllegalArgumentException(path + " is not a boolean");
            return value == 1;
        }

        int readInt(String path) {
            require(4, path);
            int value = ((bytes[position] & 0xff) << 24)
                    | ((bytes[position + 1] & 0xff) << 16)
                    | ((bytes[position + 2] & 0xff) << 8)
                    | (bytes[position + 3] & 0xff);
            position += 4;
            return value;
        }

        long readLong(String path) {
            require(8, path);
            long value = 0L;
            for (int i = 0; i < 8; i++) value = (value << 8) | (bytes[position + i] & 0xffL);
            position += 8;
            return value;
        }

        String readText(String path, int maxBytes) {
            int length = readInt(path + ".length");
            if (length < 0 || length > maxBytes) {
                throw new IllegalArgumentException(path + " length out of bounds: " + length);
            }
            byte[] raw = readFixed(length, path);
            reservation.accept(2L * length);
            return RunText.decode(raw, path, maxBytes);
        }

        String readNullableText(String path, int maxBytes) {
            int length = readInt(path + ".length");
            if (length == -1) return null;
            if (length < 0 || length > maxBytes) {
                throw new IllegalArgumentException(path + " length out of bounds: " + length);
            }
            byte[] raw = readFixed(length, path);
            reservation.accept(2L * length);
            return RunText.decode(raw, path, maxBytes);
        }

        byte[] readFixed(int length, String path) {
            require(length, path);
            reservation.accept(length);
            byte[] value = Arrays.copyOfRange(bytes, position, position + length);
            position += length;
            return value;
        }

        void finish(String path) {
            if (position != bytes.length) {
                throw new IllegalArgumentException(path + " has trailing bytes: " + (bytes.length - position));
            }
        }

        private void require(int length, String path) {
            if (length < 0 || bytes.length - position < length) {
                throw new IllegalArgumentException(path + " is truncated");
            }
        }
    }
}
