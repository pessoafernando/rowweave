package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded, redacted public Run event model and explicit page codec. */
class RunExecutionEventPageCodecTest {

    @Test
    void projectionExposesOnlySafeFieldsAndNeverEchoesRawIdentityOrText() {
        byte[] digest = digest(7);
        ExecutionEvent internal = new ExecutionEvent(
                7L, ExecutionEvent.Type.ATTEMPT_ADVANCED,
                "run-secret", "attempt-secret", 991L,
                RunStatus.RUNNING, RunStatus.FAILED,
                AttemptStatus.RUNNING, AttemptStatus.FAILED,
                1234L, "jdbc:password=secret", "sql=select secret",
                "command-secret", digest);

        RunExecutionEvent view = internal.publicView();

        assertEquals(1, view.schemaVersion());
        assertEquals(7L, view.sequence());
        assertEquals(RunExecutionEvent.Type.ATTEMPT_ADVANCED, view.type());
        assertEquals(RunStatus.RUNNING, view.runFrom());
        assertEquals(RunStatus.FAILED, view.runTo());
        assertEquals(AttemptStatus.RUNNING, view.attemptFrom());
        assertEquals(AttemptStatus.FAILED, view.attemptTo());
        assertEquals(1234L, view.eventTimeMillis());
        assertEquals(RunExecutionEvent.DiagnosticCategory.OTHER,
                view.diagnosticCategory());
        assertEquals(hexPrefix(digest), view.mutationDigestPrefix());

        Set<String> methods = new HashSet<>();
        for (Method method : RunExecutionEvent.class.getDeclaredMethods()) {
            methods.add(method.getName());
        }
        for (String forbidden : Arrays.asList(
                "runId", "attemptId", "fenceToken", "commandId",
                "commandDigest", "message", "reasonCode")) {
            assertFalse(methods.contains(forbidden), forbidden);
        }
        String printable = view.toString();
        assertFalse(printable.contains("secret"));
        assertFalse(printable.contains("jdbc"));
        assertFalse(printable.contains("sql"));
    }

    @Test
    void projectionMapsRuntimeOwnedReasonsToClosedCategories() {
        assertEquals(RunExecutionEvent.DiagnosticCategory.NONE,
                internal(null).publicView().diagnosticCategory());
        assertEquals(RunExecutionEvent.DiagnosticCategory.DISPATCH,
                internal("PRE_DISPATCH").publicView().diagnosticCategory());
        assertEquals(RunExecutionEvent.DiagnosticCategory.EXECUTION,
                internal("STAGE_TERMINAL_FAILURE").publicView().diagnosticCategory());
        assertEquals(RunExecutionEvent.DiagnosticCategory.COMMIT,
                internal("COMMIT_PREPUBLICATION_FAILED").publicView().diagnosticCategory());
        assertEquals(RunExecutionEvent.DiagnosticCategory.RESOURCE,
                internal("CONTROL_RESIDENT_BUDGET_EXCEEDED").publicView().diagnosticCategory());
        assertEquals(RunExecutionEvent.DiagnosticCategory.OTHER,
                internal("PASSWORD_SECRET").publicView().diagnosticCategory());
    }

    @Test
    void immutableModelsRejectInvalidEventsAndPages() {
        RunExecutionEvent first = event(2L);
        RunExecutionEvent second = event(3L);
        List<RunExecutionEvent> source = new ArrayList<>(Arrays.asList(first, second));
        RunExecutionEventPage page = RunExecutionEventPage.found(1L, source, true);
        source.clear();

        assertEquals(RunExecutionEventPage.State.FOUND, page.state());
        assertEquals(1L, page.afterSequence());
        assertEquals(3L, page.nextAfterSequence());
        assertTrue(page.hasMore());
        assertEquals(2, page.events().size());
        assertThrows(UnsupportedOperationException.class,
                () -> page.events().add(event(4L)));

        assertThrows(IllegalArgumentException.class, () -> RunExecutionEvent.of(
                0L, RunExecutionEvent.Type.RUN_CREATED, null, RunStatus.READY,
                null, null, 0L, RunExecutionEvent.DiagnosticCategory.NONE,
                "0000000000000000"));
        assertThrows(IllegalArgumentException.class, () -> RunExecutionEvent.of(
                1L, RunExecutionEvent.Type.RUN_CREATED, null, null,
                null, null, 0L, RunExecutionEvent.DiagnosticCategory.NONE,
                "0000000000000000"));
        assertThrows(IllegalArgumentException.class, () -> RunExecutionEvent.of(
                1L, RunExecutionEvent.Type.RUN_CREATED, null, RunStatus.READY,
                null, null, 0L, RunExecutionEvent.DiagnosticCategory.NONE,
                "not-a-digest"));
        assertThrows(IllegalArgumentException.class,
                () -> RunExecutionEventPage.found(-1L, Collections.emptyList(), false));
        assertThrows(IllegalArgumentException.class,
                () -> RunExecutionEventPage.found(1L, Collections.emptyList(), true));
        assertThrows(IllegalArgumentException.class,
                () -> RunExecutionEventPage.found(2L, Arrays.asList(first, second), false));
        assertThrows(IllegalArgumentException.class,
                () -> RunExecutionEventPage.found(0L, Arrays.asList(second, first), false));

        List<RunExecutionEvent> tooMany = new ArrayList<>();
        for (int i = 1; i <= 257; i++) tooMany.add(event(i));
        assertThrows(IllegalArgumentException.class,
                () -> RunExecutionEventPage.found(0L, tooMany, false));
    }

    @Test
    void foundNotFoundAndRetiringPagesRoundTripCanonically() {
        List<RunExecutionEventPage> pages = Arrays.asList(
                RunExecutionEventPage.found(4L,
                        Arrays.asList(event(5L), event(6L)), true),
                RunExecutionEventPage.found(6L, Collections.emptyList(), false),
                RunExecutionEventPage.notFound(9L),
                RunExecutionEventPage.retiring(11L));

        for (RunExecutionEventPage page : pages) {
            byte[] encoded = RunExecutionEventPageCodec.encode(page);
            RunExecutionEventPage decoded = RunExecutionEventPageCodec.decode(encoded);
            assertEquals(page, decoded);
            assertArrayEquals(encoded, RunExecutionEventPageCodec.encode(decoded));
        }
    }

    @Test
    void readyDeadlineEventWithoutAttemptRoundTripsCanonically() {
        ExecutionEvent internal = new ExecutionEvent(
                2L, ExecutionEvent.Type.TIMEOUT_REQUESTED,
                "run-secret", null, 0L,
                RunStatus.READY, RunStatus.FAILED,
                null, null, 100L, "TIMEOUT_REQUESTED", null,
                "deadline-command", digest(2));

        RunExecutionEvent projected = internal.publicView();
        assertEquals(RunExecutionEvent.Type.TIMEOUT_REQUESTED, projected.type());
        assertEquals(RunStatus.READY, projected.runFrom());
        assertEquals(RunStatus.FAILED, projected.runTo());
        assertNull(projected.attemptFrom());
        assertNull(projected.attemptTo());
        assertEquals(RunExecutionEvent.DiagnosticCategory.TIMEOUT,
                projected.diagnosticCategory());

        RunExecutionEventPage page = RunExecutionEventPage.found(
                1L, Collections.singletonList(projected), false);
        byte[] encoded = RunExecutionEventPageCodec.encode(page);
        assertEquals(page, RunExecutionEventPageCodec.decode(encoded));
        assertArrayEquals(encoded, RunExecutionEventPageCodec.encode(
                RunExecutionEventPageCodec.decode(encoded)));
    }

    @Test
    void truncationTrailingChecksumHeaderAndScalarCorruptionFailClosed() throws Exception {
        byte[] encoded = RunExecutionEventPageCodec.encode(
                RunExecutionEventPage.found(0L,
                        Arrays.asList(event(1L), event(2L)), false));
        for (int length = 0; length < encoded.length; length++) {
            byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(IllegalArgumentException.class,
                    () -> RunExecutionEventPageCodec.decode(truncated), "length=" + length);
        }
        assertThrows(IllegalArgumentException.class, () ->
                RunExecutionEventPageCodec.decode(Arrays.copyOf(encoded, encoded.length + 1)));

        byte[] corruptChecksum = encoded.clone();
        corruptChecksum[34] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> RunExecutionEventPageCodec.decode(corruptChecksum));

        byte[] unknownVersion = encoded.clone();
        unknownVersion[5] = 2;
        rewriteChecksum(unknownVersion);
        assertThrows(IllegalArgumentException.class,
                () -> RunExecutionEventPageCodec.decode(unknownVersion));

        byte[] unknownState = encoded.clone();
        unknownState[12] = 99;
        rewriteChecksum(unknownState);
        assertThrows(IllegalArgumentException.class,
                () -> RunExecutionEventPageCodec.decode(unknownState));

        byte[] oversizedCount = encoded.clone();
        writeInt(oversizedCount, 30, 257);
        rewriteChecksum(oversizedCount);
        assertThrows(IllegalArgumentException.class,
                () -> RunExecutionEventPageCodec.decode(oversizedCount));

        byte[] missingRunTo = encoded.clone();
        missingRunTo[44] = 0;
        rewriteChecksum(missingRunTo);
        assertThrows(IllegalArgumentException.class,
                () -> RunExecutionEventPageCodec.decode(missingRunTo));

        byte[] duplicateSequence = encoded.clone();
        System.arraycopy(duplicateSequence, 34, duplicateSequence, 64, Long.BYTES);
        rewriteChecksum(duplicateSequence);
        assertThrows(IllegalArgumentException.class,
                () -> RunExecutionEventPageCodec.decode(duplicateSequence));
    }

    @Test
    void oneSixtyFourAndTwoHundredFiftySixEntryProfilesStayBounded() {
        for (int count : new int[]{1, 64, 256}) {
            List<RunExecutionEvent> events = new ArrayList<>(count);
            for (int sequence = 1; sequence <= count; sequence++) {
                events.add(event(sequence));
            }
            RunExecutionEventPage page = RunExecutionEventPage.found(0L, events, false);
            AtomicLong encodeReservation = new AtomicLong();
            byte[] encoded = RunExecutionEventPageCodec.encode(
                    page, encodeReservation::addAndGet);
            AtomicLong decodeReservation = new AtomicLong();
            assertEquals(page, RunExecutionEventPageCodec.decode(
                    encoded, decodeReservation::addAndGet));
            assertEquals(encoded.length, encodeReservation.get());
            assertTrue(encoded.length <= RunExecutionEventPageCodec.MAX_FRAME_BYTES);
            assertTrue(decodeReservation.get() >= encoded.length);
            System.out.println("[RUN-EVENT-PAGE-PROFILE] entries=" + count
                    + " frame=" + encoded.length + "B resident="
                    + decodeReservation.get() + "B");
        }
    }

    @Test
    void encodeReservesBeforeAllocatingFinalFrame() {
        RuntimeException exhausted = new RuntimeException("resident exhausted");
        RuntimeException thrown = assertThrows(RuntimeException.class, () ->
                RunExecutionEventPageCodec.encode(
                        RunExecutionEventPage.found(
                                0L, Collections.singletonList(event(1L)), false),
                        bytes -> { throw exhausted; }));
        assertSame(exhausted, thrown);
    }

    private static ExecutionEvent internal(String reason) {
        return new ExecutionEvent(
                3L, ExecutionEvent.Type.ATTEMPT_ADVANCED,
                "run", "attempt", 1L,
                RunStatus.RUNNING, RunStatus.FAILED,
                AttemptStatus.RUNNING, AttemptStatus.FAILED,
                3L, reason, reason == null ? null : "raw-" + reason,
                "command", digest(3));
    }

    private static RunExecutionEvent event(long sequence) {
        return RunExecutionEvent.of(
                sequence, RunExecutionEvent.Type.ATTEMPT_ADVANCED,
                RunStatus.RUNNING, RunStatus.RUNNING,
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING,
                1000L + sequence, RunExecutionEvent.DiagnosticCategory.NONE,
                hexPrefix(digest((int) sequence)));
    }

    private static byte[] digest(int seed) {
        byte[] digest = new byte[32];
        for (int i = 0; i < digest.length; i++) digest[i] = (byte) (seed + i);
        return digest;
    }

    private static String hexPrefix(byte[] digest) {
        StringBuilder hex = new StringBuilder(16);
        for (int i = 0; i < 8; i++) {
            hex.append(Character.forDigit((digest[i] >>> 4) & 0xf, 16));
            hex.append(Character.forDigit(digest[i] & 0xf, 16));
        }
        return hex.toString();
    }

    private static void writeInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }

    private static void rewriteChecksum(byte[] frame) throws Exception {
        byte[] checksum = MessageDigest.getInstance("SHA-256")
                .digest(Arrays.copyOf(frame, frame.length - 32));
        System.arraycopy(checksum, 0, frame, frame.length - 32, checksum.length);
    }
}
