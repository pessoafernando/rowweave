package io.rowweave.stream.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded event-time temporal join semantics. */
class BoundedEventTimeTemporalJoinTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void matchesInclusiveIntervalInCanonicalArrivalOrderAndNeverMatchesNullKeys() {
        List<String> output = new ArrayList<>();
        try (BoundedEventTimeTemporalJoin join = join(
                128, 0, 127, 32, 64 * 1024L,
                (left, right) -> bytes(text(left) + ':' + text(right)),
                frame -> output.add(text(frame)))) {
            join.onLeft(bytes("k"), 100L, bytes("L"));
            join.onRight(bytes("k"), 90L, bytes("R-low"));
            join.onRight(bytes("k"), 120L, bytes("R-high"));
            join.onRight(bytes("k"), 121L, bytes("R-out"));
            join.onRight(bytes("other"), 100L, bytes("R-other"));
            join.onLeft(null, 100L, bytes("L-null"));
            join.onRight(null, 100L, bytes("R-null"));

            assertEquals(Arrays.asList("L:R-low", "L:R-high"), output);
            assertEquals(5, join.stateEntryCount());
            assertTrue(join.stateBytes() > 0L);
            assertEquals(128, join.ownedKeyGroupCount());
        }
    }

    @Test
    void watermarksCleanBothSidesAndLateRecordsFailClosed() {
        try (BoundedEventTimeTemporalJoin join = join(
                128, 0, 127, 32, 64 * 1024L,
                (left, right) -> bytes(text(left) + text(right)),
                ignored -> { })) {
            join.onLeft(bytes("k"), 100L, bytes("L"));
            join.onRight(bytes("k"), 110L, bytes("R"));
            assertEquals(2, join.stateEntryCount());

            join.advanceRightWatermark(120L);
            assertEquals(1, join.stateEntryCount());
            join.advanceLeftWatermark(120L);
            assertEquals(0, join.stateEntryCount());

            assertCode(StreamTemporalJoinException.Code.LATE_RECORD,
                    () -> join.onLeft(bytes("k"), 99L, bytes("late-left")));
            assertCode(StreamTemporalJoinException.Code.LATE_RECORD,
                    () -> join.onRight(bytes("k"), 109L, bytes("late-right")));
            assertCode(StreamTemporalJoinException.Code.WATERMARK_REGRESSION,
                    () -> join.advanceLeftWatermark(119L));
        }
    }

    @Test
    void exactRetractionCompensatesMatchesAndRemovesRetainedState() {
        List<String> output = new ArrayList<>();
        try (BoundedEventTimeTemporalJoin join = join(
                128, 0, 127, 32, 64 * 1024L,
                (left, right) -> bytes("+" + text(left) + ':' + text(right)),
                frame -> output.add(text(frame)))) {
            join.onLeft(bytes("k"), 100L, bytes("L"));
            join.onRight(bytes("k"), 110L, bytes("R"));
            join.retractRight(bytes("k"), 110L, bytes("R"),
                    (left, right) -> bytes(
                            "-" + text(left) + ':' + text(right)));

            assertEquals(Arrays.asList("+L:R", "-L:R"), output);
            assertEquals(1, join.stateEntryCount());
            assertCode(StreamTemporalJoinException.Code.MISSING_RETRACTION,
                    () -> join.retractRight(bytes("k"), 110L, bytes("R"),
                            (left, right) -> bytes("unexpected")));
        }
    }

    @Test
    void entryAndByteQuotasRejectBeforeGrowingState() {
        try (BoundedEventTimeTemporalJoin join = join(
                128, 0, 127, 1, 64 * 1024L,
                (left, right) -> bytes("unused"), ignored -> { })) {
            join.onLeft(bytes("a"), 10L, bytes("one"));
            long retained = join.stateBytes();
            assertCode(StreamTemporalJoinException.Code.STATE_ENTRY_LIMIT,
                    () -> join.onRight(bytes("b"), 10L, bytes("two")));
            assertEquals(1, join.stateEntryCount());
            assertEquals(retained, join.stateBytes());
            assertEquals(1, join.maximumObservedEntries());
        }

        try (BoundedEventTimeTemporalJoin join = join(
                128, 0, 127, 32, 40L,
                (left, right) -> bytes("unused"), ignored -> { })) {
            assertCode(StreamTemporalJoinException.Code.STATE_BYTE_LIMIT,
                    () -> join.onLeft(bytes("key"), 10L,
                            bytes("record-too-large-for-budget")));
            assertEquals(0, join.stateEntryCount());
            assertEquals(0L, join.stateBytes());
        }
    }

    @Test
    void rejectsKeysOutsideTheOwnedContiguousKeyGroupRange() {
        byte[] key = bytes("owned-by-some-group");
        int group = ManagedKeyedValueState.keyGroup(key, 128);
        int start = group == 0 ? 1 : 0;
        int end = group == 0 ? 127 : group - 1;
        try (BoundedEventTimeTemporalJoin join = join(
                128, start, end, 32, 64 * 1024L,
                (left, right) -> bytes("unused"), ignored -> { })) {
            assertCode(StreamTemporalJoinException.Code.UNOWNED_KEY_GROUP,
                    () -> join.onLeft(key, 10L, bytes("record")));
            assertEquals(0, join.stateEntryCount());
        }
    }

    private BoundedEventTimeTemporalJoin join(
            int maxParallelism, int keyGroupStart, int keyGroupEnd,
            int maximumEntries, long maximumStateBytes,
            BoundedEventTimeTemporalJoin.JoinFunction function,
            BoundedEventTimeTemporalJoin.Sink output) {
        return new BoundedEventTimeTemporalJoin(
                "temporal-join", -10L, 20L,
                maxParallelism, keyGroupStart, keyGroupEnd,
                maximumEntries, maximumStateBytes, 1024, 4096,
                temporaryDirectory, function, output);
    }

    private static void assertCode(
            StreamTemporalJoinException.Code code,
            org.junit.jupiter.api.function.Executable action) {
        StreamTemporalJoinException failure = assertThrows(
                StreamTemporalJoinException.class, action);
        assertEquals(code, failure.code(), failure.getMessage());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
