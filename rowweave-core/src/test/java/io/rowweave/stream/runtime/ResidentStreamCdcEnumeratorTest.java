package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamCdcEnumeratorState;
import io.rowweave.stream.contract.StreamCdcEnumeratorStateCodec;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** versioned resident snapshot-to-log enumerator. */
class ResidentStreamCdcEnumeratorTest {

    @Test
    void v1StateGoldenBytesRemainStable() {
        StreamCdcEnumeratorState state = StreamCdcEnumeratorState.initial(
                "resident-cdc-source", digest('a'), 10L,
                Arrays.asList("split-b", "split-a"));

        assertEquals(
                "UldYWENFUzEAAACSAAAAAQAAABNyZXNpZGVudC1jZGMtc291cmNlAAAAQGFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWEAAAAAAAAACgAAAAIAAAAHc3BsaXQtYQAAAAdzcGxpdC1iAAAAAAAAAAEAAAAAAAAACgBEi+qTLUIFICA5LJcz/CpKHp4sRIVPxgixa6R92eA+iw==",
                Base64.getEncoder().encodeToString(
                        StreamCdcEnumeratorStateCodec.encode(state)));
        assertEquals(1, state.version());
        assertFalse(state.hasExternalPosition());
    }

    @Test
    void v2OpaquePositionAdvancesOnlyAtCompleteTransactionBoundary() {
        List<byte[]> output = new ArrayList<>();
        StreamCdcEnumeratorState initial =
                StreamCdcEnumeratorState.initialV2(
                        "resident-cdc-source", digest('a'), 0L,
                        Arrays.asList("snapshot-all"));
        ResidentStreamCdcEnumerator enumerator =
                new ResidentStreamCdcEnumerator(
                        initial, 4096, frame -> output.add(frame.clone()));

        StreamCdcEnumeratorState handoff =
                enumerator.completeSnapshotAndHandoff(
                        "snapshot-all", bytes("0/10"));
        assertEquals(StreamCdcEnumeratorState.Phase.LOG, handoff.phase());
        assertArrayEquals(bytes("0/10"), handoff.externalPositionFrame());

        enumerator.onLogRecord(1L, after("A", "one"), null);
        assertCode(StreamCdcException.Code.CHECKPOINT_BOUNDARY,
                enumerator::checkpointState);
        enumerator.onLogRecord(2L, after("B", "two"), bytes("0/20"));
        enumerator.onLogPosition(bytes("0/25"));

        StreamCdcEnumeratorState checkpoint = enumerator.checkpointState();
        assertEquals(2L, checkpoint.lastLogSequence());
        assertArrayEquals(bytes("0/25"), checkpoint.externalPositionFrame());
        byte[] encoded = StreamCdcEnumeratorStateCodec.encode(checkpoint);
        assertArrayEquals(encoded, StreamCdcEnumeratorStateCodec.encode(
                StreamCdcEnumeratorStateCodec.decode(encoded)));
        assertEquals(2, output.size());

        ResidentStreamCdcEnumerator restored =
                new ResidentStreamCdcEnumerator(
                        StreamCdcEnumeratorStateCodec.decode(encoded),
                        4096, ignored -> { });
        assertCode(StreamCdcException.Code.LOG_SEQUENCE,
                () -> restored.onLogRecord(
                        2L, after("B", "duplicate"), bytes("0/20")));
        restored.onLogRecord(3L, tombstone("A"), bytes("0/30"));
        assertArrayEquals(bytes("0/30"),
                restored.checkpointState().externalPositionFrame());
    }

    @Test
    void v2ExternalPositionIsBoundedAndDefensivelyCopied() {
        StreamCdcEnumeratorState initial =
                StreamCdcEnumeratorState.initialV2(
                        "resident-cdc-source", digest('a'), 0L,
                        Arrays.asList("snapshot-all"));
        ResidentStreamCdcEnumerator enumerator =
                new ResidentStreamCdcEnumerator(initial, 4096, ignored -> { });
        byte[] position = bytes("0/10");
        StreamCdcEnumeratorState handoff =
                enumerator.completeSnapshotAndHandoff(
                        "snapshot-all", position);
        position[0] = 'X';
        assertArrayEquals(bytes("0/10"), handoff.externalPositionFrame());
        byte[] returned = handoff.externalPositionFrame();
        returned[0] = 'Y';
        assertArrayEquals(bytes("0/10"), handoff.externalPositionFrame());

        ResidentStreamCdcEnumerator tooLarge =
                new ResidentStreamCdcEnumerator(
                        StreamCdcEnumeratorState.initialV2(
                                "resident-cdc-source", digest('b'), 0L,
                                Arrays.asList("snapshot-all")),
                        4096, ignored -> { });
        assertCode(StreamCdcException.Code.FRAME_LIMIT,
                () -> tooLarge.completeSnapshotAndHandoff(
                        "snapshot-all", new byte[64 * 1024 + 1]));
    }

    @Test
    void snapshotHandoffAndRestoredLogHaveNoGapOrDuplicateOutput() {
        List<byte[]> committed = new ArrayList<>();
        StreamCdcEnumeratorState initial = StreamCdcEnumeratorState.initial(
                "resident-cdc-source", digest('a'), 10L,
                Arrays.asList("split-b", "split-a"));
        ResidentStreamCdcEnumerator enumerator =
                new ResidentStreamCdcEnumerator(
                        initial, 4096, frame -> committed.add(frame.clone()));

        enumerator.onSnapshotRecord("split-a", after("A", "old-a"));
        enumerator.completeSnapshotSplit("split-a");
        enumerator.onSnapshotRecord("split-b", after("B", "old-b"));
        enumerator.completeSnapshotSplit("split-b");
        StreamCdcEnumeratorState handoff = enumerator.completeHandoff();
        assertEquals(StreamCdcEnumeratorState.Phase.LOG, handoff.phase());
        assertEquals(10L, handoff.lastLogSequence());
        assertEquals(2, handoff.completedSplits().size());

        enumerator.onLogRecord(11L, after("A", "new-a"));
        byte[] checkpoint = StreamCdcEnumeratorStateCodec.encode(
                enumerator.checkpointState());
        StreamCdcEnumeratorState restoredState =
                StreamCdcEnumeratorStateCodec.decode(checkpoint);
        assertArrayEquals(checkpoint,
                StreamCdcEnumeratorStateCodec.encode(restoredState));
        ResidentStreamCdcEnumerator restored =
                new ResidentStreamCdcEnumerator(
                        restoredState, 4096,
                        frame -> committed.add(frame.clone()));

        assertCode(StreamCdcException.Code.LOG_SEQUENCE,
                () -> restored.onLogRecord(11L, after("A", "duplicate")));
        assertCode(StreamCdcException.Code.LOG_SEQUENCE,
                () -> restored.onLogRecord(13L, after("A", "gap")));
        restored.onLogRecord(12L, tombstone("B"));

        Map<String, String> materialized = materialize(committed);
        assertEquals(1, materialized.size());
        assertEquals("new-a", materialized.get("A"));
        assertFalse(materialized.containsKey("B"));
        assertEquals(4, committed.size());
        assertEquals(12L, restored.checkpointState().lastLogSequence());
    }

    @Test
    void splitOrderAndCheckpointBoundaryFailBeforeHandoffMutation() {
        List<byte[]> output = new ArrayList<>();
        ResidentStreamCdcEnumerator enumerator =
                new ResidentStreamCdcEnumerator(
                        StreamCdcEnumeratorState.initial(
                                "resident-cdc-source", digest('a'), 10L,
                                Arrays.asList("split-b", "split-a")),
                        4096, frame -> output.add(frame.clone()));

        assertCode(StreamCdcException.Code.SPLIT_ORDER,
                () -> enumerator.onSnapshotRecord(
                        "split-b", after("B", "value")));
        assertEquals(0, output.size());
        enumerator.onSnapshotRecord("split-a", after("A", "value"));
        assertCode(StreamCdcException.Code.CHECKPOINT_BOUNDARY,
                enumerator::checkpointState);
        enumerator.completeSnapshotSplit("split-a");
        assertCode(StreamCdcException.Code.SNAPSHOT_INCOMPLETE,
                enumerator::completeHandoff);
        assertEquals(StreamCdcEnumeratorState.Phase.SNAPSHOT,
                enumerator.checkpointState().phase());
        assertEquals(1, enumerator.checkpointState().completedSplits().size());
    }

    @Test
    void enumeratorStateChecksumAndBoundsFailClosed() {
        StreamCdcEnumeratorState state = StreamCdcEnumeratorState.initial(
                "resident-cdc-source", digest('a'), 10L,
                Arrays.asList("split-a", "split-b"));
        byte[] encoded = StreamCdcEnumeratorStateCodec.encode(state);
        encoded[encoded.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> StreamCdcEnumeratorStateCodec.decode(encoded));
        assertThrows(IllegalArgumentException.class,
                () -> StreamCdcEnumeratorState.initial(
                        "resident-cdc-source", digest('a'), 10L,
                        new ArrayList<String>()));
    }

    @Test
    void statusViewIsBoundedAndRedactsSourceSnapshotAndSplitIdentity() {
        StreamCdcEnumeratorState initial = StreamCdcEnumeratorState.initial(
                "sensitive-source-name", digest('a'), 10L,
                Arrays.asList("tenant-secret-split", "split-b"));
        ResidentStreamCdcEnumerator enumerator =
                new ResidentStreamCdcEnumerator(initial, 4096, ignored -> { });

        StreamCdcStatusView status = enumerator.status();

        assertEquals(StreamCdcEnumeratorState.Phase.SNAPSHOT, status.phase());
        assertEquals(2, status.totalSnapshotSplits());
        assertEquals(0, status.completedSnapshotSplits());
        assertEquals(10L, status.snapshotCutSequence());
        String rendered = status.toString();
        assertFalse(rendered.contains("sensitive-source-name"));
        assertFalse(rendered.contains("tenant-secret-split"));
        assertFalse(rendered.contains(digest('a')));
        assertTrue(rendered.length() < 256);
    }

    @Test
    void repeatedCheckpointRestoreStressHasDeterministicBoundedState()
            throws Exception {
        StressOutcome first = stressRun();
        StressOutcome second = stressRun();

        assertEquals(20_000L, first.records);
        assertEquals(20_000L, first.lastSequence);
        assertEquals(first.records, second.records);
        assertArrayEquals(first.digest, second.digest);
        assertTrue(first.maximumCheckpointBytes < 4096);
        assertEquals(first.maximumCheckpointBytes,
                second.maximumCheckpointBytes);
    }

    private static StressOutcome stressRun() throws Exception {
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        final long[] records = new long[1];
        ResidentStreamCdcEnumerator.Sink output = frame -> {
            digest.update(frame);
            records[0]++;
        };
        ResidentStreamCdcEnumerator enumerator =
                new ResidentStreamCdcEnumerator(
                        StreamCdcEnumeratorState.initial(
                                "resident-source", digest('b'), 0L,
                                Arrays.asList("split-0")),
                        4096, output);
        enumerator.completeSnapshotSplit("split-0");
        enumerator.completeHandoff();
        int maximumCheckpointBytes = 0;
        for (long sequence = 1L; sequence <= 20_000L; sequence++) {
            enumerator.onLogRecord(sequence, after(
                    "key-" + (sequence % 128L), "value-" + sequence));
            if (sequence % 257L == 0L) {
                byte[] checkpoint = StreamCdcEnumeratorStateCodec.encode(
                        enumerator.checkpointState());
                maximumCheckpointBytes = Math.max(
                        maximumCheckpointBytes, checkpoint.length);
                enumerator = new ResidentStreamCdcEnumerator(
                        StreamCdcEnumeratorStateCodec.decode(checkpoint),
                        4096, output);
            }
        }
        byte[] finalCheckpoint = StreamCdcEnumeratorStateCodec.encode(
                enumerator.checkpointState());
        maximumCheckpointBytes = Math.max(
                maximumCheckpointBytes, finalCheckpoint.length);
        return new StressOutcome(records[0],
                enumerator.checkpointState().lastLogSequence(),
                digest.digest(), maximumCheckpointBytes);
    }

    private static Map<String, String> materialize(List<byte[]> frames) {
        Map<String, String> result = new LinkedHashMap<>();
        for (byte[] frame : frames) {
            StreamUpsertRecord record = StreamUpsertRecordCodec.decode(
                    frame, 128, 1024);
            String key = text(record.stableKeyFrame());
            if (record.hasAfterImage()) {
                result.put(key, text(record.afterImageFrame()));
            } else {
                result.remove(key);
            }
        }
        return result;
    }

    private static StreamUpsertRecord after(String key, String value) {
        return StreamUpsertRecord.afterImage(bytes(key), bytes(value));
    }

    private static StreamUpsertRecord tombstone(String key) {
        return StreamUpsertRecord.tombstone(bytes(key));
    }

    private static void assertCode(
            StreamCdcException.Code code,
            org.junit.jupiter.api.function.Executable action) {
        StreamCdcException failure = assertThrows(
                StreamCdcException.class, action);
        assertEquals(code, failure.code(), failure.getMessage());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static final class StressOutcome {
        private final long records;
        private final long lastSequence;
        private final byte[] digest;
        private final int maximumCheckpointBytes;

        private StressOutcome(
                long records, long lastSequence,
                byte[] digest, int maximumCheckpointBytes) {
            this.records = records;
            this.lastSequence = lastSequence;
            this.digest = digest;
            this.maximumCheckpointBytes = maximumCheckpointBytes;
        }
    }
}
