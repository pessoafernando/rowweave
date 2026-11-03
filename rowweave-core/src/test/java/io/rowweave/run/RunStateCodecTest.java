package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunStateCodecTest {

    @Test
    void allFrameKindsRoundTripDeterministically() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunCommand create = RunCommand.createRun(
                "command-create", "run", "definition", "artifact", 3, 100L, 1L);
        RunDecision created = store.apply(create);
        RunCommand attempt = RunCommand.createAttempt(
                "command-attempt", "run", created.snapshot().revision(),
                "attempt-1", "publication-1", 2L);
        RunDecision attempted = store.apply(attempt);

        byte[] commandBytes = RunStateCodec.encodeCommand(attempt);
        RunCommand decodedCommand = RunStateCodec.decodeCommand(commandBytes);
        assertEquals(attempt.commandId(), decodedCommand.commandId());
        assertArrayEquals(attempt.commandDigest(), decodedCommand.commandDigest());
        assertArrayEquals(commandBytes, RunStateCodec.encodeCommand(decodedCommand));

        byte[] snapshotBytes = RunStateCodec.encodeSnapshot(attempted.snapshot());
        RunSnapshot decodedSnapshot = RunStateCodec.decodeSnapshot(snapshotBytes);
        assertEquals(attempted.snapshot().runId(), decodedSnapshot.runId());
        assertEquals(attempted.snapshot().revision(), decodedSnapshot.revision());
        assertEquals(attempted.snapshot().currentAttempt().publicationId(),
                decodedSnapshot.currentAttempt().publicationId());
        assertArrayEquals(snapshotBytes, RunStateCodec.encodeSnapshot(decodedSnapshot));

        byte[] eventBytes = RunStateCodec.encodeEvent(attempted.event());
        ExecutionEvent decodedEvent = RunStateCodec.decodeEvent(eventBytes);
        assertEquals(attempted.event().sequence(), decodedEvent.sequence());
        assertEquals(attempted.event().commandId(), decodedEvent.commandId());
        assertArrayEquals(attempted.event().commandDigest(), decodedEvent.commandDigest());
        assertArrayEquals(eventBytes, RunStateCodec.encodeEvent(decodedEvent));

        RunJournalRecord record = new RunJournalRecord(attempt, attempted.snapshot(), attempted.event());
        byte[] recordBytes = RunStateCodec.encodeRecord(record);
        RunJournalRecord decodedRecord = RunStateCodec.decodeRecord(recordBytes);
        assertEquals(attempt.commandId(), decodedRecord.command().commandId());
        assertEquals(attempted.snapshot().revision(), decodedRecord.snapshot().revision());
        assertEquals(attempted.event().sequence(), decodedRecord.event().sequence());
        assertArrayEquals(recordBytes, RunStateCodec.encodeRecord(decodedRecord));
    }

    @Test
    void everyTruncatedPrefixAndTrailingByteIsRejected() {
        byte[] encoded = RunStateCodec.encodeCommand(RunCommand.createRun(
                "command", "run", "definition", "artifact", 2, 0L, 1L));
        for (int length = 0; length < encoded.length; length++) {
            final byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(IllegalArgumentException.class,
                    () -> RunStateCodec.decodeCommand(truncated), "length=" + length);
        }
        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        assertThrows(IllegalArgumentException.class, () -> RunStateCodec.decodeCommand(trailing));
    }

    @Test
    void checksumUnknownHeaderAndOversizedLengthFailClosed() throws Exception {
        byte[] encoded = RunStateCodec.encodeCommand(RunCommand.createRun(
                "command", "run", "definition", "artifact", 2, 0L, 1L));

        byte[] corrupt = encoded.clone();
        corrupt[16] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> RunStateCodec.decodeCommand(corrupt));

        byte[] unknownVersion = encoded.clone();
        unknownVersion[5] = 4;
        rewriteChecksum(unknownVersion);
        assertThrows(IllegalArgumentException.class, () -> RunStateCodec.decodeCommand(unknownVersion));

        byte[] unknownKind = encoded.clone();
        unknownKind[6] = 99;
        rewriteChecksum(unknownKind);
        assertThrows(IllegalArgumentException.class, () -> RunStateCodec.decodeCommand(unknownKind));

        byte[] oversized = encoded.clone();
        oversized[8] = 0x7f;
        oversized[9] = (byte) 0xff;
        oversized[10] = (byte) 0xff;
        oversized[11] = (byte) 0xff;
        assertThrows(IllegalArgumentException.class, () -> RunStateCodec.decodeCommand(oversized));
    }

    @Test
    void recordRejectsMismatchedCommandSnapshotAndEvent() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunCommand command = RunCommand.createRun(
                "command", "run", "definition", "artifact", 2, 0L, 1L);
        RunDecision decision = store.apply(command);
        RunCommand different = RunCommand.createRun(
                "different", "run", "definition", "artifact", 2, 0L, 1L);

        assertThrows(IllegalArgumentException.class,
                () -> new RunJournalRecord(different, decision.snapshot(), decision.event()));
    }

    private static void rewriteChecksum(byte[] frame) throws Exception {
        int checksumLength = 32;
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(Arrays.copyOf(frame, frame.length - checksumLength));
        System.arraycopy(digest, 0, frame, frame.length - checksumLength, checksumLength);
    }
}
