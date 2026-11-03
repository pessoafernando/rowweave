package io.rowweave.stream.contract;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** typed source/state/Sink members behind digest-only authority evidence. */
class StreamCheckpointMemberContractTest {
    private static final String A = digest('a');
    private static final String B = digest('b');

    @Test
    void everyJointCheckpointMemberHasCanonicalIndependentWire() {
        StreamCheckpointMember[] members = {
                StreamCheckpointMember.sourcePosition(
                        "source", 0, "split-0", "log-position-v1", 42L, A, 16L),
                StreamCheckpointMember.stateHandle(
                        "aggregate", 1, 64, 127, "sum", "decimal-state-v1",
                        3, A, 4096L),
                StreamCheckpointMember.transactionalSink(
                        "sink", 1, "transaction-7", A, B, 128L),
                StreamCheckpointMember.eagerSinkCapability(
                        "audit-sink", 0, A, B),
                StreamCheckpointMember.sourceCoordinator(
                        "source", "source-coordinator-v2", B, 512L),
                StreamCheckpointMember.channelState(
                        "join", 1, "left-edge#0", 9L, A, 2_048L)
        };

        for (StreamCheckpointMember member : members) {
            byte[] frame = StreamCheckpointMemberCodec.encode(member);
            StreamCheckpointMember decoded =
                    StreamCheckpointMemberCodec.decode(frame);
            assertEquals(member, decoded);
            assertArrayEquals(frame, StreamCheckpointMemberCodec.encode(decoded));
            assertEquals(member.kind().evidenceKind(),
                    member.toEvidence().kind());
            assertEquals(member.memberId(), member.toEvidence().memberId());
            assertEquals(member.contentDigest(),
                    member.toEvidence().contentDigest());
        }
        assertArrayEquals("RWXXSCB1".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                StreamCheckpointMemberCodec.magic());
        assertArrayEquals("RWXXSCB2".getBytes(
                        java.nio.charset.StandardCharsets.US_ASCII),
                Arrays.copyOf(StreamCheckpointMemberCodec.encode(
                        members[members.length - 2]), 8));
        assertArrayEquals("RWXXSCB3".getBytes(
                        java.nio.charset.StandardCharsets.US_ASCII),
                Arrays.copyOf(StreamCheckpointMemberCodec.encode(
                        members[members.length - 1]), 8));
    }

    @Test
    void stableIdentityExcludesContentButBindsAllRestoreCoordinates() {
        StreamCheckpointMember first = StreamCheckpointMember.stateHandle(
                "aggregate", 1, 64, 127, "sum", "decimal-state-v1",
                3, A, 4096L);
        StreamCheckpointMember newContent = StreamCheckpointMember.stateHandle(
                "aggregate", 1, 64, 127, "sum", "decimal-state-v1",
                3, B, 8192L);
        StreamCheckpointMember otherRange = StreamCheckpointMember.stateHandle(
                "aggregate", 1, 128, 191, "sum", "decimal-state-v1",
                3, A, 4096L);

        assertEquals(first.memberId(), newContent.memberId());
        assertNotEquals(first.memberId(), otherRange.memberId());
        StreamCheckpointMember position42 = StreamCheckpointMember.sourcePosition(
                "source", 0, "split-0", "log-position-v1", 42L, A, 8L);
        StreamCheckpointMember position43 = StreamCheckpointMember.sourcePosition(
                "source", 0, "split-0", "log-position-v1", 43L, B, 8L);
        assertEquals(position42.memberId(), position43.memberId());
        assertCode("INVALID_KEY_GROUP_RANGE", () ->
                StreamCheckpointMember.stateHandle(
                        "aggregate", 1, 127, 64, "sum", "decimal-state-v1",
                        3, A, 1L));
        assertCode("INVALID_SOURCE_POSITION", () ->
                StreamCheckpointMember.sourcePosition(
                        "source", 0, "split-0", "log-position-v1", -1L, A, 1L));
    }

    @Test
    void checksumAndTruncationFailClosed() {
        byte[] frame = StreamCheckpointMemberCodec.encode(
                StreamCheckpointMember.sourcePosition(
                        "source", 0, "split-0", "log-position-v1", 1L, A, 8L));
        byte[] corrupt = frame.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertCode("CHECKSUM_MISMATCH", () ->
                StreamCheckpointMemberCodec.decode(corrupt));
        for (int length = 0; length < frame.length; length++) {
            byte[] truncated = Arrays.copyOf(frame, length);
            assertCode("FRAME_LENGTH_MISMATCH", () ->
                    StreamCheckpointMemberCodec.decode(truncated));
        }
    }

    private static void assertCode(
            String code, org.junit.jupiter.api.function.Executable action) {
        StreamContractException failure = assertThrows(StreamContractException.class, action);
        assertEquals(code, failure.code(), failure.getMessage());
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
