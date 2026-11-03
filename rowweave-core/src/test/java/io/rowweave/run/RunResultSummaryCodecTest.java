package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Arrays;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunResultSummaryCodecTest {

    private static final byte[] LEGACY_V1_RUNNING_SNAPSHOT = Base64.getDecoder().decode("UldSUwABAgAAAACwAAAACmxlZ2FjeS1ydW4AAAARbGVnYWN5LWRlZmluaXRpb24AAAAPbGVnYWN5LWFydGlmYWN0AAAAAQAAAAEAAAAAAAAAAgIAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAIBAAAAAQAAABBsZWdhY3ktYXR0ZW1wdC0xAAAAAAAAAAEAAAASbGVnYWN5LXB1YmxpY2F0aW9uAQAAAAAAAAAAAgAAAAAAAAAC//////////+ux0Sf9J8rE8tFykg398I2yoaGrnvNFgceMbUezuKxTg==");;

    @Test
    void v3TerminalSnapshotRoundTripsBoundedDiscardSummary() {
        RunResultSummary summary = RunResultSummary.discard(37L);
        RunSnapshot succeeded = succeeded(summary);

        byte[] encoded = RunStateCodec.encodeSnapshot(succeeded);
        RunSnapshot decoded = RunStateCodec.decodeSnapshot(encoded);

        assertEquals(3, ((encoded[4] & 0xff) << 8) | (encoded[5] & 0xff));
        assertEquals(37L, decoded.resultSummary().rowsOut());
        assertEquals(RunResultSummary.PublicationKind.DISCARD,
                decoded.resultSummary().publicationKind());
        assertNull(decoded.resultSummary().manifestGeneration());
        assertNull(decoded.resultSummary().manifestSha256());
        assertArrayEquals(encoded, RunStateCodec.encodeSnapshot(decoded));
    }

    @Test
    void manifestSummaryRequiresPositiveGenerationAndExactSha256() {
        byte[] sha = new byte[32];
        sha[0] = 7;
        byte[] sinkIdentity = new byte[32];
        sinkIdentity[0] = 13;
        RunResultSummary summary =
                RunResultSummary.manifest(11L, 4L, sha, sinkIdentity);

        byte[] encoded = RunStateCodec.encodeSnapshot(succeeded(summary));
        RunResultSummary decoded =
                RunStateCodec.decodeSnapshot(encoded).resultSummary();
        assertEquals(3, ((encoded[4] & 0xff) << 8) | (encoded[5] & 0xff));
        assertEquals(11L, decoded.rowsOut());
        assertEquals(Long.valueOf(4L), decoded.manifestGeneration());
        assertArrayEquals(sha, decoded.manifestSha256());
        assertArrayEquals(sinkIdentity, decoded.sinkIdentityDigest());
        sha[0] = 9;
        sinkIdentity[0] = 10;
        assertEquals(7, decoded.manifestSha256()[0]);
        assertEquals(13, decoded.sinkIdentityDigest()[0]);

        assertThrows(IllegalArgumentException.class,
                () -> RunResultSummary.manifest(1L, 0L, new byte[32]));
        assertThrows(IllegalArgumentException.class,
                () -> RunResultSummary.manifest(1L, 1L, new byte[31]));
        assertThrows(IllegalArgumentException.class,
                () -> RunResultSummary.manifest(
                        1L, 1L, new byte[32], new byte[31]));
        assertThrows(IllegalArgumentException.class,
                () -> RunResultSummary.discard(-1L));
    }

    @Test
    void legacyV1SnapshotRemainsReadableWithoutInventingSummary() {
        RunSnapshot decoded = RunStateCodec.decodeSnapshot(LEGACY_V1_RUNNING_SNAPSHOT);

        assertEquals("legacy-run", decoded.runId());
        assertEquals(AttemptStatus.CREATED, decoded.currentAttempt().status());
        assertNull(decoded.resultSummary());
    }

    @Test
    void v2ManifestSummaryRemainsReadableWithoutInventingSinkIdentity()
            throws Exception {
        byte[] encodedV3 = RunStateCodec.encodeSnapshot(succeeded(
                RunResultSummary.manifest(
                        11L, 4L, new byte[32], new byte[32])));
        int oldFramePrefix = encodedV3.length - 32 - 33;
        byte[] encodedV2 = new byte[encodedV3.length - 33];
        System.arraycopy(encodedV3, 0, encodedV2, 0, oldFramePrefix);
        encodedV2[4] = 0;
        encodedV2[5] = 2;
        int oldBodyLength = oldFramePrefix - 12;
        encodedV2[8] = (byte) (oldBodyLength >>> 24);
        encodedV2[9] = (byte) (oldBodyLength >>> 16);
        encodedV2[10] = (byte) (oldBodyLength >>> 8);
        encodedV2[11] = (byte) oldBodyLength;
        byte[] checksum = MessageDigest.getInstance("SHA-256")
                .digest(Arrays.copyOf(encodedV2, oldFramePrefix));
        System.arraycopy(checksum, 0, encodedV2, oldFramePrefix, checksum.length);

        RunResultSummary decoded =
                RunStateCodec.decodeSnapshot(encodedV2).resultSummary();
        assertEquals(Long.valueOf(4L), decoded.manifestGeneration());
        assertNull(decoded.sinkIdentityDigest());
    }

    @Test
    void commandProtocolOnlyAdvancesForNewSinkIdentityEvidence() {
        RunCommand unchanged = RunCommand.createRun(
                "create", "run", "definition", "artifact", 1, 0L, 1L);
        RunCommand upgraded = RunCommand.advanceAttempt(
                "succeeded", "run", 5L, "attempt", 1L,
                AttemptStatus.SUCCEEDED, false, 6L, null, null,
                RunResultSummary.manifest(
                        1L, 2L, new byte[32], new byte[32]));

        assertEquals(2, unchanged.canonicalVersion);
        assertEquals(3, upgraded.canonicalVersion);
    }

    @Test
    void succeededMutationRequiresSummaryAndNonSucceededMutationRejectsIt() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot committing = committing(store, "required");

        assertThrows(IllegalArgumentException.class, () -> RunCommand.advanceAttempt(
                "missing-summary", committing.runId(), committing.revision(),
                committing.currentAttempt().attemptId(), committing.currentAttempt().fenceToken(),
                AttemptStatus.SUCCEEDED, false, 6L, null, null));
        assertThrows(IllegalArgumentException.class, () -> RunCommand.advanceAttempt(
                "wrong-phase", committing.runId(), committing.revision(),
                committing.currentAttempt().attemptId(), committing.currentAttempt().fenceToken(),
                AttemptStatus.FAILED, false, 6L, null, null,
                RunResultSummary.discard(1L)));
    }

    @Test
    void terminalEvidenceCountsOnlyOutputRolesAndRejectsOverflow() {
        RunAttemptFence fence = new RunAttemptFence("sum-run", "sum-attempt", 1L);
        StageAssignmentRecord sink0 = StageAssignmentRecord.create(
                fence, 1L, 0, 10L, 0, 0, StageAssignmentRole.SINK,
                "worker-0", "127.0.0.1", 10001, -1, "inc-0");
        StageAssignmentRecord sink1 = StageAssignmentRecord.create(
                fence, 1L, 1, 11L, 0, 1, StageAssignmentRole.SINK,
                "worker-1", "127.0.0.1", 10002, -1, "inc-1");
        StageAssignmentSnapshot evidence = StageAssignmentSnapshot.initial(
                new StageAssignmentManifest(fence, "artifact", 1L,
                        Arrays.asList(sink0, sink1)));
        byte[] digest0 = new byte[32];
        byte[] digest1 = new byte[32];
        digest1[0] = 1;
        evidence = evidence.withAcceptance(new StageAssignmentAcceptance(
                evidence.token(0), digest0, 1L));
        evidence = evidence.withAcceptance(new StageAssignmentAcceptance(
                evidence.token(1), digest1, 1L));
        evidence = evidence.withTerminal(StageTerminalReceipt.succeeded(
                evidence.token(0), digest0, 0L, Long.MAX_VALUE, 2L));
        evidence = evidence.withTerminal(StageTerminalReceipt.succeeded(
                evidence.token(1), digest1, 0L, 1L, 2L));

        StageAssignmentSnapshot overflow = evidence;
        assertThrows(ArithmeticException.class,
                () -> RunResultSummary.discardFrom(overflow));
    }

    private static RunSnapshot succeeded(RunResultSummary summary) {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot committing = committing(store, "round-trip");
        return store.apply(RunCommand.advanceAttempt(
                "succeeded", committing.runId(), committing.revision(),
                committing.currentAttempt().attemptId(), committing.currentAttempt().fenceToken(),
                AttemptStatus.SUCCEEDED, false, 6L, null, null, summary)).snapshot();
    }

    private static RunSnapshot committing(InMemoryRunStateStore store, String suffix) {
        String runId = "run-" + suffix;
        RunSnapshot run = store.apply(RunCommand.createRun(
                "create-" + suffix, runId, "definition", "artifact", 1, 0L, 1L)).snapshot();
        run = store.apply(RunCommand.createAttempt(
                "attempt-" + suffix, runId, run.revision(),
                "attempt-1", "publication-1", 2L)).snapshot();
        run = advance(store, run, AttemptStatus.DISPATCHING, 3L);
        run = advance(store, run, AttemptStatus.RUNNING, 4L);
        return advance(store, run, AttemptStatus.COMMITTING, 5L);
    }

    private static RunSnapshot advance(InMemoryRunStateStore store, RunSnapshot run,
                                       AttemptStatus status, long time) {
        return store.apply(RunCommand.advanceAttempt(
                "to-" + status, run.runId(), run.revision(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken(),
                status, false, time, null, null)).snapshot();
    }
}
