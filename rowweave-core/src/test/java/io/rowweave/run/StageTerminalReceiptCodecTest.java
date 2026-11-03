package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.DurableSinkProviderKey;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.TelemetryCoverage;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** canonical bounded terminal evidence must remain acceptance-scoped. */
class StageTerminalReceiptCodecTest {

    private static final String V1_ROOT ="UldBTQABAAEAAABuAAAADnJ1bi12MS1maXh0dXJlAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAAC2FydGlmYWN0LXYxAAAAAAAAAAMAAAAAAAAAAQAAAAEAAAABgWlFaD6MTa6vT/ob5r0R28CAxAhLfHdIBeF4b+eqo3V1Ewxc+/vSayOOtHmuVqWGk2+8zZ5Khmaso22uWdRQsw==";
    private static final String V1_SHARD ="UldBTQABAAIAAADQAAAADnJ1bi12MS1maXh0dXJlAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAAC2FydGlmYWN0LXYxAAAAAAAAAAMAAAABAAAAAQAAAAAAAAABAAAAQDM3Y2VhODNkMmNkZDRlYjRkNzU0NjVkNGJlOWU1YjM5NmNjMzVhZTUxNmEzZTJlNWU4N2ZmZjM4ZmRjZDQ2ZWIAAAAAAAAAAAAAAGUAAAAAAAAAAAgAAAAId29ya2VyLWEAAAAIMTAuMC4wLjEAABu9AAAfpQAAAAVpbmMtYcJHzwcn9GoZnXkAEo6JzUso6goF+UhPB6Y9XfZWaTxA";
    private static final String V1_ACCEPTANCE ="UldBTQABAAQAAAFjAAAADnJ1bi12MS1maXh0dXJlAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAAC2FydGlmYWN0LXYxAAAAAAAAAAMAAAABAAAAAQAAAAAAAAABAQAAAOhSV0FNAAEAAwAAALwAAAAOcnVuLXYxLWZpeHR1cmUAAAAJYXR0ZW1wdC0xAAAAAAAAAAcAAAAAAAAAAwAAAAAAAAABAAAAAAAAAEAzN2NlYTgzZDJjZGQ0ZWI0ZDc1NDY1ZDRiZTllNWIzOTZjYzM1YWU1MTZhM2UyZTVlODdmZmYzOGZkY2Q0NmViAAAAAAAAAGUAAAAId29ya2VyLWEAAAAFaW5jLWE6yTWeIeH1h0soaJx0jLfCD4Z9MpJrqyZ5dq4/uAml2bTFyVoS4zdpLB0u3R9+MswKVSMn+msyvDnj5qEkeh/M/JmmJ1vOWKfHkJnJd3tjZm2XJZoFIYIlM1Bbp3m+V34AAAAAAAAAC/6uyD9VGoUorqRJ7Jq57zQYF2hSLdEUipXxP23l22AI";
    private static final String V1_TERMINAL_SHARD ="UldBTQABAAUAAAF5AAAADnJ1bi12MS1maXh0dXJlAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAAC2FydGlmYWN0LXYxAAAAAAAAAAMAAAABAAAAAQAAAAAAAAABAQAAAOhSV0FNAAEAAwAAALwAAAAOcnVuLXYxLWZpeHR1cmUAAAAJYXR0ZW1wdC0xAAAAAAAAAAcAAAAAAAAAAwAAAAAAAAABAAAAAAAAAEAzN2NlYTgzZDJjZGQ0ZWI0ZDc1NDY1ZDRiZTllNWIzOTZjYzM1YWU1MTZhM2UyZTVlODdmZmYzOGZkY2Q0NmViAAAAAAAAAGUAAAAId29ya2VyLWEAAAAFaW5jLWE6yTWeIeH1h0soaJx0jLfCD4Z9MpJrqyZ5dq4/uAml2bTFyVoS4zdpLB0u3R9+MswKVSMn+msyvDnj5qEkeh/M/JmmJ1vOWKfHkJnJd3tjZm2XJZoFIYIlM1Bbp3m+V34BAAAAAAAAAA0AAAAAAAAAEf////8AAAAAAAAAABMIvmTzwKUiEQERQ0Ib38P3+3QkqdO6dAHjiSXfMSb9zQ==";

    private static final String V3_ROOT ="UldBTQADAAEAAABwAAAADnJ1bi10ZTQtZ29sZGVuAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAADGFydGlmYWN0LXRlNAAAAAAAAAADAAAAAAAAAAABAAAAAQAAAAGOag7LD0SxkwoBdro/Tp2Lv4j/GDN4wL1ABA3xI0zfDhfdvAlkc1isGfS4yFrAEjJCkylXc+rC2bMBKBBSE9do";
    private static final String V3_SHARD ="UldBTQADAAIAAADXAAAADnJ1bi10ZTQtZ29sZGVuAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAADGFydGlmYWN0LXRlNAAAAAAAAAADAAAAAQAAAAEAAAAAAAAAAQAAAEA0ZGMxMTQxN2IzYzMwYmE1ZWJmNjVlYTFjOTZhNmRlMzNiYTQxMTc3MDEyOWQzYWUwYmE0MWVlZGI1NzQyZWMwAAAAAAAAAAAAAABlAAAAAgAAAAEEAAAACHdvcmtlci0xAAAACTEyNy4wLjAuMQAAF9UAABu9AAAABWluYy0xAAAAAAAmgtBuSKnmmq33xIqOkumXwY7Tezg8mUwE7VF7f47gsA==";
    private static final String V3_ACCEPTANCE ="UldBTQADAAQAAAGnAAAADnJ1bi10ZTQtZ29sZGVuAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAADGFydGlmYWN0LXRlNAAAAAAAAAADAAAAAQAAAAEAAAAAAAAAAQEAAAErUldBTQACAAMAAAD/AAAADnJ1bi10ZTQtZ29sZGVuAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAAAAAAAAMAAAAAAAAAAQAAAAAAAABANGRjMTE0MTdiM2MzMGJhNWViZjY1ZWExYzk2YTZkZTMzYmE0MTE3NzAxMjlkM2FlMGJhNDFlZWRiNTc0MmVjMAAAAAAAAABlAAAACHdvcmtlci0xAAAABWluYy0xAAAAAgAAAAEEAAAACTEyNy4wLjAuMQAAF9UAABu9AAAAAAC1wZNornK592y9dgVjJ6beRYEGG78dp6h5gqXO6hLOmYgwbb7JS5wmYrpVrb0XICqjGnLqMEpzwsP7t9fpfuRH8oWgbm4FHKDVWYVVqwI9Z9eN+wzgT9VNW9IVf69yX1/hLBV8AFHzmdbss8A6GR1t0Zn1iry0zxiU2y/DDzMd3wAAAAAAAAALes32Hq7LFZYFWF46FJcIdkQBkGmgycL6KkVyYo53s5A=";

    private static final String V3_TERMINAL_SHARD ="UldBTQADAAUAAAG9AAAADnJ1bi10ZTQtZ29sZGVuAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAADGFydGlmYWN0LXRlNAAAAAAAAAADAAAAAQAAAAEAAAAAAAAAAQEAAAErUldBTQACAAMAAAD/AAAADnJ1bi10ZTQtZ29sZGVuAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAAAAAAAAMAAAAAAAAAAQAAAAAAAABANGRjMTE0MTdiM2MzMGJhNWViZjY1ZWExYzk2YTZkZTMzYmE0MTE3NzAxMjlkM2FlMGJhNDFlZWRiNTc0MmVjMAAAAAAAAABlAAAACHdvcmtlci0xAAAABWluYy0xAAAAAgAAAAEEAAAACTEyNy4wLjAuMQAAF9UAABu9AAAAAAC1wZNornK592y9dgVjJ6beRYEGG78dp6h5gqXO6hLOmYgwbb7JS5wmYrpVrb0XICqjGnLqMEpzwsP7t9fpfuRH8oWgbm4FHKDVWYVVqwI9Z9eN+wzgT9VNW9IVf69yX1/hLBV8AFHzmdbss8A6GR1t0Zn1iry0zxiU2y/DDzMd3wEAAAAAAAAADQAAAAAAAAAR/////wAAAAAAAAAAE4vqsCfTKgRyHnkY56CFHU5LAH8vDyYL2fnJ5BbdpLCr";

    @Test
    void terminalReceiptsRoundTripWithTheExactAcceptedAssignments() throws Exception {
        StageAssignmentManifest manifest = manifest(2);
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(manifest);
        byte[] digest0 = sha256("command-0");
        byte[] digest1 = sha256("command-1");
        snapshot = snapshot.withAcceptance(new StageAssignmentAcceptance(
                snapshot.token(0), digest0, 11L));
        snapshot = snapshot.withAcceptance(new StageAssignmentAcceptance(
                snapshot.token(1), digest1, 12L));
        snapshot = snapshot.withTerminal(StageTerminalReceipt.succeeded(
                snapshot.token(0), digest0, 7L, 5L, 21L));
        snapshot = snapshot.withTerminal(StageTerminalReceipt.failed(
                snapshot.token(1), digest1, 9L, 0L,
                "USER_CODE", sha256("bounded failure detail"), 22L));

        byte[] root = StageAssignmentManifestCodec.encodeRoot(
                manifest, snapshot.acceptanceGeneration());
        List<byte[]> assignmentShards = assignmentShards(manifest);
        List<byte[]> acceptanceShards = acceptanceShards(snapshot);
        List<byte[]> terminalShards = terminalShards(snapshot);

        StageAssignmentSnapshot decoded = StageAssignmentManifestCodec.decodeSnapshot(
                root, assignmentShards, acceptanceShards, terminalShards);

        assertEquals(2, decoded.acceptedCount());
        assertEquals(2, decoded.terminalCount());
        assertTrue(decoded.allTerminal());
        assertEquals(StageTerminalOutcome.SUCCEEDED, decoded.terminal(0).outcome());
        assertEquals(5L, decoded.terminal(0).outputRows());
        assertEquals(StageTerminalOutcome.FAILED, decoded.terminal(1).outcome());
        assertEquals("USER_CODE", decoded.terminal(1).failureCategory());
        assertArrayEquals(sha256("bounded failure detail"),
                decoded.terminal(1).failureDigest());
    }

    @Test
    void terminalShardV5PersistsTelemetryButStandaloneReceiptRemainsV3() throws Exception {
        StageAssignmentManifest manifest = manifest(1);
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(manifest);
        byte[] digest = sha256("command-telemetry");
        snapshot = snapshot.withAcceptance(new StageAssignmentAcceptance(
                snapshot.token(0), digest, 11L));
        StageExecutionTelemetry telemetry = StageExecutionTelemetry.complete(
                101L, 202L, 3L, 4L, 505L, 606L);
        snapshot = snapshot.withTerminal(StageTerminalReceipt.succeeded(
                snapshot.token(0), digest, 13L, 17L, 19L, telemetry));

        byte[] root = StageAssignmentManifestCodec.encodeRoot(
                manifest, snapshot.acceptanceGeneration());
        byte[] assignment = StageAssignmentManifestCodec.encodeShard(manifest, 0);
        byte[] acceptance = StageAssignmentManifestCodec.encodeAcceptanceShard(snapshot, 0);
        byte[] terminal = StageAssignmentManifestCodec.encodeTerminalShard(snapshot, 0);
        byte[] standalone = StageAssignmentManifestCodec.encodeTerminalReceipt(
                snapshot.terminal(0));

        assertEquals(5, frameVersion(terminal));
        assertEquals(3, frameVersion(root));
        assertEquals(3, frameVersion(assignment));
        assertEquals(3, frameVersion(acceptance));
        assertEquals(3, frameVersion(standalone));
        StageAssignmentSnapshot decoded = StageAssignmentManifestCodec.decodeSnapshot(
                root, Collections.singletonList(assignment),
                Collections.singletonList(acceptance),
                Collections.singletonList(terminal));
        assertEquals(telemetry, decoded.terminal(0).telemetry());
        assertEquals(TelemetryCoverage.UNAVAILABLE,
                StageAssignmentManifestCodec.decodeTerminalReceipt(standalone)
                        .telemetry().coverage());
    }

    @Test
    void terminalShardV5RoundTripsPreparedSinkWithoutChangingStandaloneReceipt()
            throws Exception {
        StageAssignmentManifest manifest = manifest(1);
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(manifest);
        byte[] digest = sha256("command-prepared");
        snapshot = snapshot.withAcceptance(new StageAssignmentAcceptance(
                snapshot.token(0), digest, 11L));
        snapshot = snapshot.withTerminal(StageTerminalReceipt.succeeded(
                snapshot.token(0), digest, 13L, 17L, 19L));
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1,
                SchemaFingerprint.of(new DataSchema(
                        new Field("id", DataType.LongType))),
                "domain:member", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                manifest, manifest.assignment(0), "sink-op", descriptor);
        PreparedSinkReceipt prepared = new PreparedSinkReceipt(
                snapshot.token(0), digest, new PreparedSinkLocator(
                PreparedSinkLocator.CURRENT_VERSION, identity,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", new byte[]{1, 2, 3}, 3L,
                sha256("data-hash"), 23L));
        snapshot = snapshot.withPreparedSink(prepared);

        byte[] root = StageAssignmentManifestCodec.encodeRoot(
                manifest, snapshot.acceptanceGeneration());
        byte[] assignment = StageAssignmentManifestCodec.encodeShard(manifest, 0);
        byte[] acceptance = StageAssignmentManifestCodec.encodeAcceptanceShard(snapshot, 0);
        byte[] terminal = StageAssignmentManifestCodec.encodeTerminalShard(snapshot, 0);
        StageAssignmentSnapshot decoded = StageAssignmentManifestCodec.decodeSnapshot(
                root, Collections.singletonList(assignment),
                Collections.singletonList(acceptance),
                Collections.singletonList(terminal));

        assertEquals(5, frameVersion(terminal));
        assertEquals(prepared, decoded.preparedSink(0));
        assertEquals(3, frameVersion(StageAssignmentManifestCodec
                .encodeTerminalReceipt(snapshot.terminal(0))));
    }

    @Test
    void v3TerminalShardGoldenRemainsReadableAsTelemetryUnavailable() {
        StageAssignmentSnapshot decoded = StageAssignmentManifestCodec.decodeSnapshot(
                Base64.getDecoder().decode(V3_ROOT),
                Collections.singletonList(Base64.getDecoder().decode(V3_SHARD)),
                Collections.singletonList(Base64.getDecoder().decode(V3_ACCEPTANCE)),
                Collections.singletonList(Base64.getDecoder().decode(V3_TERMINAL_SHARD)));

        assertEquals(1, decoded.terminalCount());
        assertEquals(17L, decoded.terminal(0).outputRows());
        assertEquals(TelemetryCoverage.UNAVAILABLE,
                decoded.terminal(0).telemetry().coverage());
    }

    @Test
    void v1TerminalEvidenceUsesManifestIdentityAndRemainsTelemetryUnavailable()
            throws Exception {
        byte[] root = Base64.getDecoder().decode(V1_ROOT);
        byte[] shard = Base64.getDecoder().decode(V1_SHARD);
        byte[] acceptance = Base64.getDecoder().decode(V1_ACCEPTANCE);
        byte[] terminal = Base64.getDecoder().decode(V1_TERMINAL_SHARD);

        StageAssignmentSnapshot decoded = StageAssignmentManifestCodec.decodeSnapshot(
                root, Collections.singletonList(shard),
                Collections.singletonList(acceptance),
                Collections.singletonList(terminal));
        assertEquals(1, decoded.terminalCount());
        assertEquals(0, decoded.manifest().assignment(0).logicalStage());
        assertEquals(StageAssignmentRole.SOURCE_STAGE,
                decoded.manifest().assignment(0).role());
        assertEquals(-1, decoded.terminal(0).token().logicalStage(),
                "v1 token itself has no logical identity");
        assertEquals(17L, decoded.terminal(0).outputRows());
        assertEquals(TelemetryCoverage.UNAVAILABLE,
                decoded.terminal(0).telemetry().coverage());

        byte[] wrongAcceptance = acceptance.clone();
        byte[] digest = sha256("v1-command");
        int digestOffset = indexOf(wrongAcceptance, digest);
        assertTrue(digestOffset > 0);
        wrongAcceptance[digestOffset] ^= 1;
        rewriteFrameChecksum(wrongAcceptance);
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decodeSnapshot(
                        root, Collections.singletonList(shard),
                        Collections.singletonList(wrongAcceptance),
                        Collections.singletonList(terminal)));
    }

    @Test
    void versionFourIsAcceptedOnlyForTerminalShards() throws Exception {
        StageAssignmentManifest manifest = manifest(1);
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(manifest);
        byte[] digest = sha256("command-version-ceiling");
        snapshot = snapshot.withAcceptance(new StageAssignmentAcceptance(
                snapshot.token(0), digest, 1L));
        snapshot = snapshot.withTerminal(StageTerminalReceipt.succeeded(
                snapshot.token(0), digest, 1L, 1L, 2L,
                StageExecutionTelemetry.complete(1L, 2L, 3L, 4L, 5L, 6L)));
        byte[] root = StageAssignmentManifestCodec.encodeRoot(
                manifest, snapshot.acceptanceGeneration());
        byte[] shard = StageAssignmentManifestCodec.encodeShard(manifest, 0);
        byte[] acceptance = StageAssignmentManifestCodec.encodeAcceptanceShard(snapshot, 0);
        byte[] token = StageAssignmentManifestCodec.encodeToken(snapshot.token(0));
        byte[] receipt = StageAssignmentManifestCodec.encodeTerminalReceipt(snapshot.terminal(0));

        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decode(
                        rewriteFrameVersion(root, 4), Collections.singletonList(shard)));
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decode(
                        root, Collections.singletonList(rewriteFrameVersion(shard, 4))));
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decodeSnapshot(
                        root, Collections.singletonList(shard),
                        Collections.singletonList(rewriteFrameVersion(acceptance, 4))));
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decodeToken(rewriteFrameVersion(token, 4)));
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decodeTerminalReceipt(
                        rewriteFrameVersion(receipt, 4)));
    }

    @Test
    void oneShardUpdateRequiresAcceptanceAndRejectsASecondTerminal() throws Exception {
        StageAssignmentManifest manifest = manifest(1);
        StageAssignmentSnapshot empty = StageAssignmentSnapshot.initial(manifest);
        byte[] commandDigest = sha256("command");
        StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                empty.token(0), commandDigest, 1L, 1L, 2L);

        assertThrows(IllegalArgumentException.class, () -> empty.withTerminal(terminal));

        StageAssignmentSnapshot accepted = empty.withAcceptance(
                new StageAssignmentAcceptance(empty.token(0), commandDigest, 1L));
        byte[] root = StageAssignmentManifestCodec.encodeRoot(
                manifest, accepted.acceptanceGeneration());
        StageAssignmentManifestCodec.ShardSnapshot shard =
                StageAssignmentManifestCodec.decodeShardSnapshot(
                        root,
                        StageAssignmentManifestCodec.encodeShard(manifest, 0),
                        StageAssignmentManifestCodec.encodeAcceptanceShard(accepted, 0),
                        StageAssignmentManifestCodec.encodeTerminalShard(accepted, 0), 0);

        byte[] updated = StageAssignmentManifestCodec.encodeTerminalShard(shard, terminal);
        StageAssignmentSnapshot decoded = StageAssignmentManifestCodec.decodeSnapshot(
                root,
                Collections.singletonList(StageAssignmentManifestCodec.encodeShard(manifest, 0)),
                Collections.singletonList(StageAssignmentManifestCodec.encodeAcceptanceShard(accepted, 0)),
                Collections.singletonList(updated));
        assertEquals(terminal, decoded.terminal(0));
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentManifestCodec.encodeTerminalShard(shard.withTerminal(terminal), terminal));
    }

    @Test
    void receiptIdentityDigestAndOutcomeInvariantsFailClosed() throws Exception {
        StageAssignmentManifest manifest = manifest(2);
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(manifest);
        byte[] digest = sha256("command");
        snapshot = snapshot.withAcceptance(new StageAssignmentAcceptance(
                snapshot.token(0), digest, 1L));

        StageTerminalReceipt wrongOrdinal = StageTerminalReceipt.succeeded(
                snapshot.token(1), digest, 0L, 0L, 2L);
        StageAssignmentSnapshot acceptedSnapshot = snapshot;
        assertThrows(IllegalArgumentException.class,
                () -> acceptedSnapshot.withTerminal(wrongOrdinal));
        assertThrows(IllegalArgumentException.class, () -> StageTerminalReceipt.succeeded(
                acceptedSnapshot.token(0), new byte[31], 0L, 0L, 2L));
        assertThrows(IllegalArgumentException.class, () -> StageTerminalReceipt.failed(
                acceptedSnapshot.token(0), digest, 0L, 0L,
                null, sha256("failure"), 2L));
        assertThrows(IllegalArgumentException.class, () -> StageTerminalReceipt.failed(
                acceptedSnapshot.token(0), digest, 0L, 0L,
                "FAIL", null, 2L));
        assertThrows(IllegalArgumentException.class, () -> StageTerminalReceipt.lost(
                acceptedSnapshot.token(0), digest, "WORKER_LOST", sha256("lost"), 1L));

        StageTerminalReceipt lost = StageTerminalReceipt.lost(
                acceptedSnapshot.token(0), digest, "WORKER_LOST", sha256("lost"), 0L);
        assertEquals(StageTerminalOutcome.LOST, lost.outcome());
        assertEquals(0L, lost.completedAtMillis());
        assertFalse(lost.succeeded());
        assertNull(StageTerminalReceipt.succeeded(
                acceptedSnapshot.token(0), digest, 0L, 0L, 2L).failureCategory());
    }

    @Test
    void terminalShardRejectsCorruptionTruncationAndWrongCommandDigest() throws Exception {
        StageAssignmentManifest manifest = manifest(1);
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(manifest);
        byte[] digest = sha256("command");
        snapshot = snapshot.withAcceptance(new StageAssignmentAcceptance(
                snapshot.token(0), digest, 1L));
        snapshot = snapshot.withTerminal(StageTerminalReceipt.canceled(
                snapshot.token(0), digest, 3L, 0L,
                "LEADER_CHANGED", sha256("cancel"), 4L));
        byte[] root = StageAssignmentManifestCodec.encodeRoot(
                manifest, snapshot.acceptanceGeneration());
        byte[] assignment = StageAssignmentManifestCodec.encodeShard(manifest, 0);
        byte[] acceptance = StageAssignmentManifestCodec.encodeAcceptanceShard(snapshot, 0);
        byte[] terminal = StageAssignmentManifestCodec.encodeTerminalShard(snapshot, 0);

        byte[] corrupt = terminal.clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decodeShardSnapshot(
                        root, assignment, acceptance, corrupt, 0));
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decodeShardSnapshot(
                        root, assignment, acceptance,
                        Arrays.copyOf(terminal, terminal.length - 1), 0));

        StageAssignmentSnapshot wrongDigest = StageAssignmentSnapshot.initial(manifest)
                .withAcceptance(new StageAssignmentAcceptance(
                        StageAssignmentSnapshot.initial(manifest).token(0),
                        sha256("other-command"), 1L));
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decodeShardSnapshot(
                        root, assignment,
                        StageAssignmentManifestCodec.encodeAcceptanceShard(wrongDigest, 0),
                        terminal, 0));

        byte[] unknownOutcome = terminal.clone();
        int commandDigestOffset = indexOf(unknownOutcome, digest);
        assertTrue(commandDigestOffset > 0);
        unknownOutcome[commandDigestOffset + digest.length] = 99;
        rewriteFrameChecksum(unknownOutcome);
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decodeShardSnapshot(
                        root, assignment, acceptance, unknownOutcome, 0));
    }

    @Test
    void fullTerminalShardStaysBoundedAndFailureCategoryHasAHardLimit()
            throws Exception {
        String maxId = String.join("", Collections.nCopies(RunText.MAX_ID_BYTES, "x"));
        RunAttemptFence maxFence = new RunAttemptFence(maxId, maxId, Long.MAX_VALUE);
        List<StageAssignmentRecord> maxRecords = new ArrayList<>();
        for (int i = 0; i < StageAssignmentManifest.MAX_RECORDS_PER_SHARD; i++) {
            maxRecords.add(StageAssignmentRecord.decoded(
                    maxFence, Long.MAX_VALUE,
                    StageAssignmentRecord.deriveExecutionId(maxFence, Long.MAX_VALUE, i),
                    i, Long.MAX_VALUE - i, Integer.MAX_VALUE - i, Integer.MAX_VALUE - i,
                    StageAssignmentRole.WHOLE_GRAPH, maxId, maxId, 65535, 65535, maxId,
                    StageAssignmentManifest.MAX_ASSIGNMENTS, sha256("ownership-" + i)));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                maxFence, maxId, Long.MAX_VALUE, maxRecords);
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(manifest);
        String maxCategory = String.join("", Collections.nCopies(
                RunText.MAX_REASON_CODE_BYTES, "x"));
        for (int i = 0; i < manifest.assignmentCount(); i++) {
            byte[] digest = sha256("command-" + i);
            snapshot = snapshot.withAcceptance(new StageAssignmentAcceptance(
                    snapshot.token(i), digest, i));
            snapshot = snapshot.withTerminal(StageTerminalReceipt.failed(
                    snapshot.token(i), digest, Long.MAX_VALUE, Long.MAX_VALUE,
                    maxCategory, sha256("failure-" + i), Long.MAX_VALUE,
                    StageExecutionTelemetry.complete(
                            Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
                            Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)));
        }
        byte[] encoded = StageAssignmentManifestCodec.encodeTerminalShard(snapshot, 0);
        assertTrue(encoded.length <= StageAssignmentManifestCodec.MAX_SHARD_FRAME_BYTES);

        String tooLong = String.join("", Collections.nCopies(
                RunText.MAX_REASON_CODE_BYTES + 1, "x"));
        StageAssignmentSnapshot finalSnapshot = snapshot;
        assertThrows(IllegalArgumentException.class, () -> StageTerminalReceipt.failed(
                finalSnapshot.token(0), sha256("command-0"), 0L, 0L,
                tooLong, sha256("failure"), 1L));
    }

    @Test
    void fullPreparedTerminalShardSupportsEveryMaxPayloadLocator() throws Exception {
        String maxId = String.join("", Collections.nCopies(RunText.MAX_ID_BYTES, "x"));
        RunAttemptFence maxFence = new RunAttemptFence(maxId, maxId, Long.MAX_VALUE);
        List<StageAssignmentRecord> maxRecords = new ArrayList<>();
        for (int i = 0; i < StageAssignmentManifest.MAX_RECORDS_PER_SHARD; i++) {
            maxRecords.add(StageAssignmentRecord.decoded(
                    maxFence, Long.MAX_VALUE,
                    StageAssignmentRecord.deriveExecutionId(maxFence, Long.MAX_VALUE, i),
                    i, Long.MAX_VALUE - i, Integer.MAX_VALUE - i,
                    Integer.MAX_VALUE - i, StageAssignmentRole.WHOLE_GRAPH,
                    maxId, maxId, 65535, 65535, maxId,
                    StageAssignmentManifest.MAX_ASSIGNMENTS,
                    sha256("ownership-" + i)));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                maxFence, maxId, Long.MAX_VALUE, maxRecords);
        StageAssignmentSnapshot snapshot = StageAssignmentSnapshot.initial(manifest);
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, String.join("", Collections.nCopies(
                        BatchSinkDescriptor.MAX_CONNECTOR_TYPE_BYTES, "p")),
                Integer.MAX_VALUE,
                String.join("", Collections.nCopies(64, "a")),
                String.join("", Collections.nCopies(
                        BatchSinkDescriptor.MAX_DESTINATION_ID_BYTES, "d")),
                String.join("", Collections.nCopies(
                        BatchSinkDescriptor.MAX_COMMIT_DOMAIN_ID_BYTES, "c")),
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        byte[] maxPayload = new byte[PreparedSinkLocator.MAX_PAYLOAD_BYTES];
        Arrays.fill(maxPayload, (byte) 7);
        for (int i = 0; i < manifest.assignmentCount(); i++) {
            byte[] digest = sha256("command-" + i);
            snapshot = snapshot.withAcceptance(new StageAssignmentAcceptance(
                    snapshot.token(i), digest, Long.MAX_VALUE));
            snapshot = snapshot.withTerminal(StageTerminalReceipt.succeeded(
                    snapshot.token(i), digest, Long.MAX_VALUE, Long.MAX_VALUE,
                    Long.MAX_VALUE));
            SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                    manifest, manifest.assignment(i),
                    String.join("", Collections.nCopies(
                            SinkAttemptIdentity.MAX_OPERATOR_ID_BYTES, "o")),
                    descriptor);
            snapshot = snapshot.withPreparedSink(new PreparedSinkReceipt(
                    snapshot.token(i), digest, new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                    descriptor.commitDomainId(), new DurableSinkProviderKey(
                    descriptor.connectorType(), descriptor.connectorVersion(),
                    descriptor.capabilityProfile()), new DurableSinkCodecDescriptor(
                    String.join("", Collections.nCopies(
                            DurableSinkCodecDescriptor.MAX_CODEC_ID_BYTES, "k")),
                    Integer.MAX_VALUE, PreparedSinkLocator.MAX_PAYLOAD_BYTES),
                    maxPayload, MessageDigest.getInstance("SHA-256")
                    .digest(maxPayload), Long.MAX_VALUE,
                    sha256("data-" + i), Long.MAX_VALUE)));
        }

        byte[] encoded = StageAssignmentManifestCodec.encodeTerminalShard(snapshot, 0);
        assertTrue(encoded.length
                <= StageAssignmentManifestCodec.MAX_TERMINAL_SHARD_FRAME_BYTES);
        assertTrue(encoded.length < 900_000,
                "a maximum terminal shard rewrite must stay below ZK 1 MiB");
        byte[] root = StageAssignmentManifestCodec.encodeRoot(
                manifest, snapshot.acceptanceGeneration());
        byte[] assignment = StageAssignmentManifestCodec.encodeShard(manifest, 0);
        byte[] acceptance = StageAssignmentManifestCodec.encodeAcceptanceShard(snapshot, 0);
        assertThrows(IllegalArgumentException.class, () ->
                StageAssignmentManifestCodec.decodeShardSnapshot(
                        root, assignment, acceptance,
                        rewriteFrameVersion(encoded, 4), 0),
                "legacy v1-v4 readers retain their 128 KiB terminal bound");
    }

    private static StageAssignmentManifest manifest(int count) {
        RunAttemptFence fence = new RunAttemptFence("run-terminal", "attempt-1", 1L);
        List<StageAssignmentRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, 100L + i, i, 0,
                    i == count - 1 ? StageAssignmentRole.SINK
                            : StageAssignmentRole.SOURCE_STAGE,
                    "worker-" + i, "127.0.0.1", 6100 + i, -1,
                    "incarnation-" + i));
        }
        return new StageAssignmentManifest(fence, "artifact-terminal", 1L, records);
    }

    private static List<byte[]> assignmentShards(StageAssignmentManifest manifest) {
        List<byte[]> shards = new ArrayList<>();
        for (int i = 0; i < manifest.shardCount(); i++) {
            shards.add(StageAssignmentManifestCodec.encodeShard(manifest, i));
        }
        return shards;
    }

    private static List<byte[]> acceptanceShards(StageAssignmentSnapshot snapshot) {
        List<byte[]> shards = new ArrayList<>();
        for (int i = 0; i < snapshot.manifest().shardCount(); i++) {
            shards.add(StageAssignmentManifestCodec.encodeAcceptanceShard(snapshot, i));
        }
        return shards;
    }

    private static List<byte[]> terminalShards(StageAssignmentSnapshot snapshot) {
        List<byte[]> shards = new ArrayList<>();
        for (int i = 0; i < snapshot.manifest().shardCount(); i++) {
            shards.add(StageAssignmentManifestCodec.encodeTerminalShard(snapshot, i));
        }
        return shards;
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static int indexOf(byte[] bytes, byte[] needle) {
        outer:
        for (int i = 0; i <= bytes.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (bytes[i + j] != needle[j]) continue outer;
            }
            return i;
        }
        return -1;
    }

    private static void rewriteFrameChecksum(byte[] frame) throws Exception {
        int checksumOffset = frame.length - StageTerminalReceipt.DIGEST_BYTES;
        byte[] checksum = MessageDigest.getInstance("SHA-256")
                .digest(Arrays.copyOf(frame, checksumOffset));
        System.arraycopy(checksum, 0, frame, checksumOffset, checksum.length);
    }

    private static int frameVersion(byte[] frame) {
        return (frame[4] & 0xff) << 8 | frame[5] & 0xff;
    }

    private static byte[] rewriteFrameVersion(byte[] frame, int version) throws Exception {
        byte[] changed = frame.clone();
        changed[4] = (byte) (version >>> 8);
        changed[5] = (byte) version;
        rewriteFrameChecksum(changed);
        return changed;
    }
}
