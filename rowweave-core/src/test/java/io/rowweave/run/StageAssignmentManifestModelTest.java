package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageAssignmentManifestModelTest {

    private static final RunAttemptFence FENCE = new RunAttemptFence("run-Ω", "attempt-1", 7L);
    private static final long GENERATION = 3L;

    private static final String V1_ROOT ="UldBTQABAAEAAABuAAAADnJ1bi12MS1maXh0dXJlAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAAC2FydGlmYWN0LXYxAAAAAAAAAAMAAAAAAAAAAQAAAAEAAAABgWlFaD6MTa6vT/ob5r0R28CAxAhLfHdIBeF4b+eqo3V1Ewxc+/vSayOOtHmuVqWGk2+8zZ5Khmaso22uWdRQsw==";
    private static final String V1_SHARD ="UldBTQABAAIAAADQAAAADnJ1bi12MS1maXh0dXJlAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAAC2FydGlmYWN0LXYxAAAAAAAAAAMAAAABAAAAAQAAAAAAAAABAAAAQDM3Y2VhODNkMmNkZDRlYjRkNzU0NjVkNGJlOWU1YjM5NmNjMzVhZTUxNmEzZTJlNWU4N2ZmZjM4ZmRjZDQ2ZWIAAAAAAAAAAAAAAGUAAAAAAAAAAAgAAAAId29ya2VyLWEAAAAIMTAuMC4wLjEAABu9AAAfpQAAAAVpbmMtYcJHzwcn9GoZnXkAEo6JzUso6goF+UhPB6Y9XfZWaTxA";
    private static final String V1_TOKEN ="UldBTQABAAMAAAC8AAAADnJ1bi12MS1maXh0dXJlAAAACWF0dGVtcHQtMQAAAAAAAAAHAAAAAAAAAAMAAAAAAAAAAQAAAAAAAABAMzdjZWE4M2QyY2RkNGViNGQ3NTQ2NWQ0YmU5ZTViMzk2Y2MzNWFlNTE2YTNlMmU1ZTg3ZmZmMzhmZGNkNDZlYgAAAAAAAABlAAAACHdvcmtlci1hAAAABWluYy1hOsk1niHh9YdLKGicdIy3wg+GfTKSa6smeXauP7gJpdm0xclaEuM3aSwdLt0ffjLMClUjJ/prMrw54+ahJHofzA==";
    private static final String V2_PROFILE_ROOT ="UldBTQACAAEAAABrAAAADnJ1bi12Mi1wcm9maWxlAAAACWF0dGVtcHQtMQAAAAAAAAABAAAACGFydGlmYWN0AAAAAAAAAAEAAAAAAAAAAQAAAAEAAAABVDsfDs4YMLocANHTzi9lG7qccw+WgpsohxVZmZVykf8egMaKf3H1OB5C1sDLccQFryzZuw4Saxg3U25q7BA2MQ==";
    private static final String V2_PROFILE_SHARD ="UldBTQACAAIAAADPAAAADnJ1bi12Mi1wcm9maWxlAAAACWF0dGVtcHQtMQAAAAAAAAABAAAACGFydGlmYWN0AAAAAAAAAAEAAAABAAAAAQAAAAAAAAABAAAAQDcwZjVlOTk2NTI4ZGQ2NmYzNTVjMzNlOWM5MzFkOWZkOGI1MGRjYTcyYjdiMTJkMjEwZDQyOTI3YTM3OTk3OTQAAAAAAAAAAAAAAGUAAAAAAAAAAAYAAAAGd29ya2VyAAAACWxvY2FsaG9zdAAAG1n/////AAAAA2luYwAAAAAAbsqLNh1jYa303XgUrRjQk/vhngIS3AkMo1hLQQpO/J8=";

    @Test
    void rootShardsAndTokenRoundTripCanonically() {
        StageAssignmentManifest manifest = manifest(AttemptOutputProfile.DISCARD,
                assignment(0, 101L, 0, 0, StageAssignmentRole.SOURCE_STAGE,
                        "worker-a", "10.0.0.1", 7101, 8101, "inc-a"),
                assignment(1, 102L, 1, 0, StageAssignmentRole.SINK,
                        "worker-b", "10.0.0.2", 7102, -1, "inc-b"));

        byte[] root = StageAssignmentManifestCodec.encodeRoot(manifest);
        List<byte[]> shards = encodeShards(manifest);
        StageAssignmentManifest decoded = StageAssignmentManifestCodec.decode(root, shards);

        assertEquals(FENCE, decoded.fence());
        assertEquals("artifact-sha256", decoded.artifactId());
        assertEquals(GENERATION, decoded.generation());
        assertEquals(AttemptOutputProfile.DISCARD, decoded.outputProfile());
        assertEquals(2, decoded.assignmentCount());
        assertEquals(1, decoded.shardCount());
        assertEquals(manifest.assignments(), decoded.assignments());
        assertArrayEquals(root, StageAssignmentManifestCodec.encodeRoot(decoded));
        assertArrayEquals(shards.get(0), StageAssignmentManifestCodec.encodeShard(decoded, 0));

        StageAssignmentToken token = StageAssignmentToken.forAssignment(
                manifest, manifest.assignment(1), 9L);
        byte[] encodedToken = StageAssignmentManifestCodec.encodeToken(token);
        StageAssignmentToken decodedToken = StageAssignmentManifestCodec.decodeToken(encodedToken);
        assertEquals(token, decodedToken);
        assertArrayEquals(encodedToken, StageAssignmentManifestCodec.encodeToken(decodedToken));
        assertEquals(manifest.assignment(1).stageExecutionId(), decodedToken.stageExecutionId());
        assertEquals("inc-b", decodedToken.workerIncarnationId());
        assertEquals(GENERATION, decodedToken.manifestGeneration());
        assertEquals(9L, decodedToken.acceptanceGeneration());
    }

    @Test
    void systemControlAssignmentRoundTripsAsAnOutputRole() {
        StageAssignmentManifest manifest = manifest(
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                assignment(0, 101L, 0, 0,
                        StageAssignmentRole.SYSTEM_CONTROL,
                        "worker-a", "10.0.0.1", 7101, -1, "inc-a"));

        StageAssignmentManifest decoded = StageAssignmentManifestCodec.decode(
                StageAssignmentManifestCodec.encodeRoot(manifest),
                encodeShards(manifest));

        assertEquals(StageAssignmentRole.SYSTEM_CONTROL,
                decoded.assignment(0).role());
        assertEquals(1, ManifestCommitJournalSnapshot.initial(
                StageAssignmentSnapshot.initial(decoded))
                .expectedMemberCount());
    }

    @Test
    void v1AssignmentEvidenceRemainsReadableButExplicitlyLacksSourceOwnership() {
        byte[] root = Base64.getDecoder().decode(V1_ROOT);
        byte[] shard = Base64.getDecoder().decode(V1_SHARD);
        StageAssignmentManifest manifest = StageAssignmentManifestCodec.decode(
                root, Collections.singletonList(shard));
        StageAssignmentToken token = StageAssignmentManifestCodec.decodeToken(
                Base64.getDecoder().decode(V1_TOKEN));

        assertEquals(StageAssignmentRole.SOURCE_STAGE, manifest.assignment(0).role());
        assertEquals(0, manifest.assignment(0).sourceSplitCount());
        assertNull(manifest.assignment(0).sourceOwnershipDigest());
        assertTrue(manifest.assignment(0).legacySourceOwnership());
        assertEquals(AttemptOutputProfile.UNKNOWN_LEGACY, manifest.outputProfile());
        assertFalse(token.hasAssignmentRecordDigest());
        assertNull(token.assignmentRecordDigest());
        assertEquals(0, token.sourceSplitCount());
        assertNull(token.sourceOwnershipDigest());
        assertArrayEquals(Base64.getDecoder().decode(V1_TOKEN),
                StageAssignmentManifestCodec.encodeToken(token));
    }

    @Test
    void outputProfileIsFrozenIntoCanonicalRootAndLegacyConstructorFailsClosed() {
        StageAssignmentRecord record = assignment(
                0, 101L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                "worker-a", "localhost", 7001, -1, "inc-a");
        StageAssignmentManifest discard = manifest(
                AttemptOutputProfile.DISCARD, record);
        StageAssignmentManifest manifest = manifest(
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1, record);
        StageAssignmentManifest legacy = new StageAssignmentManifest(
                FENCE, "artifact-sha256", GENERATION,
                Collections.singletonList(record));

        assertNotEquals(Base64.getEncoder().encodeToString(
                        StageAssignmentManifestCodec.encodeRoot(discard)),
                Base64.getEncoder().encodeToString(
                        StageAssignmentManifestCodec.encodeRoot(manifest)));
        assertEquals(AttemptOutputProfile.UNKNOWN_LEGACY, legacy.outputProfile());
        assertEquals(AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                StageAssignmentManifestCodec.decode(
                        StageAssignmentManifestCodec.encodeRoot(manifest),
                        encodeShards(manifest)).outputProfile());
    }

    @Test
    void v2ManifestWithoutProfileDecodesAsUnknownLegacy() {
        StageAssignmentManifest decoded = StageAssignmentManifestCodec.decode(
                Base64.getDecoder().decode(V2_PROFILE_ROOT),
                Collections.singletonList(
                        Base64.getDecoder().decode(V2_PROFILE_SHARD)));

        assertEquals(AttemptOutputProfile.UNKNOWN_LEGACY,
                decoded.outputProfile());
        assertEquals("run-v2-profile", decoded.fence().runId());
        assertEquals(StageAssignmentRole.WHOLE_GRAPH,
                decoded.assignment(0).role());
    }

    @Test
    void stageExecutionIdentityIsFullDeterministicSha256() {
        StageAssignmentRecord first = assignment(0, 101L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                "worker-a", "localhost", 7001, -1, "inc-a");
        StageAssignmentRecord replay = assignment(0, 101L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                "worker-a", "localhost", 7001, -1, "inc-a");
        StageAssignmentRecord nextOrdinal = assignment(1, 102L, 0, 1, StageAssignmentRole.WHOLE_GRAPH,
                "worker-a", "localhost", 7001, -1, "inc-a");
        StageAssignmentRecord nextGeneration = StageAssignmentRecord.create(
                FENCE, GENERATION + 1L, 0, 101L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                "worker-a", "localhost", 7001, -1, "inc-a");

        assertEquals(first.stageExecutionId(), replay.stageExecutionId());
        assertEquals(64, first.stageExecutionId().length());
        assertEquals(first.stageExecutionId().toLowerCase(), first.stageExecutionId());
        assertNotEquals(first.stageExecutionId(), nextOrdinal.stageExecutionId());
        assertNotEquals(first.stageExecutionId(), nextGeneration.stageExecutionId());
    }

    @Test
    void manifestRejectsUnboundedDuplicateAndNonCanonicalAssignments() {
        StageAssignmentRecord first = assignment(0, 101L, 0, 0, StageAssignmentRole.ROUTER,
                "worker-a", "localhost", 7001, 8001, "inc-a");
        assertThrows(IllegalArgumentException.class,
                () -> new StageAssignmentManifest(FENCE, "artifact", GENERATION,
                        Arrays.asList(first, first)));
        assertThrows(IllegalArgumentException.class,
                () -> manifest(first, assignment(1, 101L, 1, 0, StageAssignmentRole.SINK,
                        "worker-b", "localhost", 7002, -1, "inc-b")));
        assertThrows(IllegalArgumentException.class,
                () -> manifest(first, assignment(1, 102L, 0, 0, StageAssignmentRole.ROUTER,
                        "worker-b", "localhost", 7002, 8002, "inc-b")));

        List<StageAssignmentRecord> tooMany = new ArrayList<>();
        for (int i = 0; i <= StageAssignmentManifest.MAX_ASSIGNMENTS; i++) {
            tooMany.add(assignment(i, i + 1L, i, 0, StageAssignmentRole.SOURCE_STAGE,
                    "worker", "localhost", 7001, -1, "inc"));
        }
        assertThrows(IllegalArgumentException.class,
                () -> new StageAssignmentManifest(FENCE, "artifact", GENERATION, tooMany));
        assertThrows(IllegalArgumentException.class,
                () -> new StageAssignmentManifest(FENCE, "artifact", GENERATION,
                        Collections.emptyList()));

        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentRecord.create(FENCE, GENERATION, -1, 1L, 0, 0,
                        StageAssignmentRole.SINK, "worker", "host", 7001, -1, "inc"));
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentRecord.create(FENCE, GENERATION, 0, 0L, 0, 0,
                        StageAssignmentRole.SINK, "worker", "host", 7001, -1, "inc"));
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentRecord.create(FENCE, GENERATION, 0, 1L, 0, 0,
                        StageAssignmentRole.SINK, "worker", "host", 0, -1, "inc"));
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentRecord.create(FENCE, GENERATION, 0, 1L, 0, 0,
                        StageAssignmentRole.SINK, "worker", "host", 7001, 0, "inc"));
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentRecord.create(FENCE, GENERATION, 0, 1L, 0, 0,
                        StageAssignmentRole.SINK, "worker", "host", 7001, -1, ""));
    }

    @Test
    void codecRejectsMissingReorderedCorruptTruncatedAndTrailingShards() {
        List<StageAssignmentRecord> records = new ArrayList<>();
        for (int i = 0; i < 65; i++) {
            records.add(assignment(i, i + 1L, i, 0, StageAssignmentRole.KEYED_OP,
                    "worker-" + i, "127.0.0.1", 7001, -1, "inc-" + i));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                FENCE, "artifact", GENERATION, records);
        byte[] root = StageAssignmentManifestCodec.encodeRoot(manifest);
        List<byte[]> shards = encodeShards(manifest);
        assertEquals(2, shards.size());

        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentManifestCodec.decode(root,
                        Collections.singletonList(shards.get(0))));
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentManifestCodec.decode(root,
                        Arrays.asList(shards.get(1), shards.get(0))));

        byte[] corrupt = shards.get(0).clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentManifestCodec.decode(root,
                        Arrays.asList(corrupt, shards.get(1))));
        byte[] truncated = Arrays.copyOf(shards.get(0), shards.get(0).length - 1);
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentManifestCodec.decode(root,
                        Arrays.asList(truncated, shards.get(1))));
        byte[] trailing = Arrays.copyOf(root, root.length + 1);
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentManifestCodec.decode(trailing, shards));

        StageAssignmentToken token = StageAssignmentToken.forAssignment(
                manifest, manifest.assignment(0), 9L);
        byte[] encodedToken = StageAssignmentManifestCodec.encodeToken(token);
        for (int length = 0; length < encodedToken.length; length++) {
            final byte[] prefix = Arrays.copyOf(encodedToken, length);
            assertThrows(IllegalArgumentException.class,
                    () -> StageAssignmentManifestCodec.decodeToken(prefix), "length=" + length);
        }
        byte[] corruptToken = encodedToken.clone();
        corruptToken[corruptToken.length / 2] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentManifestCodec.decodeToken(corruptToken));
    }

    @Test
    void acceptanceAndDecisionDefensivelyBindExactCommandDigest() {
        StageAssignmentManifest manifest = manifest(assignment(
                0, 101L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                "worker-a", "localhost", 7001, -1, "inc-a"));
        StageAssignmentToken token = StageAssignmentToken.forAssignment(
                manifest, manifest.assignment(0), 9L);
        byte[] digest = new byte[32];
        Arrays.fill(digest, (byte) 7);
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(token, digest, 123L);

        digest[0] = 9;
        assertEquals(7, acceptance.commandDigest()[0]);
        byte[] exposed = acceptance.commandDigest();
        exposed[0] = 11;
        assertEquals(7, acceptance.commandDigest()[0]);
        assertThrows(IllegalArgumentException.class,
                () -> new StageAssignmentAcceptance(token, new byte[31], 123L));
        assertThrows(IllegalArgumentException.class,
                () -> new StageAssignmentAcceptance(token, new byte[32], -1L));

        StageAcceptanceDecision accepted = StageAcceptanceDecision.accepted(acceptance);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED, accepted.code());
        assertEquals(acceptance, accepted.acceptance());
        assertEquals(StageAcceptanceDecision.Code.ALREADY_ACCEPTED,
                StageAcceptanceDecision.alreadyAccepted(acceptance).code());
        assertEquals(StageAcceptanceDecision.Code.STALE_ASSIGNMENT_GENERATION,
                StageAcceptanceDecision.staleGeneration().code());
        assertFalse(StageAcceptanceDecision.staleGeneration().hasAcceptance());
    }

    @Test
    void authorityRootAndReceiptShardsRoundTripMutableAcceptanceSeparately() {
        StageAssignmentManifest manifest = manifest(
                assignment(0, 101L, 0, 0, StageAssignmentRole.ROUTER,
                        "worker-a", "localhost", 7001, -1, "inc-a"),
                assignment(1, 102L, 1, 0, StageAssignmentRole.SINK,
                        "worker-b", "localhost", 7002, -1, "inc-b"));
        StageAssignmentToken token = StageAssignmentToken.forAssignment(
                manifest, manifest.assignment(0), 9L);
        StageAssignmentAcceptance accepted = new StageAssignmentAcceptance(
                token, new byte[32], 123L);
        StageAssignmentSnapshot snapshot = new StageAssignmentSnapshot(
                manifest, 9L, Arrays.asList(accepted, null));

        byte[] root = StageAssignmentManifestCodec.encodeRoot(manifest, 9L);
        List<byte[]> assignmentShards = encodeShards(manifest);
        List<byte[]> receiptShards = Collections.singletonList(
                StageAssignmentManifestCodec.encodeAcceptanceShard(snapshot, 0));
        StageAssignmentSnapshot decoded = StageAssignmentManifestCodec.decodeSnapshot(
                root, assignmentShards, receiptShards);

        assertEquals(9L, decoded.acceptanceGeneration());
        assertEquals(1, decoded.acceptedCount());
        assertEquals(accepted, decoded.acceptance(0));
        assertNull(decoded.acceptance(1));
        assertArrayEquals(receiptShards.get(0),
                StageAssignmentManifestCodec.encodeAcceptanceShard(decoded, 0));

        byte[] corrupt = receiptShards.get(0).clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentManifestCodec.decodeSnapshot(
                        root, assignmentShards, Collections.singletonList(corrupt)));
    }

    @Test
    void targetedShardViewValidatesRootDigestAndRewritesOnlyRequestedReceiptShard() {
        List<StageAssignmentRecord> records = new ArrayList<>();
        for (int i = 0; i < 65; i++) {
            records.add(assignment(i, i + 101L, i, 0,
                    i == 64 ? StageAssignmentRole.SINK : StageAssignmentRole.ROUTER,
                    "worker-" + i, "127.0.0.1", 7001, -1, "inc-" + i));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                FENCE, "artifact-sha256", GENERATION, records);
        StageAssignmentSnapshot empty = StageAssignmentSnapshot.initial(manifest)
                .withAcceptanceGeneration(9L);
        byte[] root = StageAssignmentManifestCodec.encodeRoot(manifest, 9L);
        byte[] assignmentShard = StageAssignmentManifestCodec.encodeShard(manifest, 1);
        byte[] acceptanceShard =
                StageAssignmentManifestCodec.encodeAcceptanceShard(empty, 1);

        StageAssignmentManifestCodec.ShardSnapshot targeted =
                StageAssignmentManifestCodec.decodeShardSnapshot(
                        root, assignmentShard, acceptanceShard, 1);
        assertEquals(65, targeted.assignmentCount());
        assertEquals(1, targeted.shardIndex());
        assertEquals(64, targeted.firstOrdinal());
        assertEquals(manifest.assignment(64), targeted.record(64));
        assertNull(targeted.acceptance(64));

        StageAssignmentToken token = StageAssignmentToken.forAssignment(
                manifest, manifest.assignment(64), 9L);
        StageAssignmentAcceptance receipt = new StageAssignmentAcceptance(
                token, new byte[32], 123L);
        byte[] rewritten = StageAssignmentManifestCodec.encodeAcceptanceShard(
                targeted, receipt);
        StageAssignmentManifestCodec.ShardSnapshot accepted =
                StageAssignmentManifestCodec.decodeShardSnapshot(
                        root, assignmentShard, rewritten, 1);
        assertEquals(receipt, accepted.acceptance(64));

        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentManifestCodec.decodeShardSnapshot(
                        root, assignmentShard, acceptanceShard, 0));
        byte[] corrupt = assignmentShard.clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> StageAssignmentManifestCodec.decodeShardSnapshot(
                        root, corrupt, acceptanceShard, 1));
    }

    private static StageAssignmentManifest manifest(StageAssignmentRecord... assignments) {
        return new StageAssignmentManifest(
                FENCE, "artifact-sha256", GENERATION, Arrays.asList(assignments));
    }

    private static StageAssignmentRecord assignment(int ordinal, long runtimeStageId,
                                                     int logicalStage, int instanceIndex,
                                                     StageAssignmentRole role,
                                                     String workerId, String host,
                                                     int rpcPort, int dataPort,
                                                     String incarnationId) {
        return StageAssignmentRecord.create(FENCE, GENERATION, ordinal, runtimeStageId,
                logicalStage, instanceIndex, role, workerId, host, rpcPort, dataPort, incarnationId);
    }

    private static List<byte[]> encodeShards(StageAssignmentManifest manifest) {
        List<byte[]> shards = new ArrayList<>();
        for (int i = 0; i < manifest.shardCount(); i++) {
            shards.add(StageAssignmentManifestCodec.encodeShard(manifest, i));
        }
        return shards;
    }

    private static StageAssignmentManifest manifest(
            AttemptOutputProfile profile, StageAssignmentRecord... assignments) {
        return new StageAssignmentManifest(FENCE, "artifact-sha256", GENERATION,
                profile, Arrays.asList(assignments));
    }
}
