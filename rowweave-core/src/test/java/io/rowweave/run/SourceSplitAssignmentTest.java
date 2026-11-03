package io.rowweave.run;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** physical source ownership is canonical and manifest-bound. */
class SourceSplitAssignmentTest {
    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));
    private static final RunAttemptFence FENCE =
            new RunAttemptFence("run-source-owner", "attempt-1", 9L);

    @Test
    void ownershipRootBindsArtifactOperatorPhysicalInstanceAndOrderedDescriptors() {
        BatchSourceSplitBinding a = binding("split-a");
        BatchSourceSplitBinding b = binding("split-b");
        SourceSplitAssignment first = SourceSplitAssignment.create(
                "artifact-a", 3, 1,
                Collections.singletonList(SourceSplitAssignment.SourceGroup.create(
                        "source-a", Arrays.asList(a, b))));
        SourceSplitAssignment replay = SourceSplitAssignment.create(
                "artifact-a", 3, 1,
                Collections.singletonList(SourceSplitAssignment.SourceGroup.create(
                        "source-a", Arrays.asList(a, b))));

        assertEquals(2, first.sourceSplitCount());
        assertEquals(first, replay);
        assertArrayEquals(first.ownershipDigest(), replay.ownershipDigest());
        assertNotEquals(first, assignment("artifact-b", "source-a", 3, 1, a, b));
        assertNotEquals(first, assignment("artifact-a", "source-b", 3, 1, a, b));
        assertNotEquals(first, assignment("artifact-a", "source-a", 4, 1, a, b));
        assertNotEquals(first, assignment("artifact-a", "source-a", 3, 2, a, b));
        assertNotEquals(first, assignment("artifact-a", "source-a", 3, 1, b, a));

        byte[] exposed = first.ownershipDigest();
        exposed[0] ^= 1;
        assertArrayEquals(replay.ownershipDigest(), first.ownershipDigest());
    }

    @Test
    void wholeGraphRootPreservesSourceGroupOrderAndBoundaries() {
        BatchSourceSplitBinding a = binding("split-a");
        BatchSourceSplitBinding b = binding("split-b");
        SourceSplitAssignment grouped = SourceSplitAssignment.create(
                "artifact", 0, 0, Arrays.asList(
                        SourceSplitAssignment.SourceGroup.create(
                                "source-a", Collections.singletonList(a)),
                        SourceSplitAssignment.SourceGroup.create(
                                "source-b", Collections.singletonList(b))));
        SourceSplitAssignment flattened = SourceSplitAssignment.create(
                "artifact", 0, 0, Collections.singletonList(
                        SourceSplitAssignment.SourceGroup.create(
                                "source-a", Arrays.asList(a, b))));
        SourceSplitAssignment reordered = SourceSplitAssignment.create(
                "artifact", 0, 0, Arrays.asList(
                        SourceSplitAssignment.SourceGroup.create(
                                "source-b", Collections.singletonList(b)),
                        SourceSplitAssignment.SourceGroup.create(
                                "source-a", Collections.singletonList(a))));

        assertEquals(2, grouped.sourceSplitCount());
        assertTrue(grouped.context().matches(grouped));
        assertEquals("source-a", grouped.context().sourceGroups().get(0).sourceOperatorId());
        assertEquals(1, grouped.context().sourceGroups().get(0).sourceSplitCount());
        assertNotEquals(grouped, flattened);
        assertNotEquals(grouped, reordered);
        assertThrows(IllegalArgumentException.class,
                () -> SourceSplitAssignment.create("artifact", 0, 0, Arrays.asList(
                        SourceSplitAssignment.SourceGroup.create(
                                "source-a", Collections.singletonList(a)),
                        SourceSplitAssignment.SourceGroup.create(
                                "source-a", Collections.singletonList(b)))));
    }

    @Test
    void exactSourceRecordAndTokenBindCanonicalRecordDigest() {
        SourceSplitAssignment ownership = assignment(
                "artifact", "source-a", 3, 1, binding("split-a"));
        StageAssignmentRecord record = StageAssignmentRecord.create(
                FENCE, 1L, 0, 101L, 3, 1, StageAssignmentRole.SOURCE_STAGE,
                "worker-a", "127.0.0.1", 7101, 8101, "inc-a", ownership);
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                FENCE, "artifact", 1L, Collections.singletonList(record));
        StageAssignmentToken token = StageAssignmentToken.forAssignment(
                manifest, record, 1L);

        assertEquals(1, record.sourceSplitCount());
        assertArrayEquals(ownership.ownershipDigest(), record.sourceOwnershipDigest());
        assertFalse(record.legacySourceOwnership());
        assertEquals(1, token.sourceSplitCount());
        assertArrayEquals(record.sourceOwnershipDigest(), token.sourceOwnershipDigest());
        assertArrayEquals(record.canonicalDigest(), token.assignmentRecordDigest());
        assertEquals(record.logicalStage(), token.logicalStage());
        assertEquals(record.instanceIndex(), token.instanceIndex());
        assertEquals(record.role(), token.role());

        StageAssignmentRecord sink = StageAssignmentRecord.create(
                FENCE, 1L, 1, 102L, 4, 0, StageAssignmentRole.SINK,
                "worker-b", "127.0.0.1", 7102, -1, "inc-b");
        assertEquals(0, sink.sourceSplitCount());
        assertNull(sink.sourceOwnershipDigest());
        assertThrows(IllegalArgumentException.class, () -> StageAssignmentRecord.create(
                FENCE, 1L, 1, 102L, 4, 0, StageAssignmentRole.SINK,
                "worker-b", "127.0.0.1", 7102, -1, "inc-b", ownership));
        assertThrows(IllegalArgumentException.class, () -> StageAssignmentRecord.create(
                FENCE, 1L, 0, 101L, 3, 0, StageAssignmentRole.SOURCE_STAGE,
                "worker-a", "127.0.0.1", 7101, 8101, "inc-a", ownership));
    }

    @Test
    void v3ManifestAndTokenRoundTripOwnershipWithoutAliasing() {
        SourceSplitAssignment ownership = assignment(
                "artifact", "source-a", 0, 0, binding("split-a"), binding("split-b"));
        StageAssignmentRecord record = StageAssignmentRecord.create(
                FENCE, 2L, 0, 101L, 0, 0, StageAssignmentRole.SOURCE_STAGE,
                "worker-a", "127.0.0.1", 7101, 8101, "inc-a", ownership);
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                FENCE, "artifact", 2L, Collections.singletonList(record));

        byte[] root = StageAssignmentManifestCodec.encodeRoot(manifest);
        byte[] shard = StageAssignmentManifestCodec.encodeShard(manifest, 0);
        StageAssignmentManifest decoded = StageAssignmentManifestCodec.decode(
                root, Collections.singletonList(shard));
        StageAssignmentToken decodedToken = StageAssignmentManifestCodec.decodeToken(
                StageAssignmentManifestCodec.encodeToken(
                        StageAssignmentSnapshot.initial(manifest).token(0)));

        assertEquals(3, unsignedShort(root, 4));
        assertEquals(3, unsignedShort(shard, 4));
        assertEquals(AttemptOutputProfile.UNKNOWN_LEGACY,
                decoded.outputProfile());
        assertEquals(record, decoded.assignment(0));
        assertArrayEquals(record.canonicalDigest(), decodedToken.assignmentRecordDigest());
        assertTrue(decodedToken.hasAssignmentRecordDigest());
    }

    private static SourceSplitAssignment assignment(
            String artifact, String operator, int logicalStage, int instanceIndex,
            BatchSourceSplitBinding... bindings) {
        return SourceSplitAssignment.create(artifact, logicalStage, instanceIndex,
                Collections.singletonList(SourceSplitAssignment.SourceGroup.create(
                        operator, Arrays.asList(bindings))));
    }

    private static BatchSourceSplitBinding binding(String splitId) {
        CollectionSource input = new CollectionSource(
                META, splitId, Collections.emptyList());
        return new BatchSourceSplitBinding(input,
                ((BatchSource) input).batchSourceSplit());
    }

    private static int unsignedShort(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 8) | (bytes[offset + 1] & 0xff);
    }
}
