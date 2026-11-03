package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** exact durable source position cut and reopen authority. */
class StreamSourcePositionCheckpointTest {
    private static final String CONTRACT = repeat('a');
    private static final String SPLIT_ID = repeat('b');

    @TempDir
    Path temporaryDirectory;

    @Test
    void barrierCutPersistsLastEmittedPositionAndRestoresItExactly() {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("payloads"), 64 * 1024L);
        StreamSourceBinding binding = binding();
        StreamSourceSplit split = binding.fixedSplits().get(0);
        StreamSourcePositionCheckpoint original = checkpoint(
                binding, split, payloads, temporaryDirectory.resolve("attempt-1"));

        original.recordEmitted(bytes("position-1"));
        original.recordEmitted(bytes("position-2"));
        StreamCheckpointMember member = original.snapshot();

        assertEquals(StreamCheckpointMember.Kind.SOURCE_POSITION, member.kind());
        assertEquals("source", member.operatorUid());
        assertEquals(2, member.subtaskIndex());
        assertEquals(SPLIT_ID, member.subjectId());
        assertEquals("test-position-v1", member.codecId());
        assertEquals(2L, member.logicalPosition());
        assertTrue(member.contentBytes() > 0L);

        StreamSourcePositionCheckpoint replacement = checkpoint(
                binding, split, payloads, temporaryDirectory.resolve("attempt-2"));
        replacement.restore(member);
        assertEquals(2L, replacement.logicalPosition());
        assertArrayEquals(bytes("position-2"),
                replacement.currentPositionFrame());
        replacement.recordEmitted(bytes("position-3"));
        assertEquals(3L, replacement.logicalPosition());
    }

    @Test
    void exactSplitPositionCanRebindToANewSubtaskWithoutChangingTheProof() {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("reassigned-payloads"), 64 * 1024L);
        StreamSourceBinding binding = binding();
        StreamSourceSplit split = binding.fixedSplits().get(0);
        StreamSourcePositionCheckpoint original = checkpoint(
                binding, split, payloads,
                temporaryDirectory.resolve("reassigned-old"));
        original.recordEmitted(bytes("position-2"));
        StreamCheckpointMember oldMember = original.snapshot();

        StreamSourcePositionCheckpoint reassigned =
                new StreamSourcePositionCheckpoint(
                        "artifact-1", "source", 0, binding, split,
                        temporaryDirectory.resolve("reassigned-new"),
                        payloads, 0L, bytes("initial"));
        reassigned.restoreReassigned(oldMember);
        assertEquals(1L, reassigned.logicalPosition());
        assertArrayEquals(bytes("position-2"),
                reassigned.currentPositionFrame());
        assertEquals(0, reassigned.snapshot().subtaskIndex(),
                "the next checkpoint must belong to the new owner");
    }

    @Test
    void memberAndEmbeddedProviderIdentityAreValidatedBeforeReopen() throws Exception {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("identity-payloads"), 64 * 1024L);
        StreamSourceBinding binding = binding();
        StreamSourceSplit split = binding.fixedSplits().get(0);
        StreamSourcePositionCheckpoint original = checkpoint(
                binding, split, payloads, temporaryDirectory.resolve("identity-1"));
        original.recordEmitted(bytes("position-1"));
        StreamCheckpointMember member = original.snapshot();
        byte[] driftedConfiguration = new byte[] {7};
        StreamSourceBinding driftedBinding = new StreamSourceBinding(
                "test-log-v1", CONTRACT, sha256Hex(driftedConfiguration),
                driftedConfiguration, Collections.singletonList(
                        new StreamSourceSplit(SPLIT_ID, "test-position-v1",
                                new byte[] {99})),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED);
        StreamSourcePositionCheckpoint drifted = checkpoint(
                driftedBinding, driftedBinding.fixedSplits().get(0), payloads,
                temporaryDirectory.resolve("identity-drift"));
        StreamCheckpointException embeddedMismatch = assertThrows(
                StreamCheckpointException.class,
                () -> drifted.restore(member));
        assertEquals(StreamCheckpointException.Code.SOURCE_POSITION_INVALID,
                embeddedMismatch.code());
        assertEquals(0L, drifted.logicalPosition());

        Files.delete(payloads.payloadPath(member.contentDigest()));

        StreamCheckpointMember wrongSplit = StreamCheckpointMember.sourcePosition(
                "source", 2, repeat('c'), member.codecId(),
                member.logicalPosition(), member.contentDigest(),
                member.contentBytes());
        StreamSourcePositionCheckpoint replacement = checkpoint(
                binding, split, payloads, temporaryDirectory.resolve("identity-2"));
        StreamCheckpointException mismatch = assertThrows(
                StreamCheckpointException.class,
                () -> replacement.restore(wrongSplit));
        assertEquals(StreamCheckpointException.Code.SOURCE_POSITION_INVALID,
                mismatch.code());
        assertEquals(0L, replacement.logicalPosition());
        assertArrayEquals(bytes("initial"),
                replacement.currentPositionFrame());
    }

    @Test
    void corruptPayloadAndStagingLimitFailWithoutChangingCurrentPosition()
            throws Exception {
        Path payloadRoot = temporaryDirectory.resolve("corrupt-payloads");
        FileCheckpointPayloadStore payloads =
                new FileCheckpointPayloadStore(payloadRoot, 64 * 1024L);
        StreamSourceBinding binding = binding();
        StreamSourceSplit split = binding.fixedSplits().get(0);
        StreamSourcePositionCheckpoint original = checkpoint(
                binding, split, payloads, temporaryDirectory.resolve("corrupt-1"));
        original.recordEmitted(bytes("position-1"));
        StreamCheckpointMember member = original.snapshot();
        Files.write(payloads.payloadPath(member.contentDigest()), new byte[] {0},
                StandardOpenOption.TRUNCATE_EXISTING);

        StreamSourcePositionCheckpoint replacement = checkpoint(
                binding, split, payloads, temporaryDirectory.resolve("corrupt-2"));
        StreamCheckpointException corrupt = assertThrows(
                StreamCheckpointException.class,
                () -> replacement.restore(member));
        assertEquals(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                corrupt.code());
        assertEquals(0L, replacement.logicalPosition());
        assertArrayEquals(bytes("initial"),
                replacement.currentPositionFrame());

        Path tinyRoot = temporaryDirectory.resolve("tiny-payloads");
        FileCheckpointPayloadStore tiny =
                new FileCheckpointPayloadStore(tinyRoot, 32L);
        StreamSourcePositionCheckpoint bounded = checkpoint(
                binding, split, tiny, temporaryDirectory.resolve("tiny-working"));
        bounded.recordEmitted(bytes("position-1"));
        StreamCheckpointException limited = assertThrows(
                StreamCheckpointException.class, bounded::snapshot);
        assertEquals(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                limited.code());
        try (Stream<Path> files = Files.list(tinyRoot.resolve("payloads"))) {
            assertEquals(0L, files.count());
        }
        try (Stream<Path> files = Files.list(
                temporaryDirectory.resolve("tiny-working"))) {
            assertEquals(0L, files.count());
        }
    }

    @Test
    void positionFramesAreBoundedAndLogicalPositionCannotOverflow() {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("bounds-payloads"), 64 * 1024L);
        StreamSourceBinding binding = binding();
        StreamSourceSplit split = binding.fixedSplits().get(0);
        StreamSourcePositionCheckpoint checkpoint =
                new StreamSourcePositionCheckpoint(
                        "artifact-1", "source", 2, binding, split,
                        temporaryDirectory.resolve("bounds-working"), payloads,
                        Long.MAX_VALUE, bytes("last"));
        StreamCheckpointException overflow = assertThrows(
                StreamCheckpointException.class,
                () -> checkpoint.recordEmitted(bytes("too-far")));
        assertEquals(StreamCheckpointException.Code.SOURCE_POSITION_INVALID,
                overflow.code());
        assertEquals(Long.MAX_VALUE, checkpoint.logicalPosition());

        byte[] oversized = new byte[4 * 1024 * 1024 + 1];
        StreamCheckpointException limited = assertThrows(
                StreamCheckpointException.class,
                () -> checkpoint.recordEmitted(oversized));
        assertEquals(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                limited.code());
    }

    private static StreamSourcePositionCheckpoint checkpoint(
            StreamSourceBinding binding, StreamSourceSplit split,
            FileCheckpointPayloadStore payloads, Path workingDirectory) {
        return new StreamSourcePositionCheckpoint(
                "artifact-1", "source", 2, binding, split,
                workingDirectory, payloads, 0L, bytes("initial"));
    }

    private static StreamSourceBinding binding() {
        byte[] configuration = new byte[] {7};
        return new StreamSourceBinding(
                "test-log-v1", CONTRACT, sha256Hex(configuration),
                configuration, Collections.singletonList(
                        new StreamSourceSplit(SPLIT_ID, "test-position-v1",
                                new byte[] {8})),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
