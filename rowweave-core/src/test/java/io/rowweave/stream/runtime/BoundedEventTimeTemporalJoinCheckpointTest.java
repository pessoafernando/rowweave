package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamStateDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** temporal state snapshots reopen in a fresh mailbox owner. */
class BoundedEventTimeTemporalJoinCheckpointTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void twoLogicalStateHandlesRestoreOneCanonicalTemporalWorkingSet() {
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("checkpoint"), 64 * 1024L);
        List<StreamStateDescriptor> descriptors = Arrays.asList(
                state("left-state"), state("right-state"));
        List<StreamCheckpointMember> members;
        try (BoundedEventTimeTemporalJoin original = join(
                temporaryDirectory.resolve("original"), ignored -> { })) {
            original.onLeft(bytes("A"), 100L, bytes("left"));
            original.advanceLeftWatermark(80L);
            members = original.snapshot(0, descriptors, store);
        }

        List<String> output = new ArrayList<>();
        try (BoundedEventTimeTemporalJoin restored = join(
                temporaryDirectory.resolve("restored"),
                frame -> output.add(text(frame)))) {
            restored.restore(0, descriptors, members, store);
            restored.onRight(bytes("A"), 110L, bytes("right"));
            assertEquals(Arrays.asList("left:right"), output);
            assertEquals(2, restored.stateEntryCount());
        }
    }

    @Test
    void mismatchedLogicalHandleFailsWithoutContaminatingFreshOwner() {
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("checkpoint-mismatch"),
                64 * 1024L);
        List<StreamStateDescriptor> descriptors = Arrays.asList(
                state("left-state"), state("right-state"));
        List<StreamCheckpointMember> members;
        try (BoundedEventTimeTemporalJoin original = join(
                temporaryDirectory.resolve("original-mismatch"),
                ignored -> { })) {
            original.onLeft(bytes("A"), 100L, bytes("left"));
            members = original.snapshot(0, descriptors, store);
        }
        StreamCheckpointMember first = members.get(0);
        List<StreamCheckpointMember> mismatched = Arrays.asList(
                first,
                StreamCheckpointMember.stateHandle(
                        "temporal-join", 0, 0, 127,
                        "unexpected-state", "temporal-record-v1", 1,
                        first.contentDigest(), first.contentBytes()));

        try (BoundedEventTimeTemporalJoin restored = join(
                temporaryDirectory.resolve("restored-mismatch"),
                ignored -> { })) {
            assertThrows(StreamTemporalJoinException.class,
                    () -> restored.restore(
                            0, descriptors, mismatched, store));
            restored.onLeft(bytes("B"), 200L, bytes("fresh"));
            assertEquals(1, restored.stateEntryCount());
        }
    }

    @Test
    void corruptSharedPayloadFailsWithoutContaminatingFreshOwner()
            throws Exception {
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("checkpoint-corrupt"),
                64 * 1024L);
        List<StreamStateDescriptor> descriptors = Arrays.asList(
                state("left-state"), state("right-state"));
        List<StreamCheckpointMember> members;
        try (BoundedEventTimeTemporalJoin original = join(
                temporaryDirectory.resolve("original-corrupt"),
                ignored -> { })) {
            original.onLeft(bytes("A"), 100L, bytes("left"));
            members = original.snapshot(0, descriptors, store);
        }
        Files.write(store.payloadPath(members.get(0).contentDigest()),
                bytes("corrupt"), StandardOpenOption.TRUNCATE_EXISTING);

        try (BoundedEventTimeTemporalJoin restored = join(
                temporaryDirectory.resolve("restored-corrupt"),
                ignored -> { })) {
            assertThrows(StreamCheckpointException.class,
                    () -> restored.restore(0, descriptors, members, store));
            restored.onLeft(bytes("B"), 200L, bytes("fresh"));
            assertEquals(1, restored.stateEntryCount());
        }
    }

    @Test
    void liveRescaleRepartitionsSharedSnapshotAndPreservesMatches() {
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("checkpoint-rescale"),
                64 * 1024L);
        List<StreamStateDescriptor> descriptors = Arrays.asList(
                state("left-state"), state("right-state"));
        List<StreamCheckpointMember> source;
        try (BoundedEventTimeTemporalJoin original = join(
                temporaryDirectory.resolve("original-rescale"),
                ignored -> { })) {
            original.onLeft(bytes("A"), 100L, bytes("left"));
            source = original.snapshot(0, descriptors, store);
        }

        List<StreamCheckpointMember> target =
                BoundedEventTimeTemporalJoin.rescaleSnapshots(
                        "temporal-join", -10L, 20L, 128, 2,
                        32, 64 * 1024L, 1_024, 4_096,
                        descriptors, source, store,
                        temporaryDirectory.resolve("rescale-working"),
                        256 * 1024L);
        int keyGroup = ManagedKeyedValueState.keyGroup(bytes("A"), 128);
        int subtask = Math.min(1, keyGroup * 2 / 128);
        List<StreamCheckpointMember> owned = new ArrayList<>();
        for (StreamCheckpointMember member : target) {
            if (member.subtaskIndex() == subtask) owned.add(member);
        }
        List<String> output = new ArrayList<>();
        try (BoundedEventTimeTemporalJoin restored = joinRange(
                temporaryDirectory.resolve("restored-rescale"),
                subtask, 2, frame -> output.add(text(frame)))) {
            restored.restore(subtask, descriptors, owned, store);
            restored.onRight(bytes("A"), 110L, bytes("right"));
            assertEquals(Arrays.asList("left:right"), output);
        }
    }

    @Test
    void liveRescaleRejectsTransferBeyondStagingLease() {
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("checkpoint-rescale-limit"),
                64 * 1024L);
        List<StreamStateDescriptor> descriptors = Arrays.asList(
                state("left-state"), state("right-state"));
        List<StreamCheckpointMember> source;
        try (BoundedEventTimeTemporalJoin original = join(
                temporaryDirectory.resolve("original-rescale-limit"),
                ignored -> { })) {
            original.onLeft(bytes("A"), 100L, bytes("left"));
            source = original.snapshot(0, descriptors, store);
        }

        StreamTemporalJoinException failure = assertThrows(
                StreamTemporalJoinException.class,
                () -> BoundedEventTimeTemporalJoin.rescaleSnapshots(
                        "temporal-join", -10L, 20L, 128, 2,
                        32, 64 * 1024L, 1_024, 4_096,
                        descriptors, source, store,
                        temporaryDirectory.resolve(
                                "rescale-limit-working"), 1L));
        assertEquals(StreamTemporalJoinException.Code.STATE_BYTE_LIMIT,
                failure.code());
    }

    private BoundedEventTimeTemporalJoin join(
            Path root, BoundedEventTimeTemporalJoin.Sink output) {
        return new BoundedEventTimeTemporalJoin(
                "temporal-join", -10L, 20L,
                128, 0, 127, 32, 64 * 1024L,
                1_024, 4_096, root,
                (left, right) -> bytes(text(left) + ':' + text(right)),
                output);
    }

    private BoundedEventTimeTemporalJoin joinRange(
            Path root, int subtask, int parallelism,
            BoundedEventTimeTemporalJoin.Sink output) {
        return new BoundedEventTimeTemporalJoin(
                "temporal-join", -10L, 20L, 128,
                OfflineKeyedStateRescaler.keyGroupStart(
                        subtask, parallelism, 128),
                OfflineKeyedStateRescaler.keyGroupEnd(
                        subtask, parallelism, 128),
                32, 64 * 1024L, 1_024, 4_096, root,
                (left, right) -> bytes(text(left) + ':' + text(right)),
                output);
    }

    private static StreamStateDescriptor state(String name) {
        return new StreamStateDescriptor(name,
                StreamStateDescriptor.Kind.KEYED_MAP,
                "temporal-record-v1", 1,
                StreamStateDescriptor.TtlMode.EVENT_TIME, 30L);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
