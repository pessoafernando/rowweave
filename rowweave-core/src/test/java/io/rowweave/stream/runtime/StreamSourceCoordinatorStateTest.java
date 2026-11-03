package io.rowweave.stream.runtime;

import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.StreamSourceAssignment;
import io.rowweave.stream.contract.StreamSourceCoordinatorSplit;
import io.rowweave.stream.contract.StreamSourceCoordinatorState;
import io.rowweave.stream.contract.StreamSourceCoordinatorStateCodec;
import io.rowweave.stream.contract.StreamSourceDiscoveryDescriptor;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamSplitChange;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** coordinator state is the checkpoint/recovery authority. */
class StreamSourceCoordinatorStateTest {

    @Test
    void duplicateDiscoveryAndAssignmentAckLossAreIdempotent() {
        StreamSourceCoordinator coordinator = coordinator(limits(4, 4, 2));
        StreamSplitDiscovery discovery = discovery(1L, 1,
                StreamSplitChange.add(split('a', 1), 3L));

        coordinator.applyDiscovery(discovery);
        StreamSourceAssignment first = coordinator.assign(
                repeat('a'), 3L, 1, "worker-a", 7L);
        coordinator.applyDiscovery(discovery);
        StreamSourceAssignment replay = coordinator.assign(
                repeat('a'), 3L, 1, "worker-a", 7L);

        assertEquals(first, replay);
        StreamSourceCoordinatorSplit assigned =
                coordinator.state().splits().get(0);
        assertEquals(StreamSourceCoordinatorSplit.Status.ASSIGNED,
                assigned.status());
        assertEquals(7L, assigned.assignment().assignmentGeneration());
        assertThrows(IllegalStateException.class, () -> coordinator.assign(
                repeat('a'), 3L, 1, "worker-b", 7L));
    }

    @Test
    void workerReplacementAndRemoveAddAbaFenceOldGenerations() {
        StreamSourceCoordinator coordinator = coordinator(limits(8, 8, 4));
        coordinator.applyDiscovery(discovery(1L, 1,
                StreamSplitChange.add(split('a', 1), 1L)));
        coordinator.assign(repeat('a'), 1L, 0, "worker-a", 2L);
        coordinator.recordPosition(repeat('a'), 1L, 2L,
                new byte[] {8, 9});
        coordinator.revokeWorker("worker-a");
        StreamSourceAssignment replacement = coordinator.assign(
                repeat('a'), 1L, 1, "worker-b", 3L);
        assertEquals("worker-b", replacement.workerIncarnation());

        coordinator.finish(repeat('a'), 1L, 3L);
        coordinator.applyDiscovery(discovery(2L, 2,
                StreamSplitChange.remove(repeat('a'), 1L)));
        coordinator.applyDiscovery(discovery(3L, 3,
                StreamSplitChange.add(split('a', 2), 2L)));
        coordinator.applyDiscovery(discovery(1L, 1,
                StreamSplitChange.add(split('a', 1), 1L)));

        StreamSourceCoordinatorSplit current =
                coordinator.state().splits().get(0);
        assertEquals(2L, current.splitGeneration());
        assertEquals(StreamSourceCoordinatorSplit.Status.PENDING,
                current.status());
        assertThrows(IllegalStateException.class, () -> coordinator.assign(
                repeat('a'), 1L, 0, "worker-a", 4L));
    }

    @Test
    void checkpointRoundTripPreservesCursorAssignmentsAndPositions() {
        StreamSourceCoordinator coordinator = coordinator(limits(4, 4, 2));
        coordinator.applyDiscovery(discovery(9L, 2,
                StreamSplitChange.add(split('b', 2), 4L),
                StreamSplitChange.add(split('a', 1), 3L)));
        coordinator.assign(repeat('a'), 3L, 0, "worker-a", 5L);
        coordinator.recordPosition(repeat('a'), 3L, 12L,
                new byte[] {6, 7});

        StreamSourceCoordinatorState state = coordinator.state();
        byte[] encoded = StreamSourceCoordinatorStateCodec.encode(state);
        StreamSourceCoordinatorState decoded =
                StreamSourceCoordinatorStateCodec.decode(encoded);
        StreamSourceCoordinator restored = StreamSourceCoordinator.restore(
                decoded, limits(4, 4, 2));

        assertEquals(state, decoded);
        assertEquals(state, restored.state());
        assertArrayEquals(new byte[] {0, 0, 0, 2},
                restored.state().cursor().frame());
        assertArrayEquals(new byte[] {6, 7}, restored.state().splits()
                .get(0).positionFrame());
    }

    @Test
    void discoveryLimitsFailBeforeStateMutation() {
        StreamSourceCoordinator coordinator = coordinator(limits(4, 4, 1));
        StreamSourceCoordinatorState before = coordinator.state();

        assertThrows(IllegalArgumentException.class,
                () -> coordinator.applyDiscovery(discovery(1L, 1,
                        StreamSplitChange.add(split('a', 1), 1L),
                        StreamSplitChange.add(split('b', 2), 1L))));
        assertEquals(before, coordinator.state());
    }

    @Test
    void knownAndPendingSplitLimitsAcceptBoundaryAndRejectBoundaryPlusOne() {
        StreamSourceCoordinator known = coordinator(limits(2, 2, 2));
        known.applyDiscovery(discovery(1L, 1,
                StreamSplitChange.add(split('a', 1), 1L)));
        assertEquals(1, known.state().splits().size());
        known.applyDiscovery(discovery(2L, 2,
                StreamSplitChange.add(split('b', 2), 1L)));
        assertEquals(2, known.state().splits().size());
        StreamSourceCoordinatorState atKnownLimit = known.state();
        assertThrows(IllegalArgumentException.class,
                () -> known.applyDiscovery(discovery(3L, 3,
                        StreamSplitChange.add(split('c', 3), 1L))));
        assertEquals(atKnownLimit, known.state());

        StreamSourceCoordinator pending = coordinator(limits(4, 2, 2));
        pending.applyDiscovery(discovery(1L, 1,
                StreamSplitChange.add(split('a', 1), 1L)));
        assertEquals(1, pendingCount(pending.state()));
        pending.applyDiscovery(discovery(2L, 2,
                StreamSplitChange.add(split('b', 2), 1L)));
        assertEquals(2, pendingCount(pending.state()));
        StreamSourceCoordinatorState atPendingLimit = pending.state();
        assertThrows(IllegalArgumentException.class,
                () -> pending.applyDiscovery(discovery(3L, 3,
                        StreamSplitChange.add(split('c', 3), 1L))));
        assertEquals(atPendingLimit, pending.state());
    }

    @Test
    void cursorAndDescriptorByteLimitsAreExactAndTransactional() {
        StreamSourceDiscoveryDescriptor cursorLimits =
                new StreamSourceDiscoveryDescriptor(
                        10L, 100L, 4, 4, 1, 32, 64);
        StreamSourceCoordinator cursor = coordinator(cursorLimits);
        cursor.applyDiscovery(new StreamSplitDiscovery(1L,
                StreamDiscoveryCursor.of(new byte[31]),
                Collections.emptyList()));
        cursor.applyDiscovery(new StreamSplitDiscovery(2L,
                StreamDiscoveryCursor.of(new byte[32]),
                Collections.emptyList()));
        StreamSourceCoordinatorState atCursorLimit = cursor.state();
        assertThrows(IllegalArgumentException.class,
                () -> cursor.applyDiscovery(new StreamSplitDiscovery(3L,
                        StreamDiscoveryCursor.of(new byte[33]),
                        Collections.emptyList())));
        assertEquals(atCursorLimit, cursor.state());

        StreamSourceCoordinator descriptor = coordinator(cursorLimits);
        descriptor.applyDiscovery(new StreamSplitDiscovery(1L,
                StreamDiscoveryCursor.initial(),
                Collections.singletonList(StreamSplitChange.add(
                        splitWithDescriptor('a', 63), 1L))));
        descriptor.applyDiscovery(new StreamSplitDiscovery(2L,
                StreamDiscoveryCursor.initial(),
                Collections.singletonList(StreamSplitChange.add(
                        splitWithDescriptor('b', 64), 1L))));
        StreamSourceCoordinatorState atDescriptorLimit = descriptor.state();
        assertThrows(IllegalArgumentException.class,
                () -> descriptor.applyDiscovery(new StreamSplitDiscovery(
                        3L, StreamDiscoveryCursor.initial(),
                        Collections.singletonList(StreamSplitChange.add(
                                splitWithDescriptor('c', 65), 1L)))));
        assertEquals(atDescriptorLimit, descriptor.state());
    }

    private static StreamSourceCoordinator coordinator(
            StreamSourceDiscoveryDescriptor limits) {
        return StreamSourceCoordinator.initial(
                "job-1", "attempt-1", "source", limits);
    }

    private static StreamSourceDiscoveryDescriptor limits(
            int known, int pending, int changes) {
        return new StreamSourceDiscoveryDescriptor(
                10L, 100L, known, pending, changes, 32, 64);
    }

    private static StreamSplitDiscovery discovery(long epoch, int cursor,
            StreamSplitChange... changes) {
        return new StreamSplitDiscovery(epoch,
                StreamDiscoveryCursor.of(new byte[] {0, 0, 0,
                        (byte) cursor}), Arrays.asList(changes));
    }

    private static StreamSplitSpec split(char id, int descriptor) {
        return new StreamSplitSpec(repeat(id), "position-v1",
                new byte[] {(byte) descriptor});
    }

    private static StreamSplitSpec splitWithDescriptor(char id, int bytes) {
        return new StreamSplitSpec(repeat(id), "position-v1",
                new byte[bytes]);
    }

    private static int pendingCount(StreamSourceCoordinatorState state) {
        int result = 0;
        for (StreamSourceCoordinatorSplit split : state.splits()) {
            if (split.status()
                    == StreamSourceCoordinatorSplit.Status.PENDING) result++;
        }
        return result;
    }

    private static String repeat(char value) {
        char[] result = new char[64];
        Arrays.fill(result, value);
        return new String(result);
    }
}
