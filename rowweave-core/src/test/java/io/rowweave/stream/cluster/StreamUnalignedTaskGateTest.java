package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamChannelState;
import io.rowweave.stream.contract.StreamChannelStateCodec;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.StreamChannelInput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** every physical task input shares one unaligned cut. */
class StreamUnalignedTaskGateTest {

    @Test
    void terminalInputAfterTriggerCompletesWithoutPhysicalBarrier(
            @TempDir Path root) throws Exception {
        CheckpointIdentity checkpoint = checkpoint("attempt-1", 1L);
        QueueInput source = new QueueInput(channel("attempt-1", "terminal"));
        source.add(StreamEnvelope.endOfInput(source.identity, 1L));
        StreamUnalignedTaskGate gate = new StreamUnalignedTaskGate(
                "sink", 0, Collections.singletonList(source),
                new FileCheckpointPayloadStore(root.resolve("payloads"),
                        64 * 1024L), Collections.emptyList(), false,
                8, 8 * 1024L, 16 * 1024L, 1024);
        java.util.concurrent.CompletableFuture<List<StreamCheckpointMember>>
                cut = gate.checkpoint(checkpoint);
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                gate.inputs().get(0).receive().kind());
        assertEquals(1, cut.get(2L, TimeUnit.SECONDS).size(),
                "a terminal source cannot emit a later physical barrier");
    }

    @Test
    void terminalInputBeforeTriggerReturnsItsCompletedCut(
            @TempDir Path root) throws Exception {
        CheckpointIdentity checkpoint = checkpoint("attempt-1", 1L);
        QueueInput source = new QueueInput(channel("attempt-1", "terminal"));
        source.add(StreamEnvelope.endOfInput(source.identity, 1L));
        StreamUnalignedTaskGate gate = new StreamUnalignedTaskGate(
                "sink", 0, Collections.singletonList(source),
                new FileCheckpointPayloadStore(root.resolve("payloads"),
                        64 * 1024L), Collections.emptyList(), false,
                8, 8 * 1024L, 16 * 1024L, 1024);
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                gate.inputs().get(0).receive().kind());
        assertEquals(1, gate.checkpoint(checkpoint)
                .get(2L, TimeUnit.SECONDS).size());
    }

    @Test
    void terminalLaneClosesAnInFlightMultiInputCut(
            @TempDir Path root) throws Exception {
        CheckpointIdentity checkpoint = checkpoint("attempt-1", 1L);
        QueueInput left = new QueueInput(channel("attempt-1", "left"));
        QueueInput right = new QueueInput(channel("attempt-1", "right"));
        left.add(StreamEnvelope.checkpointBarrier(
                left.identity, 1L, checkpoint));
        right.add(StreamEnvelope.endOfInput(right.identity, 1L));
        StreamUnalignedTaskGate gate = new StreamUnalignedTaskGate(
                "union", 0, Arrays.asList(left, right),
                new FileCheckpointPayloadStore(root.resolve("payloads"),
                        64 * 1024L), Collections.emptyList(), false,
                8, 8 * 1024L, 16 * 1024L, 1024);
        java.util.concurrent.CompletableFuture<List<StreamCheckpointMember>>
                cut = gate.checkpoint(checkpoint);
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                gate.inputs().get(0).receive().kind());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                gate.inputs().get(1).receive().kind());
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                gate.inputs().get(1).receive().kind());
        assertEquals(2, cut.get(2L, TimeUnit.SECONDS).size());
    }

    @Test
    void firstBarrierCutsEveryLaneWithoutBlockingAndPersistsCapturedFrames(
            @TempDir Path root) throws Exception {
        CheckpointIdentity checkpoint = checkpoint("attempt-1", 1L);
        QueueInput left = new QueueInput(channel("attempt-1", "left"));
        QueueInput right = new QueueInput(channel("attempt-1", "right"));
        left.add(record(left.identity, 1L, 1));
        left.add(StreamEnvelope.checkpointBarrier(
                left.identity, 2L, checkpoint));
        right.add(record(right.identity, 1L, 10));
        right.add(record(right.identity, 2L, 20));
        right.add(StreamEnvelope.checkpointBarrier(
                right.identity, 3L, checkpoint));
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 64 * 1024L);
        StreamUnalignedTaskGate gate = new StreamUnalignedTaskGate(
                "union", 0, Arrays.asList(left, right), store,
                Collections.emptyList(), false, 8, 8 * 1024L,
                16 * 1024L, 1024);

        List<StreamChannelInput> inputs = gate.inputs();
        assertEquals(StreamEnvelope.Kind.RECORD,
                inputs.get(0).receive().kind());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                inputs.get(0).receive().kind());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                inputs.get(1).receive().kind(),
                "the other lane must receive the same logical cut immediately");

        java.util.concurrent.CompletableFuture<List<StreamCheckpointMember>>
                cut = gate.checkpoint(checkpoint);
        assertFalse(cut.isDone(),
                "durability must still wait for every real lane barrier");
        assertArrayEquals(new byte[] {10},
                inputs.get(1).receive().recordFrame());
        assertArrayEquals(new byte[] {20},
                inputs.get(1).receive().recordFrame());
        assertEquals(null, inputs.get(1).poll(0L),
                "the later real barrier is swallowed by the common gate");

        List<StreamCheckpointMember> members = cut.get(2L, TimeUnit.SECONDS);
        assertEquals(2, members.size());
        assertEquals(StreamCheckpointMember.Kind.CHANNEL_STATE,
                members.get(0).kind());
        StreamChannelState leftState = restore(members.get(0), store, root);
        StreamChannelState rightState = restore(members.get(1), store, root);
        assertTrue(leftState.envelopes().isEmpty());
        assertEquals(2, rightState.envelopes().size());
        assertEquals(0L, rightState.lastConsumedSequence());
        assertEquals(3L, rightState.barrierSequence());
        assertArrayEquals(new byte[] {10},
                rightState.envelopes().get(0).recordFrame());
        assertArrayEquals(new byte[] {20},
                rightState.envelopes().get(1).recordFrame());
    }

    @Test
    void restoredFramesReplayBeforeNewTransportData(@TempDir Path root)
            throws Exception {
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 64 * 1024L);
        CheckpointIdentity checkpoint = checkpoint("attempt-1", 1L);
        QueueInput oldInput = new QueueInput(channel("attempt-1", "lane"));
        oldInput.add(StreamEnvelope.checkpointBarrier(
                oldInput.identity, 1L, checkpoint));
        oldInput.add(record(oldInput.identity, 2L, 7));
        // The record is deliberately supplied by a second lane in the first
        // test; construct an equivalent canonical member directly here.
        StreamChannelState state = StreamChannelState.capture(checkpoint,
                oldInput.identity, 0L, 2L,
                Collections.singletonList(record(oldInput.identity, 1L, 7)),
                8, 8 * 1024L, 1024);
        byte[] frame = StreamChannelStateCodec.encode(state);
        Path staged = root.resolve("channel.state");
        Files.write(staged, frame);
        FileCheckpointPayloadStore.StoredPayload stored = store.store(staged);
        StreamCheckpointMember member = StreamCheckpointMember.channelState(
                "map", 0, "lane", oldInput.identity.channelEpoch(),
                stored.digest(), stored.contentBytes());

        QueueInput current = new QueueInput(channel("attempt-2", "lane"));
        current.add(record(current.identity, 1L, 9));
        StreamUnalignedTaskGate restored = new StreamUnalignedTaskGate(
                "map", 0, Collections.singletonList(current), store,
                Collections.singletonList(member), true, 8, 8 * 1024L,
                16 * 1024L, 1024);

        StreamChannelInput input = restored.inputs().get(0);
        assertArrayEquals(new byte[] {7}, input.receive().recordFrame());
        assertArrayEquals(new byte[] {9}, input.receive().recordFrame());
    }

    @Test
    void allRestoredLanesDrainBeforeAnyNewTransportFrame(
            @TempDir Path root) throws Exception {
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 64 * 1024L);
        CheckpointIdentity checkpoint = checkpoint("attempt-1", 1L);
        List<StreamCheckpointMember> members = new java.util.ArrayList<>();
        for (String lane : Arrays.asList("old", "new")) {
            StreamChannelIdentity identity = channel("attempt-1", lane);
            List<StreamEnvelope> frames = lane.equals("old")
                    ? Arrays.asList(record(identity, 1L, 1),
                    record(identity, 2L, 2)) : Collections.emptyList();
            members.add(storeState(root, store, checkpoint, identity,
                    "map", frames));
        }
        QueueInput old = new QueueInput(channel("attempt-2", "old"));
        QueueInput newlyMapped = new QueueInput(channel("attempt-2", "new"));
        newlyMapped.add(record(newlyMapped.identity, 1L, 3));
        StreamUnalignedTaskGate gate = new StreamUnalignedTaskGate(
                "map", 0, Arrays.asList(old, newlyMapped), store,
                members, true, 8, 8 * 1024L, 16 * 1024L, 1024);
        List<StreamChannelInput> inputs = gate.inputs();
        assertArrayEquals(new byte[] {1}, inputs.get(0).poll(0L).recordFrame());
        assertEquals(null, inputs.get(1).poll(0L),
                "new transport must not pass an older replay on another lane");
        assertArrayEquals(new byte[] {2}, inputs.get(0).poll(0L).recordFrame());
        assertArrayEquals(new byte[] {3}, inputs.get(1).poll(0L).recordFrame());
    }

    @Test
    void liveStatusGenerationAdvancesBeyondMigratedReplay(
            @TempDir Path root) throws Exception {
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 64 * 1024L);
        CheckpointIdentity checkpoint = checkpoint("attempt-1", 1L);
        StreamChannelIdentity oldIdentity = channel("attempt-1", "lane");
        StreamCheckpointMember member = storeState(root, store,
                checkpoint, oldIdentity, "map", Collections.singletonList(
                StreamEnvelope.watermarkStatus(oldIdentity, 1L,
                        StreamEnvelope.WatermarkStatus.IDLE, 5L)));
        QueueInput live = new QueueInput(channel("attempt-2", "lane"));
        live.add(StreamEnvelope.watermarkStatus(live.identity, 1L,
                StreamEnvelope.WatermarkStatus.ACTIVE, 1L));
        StreamUnalignedTaskGate gate = new StreamUnalignedTaskGate(
                "map", 0, Collections.singletonList(live), store,
                Collections.singletonList(member), true,
                8, 8 * 1024L, 16 * 1024L, 1024);
        StreamChannelInput input = gate.inputs().get(0);
        assertEquals(5L, input.poll(0L).watermarkStatusGeneration());
        assertTrue(input.poll(0L).watermarkStatusGeneration() > 5L);
    }

    private static StreamCheckpointMember storeState(Path root,
            FileCheckpointPayloadStore store,
            CheckpointIdentity checkpoint, StreamChannelIdentity identity,
            String operatorUid, List<StreamEnvelope> frames)
            throws Exception {
        StreamChannelState state = StreamChannelState.capture(checkpoint,
                identity, 0L, frames.size() + 1L, frames,
                8, 8 * 1024L, 1024);
        Path staged = root.resolve(identity.channelId() + ".state");
        Files.write(staged, StreamChannelStateCodec.encode(state));
        FileCheckpointPayloadStore.StoredPayload stored = store.store(staged);
        return StreamCheckpointMember.channelState(operatorUid, 0,
                identity.channelId(), identity.channelEpoch(),
                stored.digest(), stored.contentBytes());
    }

    @Test
    void taskCheckpointWaitsForOperatorAndEveryPhysicalChannelMember(
            @TempDir Path root) throws Exception {
        CheckpointIdentity checkpoint = checkpoint("attempt-1", 1L);
        QueueInput left = new QueueInput(channel("attempt-1", "left"));
        QueueInput right = new QueueInput(channel("attempt-1", "right"));
        left.add(StreamEnvelope.checkpointBarrier(
                left.identity, 1L, checkpoint));
        right.add(StreamEnvelope.checkpointBarrier(
                right.identity, 1L, checkpoint));
        StreamUnalignedTaskGate gate = new StreamUnalignedTaskGate(
                "union", 0, Arrays.asList(left, right),
                new FileCheckpointPayloadStore(
                        root.resolve("payloads"), 64 * 1024L),
                Collections.emptyList(), false, 8, 8 * 1024L,
                16 * 1024L, 1024);
        StreamOperatorTask wrapped = gate.wrap(new StreamOperatorTask() {
            @Override public String operatorUid() { return "union"; }
        });

        java.util.concurrent.CompletableFuture<List<StreamCheckpointMember>>
                cut = wrapped.checkpoint(checkpoint);
        assertFalse(cut.isDone());
        gate.inputs().get(0).receive();
        assertFalse(cut.isDone(),
                "the operator snapshot alone cannot publish the cut");
        gate.inputs().get(1).receive();
        assertEquals(null, gate.inputs().get(1).poll(0L));
        assertEquals(2, cut.get(2L, TimeUnit.SECONDS).size());
    }

    @Test
    void failedOperatorCutDoesNotWaitForAnAbsentPhysicalBarrier(
            @TempDir Path root) throws Exception {
        CheckpointIdentity checkpoint = checkpoint("attempt-1", 1L);
        QueueInput input = new QueueInput(channel("attempt-1", "lane"));
        StreamUnalignedTaskGate gate = new StreamUnalignedTaskGate(
                "sink", 0, Collections.singletonList(input),
                new FileCheckpointPayloadStore(
                        root.resolve("payloads"), 64 * 1024L),
                Collections.emptyList(), false, 8, 8 * 1024L,
                16 * 1024L, 1024);
        IllegalStateException cause = new IllegalStateException(
                "sink segment failed before its barrier");
        StreamOperatorTask wrapped = gate.wrap(new StreamOperatorTask() {
            @Override public String operatorUid() { return "sink"; }
            @Override public java.util.concurrent.CompletableFuture<
                    List<StreamCheckpointMember>> checkpoint(
                    CheckpointIdentity ignored) {
                java.util.concurrent.CompletableFuture<
                        List<StreamCheckpointMember>> failed =
                        new java.util.concurrent.CompletableFuture<>();
                failed.completeExceptionally(cause);
                return failed;
            }
        });

        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> wrapped.checkpoint(checkpoint).get(
                        200L, TimeUnit.MILLISECONDS));
        assertEquals(cause, failure.getCause());
    }

    @Test
    void restoreRequiresOneChannelMemberForEveryPhysicalLane(
            @TempDir Path root) {
        QueueInput input = new QueueInput(channel("attempt-2", "lane"));
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> new StreamUnalignedTaskGate(
                        "map", 0, Collections.singletonList(input),
                        new FileCheckpointPayloadStore(
                                root.resolve("payloads"), 64 * 1024L),
                        Collections.emptyList(), true, 8, 8 * 1024L,
                        16 * 1024L, 1024));
        assertTrue(failure.getMessage().contains("omits"));
    }

    @Test
    void operatorAndChannelStateShareOneCheckpointByteLease(
            @TempDir Path root) throws Exception {
        CheckpointIdentity checkpoint = checkpoint("attempt-1", 1L);
        QueueInput input = new QueueInput(channel("attempt-1", "lane"));
        input.add(StreamEnvelope.checkpointBarrier(
                input.identity, 1L, checkpoint));
        long budget = 1024L;
        StreamUnalignedTaskGate gate = new StreamUnalignedTaskGate(
                "aggregate", 0, Collections.singletonList(input),
                new FileCheckpointPayloadStore(
                        root.resolve("payloads"), 64 * 1024L),
                Collections.emptyList(), false, 8, budget, budget, 1024);
        StreamOperatorTask wrapped = gate.wrap(new StreamOperatorTask() {
            @Override public String operatorUid() { return "aggregate"; }

            @Override public java.util.concurrent.CompletableFuture<
                    List<StreamCheckpointMember>> checkpoint(
                    CheckpointIdentity ignored) {
                return java.util.concurrent.CompletableFuture.completedFuture(
                        Collections.singletonList(
                                StreamCheckpointMember.stateHandle(
                                        "aggregate", 0, 0, 0, "state",
                                        "state-v1", 1, repeat('d'), budget)));
            }
        });

        java.util.concurrent.CompletableFuture<List<StreamCheckpointMember>>
                cut = wrapped.checkpoint(checkpoint);
        gate.inputs().get(0).receive();
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> cut.get(2L, TimeUnit.SECONDS));
        assertTrue(failure.getCause().getMessage().contains(
                "checkpoint state exceeds its resource lease"));
    }

    private static StreamChannelState restore(StreamCheckpointMember member,
            FileCheckpointPayloadStore store, Path root) throws Exception {
        Path restored = root.resolve(member.subjectId() + ".restored");
        store.restore(new FileCheckpointPayloadStore.StoredPayload(
                member.contentDigest(), member.contentBytes()), restored);
        return StreamChannelStateCodec.decode(Files.readAllBytes(restored),
                8, 8 * 1024L, 1024);
    }

    private static StreamEnvelope record(StreamChannelIdentity identity,
            long sequence, int value) {
        return StreamEnvelope.record(identity, sequence,
                new byte[] {(byte) value});
    }

    private static StreamChannelIdentity channel(
            String attemptId, String channelId) {
        return new StreamChannelIdentity("run", attemptId, 7L, 1L,
                channelId, 1L);
    }

    private static CheckpointIdentity checkpoint(
            String attemptId, long sequence) {
        return new CheckpointIdentity("definition", repeat('b'), "run",
                attemptId, 7L, 1L, "coordinator", sequence,
                repeat('a'), repeat('c'), 2, 1);
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static final class QueueInput implements StreamChannelInput {
        final StreamChannelIdentity identity;
        final ArrayDeque<StreamEnvelope> values = new ArrayDeque<>();

        private QueueInput(StreamChannelIdentity identity) {
            this.identity = identity;
        }

        void add(StreamEnvelope envelope) { values.addLast(envelope); }

        @Override public StreamChannelIdentity channelIdentity() {
            return identity;
        }

        @Override public StreamEnvelope receive() {
            return values.pollFirst();
        }

        @Override public StreamEnvelope poll(long timeoutMillis) {
            return values.pollFirst();
        }

        @Override public void cancel() { }

        @Override public void fail(Throwable cause) { }
    }
}
