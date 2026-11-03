package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointAuthorityReference;
import io.rowweave.stream.contract.CheckpointExpectation;
import io.rowweave.stream.contract.CheckpointExpectedTask;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.runtime.FileCheckpointAuthority;
import io.rowweave.stream.runtime.FileCheckpointMemberStore;
import io.rowweave.stream.runtime.FileStreamCheckpointCatalog;
import io.rowweave.stream.runtime.StreamCheckpointCoordinator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Rescale publication must retain the checkpoint's per-task evidence bound. */
class StreamSyntheticCheckpointPlanTest {
    @Test
    void acceptsThe256And257MemberBoundariesAcrossPhysicalTasks(
            @TempDir Path root) {
        CheckpointIdentity checkpoint = new CheckpointIdentity(
                "definition",
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "run", "attempt", 1L, 1L,
                "coordinator", 1L,
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                2, 1);
        for (int size : new int[] {256, 257}) {
            List<StreamCheckpointMember> members = new ArrayList<>();
            for (int index = 0; index < size; index++) {
                members.add(StreamCheckpointMember.sourcePosition(
                        "source", index, String.format("%064x", index),
                        "position-codec", 1L,
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        1L));
            }
            StreamSyntheticCheckpointPlan plan =
                    StreamSyntheticCheckpointPlan.from(checkpoint, members);
            int evidenceCount = 0;
            for (CheckpointTaskReceipt receipt : plan.receipts()) {
                assertTrue(receipt.evidence().size() <= 256);
                evidenceCount += receipt.evidence().size();
            }
            assertEquals(size, evidenceCount);
            assertEquals(size, plan.expectation().tasks().size());

            Path authorityRoot = root.resolve("authority-" + size);
            FileCheckpointAuthority authority =
                    new FileCheckpointAuthority(authorityRoot);
            FileCheckpointMemberStore catalog =
                    new FileCheckpointMemberStore(authorityRoot);
            for (StreamCheckpointMember member : members) {
                catalog.publish(member);
            }
            StreamCheckpointCoordinator coordinator =
                    new StreamCheckpointCoordinator(plan.expectation(),
                            authority, catalog);
            for (CheckpointTaskReceipt receipt : plan.receipts()) {
                coordinator.acknowledge(receipt);
            }
            coordinator.complete(coordinator.decideCommit());
            assertEquals(size, catalog.resolve(
                    authority.decision(checkpoint), authority).size());
            if (size > 256) {
                RemoteStreamSourceSinkRunFactory.CheckpointTransferProof proof =
                        RemoteStreamSourceSinkRunFactory
                        .checkpointTransferProof(
                                authority.decision(checkpoint), authority,
                                new FileStreamCheckpointCatalog(authorityRoot));
                assertEquals(1, StreamCheckpointTransferBundleCodec.decode(
                        proof.forTask(new StreamWorkerPlacement.Assignment(
                                0, "source", 0, "worker", "inc")))
                        .members().size());
                byte[] reassignedSplit = StreamSourceTaskAssignmentCodec
                        .encode(new io.rowweave.stream.contract
                                .StreamSourceAssignment("run", "attempt",
                                "source", String.format("%064x", 1),
                                1L, 0, "inc", 2L),
                                new io.rowweave.stream.contract
                                .StreamSourceSplit(String.format("%064x", 1),
                                "position-codec", new byte[] {1}));
                byte[] reassignedProof = proof.forTask(
                        new StreamWorkerPlacement.Assignment(
                                0, "source", 0, "worker", "inc"),
                        reassignedSplit, true);
                assertEquals(members.get(1),
                        StreamCheckpointTransferBundleCodec.decode(
                                reassignedProof).members().get(0));
                CheckpointDecision first = authority.firstDecision(checkpoint);
                io.rowweave.stream.contract.CheckpointManifest manifest =
                        authority.manifest(first.manifestDigest());
                byte[] targetProof = StreamCheckpointTransferBundleCodec
                        .encode(first, manifest,
                                Collections.singletonList(members.get(0)));
                StreamProductExecutionGraph graph =
                        StreamProductExecutionGraph.from(
                                StreamAtomicCompositionProductTest
                                .session(null).toPlanArtifact(), 2);
                StreamRemoteWorkerServer.validateBundleMembers(
                        StreamCheckpointTransferBundleCodec.decode(targetProof),
                        graph, "source", 0);
                StreamRemoteWorkerServer.validateBundleMembers(
                        StreamCheckpointTransferBundleCodec.decode(
                                reassignedProof), graph, "source", 0,
                        reassignedSplit);
                // The source graph's default ordinal need not match the
                // physical ordinal at the checkpointed rescale generation.
                StreamCheckpointMember target = members.get(0);
                CheckpointTaskReceipt liveReceipt = new CheckpointTaskReceipt(
                        checkpoint, "stream-task:999", 1L, "worker",
                        Collections.singletonList(target.toEvidence()));
                CheckpointAuthorityReference live =
                        new CheckpointAuthorityReference(
                        new CheckpointExpectation(checkpoint,
                        Collections.singletonList(
                        new CheckpointExpectedTask("stream-task:999", 1L,
                                "worker", Collections.singletonList(
                                target.toEvidence().key())))));
                live.acknowledge(liveReceipt);
                CheckpointDecision liveDecision = live.decideCommit();
                byte[] liveProof = StreamCheckpointTransferBundleCodec.encode(
                        liveDecision, live.manifest(),
                        Collections.singletonList(target));
                StreamRemoteWorkerServer.validateBundleMembers(
                        StreamCheckpointTransferBundleCodec.decode(liveProof),
                        graph, "source", 0);
                byte[] omitted = StreamCheckpointTransferBundleCodec.encode(
                        first, manifest, Collections.emptyList());
                assertThrows(IllegalArgumentException.class,
                        () -> StreamRemoteWorkerServer.validateBundleMembers(
                                StreamCheckpointTransferBundleCodec.decode(
                                        omitted), graph, "source", 0));
            }
        }
    }
}
