package io.rowweave.stream.runtime;

import io.rowweave.mem.MemoryManager;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointEvidenceKey;
import io.rowweave.stream.contract.CheckpointExpectation;
import io.rowweave.stream.contract.CheckpointExpectedTask;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointTaskReceipt;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamStateDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Retention must preserve savepoints while reclaiming all old cut layers. */
class FileCheckpointRetentionTest {
    private static final String A = digest('a');
    private static final String B = digest('b');

    @Test
    void prunesThroughSharedAuthorityAndCatalogInterfaces(
            @TempDir Path root) throws Exception {
        StreamCheckpointAuthority authority =
                new InMemoryStreamCheckpointAuthority();
        StreamCheckpointCatalog catalog =
                new InMemoryStreamCheckpointCatalog();
        FileCheckpointPayloadStore payloads =
                new FileCheckpointPayloadStore(root, 1_024L);

        Cut first = cut(root, authority, catalog, payloads,
                1L, "stream-checkpoint:first", (byte) 1);
        catalog.create("stream-savepoint:user", first.identity, authority);
        Cut second = cut(root, authority, catalog, payloads,
                2L, "stream-checkpoint:second", (byte) 2);
        Cut third = cut(root, authority, catalog, payloads,
                3L, "stream-checkpoint:third", (byte) 3);

        Map<String, CheckpointIdentity> recoveryRoots = new LinkedHashMap<>();
        recoveryRoots.put("run-1", third.identity);
        int removed = new FileCheckpointRetention(
                authority, catalog, payloads,
                recoveryRoots, Collections.singleton("run-1"))
                .retainLatest("run-1", 1);

        assertEquals(2, removed);
        assertNotNull(catalog.get("stream-savepoint:user"));
        assertNotNull(authority.decision(first.identity));
        assertNull(authority.decision(second.identity));
        assertNotNull(authority.decision(third.identity));
        assertThrows(StreamCheckpointException.class,
                () -> catalog.member(second.member.toEvidence()));
        payloads.verifyStored(first.payload);
        assertThrows(StreamCheckpointException.class,
                () -> payloads.verifyStored(second.payload));
        payloads.verifyStored(third.payload);
    }

    @Test
    void prunesManifestMemberAndPayloadButNeverNamedSavepoint(
            @TempDir Path root) throws Exception {
        FileCheckpointAuthority authority = new FileCheckpointAuthority(
                root.resolve("authority"));
        FileCheckpointMemberStore members =
                new FileCheckpointMemberStore(root);
        FileCheckpointPayloadStore payloads =
                new FileCheckpointPayloadStore(root, 1_024L);
        FileStreamSavepointStore savepoints =
                new FileStreamSavepointStore(root);

        Cut first = cut(root, authority, members, payloads, savepoints,
                1L, "stream-checkpoint:first", (byte) 1);
        savepoints.create("stream-savepoint:user", first.identity, authority);
        Cut second = cut(root, authority, members, payloads, savepoints,
                2L, "stream-checkpoint:second", (byte) 2);
        Cut third = cut(root, authority, members, payloads, savepoints,
                3L, "stream-checkpoint:third", (byte) 3);

        int removed = new FileCheckpointRetention(
                authority, members, payloads, savepoints)
                .retainLatest("run-1", 1);

        assertEquals(2, removed);
        assertNull(savepoints.get("stream-checkpoint:first"));
        assertNull(savepoints.get("stream-checkpoint:second"));
        assertNotNull(savepoints.get("stream-checkpoint:third"));
        assertNotNull(savepoints.get("stream-savepoint:user"));

        assertEquals(CheckpointDecision.Status.COMPLETED,
                authority.decision(first.identity).status());
        assertNull(authority.decision(second.identity));
        assertEquals(CheckpointDecision.Status.COMPLETED,
                authority.decision(third.identity).status());
        assertEquals(Collections.singletonList(first.member),
                members.resolve(authority.decision(first.identity), authority));
        assertEquals(Collections.singletonList(third.member),
                members.resolve(authority.decision(third.identity), authority));
        payloads.verifyStored(first.payload);
        payloads.verifyStored(third.payload);
        assertThrows(StreamCheckpointException.class,
                () -> payloads.verifyStored(second.payload));
    }

    @Test
    void nextSweepRecoversOrphansAfterReferenceRemovalCrash(
            @TempDir Path root) throws Exception {
        FileCheckpointAuthority authority = new FileCheckpointAuthority(
                root.resolve("authority"));
        FileCheckpointMemberStore members =
                new FileCheckpointMemberStore(root);
        FileCheckpointPayloadStore payloads =
                new FileCheckpointPayloadStore(root, 1_024L);
        FileStreamSavepointStore savepoints =
                new FileStreamSavepointStore(root);
        Cut retained = cut(root, authority, members, payloads, savepoints,
                1L, "stream-checkpoint:retained", (byte) 1);
        Cut orphan = cut(root, authority, members, payloads, savepoints,
                2L, "stream-checkpoint:orphan", (byte) 2);
        savepoints.removeCheckpointReference("stream-checkpoint:orphan");

        int removed = new FileCheckpointRetention(
                authority, members, payloads, savepoints)
                .retainLatest("run-1", 1);

        assertEquals(0, removed,
                "the reference was already removed before recovery");
        assertNotNull(authority.decision(retained.identity));
        assertNull(authority.decision(orphan.identity));
        payloads.verifyStored(retained.payload);
        assertThrows(StreamCheckpointException.class,
                () -> payloads.verifyStored(orphan.payload));
    }

    @Test
    void retainedWindowKeepsItsNestedManagedStatePayload(
            @TempDir Path root) throws Exception {
        Path checkpointRoot = root.resolve("checkpoints");
        FileCheckpointAuthority authority = new FileCheckpointAuthority(
                checkpointRoot.resolve("authority"));
        FileCheckpointMemberStore members =
                new FileCheckpointMemberStore(checkpointRoot);
        FileCheckpointPayloadStore payloads =
                new FileCheckpointPayloadStore(checkpointRoot, 64 * 1024L);
        FileStreamSavepointStore savepoints =
                new FileStreamSavepointStore(checkpointRoot);
        StreamCheckpointMember composite;
        try (FinalEventTimeDecimalWindow window = window(
                root.resolve("initial"), ignored -> { })) {
            window.process(new byte[] {1}, new BigDecimal("3.25"), 10L);
            composite = window.snapshot(0, payloads);
        }
        StreamCheckpointMember nested =
                FinalEventTimeDecimalWindow.nestedCheckpointMember(
                        composite, payloads);
        assertNotNull(nested);
        members.publish(composite);
        CheckpointIdentity identity = new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-1", 1L,
                1L, "coordinator-1", 1L, A, B, 1, 1);
        CheckpointExpectation expectation = new CheckpointExpectation(
                identity, Collections.singletonList(new CheckpointExpectedTask(
                "task-0", 1L, "worker-1", Collections.singletonList(
                new CheckpointEvidenceKey(
                        composite.kind().evidenceKind(),
                        composite.memberId())))));
        StreamCheckpointCoordinator coordinator =
                new StreamCheckpointCoordinator(expectation, authority, members);
        coordinator.acknowledge(new CheckpointTaskReceipt(
                identity, "task-0", 1L, "worker-1",
                Collections.singletonList(composite.toEvidence())));
        coordinator.complete(coordinator.decideCommit());
        savepoints.create("stream-checkpoint:window", identity, authority);

        new FileCheckpointRetention(authority, members, payloads, savepoints)
                .retainLatest("run-1", 1);

        payloads.verifyStored(new FileCheckpointPayloadStore.StoredPayload(
                nested.contentDigest(), nested.contentBytes()));
        List<FinalEventTimeDecimalWindow.Result> output = new ArrayList<>();
        try (FinalEventTimeDecimalWindow restored = window(
                root.resolve("restored"), output::add)) {
            restored.restore(0, composite, payloads);
            restored.advanceWatermark(100L);
        }
        assertEquals(1, output.size());
        assertEquals(new BigDecimal("3.2500000000"), output.get(0).sum());
    }

    private static FinalEventTimeDecimalWindow window(
            Path root,
            java.util.function.Consumer<FinalEventTimeDecimalWindow.Result>
                    output) {
        StreamStateDescriptor state = new StreamStateDescriptor(
                "sum", StreamStateDescriptor.Kind.KEYED_VALUE,
                FinalEventTimeDecimalWindow.DECIMAL_STATE_SERIALIZER,
                1, StreamStateDescriptor.TtlMode.NONE, 0L);
        return new FinalEventTimeDecimalWindow(
                "artifact", "window", state, 1, 0, 0,
                FinalEventTimeDecimalWindow.WindowType.TUMBLING,
                100L, 100L, 8, 16, new MemoryManager(384), root,
                64 * 1024L, 128, 1024, output);
    }

    private static Cut cut(Path root, FileCheckpointAuthority authority,
            FileCheckpointMemberStore members,
            FileCheckpointPayloadStore payloads,
            FileStreamSavepointStore savepoints, long sequence,
            String checkpointId, byte value) throws Exception {
        Path staged = root.resolve("staged-" + sequence);
        Files.write(staged, new byte[] {value, value, value});
        FileCheckpointPayloadStore.StoredPayload payload =
                payloads.store(staged);
        StreamCheckpointMember member = StreamCheckpointMember.stateHandle(
                "aggregate", 0, 0, 127, "readout", "decimal-v1", 1,
                payload.digest(), payload.contentBytes());
        members.publish(member);
        CheckpointIdentity identity = new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-1", 1L,
                1L, "coordinator-1", sequence, A, B, 1, 1);
        CheckpointExpectation expectation = new CheckpointExpectation(
                identity, Collections.singletonList(new CheckpointExpectedTask(
                "task-0", 1L, "worker-1", Collections.singletonList(
                new CheckpointEvidenceKey(
                        member.kind().evidenceKind(), member.memberId())))));
        StreamCheckpointCoordinator coordinator =
                new StreamCheckpointCoordinator(expectation, authority, members);
        coordinator.acknowledge(new CheckpointTaskReceipt(
                identity, "task-0", 1L, "worker-1",
                Collections.singletonList(member.toEvidence())));
        CheckpointDecision committed = coordinator.decideCommit();
        coordinator.complete(committed);
        savepoints.create(checkpointId, identity, authority);
        return new Cut(identity, member, payload);
    }

    private static Cut cut(Path root, StreamCheckpointAuthority authority,
            StreamCheckpointCatalog catalog,
            FileCheckpointPayloadStore payloads, long sequence,
            String checkpointId, byte value) throws Exception {
        Path staged = root.resolve("shared-staged-" + sequence);
        Files.write(staged, new byte[] {value, value, value});
        FileCheckpointPayloadStore.StoredPayload payload =
                payloads.store(staged);
        StreamCheckpointMember member = StreamCheckpointMember.stateHandle(
                "aggregate", 0, 0, 127, "readout", "decimal-v1", 1,
                payload.digest(), payload.contentBytes());
        catalog.publish(member);
        CheckpointIdentity identity = new CheckpointIdentity(
                "definition-1", A, "run-1", "attempt-1", 1L,
                1L, "coordinator-1", sequence, A, B, 1, 1);
        CheckpointExpectation expectation = new CheckpointExpectation(
                identity, Collections.singletonList(new CheckpointExpectedTask(
                "task-0", 1L, "worker-1", Collections.singletonList(
                new CheckpointEvidenceKey(
                        member.kind().evidenceKind(), member.memberId())))));
        StreamCheckpointCoordinator coordinator =
                new StreamCheckpointCoordinator(expectation, authority, catalog);
        coordinator.acknowledge(new CheckpointTaskReceipt(
                identity, "task-0", 1L, "worker-1",
                Collections.singletonList(member.toEvidence())));
        coordinator.complete(coordinator.decideCommit());
        catalog.create(checkpointId, identity, authority);
        return new Cut(identity, member, payload);
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static final class Cut {
        private final CheckpointIdentity identity;
        private final StreamCheckpointMember member;
        private final FileCheckpointPayloadStore.StoredPayload payload;

        private Cut(CheckpointIdentity identity,
                StreamCheckpointMember member,
                FileCheckpointPayloadStore.StoredPayload payload) {
            this.identity = identity;
            this.member = member;
            this.payload = payload;
        }
    }
}
