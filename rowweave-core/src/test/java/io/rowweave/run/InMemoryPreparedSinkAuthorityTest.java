package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.rpc.RpcCodec;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** prepared evidence is a second exact CAS after terminal. */
class InMemoryPreparedSinkAuthorityTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType));
    private static final BatchSinkDescriptor DESCRIPTOR = new BatchSinkDescriptor(
            1, "rowweave.manifest-file", 1, SchemaFingerprint.of(META),
            "domain:member", "domain",
            BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);

    @Test
    void preparedCasRequiresTerminalAndReplaysExactLocatorOnly() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        Fixture fixture = fixture(store, "run-prepared");
        PreparedSinkReceipt prepared = prepared(fixture, new byte[]{1, 2, 3});

        assertEquals(PreparedSinkDecision.Code.TERMINAL_REQUIRED,
                store.recordPreparedSink(prepared).code());
        assertEquals(StageTerminalDecision.Code.RECORDED,
                store.recordTerminal(fixture.terminal).code());
        assertEquals(PreparedSinkDecision.Code.RECORDED,
                store.recordPreparedSink(prepared).code());
        assertEquals(PreparedSinkDecision.Code.ALREADY_RECORDED,
                store.recordPreparedSink(prepared(fixture,
                        new byte[]{1, 2, 3})).code());
        assertEquals(PreparedSinkDecision.Code.LOCATOR_CONFLICT,
                store.recordPreparedSink(prepared(fixture,
                        new byte[]{9, 9, 9})).code());
        assertEquals(prepared, store.assignmentSnapshot(
                fixture.manifest.fence()).preparedSink(0));
    }

    @Test
    void rpcKeepsTerminalCodecBytesIndependentFromPreparedEvidence()
            throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        Fixture fixture = fixture(store, "run-rpc-prepared");
        PreparedSinkReceipt prepared = prepared(fixture, new byte[]{4, 5, 6});
        byte[] terminalBefore = StageAssignmentManifestCodec
                .encodeTerminalReceipt(fixture.terminal);

        byte[] wire = RpcCodec.encode(RpcMessage.durableStageReport(
                "worker", fixture.terminal, prepared));
        RpcMessage decoded = RpcCodec.decode(new DataInputStream(
                new ByteArrayInputStream(wire)));

        assertArrayEquals(terminalBefore, decoded.stageTerminalReceiptBytes());
        assertEquals(prepared, PreparedSinkReceiptCodec.decode(
                decoded.preparedSinkReceiptBytes()));
    }

    private static Fixture fixture(InMemoryRunStateStore store, String runId) {
        RunDecision created = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact", 2, 0L, 1L));
        RunSnapshot run = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, created.snapshot().revision(),
                "attempt", "publication", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(
                runId, "attempt", run.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 2, 0, StageAssignmentRole.SINK,
                "worker", "localhost", 7001, -1, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, "artifact", 1L, Collections.singletonList(record));
        StageAssignmentSnapshot snapshot = store.publish(manifest).snapshot();
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                snapshot.token(0), filled(1), 10L);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                store.accept(acceptance).code());
        StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                acceptance.token(), acceptance.commandDigest(), 3L, 3L, 20L);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                manifest, record, "sink-op", DESCRIPTOR);
        return new Fixture(manifest, acceptance, terminal, identity);
    }

    private static PreparedSinkReceipt prepared(Fixture fixture, byte[] payload) {
        PreparedSinkLocator locator = new PreparedSinkLocator(
                PreparedSinkLocator.CURRENT_VERSION, fixture.identity,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", payload, 3L, sha256(new byte[]{7, 8, 9}), 12L);
        return new PreparedSinkReceipt(
                fixture.acceptance.token(), fixture.acceptance.commandDigest(), locator);
    }

    private static byte[] filled(int value) {
        byte[] bytes = new byte[32];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static byte[] sha256(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (Exception impossible) { throw new AssertionError(impossible); }
    }

    private static final class Fixture {
        final StageAssignmentManifest manifest;
        final StageAssignmentAcceptance acceptance;
        final StageTerminalReceipt terminal;
        final SinkAttemptIdentity identity;

        Fixture(StageAssignmentManifest manifest,
                StageAssignmentAcceptance acceptance,
                StageTerminalReceipt terminal, SinkAttemptIdentity identity) {
            this.manifest = manifest;
            this.acceptance = acceptance;
            this.terminal = terminal;
            this.identity = identity;
        }
    }
}
