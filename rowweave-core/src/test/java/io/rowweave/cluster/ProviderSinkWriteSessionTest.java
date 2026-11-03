package io.rowweave.cluster;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.DurableBatchSinkPreparedGroup;
import io.rowweave.DurableBatchSinkProvider;
import io.rowweave.DurableBatchSinkProviderContract;
import io.rowweave.DurableBatchSinkWriteAttempt;
import io.rowweave.DurableSinkActionContext;
import io.rowweave.DurableSinkAttemptBudget;
import io.rowweave.DurableSinkCleanupContract;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.DurableSinkProviderKey;
import io.rowweave.DurableSinkResourceLease;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** provider writes stay synchronous Push under one fenced permit. */
class ProviderSinkWriteSessionTest {
    private static final DataSchema META = new DataSchema(
            new io.rowweave.Field("id", DataType.IntegerType));

    @Test
    void writesRowsDirectlyPreparesCanonicalLocatorAndClosesResourcesOnce()
            throws Exception {
        Fixture fixture = fixture();
        RecordingProvider provider = new RecordingProvider(fixture.contract, false);
        AtomicInteger reserved = new AtomicInteger();
        ProviderSinkWriteSession session = ProviderSinkWriteSession.open(
                provider, fixture.contract, fixture.output, fixture.identity,
                "publication", 3L, ProviderActionSupervisor.processWide(4),
                bytes -> reserved.addAndGet(Math.toIntExact(bytes)), () -> false);

        DataRow first = new DataRow(1); first.set(0, 1);
        DataRow second = new DataRow(1); second.set(0, 2);
        session.collect(first);
        assertEquals(1, provider.writes.get(),
                "collect must synchronously enter provider code without row buffering");
        session.collect(second);
        session.prepareCommit();

        assertEquals(2, provider.writes.get());
        assertEquals(fixture.identity, session.preparedLocator().attemptIdentity());
        assertArrayEquals(new byte[]{2}, session.preparedLocator().payload());
        assertEquals(1, provider.closes.get());
        assertEquals(96, reserved.get());
        assertFalse(provider.residentLease.isOpen());
        assertFalse(provider.handleLease.isOpen());
        session.rollbackCommit();
        assertEquals(1, provider.closes.get(), "cleanup must be exactly once");
    }

    @Test
    void zeroRowsStillPreparesAndNoncanonicalConfigurationNeverOpensAttempt()
            throws Exception {
        Fixture fixture = fixture();
        RecordingProvider zero = new RecordingProvider(fixture.contract, false);
        ProviderSinkWriteSession session = ProviderSinkWriteSession.open(
                zero, fixture.contract, fixture.output, fixture.identity,
                "publication", 3L, ProviderActionSupervisor.processWide(4),
                ignored -> { }, () -> false);
        session.prepareCommit();
        assertEquals(0, zero.writes.get());
        assertEquals(1, zero.prepares.get());

        RecordingProvider drift = new RecordingProvider(fixture.contract, true);
        IllegalArgumentException rejected = assertThrows(
                IllegalArgumentException.class, () -> ProviderSinkWriteSession.open(
                        drift, fixture.contract, fixture.output, fixture.identity,
                        "publication", 3L, ProviderActionSupervisor.processWide(4),
                        ignored -> { }, () -> false));
        assertTrue(rejected.getMessage().contains("NONCANONICAL"));
        assertEquals(0, drift.opens.get());
    }

    private static Fixture fixture() throws Exception {
        DurableSinkProviderKey key = new DurableSinkProviderKey(
                "test.provider", 1,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        DurableSinkCodecDescriptor config = new DurableSinkCodecDescriptor(
                "test.config", 1, 128);
        DurableSinkCodecDescriptor locator = new DurableSinkCodecDescriptor(
                "test.locator", 1, 128);
        DurableBatchSinkProviderContract contract = new DurableBatchSinkProviderContract(
                1, key, config, locator,
                new DurableSinkCodecDescriptor("test.outcome", 1, 128),
                DurableBatchSinkProviderContract.IdempotencyScope
                        .ATTEMPT_AND_PUBLICATION_V1,
                new DurableSinkCleanupContract(1_000L, 10, 10, true,
                        DurableBatchSinkProviderContract.OrphanOwnership
                                .ATTEMPT_PRIVATE_V1,
                        DurableBatchSinkProviderContract.QuarantinePolicy
                                .PRESERVE_ON_UNKNOWN_OR_CONFLICT,
                        DurableBatchSinkProviderContract.SafeDeleteProof
                                .ABORT_DECISION_OR_UNREFERENCED_RETENTION_V1),
                new DurableSinkAttemptBudget(1_024L, 2, 128, 1_024, 5_000L),
                DurableBatchSinkProviderContract.DeploymentCapability
                        .SIGNED_TEST_CONFORMANCE_V1);
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, key.connectorType(), key.connectorVersion(),
                SchemaFingerprint.of(META), "destination", "domain",
                key.capabilityProfile());
        ProviderBatchSink output = new ProviderBatchSink(
                META, descriptor, config, new byte[]{1});
        RunAttemptFence fence = new RunAttemptFence("run", "attempt", 1L);
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 0, 0, StageAssignmentRole.SINK,
                "worker", "localhost", 7001, -1, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, "artifact", 1L, java.util.Collections.singletonList(assignment));
        return new Fixture(contract, output, SinkAttemptIdentity.forAssignment(
                manifest, assignment, "sink-op", descriptor));
    }

    private static final class Fixture {
        final DurableBatchSinkProviderContract contract;
        final ProviderBatchSink output;
        final SinkAttemptIdentity identity;

        Fixture(DurableBatchSinkProviderContract contract,
                ProviderBatchSink output, SinkAttemptIdentity identity) {
            this.contract = contract;
            this.output = output;
            this.identity = identity;
        }
    }

    private static final class RecordingProvider implements DurableBatchSinkProvider {
        final DurableBatchSinkProviderContract contract;
        final boolean driftConfiguration;
        final AtomicInteger opens = new AtomicInteger();
        final AtomicInteger writes = new AtomicInteger();
        final AtomicInteger prepares = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        volatile DurableSinkResourceLease residentLease;
        volatile DurableSinkResourceLease handleLease;

        RecordingProvider(DurableBatchSinkProviderContract contract,
                          boolean driftConfiguration) {
            this.contract = contract;
            this.driftConfiguration = driftConfiguration;
        }

        @Override public DurableBatchSinkProviderContract contract() {
            return contract;
        }
        @Override public byte[] canonicalizeConfiguration(
                ProviderBatchSink output) {
            return driftConfiguration ? new byte[]{9} : output.configurationBytes();
        }
        @Override public byte[] canonicalizeLocator(PreparedSinkLocator locator) {
            return locator.payload();
        }
        @Override public DurableBatchSinkWriteAttempt openWriteAttempt(
                ProviderBatchSink output, SinkAttemptIdentity identity,
                DurableSinkActionContext context) {
            opens.incrementAndGet();
            residentLease = context.acquireResidentLease(96L);
            handleLease = context.acquireHandleLease();
            return new DurableBatchSinkWriteAttempt() {
                @Override public void write(DataRow row) { writes.incrementAndGet(); }
                @Override public PreparedSinkLocator prepare() {
                    prepares.incrementAndGet();
                    byte[] payload = new byte[]{(byte) writes.get()};
                    try {
                        return new PreparedSinkLocator(
                                PreparedSinkLocator.CURRENT_VERSION, identity,
                                contract.providerKey().capabilityProfile(),
                                identity.commitDomainId(), contract.providerKey(),
                                contract.locatorCodec(), payload,
                                MessageDigest.getInstance("SHA-256").digest(payload),
                                writes.get(),
                                MessageDigest.getInstance("SHA-256")
                                        .digest(new byte[]{7}),
                                10L);
                    } catch (Exception impossible) {
                        throw new AssertionError(impossible);
                    }
                }
                @Override public void close() {
                    closes.incrementAndGet();
                    residentLease.close();
                    handleLease.close();
                }
            };
        }
        @Override public DurableBatchSinkPreparedGroup openPreparedGroup(
                List<ProviderBatchSink> outputs,
                List<PreparedSinkLocator> preparedLocators,
                DurableSinkActionContext context) {
            throw new UnsupportedOperationException();
        }
    }
}
