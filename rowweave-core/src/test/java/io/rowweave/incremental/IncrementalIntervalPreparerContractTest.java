package io.rowweave.incremental;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.Field;
import io.rowweave.IncrementalSourceProvider;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.artifact.InMemoryArtifactRepository;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.RecoveryOwner;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/** embedded preparation ends at joint claim and never dispatches. */
class IncrementalIntervalPreparerContractTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("op", DataType.StringType),
            new Field("position", DataType.LongType));

    @Test
    void concurrentTriggersProduceOneJointClaimAndOneLostRace()
            throws Exception {
        Fixture fixture = fixture();
        InMemoryIncrementalAuthorityStore authority =
                initialized(fixture.definition, fixture.contract);
        FrontierRecoveryToken token = authority.claimOwnership(
                "feed", 0L, 0L, new RecoveryOwner("master", 1L)).token();
        IncrementalIntervalPreparer preparer = fixture.preparer(authority);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<IncrementalIntervalPreparationDecision> first = pool.submit(
                    () -> prepare(preparer, token, "trigger-a", start));
            Future<IncrementalIntervalPreparationDecision> second = pool.submit(
                    () -> prepare(preparer, token, "trigger-b", start));
            start.countDown();
            IncrementalIntervalPreparationDecision left = first.get();
            IncrementalIntervalPreparationDecision right = second.get();
            int claimed = (left.code()
                    == IncrementalIntervalPreparationDecision.Code.CLAIMED ? 1 : 0)
                    + (right.code()
                    == IncrementalIntervalPreparationDecision.Code.CLAIMED ? 1 : 0);
            int lost = (left.code()
                    == IncrementalIntervalPreparationDecision.Code.LOST_RACE ? 1 : 0)
                    + (right.code()
                    == IncrementalIntervalPreparationDecision.Code.LOST_RACE ? 1 : 0);
            assertEquals(1, claimed);
            assertEquals(1, lost);
            assertEquals(IncrementalFrontierAuthorityState.ACTIVE,
                    authority.record("feed").state());
            assertNotNull(authority.record("feed").activeInterval());
            assertEquals(1, authority.runSnapshot(
                    authority.record("feed").activeInterval()
                            .fence().runId()).attemptCount());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void clusterPreparationUsesOnlyTheUnifiedGateway() throws Exception {
        Fixture fixture = fixture();
        InMemoryIncrementalAuthorityStore authority =
                initialized(fixture.definition, fixture.contract);
        FrontierRecoveryToken token = authority.claimOwnership(
                "feed", 0L, 0L, new RecoveryOwner("master", 1L)).token();

        IncrementalIntervalPreparationDecision decision =
                fixture.clusterPreparer(authority).prepareNormal(
                        "feed", token, "cluster-trigger",
                        IncrementalRunMode.SNAPSHOT_V1,
                        "attempt-cluster", "publication-cluster",
                        3, 0L, 10L,
                        System.nanoTime() + 5_000_000_000L,
                        ignored -> { });

        assertEquals(IncrementalIntervalPreparationDecision.Code.CLAIMED,
                decision.code());
        assertEquals(IncrementalFrontierAuthorityState.ACTIVE,
                authority.record("feed").state());
        assertEquals(decision.claimRequest().runId(),
                decision.runSnapshot().runId());
    }

    @Test
    void exactIncrementalNoOpRechecksAuthorityBeforeAnyMaterialization()
            throws Exception {
        Fixture fixture = fixture();
        IncrementalAuthorityRecord record = new IncrementalAuthorityRecord(
                5L, "feed", 1L, fixture.definition.fingerprint(),
                IncrementalSourceProviderContractCodec.digest(fixture.contract),
                fixture.frontierContract,
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                IncrementalFrontier.int64Sequence(10L), null, null,
                0L, null, IncrementalFrontierAuthorityState.READY, null);
        RecordingAuthority authority = new RecordingAuthority(record);
        fixture.provider.captureTarget = IncrementalFrontier.int64Sequence(10L);
        IncrementalIntervalPreparer preparer = fixture.preparer(authority);

        IncrementalIntervalPreparationDecision decision = preparer.prepareNormal(
                "feed", null, "no-op",
                IncrementalRunMode.INCREMENTAL_V1,
                "attempt", "publication", 3, 0L, 10L,
                System.nanoTime() + 5_000_000_000L, ignored -> { });

        assertEquals(IncrementalIntervalPreparationDecision.Code.NO_OP,
                decision.code());
        assertNull(decision.artifactId());
        assertEquals(1, fixture.provider.captureCalls.get());
        assertEquals(0, fixture.provider.materializeCalls.get());
        assertEquals(0, authority.publishCalls.get());
        assertEquals(0, authority.applyCalls.get());
        assertEquals(2, authority.readCalls.get());
    }

    private static IncrementalIntervalPreparationDecision prepare(
            IncrementalIntervalPreparer preparer, FrontierRecoveryToken token,
            String trigger, CountDownLatch start) throws Exception {
        start.await();
        return preparer.prepareNormal(
                "feed", token, trigger, IncrementalRunMode.SNAPSHOT_V1,
                "attempt-" + trigger, "publication-" + trigger,
                3, 0L, 10L, System.nanoTime() + 5_000_000_000L,
                ignored -> { });
    }

    private static InMemoryIncrementalAuthorityStore initialized(
            IncrementalDefinition definition,
            IncrementalSourceProviderContract contract) {
        InMemoryIncrementalAuthorityStore store =
                new InMemoryIncrementalAuthorityStore();
        IncrementalInitializeRequest request = new IncrementalInitializeRequest(
                "feed", 1L, definition.fingerprint(),
                IncrementalSourceProviderContractCodec.digest(contract),
                definition.planDescriptor().frontierContractDigest(),
                contract.frontierKind());
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.INITIALIZE, "feed", "initialize",
                0L, 1L, null, null, request.transitionDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.initialize(
                        command, request)).code());
        return store;
    }

    private static Fixture fixture() throws Exception {
        byte[] frontierContract = digest("frontier-contract");
        byte[] partitions = digest("partitions");
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "ledger-source", frontierContract,
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schemaDigest(), false,
                partitions,
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-source")
                .withIncrementalPlanDescriptor(descriptor);
        ProviderBatchSink output = new ProviderBatchSink(
                META, new BatchSinkDescriptor(
                        BatchSinkDescriptor.CURRENT_VERSION, "test.manifest", 1,
                        SchemaFingerprint.of(META), "destination", "domain",
                        BatchSinkDescriptor.CapabilityProfile
                                .MANIFEST_ATOMIC_GROUP_V1, "binding"),
                new DurableSinkCodecDescriptor("test.sink", 1, 1024),
                bytes("sink-config"));
        PlanNode sink = PlanNode.sink(META, 0, output)
                .param(PlanNode.OPERATOR_UID_PARAM, "ledger-sink").build();
        PlanGraph template = new PlanGraph(Arrays.asList(source, sink), 1);
        IncrementalSourceProviderContract contract = contract();
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("source-config"));
        EmbeddedProvider provider = new EmbeddedProvider(
                contract, partitions);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "ledger-definition", template, trust, ignored -> { });
        return new Fixture(template, definition, contract, configuration,
                provider, trust, frontierContract);
    }

    private static IncrementalSourceProviderContract contract() {
        return new IncrementalSourceProviderContract(
                IncrementalSourceProviderContract.CURRENT_SPI_VERSION,
                new IncrementalSourceProviderKey("test.embedded", 1),
                new IncrementalSourceCodecDescriptor("test.config", 1, 64),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.CAPTURE_CODEC_ID, 1,
                        IncrementalCaptureResponseCodec.MAX_FRAME_BYTES),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.MATERIALIZATION_CODEC_ID,
                        1, IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES),
                IncrementalFrontierCodec.MAX_FRAME_BYTES,
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                IncrementalSourceProviderContract.OrderingGuarantee
                        .TOTAL_ORDER_NO_LATE_LEFT_V1,
                IncrementalSourceProviderContract.SnapshotGuarantee
                        .EXACT_REOPENABLE_V1,
                false,
                new IncrementalSourceActionBudget(
                        1024L * 1024L, 24L * 1024L * 1024L,
                        1, 60_000L),
                IncrementalSourceProviderContract.DeploymentCapability
                        .SIGNED_TEST_CONFORMANCE_V1);
    }

    private static byte[] schemaDigest() {
        String value = SchemaFingerprint.of(META);
        byte[] result = new byte[value.length() / 2];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) Integer.parseInt(
                    value.substring(index * 2, index * 2 + 2), 16);
        }
        return result;
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte b : value) {
            result.append(Character.forDigit((b >>> 4) & 0xf, 16));
            result.append(Character.forDigit(b & 0xf, 16));
        }
        return result.toString();
    }

    private static final class Fixture {
        final PlanGraph template;
        final IncrementalDefinition definition;
        final IncrementalSourceProviderContract contract;
        final IncrementalSourceConfig configuration;
        final EmbeddedProvider provider;
        final ObjectTrustContext trust;
        final byte[] frontierContract;

        Fixture(PlanGraph template, IncrementalDefinition definition,
                IncrementalSourceProviderContract contract,
                IncrementalSourceConfig configuration,
                EmbeddedProvider provider, ObjectTrustContext trust,
                byte[] frontierContract) {
            this.template = template;
            this.definition = definition;
            this.contract = contract;
            this.configuration = configuration;
            this.provider = provider;
            this.trust = trust;
            this.frontierContract = frontierContract;
        }

        IncrementalIntervalPreparer preparer(
                IncrementalPreparationAuthority authority) {
            return new IncrementalIntervalPreparer(
                    definition, template, contract, configuration,
                    new IncrementalSourceActionExecutor(
                            contract, configuration, provider,
                            ProviderActionSupervisor.processWide(4)),
                    new InMemoryArtifactRepository(
                            64, 32L * 1024L * 1024L),
                    authority, trust);
        }

        IncrementalIntervalPreparer clusterPreparer(
                IncrementalAuthorityGateway authority) {
            return IncrementalIntervalPreparer.forCluster(
                    definition, template, contract, configuration,
                    new IncrementalSourceActionExecutor(
                            contract, configuration, provider,
                            ProviderActionSupervisor.processWide(4)),
                    new InMemoryArtifactRepository(
                            64, 32L * 1024L * 1024L),
                    authority, trust);
        }
    }

    private static final class EmbeddedProvider
            implements IncrementalSourceProvider {
        private static final long serialVersionUID = 1L;
        final IncrementalSourceProviderContract contract;
        final byte[] partitions;
        final AtomicInteger captureCalls = new AtomicInteger();
        final AtomicInteger materializeCalls = new AtomicInteger();
        volatile IncrementalFrontier captureTarget =
                IncrementalFrontier.int64Sequence(10L);

        EmbeddedProvider(IncrementalSourceProviderContract contract,
                         byte[] partitions) {
            this.contract = contract;
            this.partitions = partitions.clone();
        }

        @Override public IncrementalSourceProviderContract contract() {
            return contract;
        }

        @Override public byte[] canonicalizeConfiguration(
                IncrementalSourceConfig configuration) {
            return configuration.configurationBytes();
        }

        @Override public IncrementalCaptureResponse captureTarget(
                IncrementalSourceConfig configuration,
                IncrementalCaptureRequest request) {
            captureCalls.incrementAndGet();
            return new IncrementalCaptureResponse(
                    captureTarget, bytes("snapshot"));
        }

        @Override public IncrementalMaterializationResponse materializeInterval(
                IncrementalSourceConfig configuration,
                CapturedTargetReceipt receipt) {
            materializeCalls.incrementAndGet();
            try {
                BatchSourceSplitDescriptor descriptor =
                        new BatchSourceSplitDescriptor(
                                BatchSourceSplitDescriptor.CURRENT_VERSION,
                                contract.providerKey().connectorType(),
                                contract.providerKey().connectorVersion(),
                                "p0", SchemaFingerprint.of(META),
                                hex(digest("snapshot")),
                                BatchSourceSplitDescriptor.ReplayGuarantee
                                        .EXACT_ORDERED_FROM_START);
                return new IncrementalMaterializationResponse(
                        contract.providerKey(), receipt, partitions,
                        Collections.singletonList(
                                new IncrementalMaterializedSplit(
                                        descriptor, bytes("open"))));
            } catch (Exception impossible) {
                throw new IllegalStateException(impossible);
            }
        }
    }

    private static final class RecordingAuthority
            implements IncrementalPreparationAuthority {
        final IncrementalAuthorityRecord record;
        final AtomicInteger readCalls = new AtomicInteger();
        final AtomicInteger publishCalls = new AtomicInteger();
        final AtomicInteger applyCalls = new AtomicInteger();

        RecordingAuthority(IncrementalAuthorityRecord record) {
            this.record = record;
        }

        @Override public IncrementalAuthorityRecord record(String feedId) {
            readCalls.incrementAndGet();
            return record;
        }

        @Override public IncrementalPendingRunDecision publishPending(
                FrontierRecoveryToken token, IncrementalClaimRequest request) {
            publishCalls.incrementAndGet();
            throw new AssertionError("no-op must not publish");
        }

        @Override public IncrementalAuthorityDecision apply(
                IncrementalAuthorityMutation mutation) {
            applyCalls.incrementAndGet();
            throw new AssertionError("no-op must not claim");
        }
    }
}
