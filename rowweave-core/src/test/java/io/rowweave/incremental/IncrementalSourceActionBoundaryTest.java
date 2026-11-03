package io.rowweave.incremental;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.IncrementalSourceProvider;
import io.rowweave.internal.ProviderActionSupervisor;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** source control actions share the fenced provider supervisor. */
class IncrementalSourceActionBoundaryTest {

    @Test
    void embeddedCaptureAndMaterializeRunInsideOneExistingSupervisorLane()
            throws Exception {
        IncrementalSourceProviderContract contract = contract();
        IncrementalSourceConfig config = config(contract);
        EmbeddedProvider provider = new EmbeddedProvider(contract, false);
        IncrementalSourceActionExecutor actions =
                new IncrementalSourceActionExecutor(
                        contract, config, provider,
                        ProviderActionSupervisor.processWide(2));
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                1L, digest("definition"), 0L, IncrementalFrontier.begin(),
                "trigger", IncrementalRunMode.SNAPSHOT_V1);

        CapturedTargetReceipt receipt = actions.capture(
                request, System.nanoTime() + 5_000_000_000L);
        assertEquals(IncrementalFrontier.int64Sequence(10L), receipt.target());
        assertArrayEquals(IncrementalSourceProviderContractCodec.digest(contract),
                receipt.sourceContractDigest());
        IncrementalMaterializationResponse materialized = actions.materialize(
                receipt, System.nanoTime() + 5_000_000_000L,
                ignored -> { });
        assertEquals(receipt, materialized.capturedTarget());
        assertEquals(contract.providerKey(), materialized.providerKey());
        assertEquals(1, materialized.splits().size());
        assertEquals(java.util.Arrays.asList("contract", "config", "capture",
                        "contract", "config", "materialize"),
                provider.calls);
        for (String thread : provider.threads) {
            assertTrue(thread.startsWith("rowweave-provider-action-"), thread);
        }
    }

    @Test
    void configurationDriftStopsBeforeCaptureAndTargetKindDriftIsRejected()
            throws Exception {
        IncrementalSourceProviderContract contract = contract();
        IncrementalSourceConfig config = config(contract);
        EmbeddedProvider configDrift = new EmbeddedProvider(contract, true);
        IncrementalSourceActionExecutor actions =
                new IncrementalSourceActionExecutor(
                        contract, config, configDrift,
                        ProviderActionSupervisor.processWide(2));
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                1L, digest("definition"), 0L, IncrementalFrontier.begin(),
                "trigger", IncrementalRunMode.SNAPSHOT_V1);
        assertThrows(IllegalArgumentException.class,
                () -> actions.capture(request,
                        System.nanoTime() + 5_000_000_000L));
        assertEquals(java.util.Arrays.asList("contract", "config"),
                configDrift.calls);

        IncrementalSourceProvider wrongKind = new EmbeddedProvider(
                contract, false) {
            @Override
            public IncrementalCaptureResponse captureTarget(
                    IncrementalSourceConfig ignored,
                    IncrementalCaptureRequest capture) {
                record("capture");
                return new IncrementalCaptureResponse(
                        IncrementalFrontier.timestampKey(
                                10L, OrderedFrontierKey.of(
                                        OrderedFrontierKey.int64(1L))),
                        bytes("snapshot"));
            }
        };
        IncrementalSourceActionExecutor wrongActions =
                new IncrementalSourceActionExecutor(
                        contract, config, wrongKind,
                        ProviderActionSupervisor.processWide(2));
        assertThrows(IllegalArgumentException.class,
                () -> wrongActions.capture(request,
                        System.nanoTime() + 5_000_000_000L));
    }

    @Test
    void staticBindingRejectsWrongContractOrOversizedDeclaredConfigBeforeCode()
            throws Exception {
        IncrementalSourceProviderContract contract = contract();
        IncrementalSourceConfig wrongDigest = new IncrementalSourceConfig(
                contract.providerKey(), new byte[32], bytes("config"));
        EmbeddedProvider provider = new EmbeddedProvider(contract, false);
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceActionExecutor(
                        contract, wrongDigest, provider,
                        ProviderActionSupervisor.processWide(2)));
        assertTrue(provider.calls.isEmpty());

        IncrementalSourceConfig tooLargeForDeclaredCodec =
                new IncrementalSourceConfig(
                        contract.providerKey(),
                        IncrementalSourceProviderContractCodec.digest(contract),
                        new byte[contract.configurationCodec()
                                .maxPayloadBytes() + 1]);
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceActionExecutor(
                        contract, tooLargeForDeclaredCodec, provider,
                        ProviderActionSupervisor.processWide(2)));
        assertTrue(provider.calls.isEmpty());
    }

    private static IncrementalSourceProviderContract contract() {
        return new IncrementalSourceProviderContract(
                IncrementalSourceProviderContract.CURRENT_SPI_VERSION,
                new IncrementalSourceProviderKey("test.embedded", 1),
                new IncrementalSourceCodecDescriptor("test.config", 1, 16),
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

    private static IncrementalSourceConfig config(
            IncrementalSourceProviderContract contract) {
        return new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("config"));
    }

    private static class EmbeddedProvider implements IncrementalSourceProvider {
        private static final long serialVersionUID = 1L;
        final IncrementalSourceProviderContract contract;
        final boolean driftConfiguration;
        final List<String> calls = new ArrayList<>();
        final List<String> threads = new ArrayList<>();

        EmbeddedProvider(
                IncrementalSourceProviderContract contract,
                boolean driftConfiguration) {
            this.contract = contract;
            this.driftConfiguration = driftConfiguration;
        }

        @Override
        public IncrementalSourceProviderContract contract() {
            record("contract");
            return contract;
        }

        @Override
        public byte[] canonicalizeConfiguration(IncrementalSourceConfig config) {
            record("config");
            return driftConfiguration ? bytes("drift")
                    : config.configurationBytes();
        }

        @Override
        public IncrementalCaptureResponse captureTarget(
                IncrementalSourceConfig config,
                IncrementalCaptureRequest request) {
            record("capture");
            return new IncrementalCaptureResponse(
                    IncrementalFrontier.int64Sequence(10L), bytes("snapshot"));
        }

        @Override
        public IncrementalMaterializationResponse materializeInterval(
                IncrementalSourceConfig config,
                CapturedTargetReceipt receipt) {
            record("materialize");
            try {
                String snapshot = hex(sha256(receipt.snapshotIdentity()));
                BatchSourceSplitDescriptor descriptor =
                        new BatchSourceSplitDescriptor(
                                BatchSourceSplitDescriptor.CURRENT_VERSION,
                                contract.providerKey().connectorType(),
                                contract.providerKey().connectorVersion(),
                                "p0", hex(digest("schema")), snapshot,
                                BatchSourceSplitDescriptor.ReplayGuarantee
                                        .EXACT_ORDERED_FROM_START);
                return new IncrementalMaterializationResponse(
                        contract.providerKey(), receipt, digest("partitions"),
                        Collections.singletonList(
                                new IncrementalMaterializedSplit(
                                        descriptor, bytes("open"))));
            } catch (Exception impossible) {
                throw new IllegalStateException(impossible);
            }
        }

        void record(String call) {
            calls.add(call);
            threads.add(Thread.currentThread().getName());
        }
    }

    private static byte[] digest(String value) throws Exception {
        return sha256(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte b : value) {
            result.append(Character.forDigit((b >>> 4) & 0xf, 16));
            result.append(Character.forDigit(b & 0xf, 16));
        }
        return result.toString();
    }
}
