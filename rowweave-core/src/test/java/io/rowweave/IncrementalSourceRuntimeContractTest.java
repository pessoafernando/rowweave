package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.IncrementalCaptureRequest;
import io.rowweave.incremental.IncrementalCaptureResponse;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalFrontierCodec;
import io.rowweave.incremental.IncrementalMaterializationResponse;
import io.rowweave.incremental.IncrementalMaterializationResponseCodec;
import io.rowweave.incremental.IncrementalMaterializedSplit;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.incremental.IncrementalSourceActionBudget;
import io.rowweave.incremental.IncrementalSourceActionExecutor;
import io.rowweave.incremental.IncrementalSourceCodecDescriptor;
import io.rowweave.incremental.IncrementalSourceConfig;
import io.rowweave.incremental.IncrementalSourceProviderContract;
import io.rowweave.incremental.IncrementalSourceProviderContractCodec;
import io.rowweave.incremental.IncrementalSourceProviderKey;
import io.rowweave.internal.ProviderActionSupervisor;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** an inert split only becomes readable through exact Worker binding. */
class IncrementalSourceRuntimeContractTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType));

    @Test
    void exactBindingOpensLazilyAndReadsOnTheCallingStageThread() throws Exception {
        Fixture fixture = fixture();
        RecordingProvider provider = new RecordingProvider(fixture.contract, false);
        BatchExecutableSource runtime = IncrementalSourceRuntime.bind(
                fixture.binding, fixture.contract, fixture.configuration,
                fixture.receipt, fixture.partitionDigest, provider,
                ProviderActionSupervisor.processWide(4),
                System.nanoTime() + 5_000_000_000L, () -> false);

        assertEquals(0, provider.openCalls.get());
        SourceExecutor<?> executor = SourceExecutorFactory.create(runtime);
        assertEquals(0, provider.openCalls.get());
        executor.open();
        assertEquals(1, provider.openCalls.get());
        assertTrue(executor.hasNext());
        assertEquals(1L, executor.next().get(0));
        assertTrue(executor.hasNext());
        assertEquals(2L, executor.next().get(0));
        assertFalse(executor.hasNext());
        executor.close();
        assertEquals(1, provider.closeCalls.get());
        assertEquals(Thread.currentThread(), provider.readThread);
        assertThrows(IllegalStateException.class,
                () -> fixture.binding.input().iterator());
    }

    @Test
    void driftAndForeignExecutorAreRejectedBeforeAnyRowRead() throws Exception {
        Fixture fixture = fixture();
        RecordingProvider provider = new RecordingProvider(fixture.contract, true);
        IncrementalSourceConfig drifted = new IncrementalSourceConfig(
                fixture.configuration.providerKey(),
                fixture.configuration.contractDigest(), bytes("drift"));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceRuntime.bind(
                        fixture.binding, fixture.contract, drifted,
                        fixture.receipt, fixture.partitionDigest, provider,
                        ProviderActionSupervisor.processWide(4),
                        System.nanoTime() + 5_000_000_000L, () -> false));
        assertEquals(0, provider.openCalls.get());

        BatchExecutableSource runtime = IncrementalSourceRuntime.bind(
                fixture.binding, fixture.contract, fixture.configuration,
                fixture.receipt, fixture.partitionDigest, provider,
                ProviderActionSupervisor.processWide(4),
                System.nanoTime() + 5_000_000_000L, () -> false);
        SourceExecutor<?> executor = SourceExecutorFactory.create(runtime);
        assertThrows(IllegalArgumentException.class, executor::open);
        assertEquals(1, provider.openCalls.get());
        assertEquals(1, provider.closeCalls.get());
    }

    @Test
    void readFailureClosesTheProviderExecutorExactlyOnce() throws Exception {
        Fixture fixture = fixture();
        RecordingProvider provider = new RecordingProvider(fixture.contract, false);
        provider.failRead = true;
        BatchExecutableSource runtime = IncrementalSourceRuntime.bind(
                fixture.binding, fixture.contract, fixture.configuration,
                fixture.receipt, fixture.partitionDigest, provider,
                ProviderActionSupervisor.processWide(4),
                System.nanoTime() + 5_000_000_000L, () -> false);
        SourceExecutor<?> executor = SourceExecutorFactory.create(runtime);
        executor.open();
        assertThrows(IllegalStateException.class, executor::hasNext);
        executor.close();
        assertEquals(1, provider.closeCalls.get());
    }

    @Test
    void cancellationObservedByTheStageClosesTheProviderExecutor() throws Exception {
        Fixture fixture = fixture();
        RecordingProvider provider = new RecordingProvider(fixture.contract, false);
        AtomicBoolean cancelled = new AtomicBoolean();
        BatchExecutableSource runtime = IncrementalSourceRuntime.bind(
                fixture.binding, fixture.contract, fixture.configuration,
                fixture.receipt, fixture.partitionDigest, provider,
                ProviderActionSupervisor.processWide(4),
                System.nanoTime() + 5_000_000_000L, cancelled::get);
        SourceExecutor<?> executor = SourceExecutorFactory.create(runtime);
        executor.open();
        cancelled.set(true);
        assertThrows(IllegalStateException.class, executor::hasNext);
        assertEquals(1, provider.closeCalls.get());
    }

    private static Fixture fixture() throws Exception {
        IncrementalSourceProviderContract contract = contract();
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("config"));
        CapturedTargetReceipt receipt = CapturedTargetReceipt.normal(
                1L, digest("definition"),
                IncrementalSourceProviderContractCodec.digest(contract),
                0L, IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(2L), bytes("snapshot"));
        byte[] partitions = digest("partitions");
        BatchSourceSplitDescriptor descriptor = new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                contract.providerKey().connectorType(),
                contract.providerKey().connectorVersion(), "p0",
                SchemaFingerprint.of(META), hex(digest("snapshot")),
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
        IncrementalSourceSplit input = new IncrementalSourceSplit(
                META, configuration, receipt, partitions,
                new IncrementalMaterializedSplit(descriptor, bytes("open")));
        return new Fixture(contract, configuration, receipt, partitions,
                new BatchSourceSplitBinding(input, descriptor));
    }

    private static IncrementalSourceProviderContract contract() {
        return new IncrementalSourceProviderContract(
                IncrementalSourceProviderContract.CURRENT_SPI_VERSION,
                new IncrementalSourceProviderKey("test.embedded", 1),
                new IncrementalSourceCodecDescriptor("test.config", 1, 64),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.CAPTURE_CODEC_ID, 1, 1024),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.MATERIALIZATION_CODEC_ID, 1,
                        IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES),
                IncrementalFrontierCodec.MAX_FRAME_BYTES,
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                IncrementalSourceProviderContract.OrderingGuarantee
                        .TOTAL_ORDER_NO_LATE_LEFT_V1,
                IncrementalSourceProviderContract.SnapshotGuarantee
                        .EXACT_REOPENABLE_V1,
                false, new IncrementalSourceActionBudget(
                        1024L * 1024L, 24L * 1024L * 1024L, 1, 60_000L),
                IncrementalSourceProviderContract.DeploymentCapability
                        .SIGNED_TEST_CONFORMANCE_V1);
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
        final IncrementalSourceProviderContract contract;
        final IncrementalSourceConfig configuration;
        final CapturedTargetReceipt receipt;
        final byte[] partitionDigest;
        final BatchSourceSplitBinding binding;

        Fixture(IncrementalSourceProviderContract contract,
                IncrementalSourceConfig configuration,
                CapturedTargetReceipt receipt, byte[] partitionDigest,
                BatchSourceSplitBinding binding) {
            this.contract = contract;
            this.configuration = configuration;
            this.receipt = receipt;
            this.partitionDigest = partitionDigest;
            this.binding = binding;
        }
    }

    private static final class RecordingProvider
            implements IncrementalSourceProvider {
        private static final long serialVersionUID = 1L;
        final IncrementalSourceProviderContract contract;
        final boolean foreignExecutor;
        final AtomicInteger openCalls = new AtomicInteger();
        final AtomicInteger closeCalls = new AtomicInteger();
        volatile boolean failRead;
        volatile Thread readThread;

        RecordingProvider(IncrementalSourceProviderContract contract,
                          boolean foreignExecutor) {
            this.contract = contract;
            this.foreignExecutor = foreignExecutor;
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
            throw new AssertionError("runtime must not recapture");
        }

        @Override public IncrementalMaterializationResponse materializeInterval(
                IncrementalSourceConfig configuration,
                CapturedTargetReceipt capturedTarget) {
            throw new AssertionError("runtime must not rematerialize");
        }

        @Override public SourceExecutor<IncrementalSourceSplit> openSplit(
                IncrementalSourceSplit input) {
            openCalls.incrementAndGet();
            IncrementalSourceSplit bound = input;
            if (foreignExecutor) {
                bound = new IncrementalSourceSplit(
                        input.getSchema(), input.configuration(),
                        input.capturedTarget(), input.partitionSetDigest(),
                        new IncrementalMaterializedSplit(
                                input.batchSourceSplit(), input.openRequest()));
            }
            return new Reader(bound, this);
        }
    }

    private static final class Reader
            extends SourceExecutor<IncrementalSourceSplit> {
        private final RecordingProvider owner;
        private int index;
        private boolean closed;

        Reader(IncrementalSourceSplit input, RecordingProvider owner) {
            super(input, input.getSchema());
            this.owner = owner;
        }

        @Override public void open() { }

        @Override public boolean hasNext() {
            owner.readThread = Thread.currentThread();
            if (owner.failRead) throw new IllegalStateException("read failed");
            return index < 2;
        }

        @Override public DataRow next() {
            if (index >= 2) throw new NoSuchElementException();
            DataRow row = new DataRow(1);
            row.set(0, (long) ++index);
            return row;
        }

        @Override public void close() {
            if (!closed) {
                closed = true;
                owner.closeCalls.incrementAndGet();
            }
        }
    }
}
