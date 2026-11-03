package io.rowweave;

import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamAsyncOrdering;
import io.rowweave.stream.api.StreamAsyncReplaySpec;
import io.rowweave.stream.api.StreamAsyncResourceSpec;
import io.rowweave.stream.api.StreamAsyncWaitSpec;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.runtime.FileStreamSavepointStore;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamFunctionProvider;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** public product execution of a transactional checkpoint. */
class LocalStreamPublicExactlyOnceTest {
    private static final String SOURCE = "public-exact-source-v1";
    private static final String SINK = "public-exact-sink-v1";
    private static final String SOURCE_CONTRACT = digest("exact-source");
    private static final String SINK_CONTRACT = digest("exact-sink");
    private static final StreamSchema SCHEMA = new StreamSchema(
            Collections.singletonList(new StreamField(
                    "value", "value", "value", DataType.StringType, false)));

    @Test
    void checkpointPublishesPreparedRowsExactlyOnce(@TempDir Path root)
            throws Exception {
        ExactSource source = new ExactSource();
        ExactSink sink = new ExactSink();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Collections.singletonList(source),
                Collections.emptyList(), Collections.singletonList(sink),
                Collections.emptyList())) {
            StreamSession session = session(client);
            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("public-exact-submit"));
            job.awaitRunning(5_000L);
            source.awaitPrefix(5_000L);
            assertEquals(Collections.emptyList(), sink.visibleRows());

            StreamCommandId command = StreamCommandId.of("public-exact-cut");
            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(command);
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    client.attachCheckpoint(job.id(), command).status());
            assertEquals(java.util.Arrays.asList("one", "two"),
                    sink.visibleRows());
            assertEquals(1, sink.commitCount());

            job.cancel(StreamCommandId.of("public-exact-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void unalignedCheckpointPublishesChannelStateWithTransactionalSink(
            @TempDir Path root) throws Exception {
        ExactSource source = new ExactSource();
        ExactSink sink = new ExactSink();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Collections.singletonList(source),
                Collections.emptyList(), Collections.singletonList(sink),
                Collections.emptyList())) {
            StreamSession session = session(client);
            session.setCheckpointConfig(
                    StreamCheckpointConfig.periodicUnaligned(
                            60_000L, 5_000L, 2));
            StreamJobHandle job = session.submit(
                    StreamSubmissionId.of("public-unaligned-submit"));
            job.awaitRunning(5_000L);
            source.awaitPrefix(5_000L);

            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                    StreamCommandId.of("public-unaligned-cut"));

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertEquals(java.util.Arrays.asList("one", "two"),
                    sink.visibleRows());
            assertEquals(1, sink.commitCount());
            assertEquals(1L, new FileStreamSavepointStore(
                    root.resolve("checkpoints"))
                    .get(checkpoint.checkpointId()).checkpointManifest()
                    .taskReceipts().stream()
                    .flatMap(receipt -> receipt.evidence().stream())
                    .filter(evidence -> evidence.kind()
                            == CheckpointEvidence.Kind.CHANNEL_STATE)
                    .count(),
                    "the Sink physical input must publish channel state");
            job.cancel(StreamCommandId.of("public-unaligned-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void commitAckLossReconcilesTheDurableDecisionWithoutDuplicateVisibility(
            @TempDir Path root) throws Exception {
        ExactSource source = new ExactSource();
        ExactSink sink = new ExactSink(true);
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Collections.singletonList(source),
                Collections.emptyList(), Collections.singletonList(sink),
                Collections.emptyList())) {
            StreamJobHandle job = session(client).submit(
                    StreamSubmissionId.of("public-exact-ack-loss-submit"));
            job.awaitRunning(5_000L);
            source.awaitPrefix(5_000L);

            StreamCommandId command = StreamCommandId.of(
                    "public-exact-ack-loss-cut");
            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(command);

            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    client.attachCheckpoint(job.id(), command).status());
            assertEquals(java.util.Arrays.asList("one", "two"),
                    sink.visibleRows());
            assertEquals(1, sink.commitCount());

            job.cancel(StreamCommandId.of("public-exact-ack-loss-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    @Test
    void publicReplayConformanceAdmitsAsyncExactlyOnce(
            @TempDir Path root) throws Exception {
        ExactSource source = new ExactSource();
        ExactSink sink = new ExactSink();
        AsyncFunctions functions = new AsyncFunctions();
        try (StreamJobClient client = RowWeave.createLocalStreamJobClient(
                root, Collections.singletonList(source),
                Collections.emptyList(), Collections.singletonList(sink),
                Collections.singletonList(functions))) {
            StreamJobHandle job = asyncSession(client).submit(
                    StreamSubmissionId.of("public-exact-async-submit"));
            job.awaitRunning(5_000L);
            source.awaitPrefix(5_000L);
            functions.awaitCompletions(5_000L);

            StreamCheckpointHandle checkpoint = job.triggerCheckpoint(
                    StreamCommandId.of("public-exact-async-cut"));
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertEquals(java.util.Arrays.asList("one", "two"),
                    sink.visibleRows());
            job.cancel(StreamCommandId.of("public-exact-async-cancel"))
                    .awaitCompletion(5_000L);
        }
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "public-exact", "public-exact", client);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                1_000L, 5_000L, 2));
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.exactOrdered(
                        SCHEMA, SOURCE, SOURCE_CONTRACT, new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                digest("exact-split"), "int32-v1",
                                new byte[] {2}))))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        SINK, SINK_CONTRACT, new byte[] {3}));
        return session;
    }

    private static StreamSession asyncSession(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "public-exact-async", "public-exact-async", client);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                60_000L, 5_000L, 1));
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 32_768L, 0, 0,
                32_768L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.exactOrdered(
                        SCHEMA, SOURCE, SOURCE_CONTRACT, new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                digest("exact-split"), "int32-v1",
                                new byte[] {2}))))
                .uid("source")
                .asyncWait(StreamAsyncWaitSpec.of(
                        asyncFunction(), SCHEMA,
                        Collections.singletonList("value"),
                        StreamAsyncOrdering.ORDERED,
                        StreamAsyncReplaySpec.identityKeyed(
                                "public-request-id-v1",
                                digest("public-replay-authority")),
                        StreamAsyncResourceSpec.of(2, 8_192L,
                                1_000L, 2, 1L, 10L,
                                StreamEmissionLimit.of(1, 4_096L))))
                .uid("lookup")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        SINK, SINK_CONTRACT, new byte[] {3}));
        return session;
    }

    private static FunctionDeploymentReference asyncFunction() {
        byte[] config = new byte[] {9};
        return new FunctionDeploymentReference(
                digest("public-async-deployment"), "lookup",
                digest("public-async-jar"), "example.PublicLookup",
                FunctionAbi.STREAM_ASYNC_V1, "config-v1",
                digest(config), config,
                new FunctionResourceProfile(4_096L, 0L, 1));
    }

    private static final class AsyncFunctions
            implements StreamFunctionProvider {
        private final java.util.concurrent.CountDownLatch completions =
                new java.util.concurrent.CountDownLatch(2);
        @Override public String deploymentSetId() {
            return asyncFunction().deploymentSetId();
        }

        @Override public Serializable resolve(
                FunctionDeploymentReference reference) {
            if (!asyncFunction().equals(reference)) {
                throw new IllegalArgumentException("unknown function");
            }
            return new PassAsync(completions);
        }

        @Override public boolean supportsAsyncReplay(
                FunctionDeploymentReference reference,
                StreamAsyncReplaySpec replay) {
            return asyncFunction().equals(reference)
                    && replay.equals(StreamAsyncReplaySpec.identityKeyed(
                    "public-request-id-v1",
                    digest("public-replay-authority")));
        }

        private void awaitCompletions(long timeoutMillis)
                throws InterruptedException {
            if (!completions.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(
                        "async completions timed out");
            }
        }
    }

    private static final class PassAsync implements StreamAsyncFunction {
        private final java.util.concurrent.CountDownLatch completions;

        private PassAsync(java.util.concurrent.CountDownLatch completions) {
            this.completions = completions;
        }

        @Override public byte[] createRequest(DataRow input) {
            return input.getString(0).getBytes(StandardCharsets.UTF_8);
        }

        @Override public byte[] invoke(StreamAsyncRequest request) {
            return request.requestFrame();
        }

        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            DataRow row = new DataRow(1);
            row.set(0, new String(responseFrame, StandardCharsets.UTF_8));
            output.collect(row);
            completions.countDown();
        }
    }

    private static final class ExactSource implements StreamSourceProvider {
        private final Semaphore wakeups = new Semaphore(0);
        private final Semaphore prefix = new Semaphore(0);
        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceContext context) {
            StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
            return new StreamSourceReader() {
                private volatile boolean canceled;
                private int next = context.hasRestoredPosition()
                        ? ByteBuffer.wrap(context.restoredPositionFrame()).getInt()
                        : 0;
                @Override public StreamSourceRecord next() throws Exception {
                    if (next < 2) {
                        DataRow row = new DataRow(1);
                        row.set(0, next == 0 ? "one" : "two");
                        next++;
                        if (next == 2) prefix.release();
                        return StreamSourceRecord.of(
                                codec.encode(row), position(next));
                    }
                    while (!canceled) {
                        if (wakeups.tryAcquire(20L, TimeUnit.MILLISECONDS)) {
                            return null;
                        }
                    }
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return position(next);
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() {
                    canceled = true;
                    wakeups.release();
                }
                @Override public void close() { cancel(); }
            };
        }
        void awaitPrefix(long timeoutMillis) throws InterruptedException {
            if (!prefix.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("source prefix timed out");
            }
        }
        private static byte[] position(int value) {
            return ByteBuffer.allocate(4).putInt(value).array();
        }
    }

    private static final class ExactSink
            implements StreamTransactionalSinkProvider {
        private final Map<String, Transaction> transactions =
                new LinkedHashMap<>();
        private final java.util.concurrent.atomic.AtomicBoolean
                loseFirstCommitAck;
        private int commits;
        private ExactSink() { this(false); }
        private ExactSink(boolean loseFirstCommitAck) {
            this.loseFirstCommitAck =
                    new java.util.concurrent.atomic.AtomicBoolean(
                            loseFirstCommitAck);
        }
        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public StreamTransactionalSinkWriter open(
                StreamTransactionalSinkContext context) {
            StreamDataRowCodec codec = new StreamDataRowCodec(SCHEMA);
            return new StreamTransactionalSinkWriter() {
                private final List<String> pending = new ArrayList<>();
                @Override public void write(byte[] recordFrame) {
                    pending.add(codec.decode(recordFrame).getString(0));
                }
                @Override public byte[] prepare(String identity) {
                    synchronized (transactions) {
                        transactions.putIfAbsent(identity,
                                new Transaction(new ArrayList<>(pending)));
                    }
                    return digest(identity).getBytes(StandardCharsets.US_ASCII);
                }
                @Override public StreamTransactionStatus query(String identity) {
                    synchronized (transactions) {
                        Transaction value = transactions.get(identity);
                        return value == null ? StreamTransactionStatus.UNKNOWN
                                : value.status;
                    }
                }
                @Override public void commit(String identity) {
                    synchronized (transactions) {
                        Transaction value = transactions.get(identity);
                        if (value.status != StreamTransactionStatus.VISIBLE) {
                            value.status = StreamTransactionStatus.VISIBLE;
                            commits++;
                        }
                    }
                    if (loseFirstCommitAck.compareAndSet(true, false)) {
                        throw new IllegalStateException(
                                "simulated commit acknowledgement loss");
                    }
                }
                @Override public void finalizeTransaction(String identity) { }
                @Override public void abort(String identity) {
                    synchronized (transactions) {
                        transactions.get(identity).status =
                                StreamTransactionStatus.ABORTED;
                    }
                }
                @Override public void close() { }
            };
        }
        List<String> visibleRows() {
            synchronized (transactions) {
                List<String> result = new ArrayList<>();
                for (Transaction value : transactions.values()) {
                    if (value.status == StreamTransactionStatus.VISIBLE) {
                        result.addAll(value.rows);
                    }
                }
                return result;
            }
        }
        int commitCount() {
            synchronized (transactions) { return commits; }
        }
    }

    private static final class Transaction {
        private final List<String> rows;
        private StreamTransactionStatus status = StreamTransactionStatus.PREPARED;
        private Transaction(List<String> rows) { this.rows = rows; }
    }

    private static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String digest(byte[] value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    value);
            StringBuilder result = new StringBuilder(64);
            for (byte one : bytes) result.append(String.format("%02x", one));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
