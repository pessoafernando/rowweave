package io.rowweave.cluster;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.Sink;
import io.rowweave.DataRowClassification;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.ops.RowValidation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** local/cluster/reopen/abort execution contract. */
class TypedRowClassificationExecutionTest {

    private static final String SOURCE_UID = "ledger.source";
    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema REJECTED_META = new DataSchema(
            new Field("source_operator_uid", DataType.StringType),
            new Field("split_identity_sha256", DataType.StringType),
            new Field("split_local_ordinal", DataType.LongType),
            new Field("quality_error_code", DataType.StringType),
            new Field("quality_rule_id", DataType.StringType),
            new Field("quality_field_index", DataType.IntegerType),
            new Field("quality_detail", DataType.StringType));
    private static final RowValidation VALIDATION = RowValidation.builder(META)
            .notNull("adaptor.required", 0)
            .integerRange("period.range", 1, 1, 12)
            .decimalRange("amount.range", 2,
                    new BigDecimal("-10"), new BigDecimal("10"))
            .build();
    private static final AtomicInteger SOURCE_OPENS = new AtomicInteger();
    private static final AtomicInteger FAILING_ABORTS = new AtomicInteger();

    @Test
    void localAndInProcessP1P2P4MatchExactOracleAndOpenEachSplitOnce(
            @TempDir Path tmp) throws Exception {
        for (int parallelism : new int[]{1, 2, 4}) {
            Result local = run(null, parallelism,
                    tmp.resolve("local-p" + parallelism), true);
            assertOracle(local);
            assertEquals(2, local.sourceOpens,
                    "local P" + parallelism + " must open each shared split once");

            Path clusterDir = tmp.resolve("inprocess-p" + parallelism);
            Files.createDirectories(clusterDir);
            try (InProcessCluster cluster = new InProcessCluster(
                    parallelism, clusterDir.toFile())) {
                Result inProcess = run(cluster, parallelism,
                        tmp.resolve("inprocess-output-p" + parallelism), true);
                assertOracle(inProcess);
                assertEquals(2, inProcess.sourceOpens,
                        "InProcess P" + parallelism + " must open each shared split once");
            }
        }
    }

    @Test
    void wholeAttemptReopenRebuildsIdenticalPositionsAndClassification(
            @TempDir Path tmp) throws Exception {
        Result first = run(null, 4, tmp.resolve("first-attempt"), true);
        Result second = run(null, 4, tmp.resolve("second-attempt"), true);

        assertEquals(first.valid, second.valid);
        assertEquals(first.rejected, second.rejected);
        assertEquals(2, first.sourceOpens);
        assertEquals(2, second.sourceOpens);
    }

    @Test
    void rejectedOutputFailureAbortsTheWholeOutputGroupAndKeepsPreviousVersion(
            @TempDir Path tmp) throws Exception {
        Path validTarget = tmp.resolve("valid.tsv");
        FileSink old = new FileSink(META, validTarget.toString());
        old.collect(row("OLD", 1, BigDecimal.ZERO));
        old.close();

        SOURCE_OPENS.set(0);
        FAILING_ABORTS.set(0);
        BatchSession session = new BatchSession("classification-abort", "classification abort");
        DataSet source = session.fromSource(inputs()).uid(SOURCE_UID);
        DataRowClassification classified = source.classifyRowsWithSourcePosition(VALIDATION);
        classified.validRows().sinkTo(new FileSink(META, validTarget.toString()));
        classified.rejectedRows().sinkTo(new FailingCollectOutput(REJECTED_META));

        RuntimeException failure = assertThrows(RuntimeException.class, session::execute);
        assertTrue(allMessages(failure).contains("injected rejected output failure"),
                allMessages(failure));
        assertEquals(Collections.singletonList("OLD|1|0"), validLines(validTarget));
        assertTrue(FAILING_ABORTS.get() > 0, "failing member must be aborted");
        try (Stream<Path> files = Files.list(tmp)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString()
                            .startsWith(".rowweave-output-")),
                    "failed classification must not leave FileSink attempt files");
        }
    }

    private static Result run(
            JobClient client, int parallelism, Path outputPrefix, boolean keyedValid) {
        SOURCE_OPENS.set(0);
        BatchSession session = client == null
                ? new BatchSession("classification-runtime", "classification runtime")
                : new BatchSession("classification-runtime", "classification runtime", client);
        session.setParallelism(parallelism);
        DataSet source = session.fromSource(inputs()).uid(SOURCE_UID);
        DataRowClassification classified = source.classifyRowsWithSourcePosition(VALIDATION);
        DataSet valid = keyedValid
                ? classified.validRows().partitionByHash("adaptor")
                : classified.validRows();
        Path validPath = outputPrefix.resolveSibling(outputPrefix.getFileName() + "-valid.tsv");
        Path rejectedPath = outputPrefix.resolveSibling(
                outputPrefix.getFileName() + "-rejected.tsv");
        valid.sinkTo(new FileSink(META, validPath.toString()));
        classified.rejectedRows().sinkTo(
                new FileSink(REJECTED_META, rejectedPath.toString()));
        session.execute();
        return new Result(validLines(validPath), rejectedLines(rejectedPath),
                SOURCE_OPENS.get());
    }

    private static CountingExactInput[] inputs() {
        return new CountingExactInput[]{
                new CountingExactInput("split-a", Arrays.asList(
                        row("A", 1, new BigDecimal("1.00")),
                        row(null, 2, BigDecimal.ZERO),
                        row("P", 13, BigDecimal.ZERO))),
                new CountingExactInput("split-b", Arrays.asList(
                        row("B", 2, new BigDecimal("99")),
                        row("C", 3, new BigDecimal("3.00")),
                        row("D", 4, BigDecimal.ZERO)))
        };
    }

    private static void assertOracle(Result result) {
        assertEquals(Arrays.asList("A|1|1.00", "C|3|3.00", "D|4|0"), result.valid);
        assertEquals(Arrays.asList(
                "split-a|1|QUALITY_RULE_VIOLATION|adaptor.required|0|NOT_NULL",
                "split-a|2|QUALITY_RULE_VIOLATION|period.range|1|INTEGER_RANGE_CLOSED",
                "split-b|0|QUALITY_RULE_VIOLATION|amount.range|2|DECIMAL_RANGE_CLOSED"),
                result.rejected);
    }

    private static List<String> validLines(Path path) {
        List<String> rows = new ArrayList<>();
        for (DataRow row : FileSink.read(META, path.toString())) {
            rows.add(row.getString(0) + "|" + row.getInteger(1) + "|"
                    + row.getBigDecimal(2).toPlainString());
        }
        Collections.sort(rows);
        return rows;
    }

    private static List<String> rejectedLines(Path path) {
        List<String> rows = new ArrayList<>();
        for (DataRow row : FileSink.read(REJECTED_META, path.toString())) {
            String split = digestFor("split-a").equals(row.getString(1))
                    ? "split-a" : digestFor("split-b").equals(row.getString(1))
                    ? "split-b" : "unknown:" + row.getString(1);
            rows.add(split + "|" + row.getLong(2) + "|" + row.getString(3)
                    + "|" + row.getString(4) + "|" + row.getInteger(5)
                    + "|" + row.getString(6));
        }
        Collections.sort(rows);
        return rows;
    }

    private static String digestFor(String splitId) {
        return io.rowweave.sched.stream.SourceRecordPosition.of(
                SOURCE_UID, descriptor(splitId), 0L).splitIdentitySha256();
    }

    private static BatchSourceSplitDescriptor descriptor(String splitId) {
        return new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                "classification-test", 1, splitId, SchemaFingerprint.of(META),
                "snapshot-v1",
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    private static String allMessages(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        for (Throwable cursor = failure; cursor != null; cursor = cursor.getCause()) {
            if (cursor.getMessage() != null) messages.append(cursor.getMessage()).append('\n');
        }
        return messages.toString();
    }

    private static final class Result {
        private final List<String> valid;
        private final List<String> rejected;
        private final int sourceOpens;

        private Result(List<String> valid, List<String> rejected, int sourceOpens) {
            this.valid = valid;
            this.rejected = rejected;
            this.sourceOpens = sourceOpens;
        }
    }

    private static final class CountingExactInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final BatchSourceSplitDescriptor descriptor;
        private final List<DataRow> rows;

        private CountingExactInput(String splitId, List<DataRow> rows) {
            this.descriptor = descriptor(splitId);
            this.rows = new ArrayList<>(rows);
        }

        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }
        @Override public Iterator<DataRow> iterator() {
            SOURCE_OPENS.incrementAndGet();
            return rows.iterator();
        }
    }

    private static final class FailingCollectOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private final DataSchema dataSchema;

        private FailingCollectOutput(DataSchema dataSchema) {
            this.dataSchema = dataSchema;
        }

        @Override public DataSchema getSchema() { return dataSchema; }
        @Override public boolean isWorkerWritable() { return true; }
        @Override public void collect(DataRow row) {
            throw new IllegalStateException("injected rejected output failure");
        }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { }
        @Override public void commitPrepared() { }
        @Override public void rollbackCommit() { FAILING_ABORTS.incrementAndGet(); }
        @Override public void finalizeCommit() { }
        @Override public void abort() { FAILING_ABORTS.incrementAndGet(); }
    }
}
