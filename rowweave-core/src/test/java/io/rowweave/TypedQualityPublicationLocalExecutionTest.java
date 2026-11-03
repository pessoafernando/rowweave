package io.rowweave;

import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.ops.RowValidation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/** canonical local publication runtime without reclassification. */
class TypedQualityPublicationLocalExecutionTest {

    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType));
    private static final AtomicInteger SOURCE_OPENS = new AtomicInteger();

    @Test
    void publishesThreeCanonicalMembersAndReadsTheExactSourceOnlyOnce(@TempDir Path tmp) {
        Path domain = tmp.resolve("quality");
        SOURCE_OPENS.set(0);
        ExecutionEnvironment session = new ExecutionEnvironment("quality-local", "quality local");
        DataRowClassification classification = session.fromSource(new CountingExactInput(Arrays.asList(
                        row("A", 1), row(null, 2), row("B", 13), row("C", 13), row("D", 4))))
                .uid("ledger.source")
                .classifyRowsWithSourcePosition(RowValidation.builder(META)
                        .notNull("adaptor.required", 0)
                        .integerRange("period.range", 1, 1, 12)
                        .build());
        classification.writeToManifest(TypedQualityManifest.of(
                domain.toString(), "valid", "rejected", "summary"));

        assertTimeoutPreemptively(Duration.ofSeconds(15), session::execute);

        CommitManifest manifest = new ManifestFileStore(domain).latest();
        assertEquals(3, manifest.members().size());
        long generation = manifest.generation();
        assertEquals(2, read(new ManifestFileSource(
                META, domain.toString(), "valid", generation)).size());
        assertEquals(3, read(new ManifestFileSource(
                classification.rejectedRows().getSchema(),
                domain.toString(), "rejected", generation)).size());
        assertEquals(Arrays.asList(
                "ACCEPTED_TOTAL|||-1||2",
                "REJECTED_REASON|QUALITY_RULE_VIOLATION|adaptor.required|0|NOT_NULL|1",
                "REJECTED_REASON|QUALITY_RULE_VIOLATION|period.range|1|INTEGER_RANGE_CLOSED|2",
                "REJECTED_TOTAL|||-1||3"), summaryLines(read(new ManifestFileSource(
                TypedQualityManifest.summarySchema(),
                domain.toString(), "summary", generation))));
        assertEquals(1, SOURCE_OPENS.get(), "classifier must evaluate the exact source once");
    }

    @Test
    void syntheticPublicationOrdersAreAtomicallyAdmittedBeforeOpeningSource(@TempDir Path tmp) {
        System.setProperty("rowweave.worker.operator.maxMemory", "32768");
        System.setProperty("rowweave.worker.operator.minMemory", "8192");
        try {
            SOURCE_OPENS.set(0);
            ExecutionEnvironment session = new ExecutionEnvironment("quality-memory-admission", "quality memory admission");
            DataRowClassification classification = session.fromSource(new CountingExactInput(Arrays.asList(
                            row("A", 1), row(null, 2))))
                    .uid("ledger.source")
                    .classifyRowsWithSourcePosition(RowValidation.builder(META)
                            .notNull("adaptor.required", 0)
                            .integerRange("period.range", 1, 1, 12)
                            .build());
            classification.writeToManifest(TypedQualityManifest.of(
                    tmp.resolve("quality-admission").toString(),
                    "valid", "rejected", "summary"));

            io.rowweave.mem.ManagedMemory pool =
                    io.rowweave.mem.ManagedMemory.ofBytes(24576);
            assertThrows(io.rowweave.mem.MemoryBudgetExceededException.class,
                    () -> new io.rowweave.plan.PushDagExecutor(2, pool)
                            .runWholeGraph(session.toPlanGraph()));

            assertEquals(0, SOURCE_OPENS.get(),
                    "all graph and synthetic publication owners must be admitted before source body");
            assertEquals(24576, pool.availableBytes(),
                    "failed whole-execution admission must leave no partial guarantees");
            assertEquals(0, pool.snapshot().activeOwnerCount());
        } finally {
            System.clearProperty("rowweave.worker.operator.maxMemory");
            System.clearProperty("rowweave.worker.operator.minMemory");
        }
    }

    private static List<DataRow> read(Source input) {
        List<DataRow> rows = new ArrayList<>();
        for (DataRow row : input) rows.add(row);
        return rows;
    }

    private static List<String> summaryLines(List<DataRow> rows) {
        List<String> lines = new ArrayList<>();
        for (DataRow row : rows) {
            lines.add(row.getString(0) + "|" + row.getString(1) + "|"
                    + row.getString(2) + "|" + row.getInteger(3) + "|"
                    + row.getString(4) + "|" + row.getLong(5));
        }
        return lines;
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    private static final class CountingExactInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final List<DataRow> rows;
        private final BatchSourceSplitDescriptor descriptor = new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                "typed-quality-test", 1, "quality-split", SchemaFingerprint.of(META),
                "snapshot-v1",
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);

        private CountingExactInput(List<DataRow> rows) {
            this.rows = new ArrayList<>(rows);
        }

        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }
        @Override public Iterator<DataRow> iterator() {
            SOURCE_OPENS.incrementAndGet();
            return rows.iterator();
        }
    }
}
