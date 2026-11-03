package io.rowweave.cluster;

import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.SourceExecutor;
import io.rowweave.JdbcSnapshotSource;
import io.rowweave.JdbcSnapshotReadHandle;
import io.rowweave.JdbcSnapshotReadRequest;
import io.rowweave.JdbcSnapshotSourceProvider;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** public Source[] JDBC snapshots execute identically in local and cluster modes. */
class JdbcSnapshotRuntimeTest {

    private static final DataSchema INPUT_META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema OUTPUT_META = ParallelGroupByAgg
            .of(INPUT_META, "adaptor").sum("amount").getOutputSchema();
    private static final String QUERY = digest('a');
    private static final String SNAPSHOT = digest('b');

    @AfterEach
    void resetProvider() {
        RuntimeProvider.reset();
    }

    @Test
    void localP1P2AndP4MatchIndependentOracle(@TempDir Path temp) {
        Map<String, BigDecimal> expected = oracle();
        assertEquals(expected, run(null, 1, temp.resolve("local-p1.tsv")));
        assertOpenCloseCounts(2);

        RuntimeProvider.reset();
        assertEquals(expected, run(null, 2, temp.resolve("local-p2.tsv")));
        assertOpenCloseCounts(2);

        RuntimeProvider.reset();
        assertEquals(expected, run(null, 4, temp.resolve("local-p4.tsv")));
        assertOpenCloseCounts(2);
    }

    @Test
    @Tag("multiprocess")
    void inProcessP1P2AndP4MatchIndependentOracle(@TempDir Path temp)
            throws Exception {
        Map<String, BigDecimal> expected = oracle();
        try (InProcessCluster cluster = new InProcessCluster(
                1, temp.resolve("cluster-p1").toFile())) {
            assertEquals(expected, run(cluster, 1, temp.resolve("cluster-p1.tsv")));
        }
        assertOpenCloseCounts(2);

        RuntimeProvider.reset();
        try (InProcessCluster cluster = new InProcessCluster(
                2, temp.resolve("cluster-p2").toFile())) {
            assertEquals(expected, run(cluster, 2, temp.resolve("cluster-p2.tsv")));
        }
        assertOpenCloseCounts(2);

        RuntimeProvider.reset();
        try (InProcessCluster cluster = new InProcessCluster(
                4, temp.resolve("cluster-p4").toFile())) {
            assertEquals(expected, run(cluster, 4, temp.resolve("cluster-p4.tsv")));
        }
        assertOpenCloseCounts(2);
    }

    @Test
    void freshAttemptReopensSameSnapshotFromFirstRowAfterCredentialRotation() {
        RuntimeProvider.install("retry-part", rows(
                "a", "1.2500", "b", "2.0000"));
        JdbcSnapshotSource frozen = input("retry-part", digest('c'));
        String snapshotId = frozen.batchSourceSplit().snapshotId();

        RuntimeProvider.credentialGeneration.set(1);
        SourceExecutor<?> interrupted = frozen.createSourceExecutor();
        interrupted.open();
        assertEquals("a", interrupted.next().get(0));
        interrupted.close();

        RuntimeProvider.credentialGeneration.set(2);
        SourceExecutor<?> replacementAttempt = frozen.createSourceExecutor();
        replacementAttempt.open();
        List<String> pickedValues = new ArrayList<>();
        while (replacementAttempt.hasNext()) {
            pickedValues.add((String) replacementAttempt.next().get(0));
        }

        assertEquals(Arrays.asList("a", "b"), pickedValues);
        assertEquals(snapshotId, frozen.batchSourceSplit().snapshotId());
        assertEquals(Arrays.asList(1, 2),
                RuntimeProvider.openedCredentialGenerations);
        assertEquals(Arrays.asList(SNAPSHOT, SNAPSHOT),
                RuntimeProvider.openedSnapshots);
        assertOpenCloseCounts(2);
    }

    private static Map<String, BigDecimal> run(
            JobClient client, int parallelism, Path output) {
        RuntimeProvider.install("part-a", rows(
                "a", "1.2500", "b", "2.0000"));
        RuntimeProvider.install("part-b", rows(
                "a", "3.7500", "b", "-0.5000"));
        BatchSession session = client == null
                ? new BatchSession("jdbc-source", "jdbc-source")
                : new BatchSession("jdbc-source", "jdbc-source", client);
        session.setParallelism(parallelism);
        DataSet source = session.fromSource(
                input("part-a", digest('c')),
                input("part-b", digest('d')));
        source.groupBy(ParallelGroupByAgg.of(INPUT_META, "adaptor").sum("amount"))
                .sinkTo(new FileSink(OUTPUT_META, output.toString()));
        session.execute();

        Map<String, BigDecimal> result = new HashMap<>();
        try (io.rowweave.FileSource.Reader values =
                     new io.rowweave.FileSource(OUTPUT_META, output.toString())
                             .openReader()) {
            while (values.hasNext()) {
                DataRow row = values.next();
                result.put((String) row.get(0), (BigDecimal) row.get(1));
            }
        }
        return result;
    }

    private static JdbcSnapshotSource input(
            String partitionRef, String partitionDigest) {
        return new JdbcSnapshotSource(
                INPUT_META, new RuntimeProvider(), "endpoint", "credential",
                "ledger", "ledger-query", partitionRef, "snapshot",
                QUERY, partitionDigest, SNAPSHOT, 128, 30, 4096);
    }

    private static Object[][] rows(String... cells) {
        Object[][] rows = new Object[cells.length / 2][2];
        for (int index = 0; index < rows.length; index++) {
            rows[index][0] = cells[index * 2];
            rows[index][1] = new BigDecimal(cells[index * 2 + 1]);
        }
        return rows;
    }

    private static Map<String, BigDecimal> oracle() {
        Map<String, BigDecimal> expected = new HashMap<>();
        expected.put("a", new BigDecimal("5.0000000000"));
        expected.put("b", new BigDecimal("1.5000000000"));
        return expected;
    }

    private static void assertOpenCloseCounts(int expected) {
        assertEquals(expected, RuntimeProvider.opens.get());
        assertEquals(expected, RuntimeProvider.closes.get());
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    public static final class RuntimeProvider implements JdbcSnapshotSourceProvider {
        private static final long serialVersionUID = 1L;
        private static final Map<String, Object[][]> PARTITIONS =
                new ConcurrentHashMap<>();
        static final AtomicInteger opens = new AtomicInteger();
        static final AtomicInteger closes = new AtomicInteger();
        static final AtomicInteger credentialGeneration = new AtomicInteger();
        static final List<Integer> openedCredentialGenerations =
                new CopyOnWriteArrayList<>();
        static final List<String> openedSnapshots = new CopyOnWriteArrayList<>();

        public RuntimeProvider() { }

        static void install(String partition, Object[][] rows) {
            PARTITIONS.put(partition, copy(rows));
        }

        static void reset() {
            PARTITIONS.clear();
            opens.set(0);
            closes.set(0);
            credentialGeneration.set(0);
            openedCredentialGenerations.clear();
            openedSnapshots.clear();
        }

        @Override public String providerType() { return "runtime-jdbc"; }
        @Override public int providerVersion() { return 1; }

        @Override
        public JdbcSnapshotReadHandle open(JdbcSnapshotReadRequest request) {
            Object[][] rows = PARTITIONS.get(request.partitionRef());
            if (rows == null || !QUERY.equals(request.queryDigest())
                    || !SNAPSHOT.equals(request.snapshotId())) {
                throw new IllegalStateException("fixture identity mismatch");
            }
            try {
                CachedRowSet resultSet = RowSetProvider.newFactory()
                        .createCachedRowSet();
                RowSetMetaDataImpl metadata = new RowSetMetaDataImpl();
                metadata.setColumnCount(2);
                metadata.setColumnName(1, "adaptor");
                metadata.setColumnType(1, Types.VARCHAR);
                metadata.setColumnName(2, "amount");
                metadata.setColumnType(2, Types.DECIMAL);
                resultSet.setMetaData(metadata);
                for (int index = 0; index < rows.length; index++) {
                    resultSet.moveToInsertRow();
                    resultSet.updateString(1, (String) rows[index][0]);
                    resultSet.updateBigDecimal(2, (BigDecimal) rows[index][1]);
                    resultSet.insertRow();
                }
                resultSet.moveToCurrentRow();
                resultSet.beforeFirst();
                opens.incrementAndGet();
                openedCredentialGenerations.add(credentialGeneration.get());
                openedSnapshots.add(request.snapshotId());
                return new RuntimeHandle(request, resultSet);
            } catch (SQLException failure) {
                throw new IllegalStateException("fixture open failed");
            }
        }

        private static Object[][] copy(Object[][] rows) {
            Object[][] copy = new Object[rows.length][];
            for (int index = 0; index < rows.length; index++) {
                copy[index] = rows[index].clone();
            }
            return copy;
        }
    }

    private static final class RuntimeHandle implements JdbcSnapshotReadHandle {
        private final JdbcSnapshotReadRequest request;
        private final CachedRowSet resultSet;
        private boolean closed;

        private RuntimeHandle(
                JdbcSnapshotReadRequest request, CachedRowSet resultSet) {
            this.request = request;
            this.resultSet = resultSet;
        }

        @Override public String providerType() { return "runtime-jdbc"; }
        @Override public int providerVersion() { return 1; }
        @Override public String queryDigest() { return request.queryDigest(); }
        @Override public String partitionDigest() { return request.partitionDigest(); }
        @Override public String snapshotId() { return request.snapshotId(); }
        @Override public String schemaFingerprint() {
            return SchemaFingerprint.of(INPUT_META);
        }
        @Override public java.sql.ResultSet resultSet() { return resultSet; }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            RuntimeProvider.closes.incrementAndGet();
            try {
                resultSet.close();
            } catch (SQLException failure) {
                throw new IllegalStateException("fixture close failed");
            }
        }
    }
}
