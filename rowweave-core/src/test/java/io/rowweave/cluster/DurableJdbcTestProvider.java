package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.JdbcSnapshotSource;
import io.rowweave.JdbcSnapshotReadHandle;
import io.rowweave.JdbcSnapshotReadRequest;
import io.rowweave.JdbcSnapshotSourceProvider;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** Test-only reopenable JDBC snapshot used by durable retry/recovery integration gates. */
public final class DurableJdbcTestProvider implements JdbcSnapshotSourceProvider {
    private static final long serialVersionUID = 1L;
    private static final DataSchema META =
            new DataSchema(new Field("id", DataType.IntegerType));
    private static final String QUERY = digest('a');
    private static final String PARTITION = digest('b');
    private static final String SNAPSHOT = digest('c');
    private static volatile int[] snapshotRows = new int[0];

    static final AtomicInteger credentialGeneration = new AtomicInteger();
    static final AtomicInteger opens = new AtomicInteger();
    static final AtomicInteger closes = new AtomicInteger();
    static final List<Integer> openedCredentialGenerations =
            new CopyOnWriteArrayList<>();
    static final List<String> openedCredentialRefs = new CopyOnWriteArrayList<>();
    static final List<String> openedSnapshots = new CopyOnWriteArrayList<>();

    public DurableJdbcTestProvider() { }

    static void reset(int... rows) {
        snapshotRows = rows.clone();
        credentialGeneration.set(0);
        opens.set(0);
        closes.set(0);
        openedCredentialGenerations.clear();
        openedCredentialRefs.clear();
        openedSnapshots.clear();
    }

    static JdbcSnapshotSource input() {
        return new JdbcSnapshotSource(
                META, new DurableJdbcTestProvider(), "endpoint-prod",
                "credential-prod", "domain", "ledger-query", "partition-0",
                "snapshot-prod", QUERY, PARTITION, SNAPSHOT, 128, 30, 4096);
    }

    @Override public String providerType() { return "durable-jdbc-fixture"; }
    @Override public int providerVersion() { return 1; }

    @Override
    public JdbcSnapshotReadHandle open(JdbcSnapshotReadRequest request) {
        if (!QUERY.equals(request.queryDigest())
                || !PARTITION.equals(request.partitionDigest())
                || !SNAPSHOT.equals(request.snapshotId())) {
            throw new IllegalStateException("fixture identity mismatch");
        }
        try {
            CachedRowSet resultSet = RowSetProvider.newFactory().createCachedRowSet();
            RowSetMetaDataImpl metadata = new RowSetMetaDataImpl();
            metadata.setColumnCount(1);
            metadata.setColumnName(1, "id");
            metadata.setColumnType(1, Types.INTEGER);
            resultSet.setMetaData(metadata);
            int[] rows = snapshotRows.clone();
            for (int row : rows) {
                resultSet.moveToInsertRow();
                resultSet.updateInt(1, row);
                resultSet.insertRow();
            }
            resultSet.moveToCurrentRow();
            resultSet.beforeFirst();
            opens.incrementAndGet();
            openedCredentialGenerations.add(credentialGeneration.get());
            openedCredentialRefs.add(request.credentialRef());
            openedSnapshots.add(request.snapshotId());
            return new Handle(request, resultSet);
        } catch (SQLException failure) {
            throw new IllegalStateException("fixture open failed");
        }
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static final class Handle implements JdbcSnapshotReadHandle {
        private final JdbcSnapshotReadRequest request;
        private final CachedRowSet resultSet;
        private boolean closed;

        private Handle(JdbcSnapshotReadRequest request, CachedRowSet resultSet) {
            this.request = request;
            this.resultSet = resultSet;
        }

        @Override public String providerType() { return "durable-jdbc-fixture"; }
        @Override public int providerVersion() { return 1; }
        @Override public String queryDigest() { return request.queryDigest(); }
        @Override public String partitionDigest() { return request.partitionDigest(); }
        @Override public String snapshotId() { return request.snapshotId(); }
        @Override public String schemaFingerprint() {
            return SchemaFingerprint.of(META);
        }
        @Override public java.sql.ResultSet resultSet() { return resultSet; }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            closes.incrementAndGet();
            try {
                resultSet.close();
            } catch (SQLException failure) {
                throw new IllegalStateException("fixture close failed");
            }
        }
    }
}
