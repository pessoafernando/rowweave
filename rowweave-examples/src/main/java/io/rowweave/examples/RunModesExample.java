package io.rowweave.examples;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.InProcessCluster;
import io.rowweave.cluster.JobClient;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
  * SDK usage for three run modes. The same job DAG runs with a different injected {@link JobClient}:
 * <ol>
  * <li>Local (default): {@code RowWeave.createBatchSession(...)} reuses the in-process, port-free Master+Worker runtime.</li>
  * <li>In-process cluster: inject {@link InProcessCluster} for isolated Master+Worker lifecycles in one JVM.</li>
  * <li>Cluster: inject {@code ClusterJobClient(masterIp, port)} or {@code ZkRemoteJobClient(zk)} after starting separate processes;
  * {@link #clusterModeSketch()} shows the code but does not execute it.</li>
 * </ol>
 *
  * <p>All modes use the same DataSet API and produce equivalent result sets. This example runs modes 1 and 2
 * and asserts bit-identical results, demonstrating that switching modes does not change the output.
 */
public final class RunModesExample {

        /** Detail schema: region and amount. */
    private static final DataSchema DETAIL_META = new DataSchema(
            new Field("region", DataType.StringType),
            new Field("amount", DataType.BigDecimalType));

        /** Result schema: region and sum0 (group sum, BigDecimal scale 10). */
    private static final DataSchema RESULT_META = new DataSchema(
            new Field("region", DataType.StringType),
            new Field("sum0", DataType.BigDecimalType));

    private RunModesExample() {
    }

    /**
          * Builds the mode-independent job DAG: group by region, sum amount, and write outputPath.
          * Every mode calls it with an BatchSession backed by a different client.
     */
        private static void buildAndExecute(BatchSession session, List<DataRow> details, String outputPath) {
        DataSet result = session.fromSource(detailInput(details))
                .groupBy("region").sum("amount");
        result.sinkTo(new FileSink(result.getSchema(), outputPath));
        session.execute();
    }

        /** Local mode: use the default in-process Batch Runtime without injecting a client. */
    public static void runSingleMachine(List<DataRow> details, String outputPath) {
        BatchSession session = RowWeave.createBatchSession("run-modes-single");
                session.setParallelism(2);                     // Optional parallelism for local keyed shuffle.
                buildAndExecute(session, details, outputPath);
    }

        /** Isolated in-process cluster: inject InProcessCluster for same-JVM Master+Worker and AutoCloseable lifecycle. */
    public static void runInProcessCluster(List<DataRow> details, String outputPath, File tmpDir) {
                // The worker-slot hint must be at least the requested parallelism; P=2, so 4 is sufficient.
        try (InProcessCluster cluster = RowWeave.startInProcessCluster(4, tmpDir)) {
            BatchSession session = new BatchSession("run-modes-inproc", "run-modes-inproc", cluster);
            session.setParallelism(2);
                        buildAndExecute(session, details, outputPath);
        }
    }

    /**
          * Separate-process cluster SDK sketch; start those roles before using the connection.
     * <pre>{@code
          * // Fixed Master IP:
     * JobClient client = new ClusterJobClient("192.168.1.10", 19000);
          * // Or multi-Master HA with a ZooKeeper connection string; requires rowweave-registry-zk on the classpath:
     * // JobClient client = new ZkRemoteJobClient("zk1:2181,zk2:2181,zk3:2181");
          * BatchSession session = new BatchSession("job", "job", client);
     * session.setParallelism(4);
     * DataSet result = session.fromSource(input).groupBy("region").sum("amount");
          * // In cluster mode a Sink must be worker-writable: FileSink can, CollectingSink cannot.
     * result.sinkTo(new FileSink(result.getSchema(), "/shared/out.tsv"));
          * session.execute(); // Rows do not return to the client; read them with new FileSource(schema, path).openReader()
     * }</pre>
     */
    public static void clusterModeSketch() {
                // Documentation only: start cluster Master/Worker with ClusterMain; see deployment docs.
    }

        /** Convenience factory for one detail row. */
    public static DataRow detail(String region, String amount) {
        DataRow r = new DataRow(2);
        r.set(0, region);
        r.set(1, amount == null ? null : new BigDecimal(amount));
        return r;
    }

    private static Source detailInput(List<DataRow> details) {
        return new CollectionSource(DETAIL_META, new ArrayList<>(details));
    }

        /** Reads the result file into a sorted "region=sum" list for cross-mode comparison. */
    private static List<String> fingerprint(String path) {
        List<String> fp = new ArrayList<>();
        try (FileSource.Reader rows = new FileSource(RESULT_META, path).openReader()) {
            while (rows.hasNext()) {
                DataRow r = rows.next();
                fp.add(r.getString(0) + "=" + r.getBigDecimal(1).toPlainString());
            }
        }
        fp.sort(null);
        return fp;
    }

    public static void main(String[] args) {
        List<DataRow> details = new ArrayList<>();
        details.add(detail("C1", "100.00"));
        details.add(detail("C1", "200.00"));
        details.add(detail("C2", "50.50"));
        details.add(detail("C2", "49.50"));
        details.add(detail("C3", "300.00"));

        String tmp = System.getProperty("java.io.tmpdir");
        String outSingle = tmp + "/rowweave-runmodes-single.tsv";
        String outInproc = tmp + "/rowweave-runmodes-inproc.tsv";

                // Local mode.
        runSingleMachine(details, outSingle);
                // Isolated in-process cluster.
        runInProcessCluster(details, outInproc, new File(tmp, "rowweave-runmodes-lc"));

                // Verify equivalent result sets across execution modes.
        List<String> fpSingle = fingerprint(outSingle);
        List<String> fpInproc = fingerprint(outInproc);
                System.out.println("Local result:       " + fpSingle);
                System.out.println("In-process result:  " + fpInproc);
                System.out.println("Equivalent result sets: " + fpSingle.equals(fpInproc));
        if (!fpSingle.equals(fpInproc)) {
                        throw new IllegalStateException("Local and in-process results are not equivalent");
        }
    }
}
