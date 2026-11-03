package io.rowweave.monitor.http;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.ClusterJobClient;
import io.rowweave.cluster.WorkerServer;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End to end: a real cluster job with the monitor attached, served over the monitor HTTP
 * endpoint, and the page API checked against the real DAG. The monitor is fed by genuine
 * execution rather than mocks, which shows the whole path from job to collection to HTTP to the
 * data behind the front end works.
 */
@Tag("multiprocess")
class MonitorEndToEndTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static List<DataRow> rows(int n) {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < n; i++) { DataRow r = new DataRow(2); r.set(0, i % 8); r.set(1, new BigDecimal(i % 50)); rs.add(r); }
        return rs;
    }

    private static String httpGet(int port, String path) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL("http://127.0.0.1:" + port + path).openConnection();
        c.setConnectTimeout(3000); c.setReadTimeout(3000);
        int code = c.getResponseCode();
        StringBuilder sb = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(
                code < 400 ? c.getInputStream() : c.getErrorStream(), StandardCharsets.UTF_8))) {
            String line; while ((line = r.readLine()) != null) sb.append(line);
        }
        return sb.toString();
    }

    @Test
    void realClusterJobShowsInMonitorHttp(@TempDir Path tmp) throws Exception {
        InMemoryJobMonitor mon = new InMemoryJobMonitor(100);
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        MonitorHttpServer http = new MonitorHttpServer(mon, 0, null);
        int hport = http.start();
        try (MasterServer m = new MasterServer(5000, 0, new InProcessRegistry(), mon);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {

            // A real groupBy job: source, keyed shuffle, sink, therefore a multi-stage DAG.
            BatchSession s = new BatchSession("e2e", "e2e", new ClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            Path f = tmp.resolve("e2e.tsv");
            s.fromSource(new CollectionSource(META, rows(1600)))
                    .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                    .sinkTo(new FileSink(
                            ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema(), f.toString()));
            s.execute();

            // The monitor HTTP /api/jobs endpoint reflects the real job.
            String jobs = httpGet(hport, "/api/jobs");
            assertTrue(jobs.contains("\"state\":\"FINISHED\""), "monitor API reports the job finished, actual:" + jobs);

            // Take the jobId, then read the detail to check the real DAG: nodes and edges come from a real ExecutionGraph.
            String jobId = jobs.replaceAll(".*\"jobId\":\"([^\"]+)\".*", "$1");
            String detail = httpGet(hport, "/api/jobs/" + jobId);
            assertTrue(detail.contains("\"stages\""), "detail carries stages");
            assertTrue(detail.contains("\"dag\""), "detail carries the real DAG");
            assertTrue(detail.contains("\"role\":\"SINK\""), "includes a SINK stage (real keyed shuffle topology)");
            assertTrue(detail.contains("\"nodes\"") && detail.contains("\"edges\""), "DAG has nodes + edges");

            // Once the job finishes, per-stage pushes mark every stage FINISHED with row counts, rather than leaving it at RUNNING.
            assertTrue(detail.contains("\"state\":\"FINISHED\""), "stage marked FINISHED via onStageUpdate (not left in RUNNING)");
            assertTrue(detail.matches(".*\"outputRows\":[1-9][0-9]*.*"), "at least one stage reported output rows: " + detail);
        } finally {
            http.stop();
        }
    }
}
