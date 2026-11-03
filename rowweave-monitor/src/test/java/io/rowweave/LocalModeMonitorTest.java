package io.rowweave;

import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Local mode starts the optional dashboard through the same ServiceLoader path as cluster mode. */
class LocalModeMonitorTest {

    private static final DataSchema SCHEMA = new DataSchema(
            new Field("key", DataType.StringType),
            new Field("amount", DataType.BigDecimalType));

    private final Properties saved = new Properties();

    @AfterEach
    void restore() {
        for (String name : new String[]{
                "rowweave.monitor.enable", "rowweave.monitor.port",
                "rowweave.monitor.history"}) {
            if (saved.containsKey(name)) {
                System.setProperty(name, saved.getProperty(name));
            } else {
                System.clearProperty(name);
            }
        }
        InProcessBatchRuntime.setProcessWideMonitor(null);
        InProcessBatchRuntime.resetProcessWideForTesting();
    }

    @Test
    void enabledLocalRunServesLiveJobDashboard() throws Exception {
        set("rowweave.monitor.enable", "true");
        set("rowweave.monitor.port", "0");
        set("rowweave.monitor.history", "8");
        InProcessBatchRuntime.setProcessWideMonitor(null);
        InProcessBatchRuntime.resetProcessWideForTesting();

        BatchSession environment = RowWeave.createBatchSession("local-http");
        environment.setParallelism(2);
        DataSet grouped = environment.fromSource(source(rows()))
                .groupBy(ParallelGroupByAgg.of(SCHEMA, "key").sum("amount"));
        grouped.sinkTo(new WorkerCollectingOutput(grouped.getSchema()));
        environment.execute();

        int port = InProcessBatchRuntime.processWideMonitorPortForTesting();
        assertTrue(port > 0, "local monitor HTTP did not bind");
        String jobs = get(port, "/api/jobs");
        assertTrue(jobs.contains("\"state\":\"FINISHED\""), jobs);

        Matcher jobId = Pattern.compile("\"jobId\":\"([^\"]+)\"").matcher(jobs);
        assertTrue(jobId.find(), "monitor response did not contain a job id: " + jobs);
        String detail = get(port, "/api/jobs/" + jobId.group(1));
        assertTrue(detail.contains("\"dag\""), detail);
        assertTrue(detail.contains("\"instances\""), detail);
    }

    private void set(String name, String value) {
        String previous = System.getProperty(name);
        if (previous != null) {
            saved.setProperty(name, previous);
        }
        System.setProperty(name, value);
    }

    private static List<DataRow> rows() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            DataRow row = new DataRow(2);
            row.set(0, "key-" + (i % 3));
            row.set(1, new BigDecimal(i));
            rows.add(row);
        }
        return rows;
    }

    private static Source source(List<DataRow> rows) {
        return new Source() {
            @Override public DataSchema getSchema() { return SCHEMA; }
            @Override public Iterator<DataRow> iterator() {
                return new ArrayList<>(rows).iterator();
            }
        };
    }

    private static String get(int port, String path) throws Exception {
        HttpURLConnection connection = (HttpURLConnection) new URL(
                "http://127.0.0.1:" + port + path).openConnection();
        connection.setConnectTimeout(2_000);
        connection.setReadTimeout(2_000);
        try (InputStream input = connection.getInputStream();
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                output.write(buffer, 0, read);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        } finally {
            connection.disconnect();
        }
    }
}
