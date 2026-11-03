package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.GroupReduceFunction;
import io.rowweave.GroupCombineFunction;
import io.rowweave.BatchSession;
import io.rowweave.ManifestFileSource;
import io.rowweave.ObjectStorageReadHandle;
import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.DataRowClassification;
import io.rowweave.TypedQualityManifest;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SchemaConversion;
import io.rowweave.ops.RowValidation;
import io.rowweave.ops.SortField;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class RealProcessClusterTest {

    private static final int N = 20_000;
    private static final int KEYS = 100;

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();
    private static final DataSchema ORDER_META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType),
            new Field("created_at", DataType.TimestampType),
            new Field("pad", DataType.StringType));
    private static final DataSchema EXACT_META = new DataSchema(
            new Field("k", DataType.BigDecimalType), new Field("pad", DataType.StringType));
    private static final DataSchema REDUCE_OUT = new DataSchema(
            new Field("k", DataType.BigDecimalType), new Field("count", DataType.IntegerType));
    private static final DataSchema COGROUP_OUT = new DataSchema(
            new Field("k", DataType.BigDecimalType),
            new Field("leftCount", DataType.IntegerType), new Field("rightCount", DataType.IntegerType));
    private static final DataSchema WIDE_META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType),
            new Field("pad", DataType.StringType));
    private static final DataSchema WIDE_GROUPED =
            ParallelGroupByAgg.of(WIDE_META, "k").sum("amt").getOutputSchema();
    private static final DataSchema WIDE_COUNT_OUT = new DataSchema(
            new Field("count", DataType.IntegerType));
    private static final DataSchema WIDE_COGROUP_OUT = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("leftCount", DataType.IntegerType), new Field("rightCount", DataType.IntegerType));
    private static final GroupReduceFunction EXACT_COUNT = new GroupReduceFunction() {
        @Override public DataSchema getResultSchema() { return REDUCE_OUT; }
        @Override public void reduce(Iterable<DataRow> rows, Collector collector) {
            BigDecimal key = null;
            int count = 0;
            for (DataRow row : rows) {
                key = (BigDecimal) row.get(0);
                count++;
            }
            DataRow out = new DataRow(2);
            out.set(0, key);
            out.set(1, count);
            collector.collect(out);
        }
    };
    private static final CoGroupFunction EXACT_COGROUP_COUNT = new CoGroupFunction() {
        @Override public DataSchema getResultSchema() { return COGROUP_OUT; }
        @Override public void coGroup(Iterable<DataRow> left, Iterable<DataRow> right, Collector collector) {
            BigDecimal key = null;
            int leftCount = 0;
            for (DataRow row : left) {
                key = (BigDecimal) row.get(0);
                leftCount++;
            }
            int rightCount = 0;
            for (DataRow row : right) {
                if (key == null) {
                    key = (BigDecimal) row.get(0);
                }
                rightCount++;
            }
            DataRow out = new DataRow(3);
            out.set(0, key);
            out.set(1, leftCount);
            out.set(2, rightCount);
            collector.collect(out);
        }
    };
    private static final GroupCombineFunction COMBINE_SUM = new GroupCombineFunction() {
        @Override public void combine(Iterable<DataRow> rows, Collector collector) {
            BigDecimal sum = BigDecimal.ZERO;
            for (DataRow row : rows) {
                sum = sum.add((BigDecimal) row.get(1));
            }
            DataRow out = new DataRow(2);
            out.set(0, "combined");
            out.set(1, sum);
            collector.collect(out);
        }
    };
    private static final GroupCombineFunction WIDE_COMBINE_SUM = new GroupCombineFunction() {
        @Override public void combine(Iterable<DataRow> rows, Collector collector) {
            BigDecimal sum = BigDecimal.ZERO;
            for (DataRow row : rows) {
                sum = sum.add((BigDecimal) row.get(1));
            }
            DataRow out = new DataRow(3);
            out.set(0, "combined");
            out.set(1, sum);
            out.set(2, "done");
            collector.collect(out);
        }
    };
    private static final GroupReduceFunction WIDE_REDUCE_COUNT = new GroupReduceFunction() {
        @Override public DataSchema getResultSchema() { return WIDE_COUNT_OUT; }
        @Override public void reduce(Iterable<DataRow> rows, Collector collector) {
            int count = 0;
            for (DataRow ignored : rows) {
                count++;
            }
            DataRow out = new DataRow(1);
            out.set(0, count);
            collector.collect(out);
        }
    };
    private static final CoGroupFunction WIDE_COGROUP_COUNT = new CoGroupFunction() {
        @Override public DataSchema getResultSchema() { return WIDE_COGROUP_OUT; }
        @Override public void coGroup(Iterable<DataRow> left, Iterable<DataRow> right, Collector collector) {
            String key = null;
            int leftCount = 0;
            for (DataRow row : left) {
                key = (String) row.get(0);
                leftCount++;
            }
            int rightCount = 0;
            for (DataRow row : right) {
                if (key == null) {
                    key = (String) row.get(0);
                }
                rightCount++;
            }
            DataRow out = new DataRow(3);
            out.set(0, key);
            out.set(1, leftCount);
            out.set(2, rightCount);
            collector.collect(out);
        }
    };

    private static Process launch(List<String> jvmArgs, String readyPrefix, long timeoutMs,
                                  StringBuilder readyOut) throws Exception {
        return launch(jvmArgs, readyPrefix, timeoutMs, readyOut, null);
    }

    private static Process launch(List<String> jvmArgs, String readyPrefix, long timeoutMs,
                                  StringBuilder readyOut, List<String> processLog) throws Exception {
        String cp = System.getProperty("java.class.path");
        List<String> cmd = io.rowweave.test.ForkedJvm.baseCommand();
        io.rowweave.test.ForkedJvm.appendJvmArguments(cmd, jvmArgs);
        cmd.add("-cp");
        cmd.add(cp);
        String muxMode = System.getProperty(ShuffleTransportConfig.MUX_PROPERTY);
        if (muxMode != null) {
            cmd.add("-D" + ShuffleTransportConfig.MUX_PROPERTY + "=" + muxMode);
        }
        cmd.add("io.rowweave.cluster.ClusterMain");
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
        long deadline = System.currentTimeMillis() + timeoutMs;
        final boolean[] ready = {false};
        Thread reader = new Thread(() -> {
            try {
                String line;
                while ((line = r.readLine()) != null) {
                    if (processLog != null) {
                        synchronized (processLog) {
                            processLog.add(line);
                        }
                    }
                    if (!ready[0] && line.startsWith(readyPrefix)) {
                        synchronized (readyOut) {
                            readyOut.append(line.substring(readyPrefix.length()).trim());
                            ready[0] = true;
                            readyOut.notifyAll();
                        }
                    }
                }
            } catch (Exception ignored) {
            }
        }, "launch-reader");
        reader.setDaemon(true);
        reader.start();
        synchronized (readyOut) {
            while (!ready[0] && System.currentTimeMillis() < deadline) {
                readyOut.wait(Math.max(1, deadline - System.currentTimeMillis()));
            }
        }
        if (!ready[0]) {
            p.destroyForcibly();
            fail("process not ready within " + timeoutMs + " ms (marker " + readyPrefix + ")");
        }
        return p;
    }

    private static void kill(Process p) {
        if (p != null) {
            p.destroyForcibly();
            try {
                p.waitFor(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static List<DataRow> rows() {
        List<DataRow> rows = new ArrayList<>(N);
        for (int i = 0; i < N; i++) {
            DataRow r = new DataRow(2);
            r.set(0, "k" + (i % KEYS));
            r.set(1, new BigDecimal(i % 83));
            rows.add(r);
        }
        return rows;
    }

    private static List<String> sortedKeyVal(List<DataRow> rows) {
        List<String> out = new ArrayList<>();
        for (DataRow r : rows) {
            out.add(r.get(0) + "=" + ((BigDecimal) r.get(1)).stripTrailingZeros().toPlainString());
        }
        out.sort(null);
        return out;
    }

    private static PlanNode fileSource(Path path, DataSchema meta, List<DataRow> rows) {
        FileSink writer = new FileSink(meta, path.toString());
        for (DataRow row : rows) {
            writer.collect(row);
        }
        writer.close();
        return PlanNode.sourceFromInputs(meta,
                Collections.singletonList(new FileSource(meta, path.toString())));
    }

    @Test
    void groupBySumRealThreeProcessCluster(@TempDir Path tmp) throws Exception {
        List<DataRow> rows = rows();
        PlanNode clusterSource = fileSource(tmp.resolve("real-gb-source.tsv"), META, rows);

        List<DataRow> single = new ArrayList<>();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("groupBySum", GB, 0).listParam("keys", Arrays.asList("k")).param("measure", "amt").build()
        )), single::add);
        assertEquals(KEYS, single.size());

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder mp = new StringBuilder();
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, mp);
            int masterPort = Integer.parseInt(mp.toString());
            assertTrue(masterPort > 0, "the master port is known");

            StringBuilder w0 = new StringBuilder();
            worker0 = launch(Arrays.asList("-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort, "-Drowweave.worker.slots=32",
                    "-Drowweave.worker.host=127.0.0.1"),
                    "WORKER_READY=", 30_000, w0);
            StringBuilder w1 = new StringBuilder();
            worker1 = launch(Arrays.asList("-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort, "-Drowweave.worker.slots=32",
                    "-Drowweave.worker.host=127.0.0.1"),
                    "WORKER_READY=", 30_000, w1);

            File outFile = tmp.resolve("real-gb.tsv").toFile();
            FileSink output = new FileSink(GB, outFile.getAbsolutePath());
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    clusterSource,
                    PlanNode.unary("groupBySum", GB, 0).listParam("keys", Arrays.asList("k")).param("measure", "amt").build(),
                    PlanNode.sink(GB, 1, output).build()));
            long rowCount = new ClusterJobClient("127.0.0.1", masterPort).submitToSink(graph);

            List<DataRow> cluster = FileSink.read(GB, outFile.getAbsolutePath());
            assertEquals(KEYS, rowCount, "JobResult rowCount");
            assertEquals(single.size(), cluster.size(), "the real cluster group count matches the single process run");
            assertEquals(sortedKeyVal(single), sortedKeyVal(cluster), "groupBySum on the three-process cluster is bit-identical to the single process run");
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    /** schemaConvert is rebuilt as a core built-in in independent Master/Worker JVMs. */
    @Test
    void schemaConversionRealThreeProcessClusterMatchesIndependentOracle(@TempDir Path tmp) throws Exception {
        DataSchema inputMeta = new DataSchema(
                new Field("id", DataType.IntegerType),
                new Field("cents", DataType.LongType),
                new Field("memo", DataType.StringType));
        SchemaConversion conversion = SchemaConversion.builder(inputMeta)
                .required("id", new Field("adaptor_id", DataType.LongType),
                        SchemaConversion.Cast.INTEGER_TO_LONG)
                .required("cents", new Field("amount", DataType.BigDecimalType),
                        SchemaConversion.Cast.LONG_TO_DECIMAL)
                .nullable("memo", new Field("description", DataType.StringType),
                        SchemaConversion.Cast.IDENTITY)
                .defaulted(new Field("district", DataType.StringType), "AAA")
                .build();
        DataSchema outputMeta = conversion.getOutputSchema();
        List<DataRow> rows = Arrays.asList(
                rowValues(2, -5L, null),
                rowValues(1, 10L, "first"),
                rowValues(3, Long.MAX_VALUE, "last"));
        PlanNode source = fileSource(tmp.resolve("schema-source.tsv"), inputMeta, rows);
        PlanNode converted = PlanNode.unary("schemaConvert", outputMeta, 0)
                .listParam("sourceIndices", conversion.sourceIndices())
                .listParam("casts", conversion.casts())
                .listParam("nullPolicies", conversion.nullPolicies())
                .listParam("defaultKinds", conversion.defaultKinds())
                .listParam("defaultValues", conversion.defaultValues())
                .build();

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder masterPortText = new StringBuilder();
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, masterPortText);
            int masterPort = Integer.parseInt(masterPortText.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=8",
                    "-Drowweave.worker.host=127.0.0.1");
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());

            Path output = tmp.resolve("schema-output.tsv");
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    source, converted,
                    PlanNode.sink(outputMeta, 1,
                            new FileSink(outputMeta, output.toString())).build()), 4);
            long rowCount = new ClusterJobClient("127.0.0.1", masterPort).submitToSink(graph);

            List<String> actual = new ArrayList<>();
            for (DataRow row : FileSink.read(outputMeta, output.toString())) {
                actual.add(row.getLong(0) + "|" + row.getBigDecimal(1).toPlainString()
                        + "|" + row.get(2) + "|" + row.getString(3));
            }
            Collections.sort(actual);
            assertEquals(3L, rowCount);
            assertEquals(Arrays.asList(
                    "1|10|first|AAA",
                    "2|-5|null|AAA",
                    "3|9223372036854775807|last|AAA"), actual);
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    /** rowValidate is rebuilt as a core built-in in independent Master/Worker JVMs. */
    @Test
    void rowValidationRealThreeProcessClusterMatchesIndependentOracle(@TempDir Path tmp) throws Exception {
        DataSchema meta = new DataSchema(
                new Field("adaptor", DataType.StringType),
                new Field("period", DataType.IntegerType),
                new Field("amount", DataType.BigDecimalType));
        RowValidation validation = RowValidation.builder(meta)
                .notNull("adaptor.required", 0)
                .stringUtf8BytesRange("adaptor.length", 0, 1, 16)
                .integerRange("period.range", 1, 1, 12)
                .decimalRange("amount.range", 2,
                        new BigDecimal("-100.00"), new BigDecimal("100.00"))
                .build();
        List<DataRow> rows = Arrays.asList(
                rowValues("C003", 12, new BigDecimal("-5.50")),
                rowValues("A001", 1, new BigDecimal("10.000")),
                rowValues("B002", 2, BigDecimal.ZERO));
        PlanNode source = fileSource(tmp.resolve("quality-source.tsv"), meta, rows);
        PlanNode checked = PlanNode.unary("rowValidate", meta, 0)
                .listParam("ruleIds", validation.ruleIds())
                .listParam("ruleKinds", validation.ruleKinds())
                .listParam("fieldIndices", validation.fieldIndices())
                .listParam("lowerBounds", validation.lowerBounds())
                .listParam("upperBounds", validation.upperBounds())
                .build();

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder masterPortText = new StringBuilder();
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, masterPortText);
            int masterPort = Integer.parseInt(masterPortText.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=8",
                    "-Drowweave.worker.host=127.0.0.1");
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());

            Path output = tmp.resolve("quality-output.tsv");
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    source, checked,
                    PlanNode.sink(meta, 1,
                            new FileSink(meta, output.toString())).build()), 4);
            long rowCount = new ClusterJobClient(
                    "127.0.0.1", masterPort).submitToSink(graph);

            List<String> actual = new ArrayList<>();
            for (DataRow row : FileSink.read(meta, output.toString())) {
                actual.add(row.getString(0) + "|" + row.getInteger(1) + "|"
                        + row.getBigDecimal(2).toPlainString());
            }
            Collections.sort(actual);
            assertEquals(3L, rowCount);
            assertEquals(Arrays.asList(
                    "A001|1|10.000", "B002|2|0", "C003|12|-5.50"), actual);
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    /** source-local validation position is assembled inside independent Worker JVMs. */
    @Test
    void positionedRowValidationRealThreeProcessClusterMatchesOracle(
            @TempDir Path tmp) throws Exception {
        DataSchema meta = new DataSchema(
                new Field("adaptor", DataType.StringType),
                new Field("period", DataType.IntegerType),
                new Field("amount", DataType.BigDecimalType));
        RowValidation validation = RowValidation.builder(meta)
                .notNull("adaptor.required", 0)
                .integerRange("period.range", 1, 1, 12)
                .decimalRange("amount.range", 2,
                        new BigDecimal("-100"), new BigDecimal("100"))
                .build();
        PlanNode source = PlanNode.sourceFromInputs(meta,
                        Collections.singletonList(new io.rowweave.CollectionSource(
                                meta, "quality-position-split", Arrays.asList(
                                rowValues("C003", 12, new BigDecimal("-5.50")),
                                rowValues("A001", 1, new BigDecimal("10.000")),
                                rowValues("B002", 2, BigDecimal.ZERO)))))
                .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger.source");
        PlanNode checked = PlanNode.unary("rowValidate", meta, 0)
                .param("sourcePositionMode", "EXACT_SOURCE_V1")
                .listParam("ruleIds", validation.ruleIds())
                .listParam("ruleKinds", validation.ruleKinds())
                .listParam("fieldIndices", validation.fieldIndices())
                .listParam("lowerBounds", validation.lowerBounds())
                .listParam("upperBounds", validation.upperBounds())
                .build();

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder masterPortText = new StringBuilder();
            master = launch(Arrays.asList(
                    "-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, masterPortText);
            int masterPort = Integer.parseInt(masterPortText.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=8",
                    "-Drowweave.worker.host=127.0.0.1");
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());

            Path output = tmp.resolve("positioned-quality-output.tsv");
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    source, checked,
                    PlanNode.sink(meta, 1,
                            new FileSink(meta, output.toString())).build()), 4);
            long rowCount = new ClusterJobClient(
                    "127.0.0.1", masterPort).submitToSink(graph);

            List<String> actual = new ArrayList<>();
            for (DataRow row : FileSink.read(meta, output.toString())) {
                actual.add(row.getString(0) + "|" + row.getInteger(1) + "|"
                        + row.getBigDecimal(2).toPlainString());
            }
            Collections.sort(actual);
            assertEquals(3L, rowCount);
            assertEquals(Arrays.asList(
                    "A001|1|10.000", "B002|2|0", "C003|12|-5.50"), actual);
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    /** classifier/routes preserve exact positions in independent M+2W JVMs. */
    @Test
    void typedRowClassificationRealThreeProcessClusterMatchesExactOracle(
            @TempDir Path tmp) throws Exception {
        DataSchema meta = new DataSchema(
                new Field("adaptor", DataType.StringType),
                new Field("period", DataType.IntegerType),
                new Field("amount", DataType.BigDecimalType));
        DataSchema rejectedMeta = new DataSchema(
                new Field("source_operator_uid", DataType.StringType),
                new Field("split_identity_sha256", DataType.StringType),
                new Field("split_local_ordinal", DataType.LongType),
                new Field("quality_error_code", DataType.StringType),
                new Field("quality_rule_id", DataType.StringType),
                new Field("quality_field_index", DataType.IntegerType),
                new Field("quality_detail", DataType.StringType));
        RowValidation validation = RowValidation.builder(meta)
                .notNull("adaptor.required", 0)
                .integerRange("period.range", 1, 1, 12)
                .decimalRange("amount.range", 2,
                        new BigDecimal("-10"), new BigDecimal("10"))
                .build();
        io.rowweave.CollectionSource splitA = new io.rowweave.CollectionSource(
                meta, "classification-split-a", Arrays.asList(
                rowValues("A", 1, new BigDecimal("1.00")),
                rowValues(null, 2, BigDecimal.ZERO),
                rowValues("P", 13, BigDecimal.ZERO)));
        io.rowweave.CollectionSource splitB = new io.rowweave.CollectionSource(
                meta, "classification-split-b", Arrays.asList(
                rowValues("B", 2, new BigDecimal("99")),
                rowValues("C", 3, new BigDecimal("3.00")),
                rowValues("D", 4, BigDecimal.ZERO)));

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder masterPortText = new StringBuilder();
            master = launch(Arrays.asList(
                    "-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, masterPortText);
            int masterPort = Integer.parseInt(masterPortText.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=16",
                    "-Drowweave.worker.host=127.0.0.1");
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());

            Path validPath = tmp.resolve("classification-valid.tsv");
            Path rejectedPath = tmp.resolve("classification-rejected.tsv");
            BatchSession session = new BatchSession(
                    "classification-multiprocess", "classification multiprocess",
                    new ClusterJobClient("127.0.0.1", masterPort));
            session.setParallelism(4);
            io.rowweave.DataSet source = session.fromSource(splitA, splitB)
                    .uid("ledger.source");
            DataRowClassification classified =
                    source.classifyRowsWithSourcePosition(validation);
            classified.validRows().partitionByHash("adaptor")
                    .sinkTo(new FileSink(meta, validPath.toString()));
            classified.rejectedRows()
                    .sinkTo(new FileSink(rejectedMeta, rejectedPath.toString()));
            session.execute();

            List<String> valid = new ArrayList<>();
            for (DataRow row : FileSink.read(meta, validPath.toString())) {
                valid.add(row.getString(0) + "|" + row.getInteger(1) + "|"
                        + row.getBigDecimal(2).toPlainString());
            }
            Collections.sort(valid);
            assertEquals(Arrays.asList("A|1|1.00", "C|3|3.00", "D|4|0"), valid);

            String splitADigest = io.rowweave.sched.stream.SourceRecordPosition.of(
                    "ledger.source", splitA.batchSourceSplit(), 0L).splitIdentitySha256();
            String splitBDigest = io.rowweave.sched.stream.SourceRecordPosition.of(
                    "ledger.source", splitB.batchSourceSplit(), 0L).splitIdentitySha256();
            List<String> rejected = new ArrayList<>();
            for (DataRow row : FileSink.read(rejectedMeta, rejectedPath.toString())) {
                String split = splitADigest.equals(row.getString(1)) ? "a"
                        : splitBDigest.equals(row.getString(1)) ? "b" : "unknown";
                rejected.add(split + "|" + row.getLong(2) + "|" + row.getString(3)
                        + "|" + row.getString(4) + "|" + row.getInteger(5)
                        + "|" + row.getString(6));
            }
            Collections.sort(rejected);
            assertEquals(Arrays.asList(
                    "a|1|QUALITY_RULE_VIOLATION|adaptor.required|0|NOT_NULL",
                    "a|2|QUALITY_RULE_VIOLATION|period.range|1|INTEGER_RANGE_CLOSED",
                    "b|0|QUALITY_RULE_VIOLATION|amount.range|2|DECIMAL_RANGE_CLOSED"),
                    rejected);
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    /** the reserved Object quality graph is identical in independent M+2W JVMs. */
    @Test
    void objectQualityRealThreeProcessClusterP1P2P4MatchesOracle(
            @TempDir Path tmp) throws Exception {
        DataSchema meta = new DataSchema(
                new Field("id", DataType.LongType),
                new Field("amount", DataType.BigDecimalType));
        Path partA = tmp.resolve("object-quality-a.tsv");
        Path partB = tmp.resolve("object-quality-b.tsv");
        byte[] first = "2\t2.00\nbad\t9.0\n1\t1.00\n".getBytes(StandardCharsets.UTF_8);
        byte[] secondPrefix = "3\t3.00\r\n4\t4.00\n".getBytes(StandardCharsets.UTF_8);
        byte[] second = Arrays.copyOf(secondPrefix, secondPrefix.length + 3);
        second[secondPrefix.length] = (byte) 0xc3;
        second[secondPrefix.length + 1] = (byte) 0x28;
        second[secondPrefix.length + 2] = (byte) '\n';
        Files.write(partA, first);
        Files.write(partB, second);

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder masterPortText = new StringBuilder();
            master = launch(Arrays.asList(
                    "-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, masterPortText);
            int masterPort = Integer.parseInt(masterPortText.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=16",
                    "-Drowweave.worker.host=127.0.0.1");
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());
            ClusterJobClient client = new ClusterJobClient("127.0.0.1", masterPort);

            QualitySnapshot p1 = runObjectQuality(
                    client, 1, meta, partA, partB, tmp.resolve("object-quality-p1"));
            assertObjectQualityOracle(p1);
            QualitySnapshot p2 = runObjectQuality(
                    client, 2, meta, partA, partB, tmp.resolve("object-quality-p2"));
            QualitySnapshot p4 = runObjectQuality(
                    client, 4, meta, partA, partB, tmp.resolve("object-quality-p4"));
            assertEquals(p1, p2);
            assertEquals(p1, p4);
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    private static QualitySnapshot runObjectQuality(
            JobClient client, int parallelism, DataSchema meta,
            Path partA, Path partB, Path domain) throws Exception {
        BatchSession session = new BatchSession(
                "object-quality-multiprocess", "object quality multiprocess", client);
        session.setParallelism(parallelism);
        session.classifyObjectStorageRows(
                        "ledger.object.source", objectInput(meta, partA), objectInput(meta, partB))
                .writeToManifest(TypedQualityManifest.of(
                        domain.toString(), "valid", "rejected", "summary"));
        session.execute();
        CommitManifest manifest = new ManifestFileStore(domain).latest();
        long generation = manifest.generation();
        return new QualitySnapshot(
                qualityLines(new ManifestFileSource(meta, domain.toString(), "valid", generation)),
                qualityLines(new ManifestFileSource(objectRejectedMeta(), domain.toString(),
                        "rejected", generation)),
                qualityLines(new ManifestFileSource(TypedQualityManifest.summarySchema(),
                        domain.toString(), "summary", generation)));
    }

    private static ObjectStorageSnapshotSource objectInput(DataSchema meta, Path file)
            throws Exception {
        byte[] content = Files.readAllBytes(file);
        return new ObjectStorageSnapshotSource(
                meta, new SharedFileObjectProvider(), "local-fixture", "none", "quality",
                file.toAbsolutePath().toString(), "fixture-v1", content.length,
                sha256(content), ObjectStorageSnapshotSource.MIN_ROW_BYTES);
    }

    private static List<String> qualityLines(ManifestFileSource input) {
        List<String> result = new ArrayList<>();
        for (DataRow row : input) {
            StringBuilder line = new StringBuilder();
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) line.append('|');
                Object value = row.get(i);
                line.append(value instanceof BigDecimal
                        ? ((BigDecimal) value).toPlainString() : value);
            }
            result.add(line.toString());
        }
        return result;
    }

    private static void assertObjectQualityOracle(QualitySnapshot snapshot) {
        assertEquals(Arrays.asList("1|1.00", "2|2.00", "4|4.00"), snapshot.valid);
        List<String> diagnostics = new ArrayList<>();
        for (String row : snapshot.rejected) {
            String[] fields = row.split("\\|", -1);
            assertEquals("ledger.object.source", fields[0]);
            assertTrue(fields[1].matches("[0-9a-f]{64}"));
            diagnostics.add(fields[3] + "|" + fields[2]);
        }
        Collections.sort(diagnostics);
        assertEquals(Arrays.asList(
                "QUALITY_RAW_CR_INVALID|0",
                "QUALITY_RAW_FORMAT_INVALID|1",
                "QUALITY_RAW_UTF8_INVALID|2"), diagnostics);
        assertEquals(Arrays.asList(
                "ACCEPTED_TOTAL|||-1||3",
                "REJECTED_REASON|QUALITY_RAW_CR_INVALID|OBJECT_ROW_DECODE_V1|-1|CR|1",
                "REJECTED_REASON|QUALITY_RAW_FORMAT_INVALID|OBJECT_ROW_DECODE_V1|-1|ROWWEAVE_ROW_TEXT_V1|1",
                "REJECTED_REASON|QUALITY_RAW_UTF8_INVALID|OBJECT_ROW_DECODE_V1|-1|UTF8|1",
                "REJECTED_TOTAL|||-1||3"), snapshot.summary);
    }

    private static DataSchema objectRejectedMeta() {
        return new DataSchema(
                new Field("source_operator_uid", DataType.StringType),
                new Field("split_identity_sha256", DataType.StringType),
                new Field("split_local_ordinal", DataType.LongType),
                new Field("quality_error_code", DataType.StringType),
                new Field("quality_rule_id", DataType.StringType),
                new Field("quality_field_index", DataType.IntegerType),
                new Field("quality_detail", DataType.StringType));
    }

    private static String sha256(byte[] value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
        StringBuilder result = new StringBuilder(64);
        for (byte item : digest) {
            result.append(Character.forDigit((item >>> 4) & 0xf, 16));
            result.append(Character.forDigit(item & 0xf, 16));
        }
        return result.toString();
    }

    private static final class QualitySnapshot {
        private final List<String> valid;
        private final List<String> rejected;
        private final List<String> summary;

        private QualitySnapshot(
                List<String> valid, List<String> rejected, List<String> summary) {
            this.valid = valid;
            this.rejected = rejected;
            this.summary = summary;
        }

        @Override public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof QualitySnapshot)) return false;
            QualitySnapshot that = (QualitySnapshot) other;
            return valid.equals(that.valid) && rejected.equals(that.rejected)
                    && summary.equals(that.summary);
        }

        @Override public int hashCode() {
            return java.util.Objects.hash(valid, rejected, summary);
        }
    }

    public static final class SharedFileObjectProvider
            implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        public SharedFileObjectProvider() { }
        @Override public String providerType() { return "shared-file-quality-fixture"; }
        @Override public int providerVersion() { return 1; }
        @Override public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            try {
                return new SharedFileObjectHandle(request,
                        Files.newInputStream(java.nio.file.Paths.get(request.objectKey())));
            } catch (IOException failure) {
                throw new IllegalStateException("fixture file unavailable", failure);
            }
        }
    }

    private static final class SharedFileObjectHandle implements ObjectStorageReadHandle {
        private final ObjectStorageReadRequest request;
        private final InputStream stream;
        private SharedFileObjectHandle(ObjectStorageReadRequest request, InputStream stream) {
            this.request = request;
            this.stream = stream;
        }
        @Override public String providerType() { return "shared-file-quality-fixture"; }
        @Override public int providerVersion() { return 1; }
        @Override public String versionId() { return request.versionId(); }
        @Override public long contentLength() { return request.contentLength(); }
        @Override public InputStream stream() { return stream; }
        @Override public void close() {
            try {
                stream.close();
            } catch (IOException failure) {
                throw new IllegalStateException("fixture close failed", failure);
            }
        }
    }

    private static DataRow rowValues(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    @Test
    void combineGroupSpillRealThreeProcessCluster(@TempDir Path tmp) throws Exception {
        List<DataRow> rows = rows();
        PlanNode clusterSource = fileSource(tmp.resolve("real-combine-source.tsv"), META, rows);
        Map<String, String> params = new LinkedHashMap<>();
        params.put("shards", "4");
        PlanNode combine = PlanNode.unaryFnWithParams("combineGroup", META, 0,
                params, Collections.emptyMap(), COMBINE_SUM);

        List<DataRow> single = new ArrayList<>();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows), combine), 1), single::add);
        assertEquals(4, single.size());

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder mp = new StringBuilder();
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, mp);
            int masterPort = Integer.parseInt(mp.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=8",
                    "-Drowweave.worker.host=127.0.0.1");
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());

            Path output = tmp.resolve("real-combine.tsv");
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    clusterSource, combine,
                    PlanNode.sink(META, 1, new FileSink(META, output.toString())).build()), 4);
            long rowCount = new ClusterJobClient("127.0.0.1", masterPort).submitToSink(graph);
            List<DataRow> cluster = FileSink.read(META, output.toString());

            assertEquals(single.size(), rowCount);
            assertEquals(sortedKeyVal(single), sortedKeyVal(cluster));
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    @Test
    void orderBySpillRealThreeProcessClusterMatchesP1(@TempDir Path tmp) throws Exception {
        Path spillBase = tmp.toRealPath();
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            DataRow row = new DataRow(4);
            row.set(0, i % 97 == 0 ? null : "k" + (i % 211));
            row.set(1, i % 89 == 0 ? null : new BigDecimal(i % 1000).movePointLeft(i % 3));
            Timestamp timestamp = new Timestamp(1_700_000_000_000L);
            timestamp.setNanos((int) ((i * 2654435761L) % 1_000_000L));
            row.set(2, timestamp);
            row.set(3, "wide-spill-payload-" + (i % 31) + "-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
            rows.add(row);
        }
        PlanNode clusterSource = fileSource(tmp.resolve("real-order-source.tsv"), ORDER_META, rows);
        List<String> sort = Arrays.asList(
                PlanOps.encodeSortField(new SortField(
                        2, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                PlanOps.encodeSortField(new SortField(
                        1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)),
                PlanOps.encodeSortField(new SortField(
                        0, SortField.Direction.ASC, SortField.NullOrder.NULLS_FIRST)));

        List<DataRow> single = new ArrayList<>();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(
                PlanNode.source(ORDER_META, rows),
                PlanNode.unary("orderBy", ORDER_META, 0).listParam("sort", sort).build()
        ), 1), single::add);

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder mp = new StringBuilder();
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, mp);
            int masterPort = Integer.parseInt(mp.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=8",
                    "-Drowweave.worker.host=127.0.0.1",
                    "-Drowweave.memory.bytes=524288",
                    "-Drowweave.worker.operator.minMemory=262144",
                    "-Drowweave.worker.operator.maxMemory=262144",
                    "-Drowweave.worker.spill.dir=" + spillBase.resolve("bounded-worker-spill"),
                    "-Drowweave.worker.spill.maxBytes=16777216",
                    "-Drowweave.worker.spill.jobMaxBytes=8388608",
                    "-Drowweave.worker.spill.minFreeBytes=0");
            StringBuilder w0 = new StringBuilder();
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000, w0);
            StringBuilder w1 = new StringBuilder();
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000, w1);

            File outFile = tmp.resolve("real-order.tsv").toFile();
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    clusterSource,
                    PlanNode.unary("orderBy", ORDER_META, 0).listParam("sort", sort).build(),
                    PlanNode.sink(ORDER_META, 1,
                            new FileSink(ORDER_META, outFile.getAbsolutePath())).build()
            ), 4);
            long rowCount = new ClusterJobClient("127.0.0.1", masterPort).submitToSink(graph);
            List<DataRow> cluster = FileSink.read(ORDER_META, outFile.getAbsolutePath());

            assertEquals(single.size(), rowCount);
            assertEquals(orderRows(single), orderRows(cluster),
                    "orderBy under tiny-memory spill on the three-process cluster at parallelism 4 matches parallelism 1 row by row");
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    @Test
    void orderByClusterDiskQuotaFailureDoesNotCommitOutput(@TempDir Path tmp) throws Exception {
        Path spillBase = tmp.toRealPath();
        String padding = new String(new char[1024]).replace('\0', 'x');
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 30_000; i++) {
            DataRow row = new DataRow(4);
            row.set(0, "k" + (i % 211));
            row.set(1, new BigDecimal(i));
            row.set(2, new Timestamp(1_700_000_000_000L + i));
            row.set(3, padding);
            rows.add(row);
        }
        PlanNode clusterSource = fileSource(tmp.resolve("quota-source.tsv"), ORDER_META, rows);
        List<String> sort = Collections.singletonList(PlanOps.encodeSortField(new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_FIRST)));

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder mp = new StringBuilder();
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, mp);
            int masterPort = Integer.parseInt(mp.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=8",
                    "-Drowweave.worker.host=127.0.0.1",
                    "-Drowweave.memory.bytes=524288",
                    "-Drowweave.worker.operator.minMemory=262144",
                    "-Drowweave.worker.operator.maxMemory=262144",
                    "-Drowweave.worker.spill.dir=" + spillBase.resolve("quota-worker-spill"),
                    "-Drowweave.worker.spill.maxBytes=1048576",
                    "-Drowweave.worker.spill.jobMaxBytes=1048576",
                    "-Drowweave.worker.spill.minFreeBytes=0");
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());

            Path output = tmp.resolve("quota-output.tsv");
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    clusterSource,
                    PlanNode.unary("orderBy", ORDER_META, 0).listParam("sort", sort).build(),
                    PlanNode.sink(ORDER_META, 1,
                            new FileSink(ORDER_META, output.toString())).build()), 4);
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> new ClusterJobClient("127.0.0.1", masterPort).submitToSink(graph));

            assertTrue(failure.getMessage().contains("SPILL_JOB_QUOTA_EXCEEDED"),
                    failure.getMessage());
            assertTrue(!Files.exists(output) || Files.size(output) == 0,
                    "quota failure must not publish sorted rows");
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    @Test
    void topSpillRealThreeProcessClusterMatchesP1(@TempDir Path tmp) throws Exception {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            DataRow row = new DataRow(4);
            row.set(0, i % 97 == 0 ? null : "k" + (i % 211));
            row.set(1, i % 89 == 0 ? null : new BigDecimal(i % 1000).movePointLeft(i % 3));
            Timestamp timestamp = new Timestamp(1_700_000_000_000L + (i % 401));
            timestamp.setNanos((int) ((i * 2654435761L) % 1_000_000L));
            row.set(2, timestamp);
            row.set(3, "wide-top-payload-" + (i % 31) + "-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
            rows.add(row);
        }
        PlanNode clusterSource = fileSource(tmp.resolve("real-top-source.tsv"), ORDER_META, rows);
        List<String> sort = Arrays.asList(
                PlanOps.encodeSortField(new SortField(
                        2, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                PlanOps.encodeSortField(new SortField(
                        1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)),
                PlanOps.encodeSortField(new SortField(
                        0, SortField.Direction.ASC, SortField.NullOrder.NULLS_FIRST)));
        PlanNode top = PlanNode.unary("top", ORDER_META, 0)
                .param("n", "5000").listParam("sort", sort).build();

        List<DataRow> single = new ArrayList<>();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(
                PlanNode.source(ORDER_META, rows), top), 1), single::add);
        assertEquals(5_000, single.size());

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder mp = new StringBuilder();
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, mp);
            int masterPort = Integer.parseInt(mp.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=8",
                    "-Drowweave.worker.host=127.0.0.1",
                    "-Drowweave.memory.bytes=524288",
                    "-Drowweave.worker.operator.minMemory=262144",
                    "-Drowweave.worker.operator.maxMemory=262144");
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());

            File outFile = tmp.resolve("real-top.tsv").toFile();
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    clusterSource, top,
                    PlanNode.sink(ORDER_META, 1,
                            new FileSink(ORDER_META, outFile.getAbsolutePath())).build()), 4);
            long rowCount = new ClusterJobClient("127.0.0.1", masterPort).submitToSink(graph);
            List<DataRow> cluster = FileSink.read(ORDER_META, outFile.getAbsolutePath());

            assertEquals(5_000, rowCount);
            assertEquals(orderRows(single), orderRows(cluster),
                    "Top under tiny-memory spill on the three-process cluster at parallelism 4 matches the fully ordered prefix at parallelism 1 row by row");
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    @Test
    void groupedAndJoinSortsSpillInRealThreeProcessCluster(@TempDir Path tmp) throws Exception {
        List<DataRow> leftRows = exactRows(120, false);
        List<DataRow> rightRows = exactRows(120, true);
        List<DataRow> joinLeft = uniqueJoinRows(6_000, 2);
        List<DataRow> joinRight = uniqueJoinRows(6_000, 0);
        PlanGraph reduce = reduceGraph(leftRows, null, 1);
        PlanGraph coGroup = coGroupGraph(leftRows, rightRows, null, 1);
        PlanGraph join = joinGraph(joinLeft, joinRight, null, 1);
        List<String> expectedReduce = executeRows(reduce);
        List<String> expectedCoGroup = executeRows(coGroup);
        List<String> expectedJoin = executeRows(join);
        assertEquals(200, expectedReduce.size());
        assertEquals(200, expectedCoGroup.size());
        assertEquals(joinLeft.size(), expectedJoin.size(), "scale variants of each unique key must join once");

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder mp = new StringBuilder();
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, mp);
            int masterPort = Integer.parseInt(mp.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=8",
                    "-Drowweave.worker.host=127.0.0.1",
                    "-Drowweave.memory.bytes=1048576",
                    "-Drowweave.worker.operator.minMemory=262144",
                    "-Drowweave.worker.operator.maxMemory=262144");
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());

            Path reduceFile = tmp.resolve("real-reduce.tsv");
            Path coGroupFile = tmp.resolve("real-cogroup.tsv");
            Path joinFile = tmp.resolve("real-smj.tsv");
            ClusterJobClient client = new ClusterJobClient("127.0.0.1", masterPort);
            long reduceCount = client.submitToSink(reduceGraph(leftRows, reduceFile, 4));
            long coGroupCount = client.submitToSink(coGroupGraph(leftRows, rightRows, coGroupFile, 4));
            long joinCount = client.submitToSink(joinGraph(joinLeft, joinRight, joinFile, 4));

            assertEquals(expectedReduce.size(), reduceCount);
            assertEquals(expectedCoGroup.size(), coGroupCount);
            assertEquals(expectedJoin.size(), joinCount);
            assertEquals(expectedReduce, readRows(REDUCE_OUT, reduceFile));
            assertEquals(expectedCoGroup, readRows(COGROUP_OUT, coGroupFile));
            assertEquals(expectedJoin, readRows(join.node(2).outMeta(), joinFile));
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    @Test
    void joinStrategiesSurviveIndependentMasterAndWorkerProcesses(@TempDir Path tmp) throws Exception {
        List<DataRow> left = uniqueJoinRows(120, 2);
        List<DataRow> right = uniqueJoinRows(19, 0);
        PlanGraph oracle = joinGraph(left, right, null, 1, "SORT_MERGE");
        List<String> expected = executeRows(oracle);
        assertEquals(19, expected.size());

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        List<String> worker0Log = Collections.synchronizedList(new ArrayList<>());
        List<String> worker1Log = Collections.synchronizedList(new ArrayList<>());
        try {
            StringBuilder masterReady = new StringBuilder();
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, masterReady);
            int masterPort = Integer.parseInt(masterReady.toString());
            String logback = new File(RealProcessClusterTest.class.getProtectionDomain().getCodeSource()
                    .getLocation().toURI()).toPath().resolve("logback-process.xml").toString();
            List<String> workerArgs = Arrays.asList(
                    "-Dlogback.configurationFile=" + logback,
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=16",
                    "-Drowweave.worker.host=127.0.0.1");
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000,
                    new StringBuilder(), worker0Log);
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000,
                    new StringBuilder(), worker1Log);

            ClusterJobClient client = new ClusterJobClient("127.0.0.1", masterPort);
            assertProcessStrategy(client, left, right, "HASH", "HASH", expected, tmp,
                    worker0Log, worker1Log);
            assertProcessStrategy(client, left, right, "SORT_MERGE", "SORT_MERGE", expected, tmp,
                    worker0Log, worker1Log);
            assertProcessStrategy(client, left, right, null, "AUTO", expected, tmp,
                    worker0Log, worker1Log);
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    private static void assertProcessStrategy(
            ClusterJobClient client, List<DataRow> left, List<DataRow> right,
            String hint, String strategy, List<String> expected, Path tmp,
            List<String> worker0Log, List<String> worker1Log) {
        Path output = tmp.resolve("process-join-" + strategy + ".tsv");
        int before0 = worker0Log.size();
        int before1 = worker1Log.size();
        long rows = client.submitToSink(joinGraph(left, right, output, 2, hint));
        assertEquals(expected.size(), rows);
        assertEquals(expected, readRows(exactJoinMeta(), output));

        List<String> jobLog = new ArrayList<>();
        synchronized (worker0Log) {
            jobLog.addAll(worker0Log.subList(before0, worker0Log.size()));
        }
        synchronized (worker1Log) {
            jobLog.addAll(worker1Log.subList(before1, worker1Log.size()));
        }
        long witnesses = jobLog.stream()
                .filter(line -> line.contains("ROWWEAVE_JOIN_RUNTIME")
                        && line.contains("strategy=" + strategy)
                        && line.contains("decision="))
                .count();
        assertTrue(witnesses >= 2,
                "independent P=2 workers missing strategy witnesses for " + strategy + ": " + jobLog);
    }

    @Test
    void equalKeyBlocksSpillByBytesInRealThreeProcessCluster(@TempDir Path tmp) throws Exception {
        List<DataRow> combineRows = wideRows(800, "same", 2_048);
        List<DataRow> joinLeft = wideRows(1, "same", 2_048);
        List<DataRow> joinRight = wideRows(320, "same", 2_048);
        List<DataRow> coGroupLeft = wideRows(260, "same", 2_048);
        List<DataRow> coGroupRight = wideRows(260, "same", 2_048);

        PlanGraph combine = wideCombineGraph(combineRows, null, 1);
        PlanGraph reduce = wideReduceGraph(combineRows, null, 1);
        PlanGraph join = wideJoinGraph(joinLeft, joinRight, null, 1);
        PlanGraph coGroup = wideCoGroupGraph(coGroupLeft, coGroupRight, null, 1);
        List<String> expectedCombine = executeRows(combine);
        List<String> expectedReduce = executeRows(reduce);
        List<String> expectedJoin = executeRows(join);
        List<String> expectedCoGroup = executeRows(coGroup);
        assertEquals(4, expectedCombine.size(), "combine shards");
        assertEquals(Collections.singletonList("800"), expectedReduce);
        assertEquals(320, expectedJoin.size());
        assertEquals(Collections.singletonList("same|260|260"), expectedCoGroup);

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder mp = new StringBuilder();
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, mp);
            int masterPort = Integer.parseInt(mp.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=8",
                    "-Drowweave.worker.host=127.0.0.1",
                    "-Drowweave.memory.bytes=1048576",
                    "-Drowweave.worker.operator.minMemory=262144",
                    "-Drowweave.worker.operator.maxMemory=262144");
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());

            Path combineFile = tmp.resolve("real-byte-combine.tsv");
            Path reduceFile = tmp.resolve("real-byte-reduce.tsv");
            Path joinFile = tmp.resolve("real-byte-smj.tsv");
            Path coGroupFile = tmp.resolve("real-byte-cogroup.tsv");
            ClusterJobClient client = new ClusterJobClient("127.0.0.1", masterPort);
            long combineCount = client.submitToSink(wideCombineGraph(combineRows, combineFile, 4));
            long reduceCount = client.submitToSink(wideReduceGraph(combineRows, reduceFile, 4));
            long joinCount = client.submitToSink(wideJoinGraph(joinLeft, joinRight, joinFile, 4));
            long coGroupCount = client.submitToSink(
                    wideCoGroupGraph(coGroupLeft, coGroupRight, coGroupFile, 4));

            assertEquals(expectedCombine.size(), combineCount);
            assertEquals(expectedReduce.size(), reduceCount);
            assertEquals(expectedJoin.size(), joinCount);
            assertEquals(expectedCoGroup.size(), coGroupCount);
            assertEquals(expectedCombine, readRows(WIDE_META, combineFile));
            assertEquals(expectedReduce, readRows(WIDE_COUNT_OUT, reduceFile));
            assertEquals(expectedJoin, readRows(join.node(2).outMeta(), joinFile));
            assertEquals(expectedCoGroup, readRows(WIDE_COGROUP_OUT, coGroupFile));
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    @Test
    void demandActivatedUnaryBreakersInRealThreeProcessCluster(@TempDir Path tmp) throws Exception {
        List<DataRow> rows = wideKeyedRows(800, 2, 1_024);
        List<String> expected = executeRows(wideUnaryBreakerGraph(rows, null, 1));
        assertEquals(800, expected.size());

        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder mp = new StringBuilder();
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, mp);
            int masterPort = Integer.parseInt(mp.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=8",
                    "-Drowweave.worker.host=127.0.0.1",
                    "-Drowweave.memory.bytes=4194304",
                    "-Drowweave.worker.operator.minMemory=262144",
                    "-Drowweave.worker.operator.maxMemory=4194304");
            worker0 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", 30_000, new StringBuilder());

            Path output = tmp.resolve("real-demand-unary.tsv");
            long rowCount = new ClusterJobClient("127.0.0.1", masterPort)
                    .submitToSink(wideUnaryBreakerGraph(rows, output, 4));
            assertEquals(expected.size(), rowCount);
            assertEquals(expected, readRows(WIDE_GROUPED, output));
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    private static List<DataRow> exactRows(int rounds, boolean reverseScalePattern) {
        List<DataRow> rows = new ArrayList<>(rounds * 100);
        String pad = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        for (int round = 0; round < rounds; round++) {
            int pattern = reverseScalePattern ? (round + 1) % 3 : round % 3;
            int scale = pattern == 1 ? 0 : 2;
            for (int key = 0; key < 100; key++) {
                DataRow row = new DataRow(2);
                row.set(0, scale == 0 ? new BigDecimal(Integer.toString(key))
                        : new BigDecimal(key + ".00"));
                row.set(1, pad + round);
                rows.add(row);
            }
        }
        return rows;
    }

    private static List<DataRow> uniqueJoinRows(int count, int scale) {
        List<DataRow> rows = new ArrayList<>(count);
        String pad = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
        for (int key = 0; key < count; key++) {
            DataRow row = new DataRow(2);
            row.set(0, scale == 0 ? new BigDecimal(Integer.toString(key))
                    : new BigDecimal(key + ".00"));
            row.set(1, pad + key);
            rows.add(row);
        }
        return rows;
    }

    private static List<DataRow> wideRows(int count, String key, int payloadChars) {
        List<DataRow> rows = new ArrayList<>(count);
        char[] chars = new char[payloadChars];
        Arrays.fill(chars, 'w');
        String payload = new String(chars);
        for (int i = 0; i < count; i++) {
            DataRow row = new DataRow(3);
            row.set(0, key);
            row.set(1, BigDecimal.ONE);
            row.set(2, payload + i);
            rows.add(row);
        }
        return rows;
    }

    private static List<DataRow> wideKeyedRows(int keys, int copies, int payloadChars) {
        List<DataRow> rows = new ArrayList<>(keys * copies);
        char[] chars = new char[payloadChars];
        Arrays.fill(chars, 'u');
        String payload = new String(chars);
        for (int key = 0; key < keys; key++) {
            String keyValue = String.format("k%04d-", key) + payload;
            for (int copy = 0; copy < copies; copy++) {
                DataRow row = new DataRow(3);
                row.set(0, keyValue);
                row.set(1, BigDecimal.ONE);
                row.set(2, payload + copy);
                rows.add(row);
            }
        }
        return rows;
    }

    private static PlanGraph wideUnaryBreakerGraph(List<DataRow> rows, Path output, int parallelism) {
        List<String> keys = Collections.singletonList("k");
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.source(WIDE_META, rows));
        nodes.add(PlanNode.unary("groupBySum", WIDE_GROUPED, 0)
                .listParam("keys", keys).param("measure", "amt").build());
        nodes.add(PlanNode.unary("distinct", WIDE_GROUPED, 1).listParam("keys", keys).build());
        nodes.add(PlanNode.unary("orderBy", WIDE_GROUPED, 2)
                .listParam("sort", Collections.singletonList(PlanOps.encodeSortField(new SortField(
                        0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)))).build());
        if (output != null) {
            nodes.add(PlanNode.sink(WIDE_GROUPED, 3,
                    new FileSink(WIDE_GROUPED, output.toString())).build());
        }
        return new PlanGraph(nodes, parallelism);
    }

    private static PlanGraph wideCombineGraph(List<DataRow> rows, Path output, int parallelism) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("shards", "4");
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.source(WIDE_META, rows));
        nodes.add(PlanNode.unaryFnWithParams("combineGroup", WIDE_META, 0,
                params, Collections.emptyMap(), WIDE_COMBINE_SUM));
        if (output != null) {
            nodes.add(PlanNode.sink(WIDE_META, 1, new FileSink(WIDE_META, output.toString())).build());
        }
        return new PlanGraph(nodes, parallelism);
    }

    private static PlanGraph wideReduceGraph(List<DataRow> rows, Path output, int parallelism) {
        Map<String, List<String>> lists = new LinkedHashMap<>();
        lists.put("keys", Collections.emptyList());
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.source(WIDE_META, rows));
        nodes.add(PlanNode.unaryFnWithParams("reduceGroup", WIDE_COUNT_OUT, 0,
                Collections.emptyMap(), lists, WIDE_REDUCE_COUNT));
        if (output != null) {
            nodes.add(PlanNode.sink(WIDE_COUNT_OUT, 1,
                    new FileSink(WIDE_COUNT_OUT, output.toString())).build());
        }
        return new PlanGraph(nodes, parallelism);
    }

    private static PlanGraph wideJoinGraph(List<DataRow> left, List<DataRow> right, Path output, int parallelism) {
        DataSchema outMeta = SortMergeJoin.of(WIDE_META, WIDE_META,
                new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();
        PlanNode join = PlanNode.binary("join", outMeta, 0, 1)
                .param("joinType", JoinType.INNER.name())
                .listParam("leftKeys", Collections.singletonList("k"))
                .listParam("rightKeys", Collections.singletonList("k"))
                .build();
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.source(WIDE_META, left));
        nodes.add(PlanNode.source(WIDE_META, right));
        nodes.add(join);
        if (output != null) {
            nodes.add(PlanNode.sink(outMeta, 2, new FileSink(outMeta, output.toString())).build());
        }
        return new PlanGraph(nodes, parallelism);
    }

    private static PlanGraph wideCoGroupGraph(
            List<DataRow> left, List<DataRow> right, Path output, int parallelism) {
        Map<String, List<String>> lists = new LinkedHashMap<>();
        lists.put("leftKeys", Collections.singletonList("k"));
        lists.put("rightKeys", Collections.singletonList("k"));
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.source(WIDE_META, left));
        nodes.add(PlanNode.source(WIDE_META, right));
        nodes.add(PlanNode.binaryFn("coGroup", WIDE_COGROUP_OUT, 0, 1,
                Collections.emptyMap(), lists, WIDE_COGROUP_COUNT));
        if (output != null) {
            nodes.add(PlanNode.sink(WIDE_COGROUP_OUT, 2,
                    new FileSink(WIDE_COGROUP_OUT, output.toString())).build());
        }
        return new PlanGraph(nodes, parallelism);
    }

    private static PlanGraph reduceGraph(List<DataRow> rows, Path output, int parallelism) {
        Map<String, List<String>> lists = new LinkedHashMap<>();
        lists.put("keys", Collections.singletonList("k"));
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.source(EXACT_META, rows));
        nodes.add(PlanNode.unaryFnWithParams("reduceGroup", REDUCE_OUT, 0,
                Collections.emptyMap(), lists, EXACT_COUNT));
        if (output != null) {
            nodes.add(PlanNode.sink(REDUCE_OUT, 1, new FileSink(REDUCE_OUT, output.toString())).build());
        }
        return new PlanGraph(nodes, parallelism);
    }

    private static PlanGraph coGroupGraph(List<DataRow> left, List<DataRow> right, Path output, int parallelism) {
        Map<String, List<String>> lists = new LinkedHashMap<>();
        lists.put("leftKeys", Collections.singletonList("k"));
        lists.put("rightKeys", Collections.singletonList("k"));
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.source(EXACT_META, left));
        nodes.add(PlanNode.source(EXACT_META, right));
        nodes.add(PlanNode.binaryFn("coGroup", COGROUP_OUT, 0, 1,
                Collections.emptyMap(), lists, EXACT_COGROUP_COUNT));
        if (output != null) {
            nodes.add(PlanNode.sink(COGROUP_OUT, 2,
                    new FileSink(COGROUP_OUT, output.toString())).build());
        }
        return new PlanGraph(nodes, parallelism);
    }

    private static PlanGraph joinGraph(List<DataRow> left, List<DataRow> right, Path output, int parallelism) {
        return joinGraph(left, right, output, parallelism, "SORT_MERGE");
    }

    private static PlanGraph joinGraph(List<DataRow> left, List<DataRow> right, Path output,
                                       int parallelism, String hint) {
        DataSchema outMeta = exactJoinMeta();
        PlanNode.Builder join = PlanNode.binary("join", outMeta, 0, 1)
                .param("joinType", JoinType.INNER.name())
                .listParam("leftKeys", Collections.singletonList("k"))
                .listParam("rightKeys", Collections.singletonList("k"));
        if (hint != null) join.param("joinHint", hint);
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.source(EXACT_META, left));
        nodes.add(PlanNode.source(EXACT_META, right));
        nodes.add(join.build());
        if (output != null) {
            nodes.add(PlanNode.sink(outMeta, 2, new FileSink(outMeta, output.toString())).build());
        }
        return new PlanGraph(nodes, parallelism);
    }

    private static DataSchema exactJoinMeta() {
        return SortMergeJoin.of(EXACT_META, EXACT_META,
                new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();
    }

    private static List<String> executeRows(PlanGraph graph) {
        List<DataRow> rows = new ArrayList<>();
        new PushDagExecutor(graph.parallelism()).execute(graph, rows::add);
        return renderRows(rows);
    }

    private static List<String> readRows(DataSchema meta, Path path) {
        return renderRows(FileSink.read(meta, path.toString()));
    }

    private static List<String> renderRows(List<DataRow> rows) {
        List<String> rendered = new ArrayList<>(rows.size());
        for (DataRow row : rows) {
            StringBuilder value = new StringBuilder();
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) value.append('|');
                Object cell = row.get(i);
                value.append(cell instanceof BigDecimal
                        ? ((BigDecimal) cell).toPlainString() : String.valueOf(cell));
            }
            rendered.add(value.toString());
        }
        Collections.sort(rendered);
        return rendered;
    }

    private static List<String> orderRows(List<DataRow> rows) {
        List<String> out = new ArrayList<>(rows.size());
        for (DataRow row : rows) {
            BigDecimal amount = (BigDecimal) row.get(1);
            Timestamp timestamp = (Timestamp) row.get(2);
            out.add(String.valueOf(row.get(0)) + "|"
                    + (amount == null ? "null" : amount.toPlainString()) + "|"
                    + timestamp.getTime() + ':' + timestamp.getNanos() + '|' + row.get(3));
        }
        return out;
    }

    @Test
    void groupBySumPerfSingleVsCluster(@TempDir Path tmp) throws Exception {
        List<DataRow> rows = rows();
        PlanNode clusterSource = fileSource(tmp.resolve("perf-gb-source.tsv"), META, rows);
        PlanNode gbNode = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();

        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(PlanNode.source(META, rows), gbNode)), r -> { });
        long t = System.nanoTime();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(PlanNode.source(META, rows), gbNode)), r -> { });
        long single1 = (System.nanoTime() - t) / 1_000_000;

        new PushDagExecutor(4).execute(new PlanGraph(Arrays.asList(PlanNode.source(META, rows), gbNode)), r -> { });
        t = System.nanoTime();
        new PushDagExecutor(4).execute(new PlanGraph(Arrays.asList(PlanNode.source(META, rows), gbNode)), r -> { });
        long single4 = (System.nanoTime() - t) / 1_000_000;

        Process master = null, worker0 = null, worker1 = null;
        try {
            StringBuilder mp = new StringBuilder();
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000, mp);
            int masterPort = Integer.parseInt(mp.toString());
            worker0 = launch(Arrays.asList("-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort, "-Drowweave.worker.slots=32",
                    "-Drowweave.worker.host=127.0.0.1"), "WORKER_READY=", 30_000, new StringBuilder());
            worker1 = launch(Arrays.asList("-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort, "-Drowweave.worker.slots=32",
                    "-Drowweave.worker.host=127.0.0.1"), "WORKER_READY=", 30_000, new StringBuilder());

            File outFile = tmp.resolve("perf-gb.tsv").toFile();
            FileSink output = new FileSink(GB, outFile.getAbsolutePath());
            PlanGraph graph = new PlanGraph(Arrays.asList(
                    clusterSource, gbNode, PlanNode.sink(GB, 1, output).build()));

            new ClusterJobClient("127.0.0.1", masterPort).submitToSink(graph);
            File outFile2 = tmp.resolve("perf-gb2.tsv").toFile();
            PlanGraph graph2 = new PlanGraph(Arrays.asList(
                    clusterSource, gbNode, PlanNode.sink(GB, 1, new FileSink(GB, outFile2.getAbsolutePath())).build()));
            t = System.nanoTime();
            long rc = new ClusterJobClient("127.0.0.1", masterPort).submitToSink(graph2);
            long clusterMs = (System.nanoTime() - t) / 1_000_000;
            assertEquals(KEYS, rc, "the cluster produces every group");

            System.out.println("\n========== groupBySum performance (N=" + N + ", keys=" + KEYS + ") ==========");
            System.out.println("single process, P=1 serial   : " + single1 + " ms");
            System.out.println("single process, P=4 keyed    : " + single4 + " ms");
            System.out.println("three-process cluster        : " + clusterMs + " ms (includes RPC, forked processes and socket shuffle)");
            System.out.printf("cluster overhead vs P=4      : %.1fx%n", (double) clusterMs / Math.max(1, single4));
            System.out.println("================================================================\n");
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }
}

