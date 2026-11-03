package external;

import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import io.rowweave.Collector;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.MapPartitionEmissionLimit;
import io.rowweave.MapPartitionFunction;
import io.rowweave.PartitionDataRowCursor;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

/** External source compiled only against the packaged public core JAR. */
public final class ExternalMapPartitionConsumer {
    public static void main(String[] args) throws Exception {
        Path expectedCore = Paths.get(args[0]).toRealPath();
        Path actualCore = Paths.get(DataSet.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI()).toRealPath();
        if (!expectedCore.equals(actualCore)) {
            throw new IllegalStateException("core not loaded from packaged JAR");
        }
        // The published core JAR and this consumer must both carry Java 8
        // bytecode, otherwise a host toolchain could silently stand in for it.
        verifyJava8Jar(expectedCore);
        if (args.length > 1) {
            verifyJava8Class(Paths.get(args[1]), "external MapPartition consumer");
        }
        final DataSchema meta = new DataSchema(
                new Field("id", DataType.LongType));
        MapPartitionFunction function = new MapPartitionFunction() {
            private static final long serialVersionUID = 1L;
            @Override public DataSchema getResultSchema() { return meta; }
            @Override public void mapPartition(
                    PartitionDataRowCursor rows, Collector collector) {
                DataRow row;
                while ((row = rows.next()) != null) collector.collect(row);
            }
        };
        MapPartitionEmissionLimit limit =
                new MapPartitionEmissionLimit(16L, 4096L);
        DataSet mapped = DataSet.source(
                meta, Collections.<DataRow>emptyList())
                .mapPartition(function, limit);
        if (mapped.getSchema().getFieldCount() != 1
                || limit.maxOutputRowsPerPartition() != 16L) {
            throw new IllegalStateException("public map-partition contract drift");
        }
        io.rowweave.monitor.MetricSet.builder("java8")
                .tag("jobId", "java8-monitor")
                .gauge("java8.rows", 0L, "rows").build();
        io.rowweave.stream.api.StreamJobId monitorJob =
                io.rowweave.stream.api.StreamJobId.of("java8-monitor");
        new io.rowweave.monitor.StreamMonitorBridge(
                io.rowweave.monitor.JobMonitorRegistry.NOOP).submit(
                monitorJob, "Java 8 monitor", io.rowweave.stream.api.StreamJobStatus.SUBMITTED,
                io.rowweave.stream.api.StreamJobEvidence.unavailable(monitorJob,
                        new io.rowweave.stream.api.StreamJobProgress(0, 0L, 0, 0L, 0L),
                        System.currentTimeMillis()));
        System.out.println("MAP_PARTITION_JAVA8_OK");
    }

    private static void verifyJava8Jar(Path jar) throws Exception {
        int classes = 0;
        try (JarFile file = new JarFile(jar.toFile())) {
            Enumeration<JarEntry> entries = file.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (!entry.getName().endsWith(".class")) continue;
                classes++;
                try (InputStream input = file.getInputStream(entry)) {
                    verifyMajor(input, entry.getName());
                }
            }
        }
        if (classes == 0) throw new AssertionError("empty JAR: " + jar);
    }

    private static void verifyJava8Class(Path classFile, String label)
            throws Exception {
        try (InputStream input = Files.newInputStream(classFile)) {
            verifyMajor(input, label + " " + classFile.getFileName());
        }
    }

    private static void verifyMajor(InputStream input, String label)
            throws Exception {
        DataInputStream stream = new DataInputStream(input);
        int magic = stream.readInt();
        int minor = stream.readUnsignedShort();
        int major = stream.readUnsignedShort();
        if (magic != 0xCAFEBABE || minor != 0 || major != 52) {
            throw new AssertionError("non-Java-8 class: " + label
                    + " magic=" + Integer.toHexString(magic)
                    + " minor=" + minor + " major=" + major);
        }
    }
}
