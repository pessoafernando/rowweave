package io.rowweave.soak;

import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.TelemetryCoverage;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("multiprocess")
class ManagedMemorySoakClusterTest {
    @Test
    void independentMasterAndWorkersExecuteTheRealBatchBreakerPath()
            throws Exception {
        Path root = Files.createTempDirectory("managed-memory-cluster-");
        ManagedMemorySoakConfig config = ManagedMemorySoakConfig.parse(
                new String[] {"--duration-seconds=5",
                        "--rows-per-job=100000",
                        "--key-cardinality=25000",
                        "--output=" + root});
        Path source = root.resolve("source.tsv");
        Path output = root.resolve("output.tsv");
        ManagedMemorySoakWorkload.writeSource(
                source, config.rowsPerJob, config.keyCardinality, 11);

        try (ManagedMemorySoakCluster cluster =
                     ManagedMemorySoakCluster.start(config)) {
            assertEquals(3, cluster.roles().size());
            assertTrue(cluster.roles().stream()
                    .allMatch(role -> role.process.isAlive()));
            for (ManagedMemorySoakCluster.RoleProcess role : cluster.roles()) {
                long pid = StreamSoakResourceSampler.process(role.process).pid;
                Process commandLine = new ProcessBuilder(
                        System.getProperty("java.home") + java.io.File.separator
                                + "bin" + java.io.File.separator + "jcmd",
                        Long.toString(pid), "VM.command_line")
                        .redirectErrorStream(true).start();
                byte[] commandOutput = readAll(commandLine.getInputStream());
                assertEquals(0, commandLine.waitFor());
                String command = new String(commandOutput,
                        StandardCharsets.UTF_8);
                assertTrue(command.contains("-Xmx704m"),
                        role.role + " must leave process RSS headroom");
                assertTrue(command.contains(
                                "-Drowweave.control.residentBytes=234881024"),
                        role.role + " must use the frozen control budget");
                assertTrue(command.contains(
                                "-Drowweave.deployment.memoryBytes=16777216"),
                        role.role + " must use the frozen deployment budget");
                assertTrue(command.contains(
                                "-Drowweave.shuffle.ingressResidentBytes=100663296"),
                        role.role + " must use the frozen ingress budget");
            }

            ExecutionReport report = cluster.submit(
                    source, output, "cluster-path-11");

            assertEquals(config.keyCardinality + 1L, report.rowsOut());
            assertEquals(TelemetryCoverage.COMPLETE,
                    report.resourceTelemetryCoverage());
            assertTrue(report.acquiredBytes() > 0L);
            assertTrue(report.peakReservedBytes() > 0L);
            assertTrue(report.withinBudget());
            assertTrue(report.pressureSpillCount() > 0L
                            || report.spillRunCount() > 0L,
                    report.summary());
            assertEquals(config.keyCardinality + 1L,
                    ManagedMemorySoakWorkload.verify(output,
                            config.rowsPerJob,
                            config.keyCardinality, 11).rows);
        }
    }

    private static byte[] readAll(java.io.InputStream input) throws Exception {
        try (java.io.InputStream source = input;
             java.io.ByteArrayOutputStream output =
                     new java.io.ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            for (int read; (read = source.read(buffer)) >= 0; ) {
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        }
    }
}
