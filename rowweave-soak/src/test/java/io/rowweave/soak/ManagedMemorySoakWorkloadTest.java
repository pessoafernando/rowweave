package io.rowweave.soak;

import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManagedMemorySoakWorkloadTest {
    @Test
    void streamsHighCardinalityDecimalOracleWithNullGroup()
            throws Exception {
        Path root = Files.createTempDirectory("managed-memory-workload-");
        Path source = root.resolve("source.tsv");
        Path output = root.resolve("output.tsv");
        int rows = 10_000;
        int keys = 1_000;
        int seed = 17;

        String sourceSha = ManagedMemorySoakWorkload.writeSource(
                source, rows, keys, seed);
        ExecutionReport report = new PushDagExecutor(1)
                .runSinkGraphReported(ManagedMemorySoakWorkload.graph(
                        source, output, 1));
        ManagedMemorySoakWorkload.OracleResult oracle =
                ManagedMemorySoakWorkload.verify(
                        output, rows, keys, seed);

        assertTrue(sourceSha.matches("[0-9a-f]{64}"));
        assertEquals(keys + 1L, report.rowsOut());
        assertEquals(keys + 1L, oracle.rows);
        assertTrue(oracle.sha256.matches("[0-9a-f]{64}"));
        assertEquals("322bcf45ae863566cad32ee0876b778fbb93a0383dfffed76127f4de3aaf49f6",
                oracle.oracleSha256);
        assertTrue(report.acquiredBytes() > 0L);
        assertTrue(report.peakReservedBytes() > 0L);
        assertTrue(report.withinBudget());
    }

    @Test
    void oracleRejectsMissingOrAlteredRows() throws Exception {
        Path root = Files.createTempDirectory("managed-memory-oracle-bad-");
        Path source = root.resolve("source.tsv");
        Path output = root.resolve("output.tsv");
        ManagedMemorySoakWorkload.writeSource(source, 1_000, 100, 3);
        new PushDagExecutor(1).runSinkGraphReported(
                ManagedMemorySoakWorkload.graph(source, output, 1));
        Files.write(output, new byte[] {'x'},
                java.nio.file.StandardOpenOption.APPEND);

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> ManagedMemorySoakWorkload.verify(
                        output, 1_000, 100, 3));
        assertTrue(failure.getMessage().startsWith(
                "MANAGED_MEMORY_ORACLE_MISMATCH:"));
    }
}
