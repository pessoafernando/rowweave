package io.rowweave.examples;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Qualifies inventory examples that have no dedicated indexed test. */
class InventoryExampleQualificationTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void amountRollupPreservesDecimalScaleAndOrder() throws Exception {
        Path output = temporaryDirectory.resolve("amount-rollup.tsv");
        AmountRollupExample.run(Arrays.asList(
                AmountRollupExample.detail("C1", "alpha", "100.00"),
                AmountRollupExample.detail("C1", "alpha", "200.00"),
                AmountRollupExample.detail("C1", "beta", "50.50"),
                AmountRollupExample.detail("C2", "alpha", "300.30")),
                output.toString());
        assertEquals(Arrays.asList(
                        "C1\talpha\t300.0000000000",
                        "C1\tbeta\t50.5000000000",
                        "C2\talpha\t300.3000000000"),
                Files.readAllLines(output));
    }

    @Test
    void groupReduceEmitsCountAndFullPrecisionTotal() throws Exception {
        Path output = temporaryDirectory.resolve("group-stat.tsv");
        GroupStatExample.run(Arrays.asList(
                GroupStatExample.detail("C1", "100.00"),
                GroupStatExample.detail("C1", "200.00"),
                GroupStatExample.detail("C2", "50.50"),
                GroupStatExample.detail("C2", null)),
                output.toString());
        List<String> actual = new ArrayList<>(Files.readAllLines(output));
        actual.sort(null);
        assertEquals(Arrays.asList(
                        "C1\t2\t300.0000000000",
                        "C2\t2\t50.5000000000"),
                actual);
    }

    @Test
    void leftJoinKeepsUnmatchedEventsAndNullKeysDoNotMatch() throws Exception {
        Path output = temporaryDirectory.resolve("join-dim-enrich.tsv");
        JoinDimEnrichExample.run(Arrays.asList(
                JoinDimEnrichExample.event("C1", "100.00"),
                JoinDimEnrichExample.event("C2", "200.00"),
                JoinDimEnrichExample.event("C9", "50.00"),
                JoinDimEnrichExample.event(null, "70.00")),
                Arrays.asList(
                        JoinDimEnrichExample.dim("C1", "North"),
                        JoinDimEnrichExample.dim("C2", "Harbor"),
                        JoinDimEnrichExample.dim(null, "Must Not Match")),
                output.toString());
        List<String> actual = new ArrayList<>(Files.readAllLines(output));
        actual.sort(null);
        assertEquals(Arrays.asList(
                        "C1\t100.00\tNorth",
                        "C2\t200.00\tHarbor",
                        "C9\t50.00\t\\N",
                        "\\N\t70.00\t\\N"),
                actual);
    }

    @Test
    void flatMapEmitsWordsWithoutAnIntermediateRowList() {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            WordCountExample.run(Arrays.asList("alpha beta", "beta alpha gamma"));
        } finally {
            System.setOut(original);
        }
        String output = captured.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("alpha, 2"), output);
        assertTrue(output.contains("beta, 2"), output);
        assertTrue(output.contains("gamma, 1"), output);
    }

    @Test
    void atomicStreamCompositionCompilesAllPublishedNodeKinds() throws Exception {
        assertEquals(Arrays.asList("calc", "flat-map", "union", "keyed-process",
                        "tagged-output", "async-wait"),
                StreamAtomicCompositionExample.run());
    }
}
