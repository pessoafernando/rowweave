package io.rowweave.examples.scenariolab;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JUnit wrapper for ScenarioLab: one dynamic test per batch, each holding 100 scenarios whose
 * individual outcomes the runner reports. All 100 batches come to 10000 scenarios. To work on a
 * single batch, run the ScenarioLabRunner main class with that batch number as an argument.
 */
class ScenarioLabBatchTest {

    @TestFactory
    List<DynamicTest> allBatches() {
        List<BatchDesign> batches = ScenarioOutcome.Registry.allBatches();
        List<DynamicTest> tests = new ArrayList<>();
        for (BatchDesign batch : batches) {
            tests.add(DynamicTest.dynamicTest(
                    String.format("B%03d %s", batch.batchNo(), batch.title()),
                    () -> runBatch(batch)));
        }
        return tests;
    }

    private static void runBatch(BatchDesign batch) throws Exception {
        Path workRoot = Paths.get("target", "scenariolab-junit",
                String.format("B%03d", batch.batchNo()));
        List<BatchDesign> one = new ArrayList<>();
        one.add(batch);
        ScenarioLabRunner.RunSummary summary = ScenarioLabRunner.run(one, workRoot);
        assertTrue(summary.errored == 0 && summary.failed == 0,
                () -> summary.summaryLine() + " — see target/scenariolab-junit/"
                        + String.format("B%03d", batch.batchNo()) + " for the failing scenarios"
                        + System.lineSeparator() + failureDigest(summary));
    }

    private static String failureDigest(ScenarioLabRunner.RunSummary summary) {
        StringBuilder sb = new StringBuilder();
        int n = 0;
        for (ScenarioOutcome o : summary.outcomes) {
            if (!o.passed() && n < 10) {
                sb.append(o.scenarioId).append(": ").append(o.message.split("\n")[0]).append("\n");
                n++;
            }
        }
        return sb.toString();
    }
}
