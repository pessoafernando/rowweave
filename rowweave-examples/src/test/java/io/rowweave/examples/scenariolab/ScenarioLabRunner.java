package io.rowweave.examples.scenariolab;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Full runner for ScenarioLab: all 100 batches of 100 scenarios, 10000 scenarios in total,
 * with a report written out.
 *
 * <p>Usage: {@code java io.rowweave.examples.scenariolab.ScenarioLabRunner [output directory]
 * [batch numbers...]}. With no batch numbers every batch runs; with them only the named batches
 * run, which is how a single batch is debugged.
 */
public final class ScenarioLabRunner {

    private ScenarioLabRunner() {
    }

    public static void main(String[] args) throws Exception {
        Path outDir = Paths.get(args.length > 0 ? args[0] : "target/scenariolab-report");
        List<Integer> only = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            only.add(Integer.parseInt(args[i]));
        }
        List<BatchDesign> batches = ScenarioOutcome.Registry.allBatches();
        if (!only.isEmpty()) {
            List<BatchDesign> picked = new ArrayList<>();
            for (BatchDesign b : batches) {
                if (only.contains(b.batchNo())) {
                    picked.add(b);
                }
            }
            batches = picked;
        }
        Files.createDirectories(outDir);
        RunSummary summary = run(batches, outDir);
        writeReport(summary, outDir.resolve("report.md"));
        System.out.println(summary.summaryLine());
        System.out.println("report: " + outDir.resolve("report.md").toAbsolutePath());
        if (summary.failed + summary.errored > 0) {
            System.exit(1);
        }
    }

    /** Run a group of batches and produce the roll-up. */
    public static RunSummary run(List<BatchDesign> batches, Path workRoot) throws IOException {
        if (Files.exists(workRoot)) {
            try (java.util.stream.Stream<Path> walk = Files.walk(workRoot)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
        Files.createDirectories(workRoot);
        RunSummary summary = new RunSummary();
        long t0 = System.currentTimeMillis();
        for (BatchDesign batch : batches) {
            String batchId = String.format("B%03d", batch.batchNo());
            Path batchDir = workRoot.resolve(batchId);
            Files.createDirectories(batchDir);
            List<ScenarioDef> scenarios;
            try {
                scenarios = batch.scenarios();
            } catch (Throwable t) {
                summary.outcomes.add(new ScenarioOutcome(batchId, batchId + "-BATCH",
                        "failed to build scenarios: " + t,
                        ScenarioOutcome.Status.ERROR, String.valueOf(t), 0));
                continue;
            }
            if (scenarios.size() != 100) {
                summary.outcomes.add(new ScenarioOutcome(batchId, batchId + "-BATCH",
                        "batch must contain exactly 100 scenarios but has " + scenarios.size(),
                        ScenarioOutcome.Status.ERROR, "design error", 0));
                continue;
            }
            for (ScenarioDef sc : scenarios) {
                summary.outcomes.add(runOne(batchId, batchDir, sc));
            }
        }
        summary.totalMillis = System.currentTimeMillis() - t0;
        for (ScenarioOutcome o : summary.outcomes) {
            if (o.status == ScenarioOutcome.Status.PASS) {
                summary.passed++;
            } else if (o.status == ScenarioOutcome.Status.FAIL) {
                summary.failed++;
            } else {
                summary.errored++;
            }
        }
        return summary;
    }

    private static ScenarioOutcome runOne(String batchId, Path batchDir, ScenarioDef sc) {
        long t0 = System.currentTimeMillis();
        String filePrefix = batchDir.resolve(sc.id).toString();
        try {
            if (sc.kind == ScenarioDef.Kind.EXPECT_THROWS) {
                try {
                    sc.body().run(null);
                    return new ScenarioOutcome(batchId, sc.id, sc.title, ScenarioOutcome.Status.FAIL,
                            "expected "
                                    + sc.expectedException().getSimpleName() + " but nothing was thrown",
                            System.currentTimeMillis() - t0);
                } catch (Exception ex) {
                    try {
                        ScenarioLab.assertExpectedFailure(
                                ex, sc.expectedException(), sc.expectedMessageFragment());
                        return new ScenarioOutcome(batchId, sc.id, sc.title,
                                ScenarioOutcome.Status.PASS, "threw expected "
                                + ex.getClass().getSimpleName() + ": " + firstLine(ex),
                                System.currentTimeMillis() - t0);
                    } catch (AssertionError mismatch) {
                        return new ScenarioOutcome(batchId, sc.id, sc.title, ScenarioOutcome.Status.FAIL,
                                "expected " + sc.expectedException().getSimpleName()
                                        + " containing '" + sc.expectedMessageFragment() + "' but got "
                                        + ex.getClass().getSimpleName() + ": " + firstLine(ex),
                                System.currentTimeMillis() - t0);
                    }
                }
            }
            ScenarioLab lab = new ScenarioLab(filePrefix);
            sc.body().run(lab);
            lab.commitAndVerify();
            return new ScenarioOutcome(batchId, sc.id, sc.title, ScenarioOutcome.Status.PASS, "ok",
                    System.currentTimeMillis() - t0);
        } catch (AssertionError ae) {
            return new ScenarioOutcome(batchId, sc.id, sc.title, ScenarioOutcome.Status.FAIL,
                    firstLine(ae) + " | " + trim(ae.getMessage(), 1500),
                    System.currentTimeMillis() - t0);
        } catch (Throwable t) {
            return new ScenarioOutcome(batchId, sc.id, sc.title, ScenarioOutcome.Status.ERROR,
                    t.getClass().getName() + ": " + trim(String.valueOf(t.getMessage()), 1500),
                    System.currentTimeMillis() - t0);
        }
    }

    private static String firstLine(Throwable t) {
        String m = String.valueOf(t.getMessage());
        int nl = m.indexOf('\n');
        return nl > 0 ? m.substring(0, nl) : m;
    }

    private static String trim(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "...(truncated)";
    }

    private static void writeReport(RunSummary summary, Path report) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("# ScenarioLab run report\n\n");
        sb.append(summary.summaryLine()).append("\n\n");
        sb.append("- java: `").append(System.getProperty("java.version")).append("`\n");
        sb.append("- engine: `").append(engineCodeSource()).append("`\n\n");
        String currentBatch = null;
        for (ScenarioOutcome o : summary.outcomes) {
            if (!o.batchId.equals(currentBatch)) {
                currentBatch = o.batchId;
                sb.append("\n## ").append(currentBatch).append("\n\n");
            }
            sb.append("- ").append(o.status == ScenarioOutcome.Status.PASS ? "PASS"
                    : o.status == ScenarioOutcome.Status.FAIL ? "**FAIL**" : "**ERROR**")
                    .append(" ").append(o.scenarioId).append(" — ").append(o.title);
            if (!o.passed()) {
                sb.append("\n  - ").append(o.message.replace("\n", "\n    "));
            }
            sb.append("\n");
        }
        Files.write(report, sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String engineCodeSource() {
        try {
            return io.rowweave.ExecutionEnvironment.class.getProtectionDomain()
                    .getCodeSource().getLocation().toExternalForm();
        } catch (RuntimeException unavailable) {
            return "unavailable:" + unavailable.getClass().getSimpleName();
        }
    }

    /** Roll-up of a run. */
    public static final class RunSummary {
        public final List<ScenarioOutcome> outcomes = new ArrayList<>();
        public int passed;
        public int failed;
        public int errored;
        public long totalMillis;

        public String summaryLine() {
            return "total=" + outcomes.size() + " pass=" + passed + " fail=" + failed
                    + " error=" + errored + " in " + (totalMillis / 1000) + "s";
        }
    }
}
