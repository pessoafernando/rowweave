package io.rowweave.examples.scenariolab;

import java.util.ArrayList;
import java.util.List;

/** The result of one scenario run. */
public final class ScenarioOutcome {

    public enum Status { PASS, FAIL, ERROR }

    public final String batchId;
    public final String scenarioId;
    public final String title;
    public final Status status;
    public final String message;
    public final long millis;

    public ScenarioOutcome(String batchId, String scenarioId, String title,
                           Status status, String message, long millis) {
        this.batchId = batchId;
        this.scenarioId = scenarioId;
        this.title = title;
        this.status = status;
        this.message = message;
        this.millis = millis;
    }

    public boolean passed() {
        return status == Status.PASS;
    }

    /** Static registry of every batch. */
    public static final class Registry {

        private Registry() {
        }

        public static List<BatchDesign> allBatches() {
            List<BatchDesign> all = new ArrayList<>();
            all.addAll(io.rowweave.examples.scenariolab.batches.FamilyA.batches());
            all.addAll(io.rowweave.examples.scenariolab.batches.FamilyB.batches());
            all.addAll(io.rowweave.examples.scenariolab.batches.FamilyC.batches());
            all.addAll(io.rowweave.examples.scenariolab.batches.FamilyD.batches());
            all.addAll(io.rowweave.examples.scenariolab.batches.FamilyE.batches());
            all.addAll(io.rowweave.examples.scenariolab.batches.FamilyF.batches());
            all.addAll(io.rowweave.examples.scenariolab.batches.FamilyG.batches());
            all.addAll(io.rowweave.examples.scenariolab.batches.FamilyH.batches());
            all.addAll(io.rowweave.examples.scenariolab.batches.FamilyI.batches());
            all.addAll(io.rowweave.examples.scenariolab.batches.FamilyJ.batches());
            return all;
        }
    }
}
