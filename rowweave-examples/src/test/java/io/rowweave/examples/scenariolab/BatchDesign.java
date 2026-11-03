package io.rowweave.examples.scenariolab;

import java.util.List;

/** Batch design interface: one batch is one independently designed scenario family of exactly 100 scenarios. */
public interface BatchDesign {

    /** Batch number, 1..100. */
    int batchNo();

    /** Batch title, a note on the independent design. */
    String title();

    /** The 100 scenarios of this batch. */
    List<ScenarioDef> scenarios();
}
