package io.rowweave.run;

/**
 * Optional backend capability that reads a Run snapshot and its durable execution evidence
 * from one authority-defined coherent view.
 */
public interface DurableExecutionEvidenceAuthority {
    DurableExecutionEvidenceView executionEvidence(String runId);
}
