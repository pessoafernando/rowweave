package io.rowweave.examples;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.PortableBatchPlan;
import io.rowweave.portable.PortableBindingVerifier;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableSinkBinding;
import io.rowweave.portable.PortableSourceBinding;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineCompositionExampleTest {
    @Test
    void exampleBatchCarrierPassesThePortablePreflight() {
        PortableBatchPlan plan = PipelineCompositionExample.batchPlan("gate");
        PortablePlanCompiler.preflight(plan, new PortableBindingVerifier() {
            @Override public void verifySource(PortableSourceBinding binding) { }
            @Override public void verifyFunction(FunctionDeploymentReference reference) { }
            @Override public void verifySink(PortableSinkBinding binding) { }
        });
        PlanArtifact carrier = PipelineCompositionExample.batchArtifact("gate");
        assertEquals(PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1,
                carrier.payloadFormat());
    }

    @Test
    void coversPublishedCompositionScenarios() {
        assertEquals("batch=1,stream=1,mixed=[batch, stream],"
                        + "shared=[shared, a-batch, b-stream],outputs=2,"
                        + "revisionChanged=true,handoffBound=true",
                PipelineCompositionExample.run());
    }
}
