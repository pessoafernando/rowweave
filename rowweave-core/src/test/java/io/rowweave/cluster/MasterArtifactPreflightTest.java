package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.OperatorDescriptor;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MasterArtifactPreflightTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));

    @Test
    void masterRejectsCorruptUnknownFeatureAndDescriptorMismatchBeforeWorkerDiscovery() {
        PlanArtifact valid = validArtifact();
        byte[] corrupt = PlanArtifactCodec.encode(valid);
        corrupt[corrupt.length - 1] ^= 1;

        List<String> unknownFeatures = new ArrayList<>(valid.requiredFeatures());
        unknownFeatures.add("vendor.future.required.v9");
        PlanArtifact unknown = copy(valid, unknownFeatures, valid.operators());

        List<OperatorDescriptor> mismatchedDescriptors = new ArrayList<>(valid.operators());
        OperatorDescriptor sink = mismatchedDescriptors.get(1);
        mismatchedDescriptors.set(1, new OperatorDescriptor(
                sink.operatorId(), sink.kind(), "wrong-sink-type",
                sink.schemaFingerprint(), sink.inputOperatorIds()));
        PlanArtifact mismatch = copy(valid, valid.requiredFeatures(), mismatchedDescriptors);

        try (MasterServer master = new MasterServer(5_000L)) {
            assertPreflightFailure(master, corrupt, "checksum");
            assertPreflightFailure(master, PlanArtifactCodec.encode(unknown), "unsupported required feature");
            assertPreflightFailure(master, PlanArtifactCodec.encode(mismatch), "descriptor differs");
            PlanArtifact wrongDeployment = PlanArtifactDeploymentBinding.bind(
                    valid, repeat('a', 64));
            assertPreflightFailure(master, PlanArtifactCodec.encode(wrongDeployment),
                    "deployment set");
        }
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    private static void assertPreflightFailure(MasterServer master, byte[] bytes, String expected) {
        RpcMessage result = master.handler().apply(
                RpcMessage.submitPlanArtifact(bytes, "publication-preflight"));
        assertFalse(result.success());
        assertTrue(result.errorMessage().contains(expected), result.errorMessage());
        assertFalse(result.errorMessage().contains("no registered worker"), result.errorMessage());
    }

    private static PlanArtifact validArtifact() {
        DataRow row = new DataRow(1);
        row.set(0, 1);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.singletonList(row))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        return LegacyPlanArtifactAdapter.wrap("master-preflight", graph);
    }

    private static PlanArtifact copy(
            PlanArtifact original, List<String> required, List<OperatorDescriptor> operators) {
        return new PlanArtifact(
                original.definitionId(), original.planSchemaVersion(), original.minimumReaderVersion(),
                original.runtimeTarget(), original.boundedness(), original.changelogMode(), original.timeMode(),
                required, original.optionalFeatures(), operators, original.payloadFormat(), original.payload());
    }
}

