package io.rowweave.plan;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FilterFunction;
import io.rowweave.DataSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** An immutable metadata copy and a SOURCE rebuild must not drop identity or payload. */
class PlanNodeIdentityFidelityTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.IntegerType));
    private static final String UID_PARAM = "__operator_uid__";

    @Test
    void metadataCopiesPreserveCodeLambdaPayload() {
        FilterFunction function = row -> ((Integer) row.get(0)) > 0;
        PlanNode original = PlanNode.unaryFn("filterFn", META, 0, function);

        PlanNode decorated = original
                .withName("positive ids")
                .withLocation("Ledger.java:41")
                .withParam(UID_PARAM, "positive-filter")
                .withParam("__fused_chain_len__", "2");

        assertSame(function, decorated.userFunction(), "metadata copy silently removed executable payload");
        assertEquals("positive-filter", decorated.param(UID_PARAM));
        assertEquals("2", decorated.param("__fused_chain_len__"));

        PlanGraph roundTrip = PlanGraph.fromBytes(new PlanGraph(Arrays.asList(
                PlanNode.placeholderSource(META), decorated)).toBytes());
        assertNotNull(roundTrip.node(1).userFunction(), "wire round-trip removed executable payload");
        assertEquals("positive-filter", roundTrip.node(1).param(UID_PARAM));
    }

    @Test
    void sourceMetadataSurvivesWireSubgraphAndInputReplacement() {
        PlanNode source = PlanNode.placeholderSource(META)
                .withName("ledger source")
                .withLocation("Ledger.java:12")
                .withParam(UID_PARAM, "ledger-source")
                .withParam("connector.mode", "snapshot");
        PlanGraph graph = new PlanGraph(Arrays.asList(
                source,
                PlanNode.unary("filter", META, 0).param("expr", "id > 0").build()));

        PlanGraph roundTrip = PlanGraph.fromBytes(graph.toBytes());
        assertSourceMetadata(roundTrip.node(0));

        PlanGraph subGraph = graph.subGraphRootedAt(1);
        assertSourceMetadata(subGraph.node(0));

        PlanGraph replaced = graph.replaceSourceInputs(0, Collections.emptyList());
        assertSourceMetadata(replaced.node(0));
    }

    private static void assertSourceMetadata(PlanNode source) {
        assertEquals("ledger source", source.name());
        assertEquals("Ledger.java:12", source.location());
        assertEquals("ledger-source", source.param(UID_PARAM));
        assertEquals("snapshot", source.param("connector.mode"));
    }
}
