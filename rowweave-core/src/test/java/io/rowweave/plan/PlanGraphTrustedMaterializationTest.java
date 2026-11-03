package io.rowweave.plan;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FilterFunction;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import java.io.InvalidClassException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Once the plan structure is scanned, each isolated object blob is loaded under its own semantic root type. */
class PlanGraphTrustedMaterializationTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void exactBuiltInPlanMaterializesUnderMtlsTrust() {
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META,
                        Collections.<Source>singletonList(new FileSource(META, "/tmp/not-opened"))),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()));

        PlanGraph decoded = PlanGraph.fromBytes(graph.toBytes(),
                ObjectTrustContext.mtlsBuiltIns(), ignored -> { });

        assertEquals(FileSource.class, decoded.node(0).inputs().get(0).getClass());
        assertEquals(DiscardSink.class, decoded.root().output().getClass());
    }

    @Test
    void applicationInputRequiresLocalOrSignedDeploymentTrust() {
        PlanGraph graph = new PlanGraph(Collections.singletonList(
                PlanNode.sourceFromInputs(META,
                        Collections.<Source>singletonList(new TestInput()))));
        byte[] bytes = graph.toBytes();

        assertEquals(TestInput.class, PlanGraph.fromBytes(bytes,
                ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { })
                .node(0).inputs().get(0).getClass());
        UncheckedIOException rejected = assertThrows(UncheckedIOException.class,
                () -> PlanGraph.fromBytes(bytes, ObjectTrustContext.mtlsBuiltIns(), ignored -> { }));
        assertTrue(hasCause(rejected, InvalidClassException.class));
    }

    @Test
    void operatorTypeIdSelectsConcreteFunctionRoot() {
        FilterFunction wrongForMap = row -> true;
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.placeholderSource(META),
                PlanNode.unaryFn("map", META, 0, (Serializable) wrongForMap)));

        UncheckedIOException rejected = assertThrows(UncheckedIOException.class, graph::toBytes);
        assertTrue(hasCause(rejected, InvalidClassException.class));
    }

    @Test
    void secureWriterAppliesTheSameCodeSourcePolicyAsReader() {
        PlanGraph application = new PlanGraph(Collections.singletonList(
                PlanNode.sourceFromInputs(META,
                        Collections.<Source>singletonList(new TestInput()))));
        assertThrows(UncheckedIOException.class,
                () -> application.toBytes(ObjectTrustContext.mtlsBuiltIns(), ignored -> { }));

        PlanGraph builtIn = new PlanGraph(Collections.singletonList(
                PlanNode.sourceFromInputs(META,
                        Collections.<Source>singletonList(new FileSource(META, "/tmp/not-opened")))));
        builtIn.toBytes(ObjectTrustContext.mtlsBuiltIns(), ignored -> { });
    }

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (type.isInstance(current)) return true;
        }
        return false;
    }

    private static final class TestInput implements Source {
        @Override public DataSchema getSchema() { return META; }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
    }
}
