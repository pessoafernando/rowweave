package io.rowweave;

import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** restore the v1 Source[] validation boundary without opening a source. */
class BatchSourceInputCompatibilityTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", "id_alias", DataType.IntegerType),
            new Field("name", DataType.StringType));

    @Test
    void jobSessionRejectsMissingInputsBeforeAnyIteration() {
        ExecutionEnvironment session = new ExecutionEnvironment("source-validation", "source-validation");
        assertFailure(() -> session.fromSource((Source) null), "input[0]", "null");
        assertFailure(() -> session.fromSource((Source[]) null), "at least one");
        assertFailure(session::fromSource, "at least one");

        ProbeInput valid = new ProbeInput(META);
        assertFailure(() -> session.fromSource(valid, null), "input[1]", "null");
        assertEquals(0, valid.iterations(), "validation must not iterate a preceding valid split");
    }

    @Test
    void jobSessionRejectsMalformedMetadataBeforeAnyIteration() {
        ExecutionEnvironment session = new ExecutionEnvironment("source-validation", "source-validation");
        ProbeInput nullMeta = new ProbeInput(null);
        ProbeInput nullField = new ProbeInput(new DataSchema((Field) null));
        ProbeInput nullName = new ProbeInput(new DataSchema(
                new Field(null, "alias", DataType.IntegerType)));
        ProbeInput nullType = new ProbeInput(new DataSchema(
                new Field("id", "id", null)));

        assertFailure(() -> session.fromSource(nullMeta), "input[0]", "dataSchema");
        assertFailure(() -> session.fromSource(nullField), "input[0]", "field[0]");
        assertFailure(() -> session.fromSource(nullName), "input[0]", "name");
        assertFailure(() -> session.fromSource(nullType), "input[0]", "dataType");
        assertEquals(0, nullMeta.iterations());
        assertEquals(0, nullField.iterations());
        assertEquals(0, nullName.iterations());
        assertEquals(0, nullType.iterations());
    }

    @Test
    void jobSessionRestoresV1NameAndTypeCompatibilityRules() {
        ExecutionEnvironment session = new ExecutionEnvironment("source-validation", "source-validation");
        ProbeInput first = new ProbeInput(META);
        ProbeInput fieldCount = new ProbeInput(new DataSchema(
                new Field("id", DataType.IntegerType)));
        ProbeInput fieldName = new ProbeInput(new DataSchema(
                new Field("other", DataType.IntegerType),
                new Field("name", DataType.StringType)));
        ProbeInput fieldType = new ProbeInput(new DataSchema(
                new Field("id", DataType.LongType),
                new Field("name", DataType.StringType)));

        assertFailure(() -> session.fromSource(first, fieldCount),
                "input[1]", "field count");
        assertFailure(() -> session.fromSource(first, fieldName),
                "input[1]", "field[0]", "name");
        assertFailure(() -> session.fromSource(first, fieldType),
                "input[1]", "field[0]", "dataType");

        DataSchema aliasOnlyDifference = new DataSchema(
                new Field("id", "different_alias", DataType.IntegerType),
                new Field("name", "another_alias", DataType.StringType));
        DataSet accepted = session.fromSource(first, new ProbeInput(aliasOnlyDifference));
        assertSame(META, accepted.getSchema(),
                "v1 compatibility uses first meta and compares field name + type, not alias");
        assertEquals(0, first.iterations(), "building a source must not open it");
    }

    @Test
    void singleInputKeepsItsDescriptorInsteadOfWrappingItAsGenericIterable() {
        ExecutionEnvironment session = new ExecutionEnvironment("source-validation", "source-validation");
        ProbeInput input = new ProbeInput(META);
        DataSet source = session.fromSource(input);
        source.sinkTo(new DiscardSink(META));

        PlanNode sourceNode = session.toPlanGraph().node(0);
        assertSame(input, sourceNode.inputs().get(0),
                "a single File/DB/custom Source must retain its specialized executor descriptor");
        assertEquals(0, input.iterations(), "plan compilation must not iterate the source");
    }

    @Test
    void directPlanSourceValidatesEveryInputAgainstDeclaredMetadata() {
        ProbeInput valid = new ProbeInput(META);
        ProbeInput wrong = new ProbeInput(new DataSchema(
                new Field("id", DataType.IntegerType),
                new Field("amount", DataType.StringType)));

        assertFailure(() -> PlanNode.sourceFromInputs(META, Arrays.asList(valid, wrong)),
                "input[1]", "field[1]", "name");
        assertFailure(() -> PlanNode.sourceFromInputs(META, Arrays.asList(valid, null)),
                "input[1]", "null");
        assertFailure(() -> PlanNode.sourceFromInputs(META, null), "inputs", "null");
        assertEquals(0, valid.iterations());
        assertEquals(0, wrong.iterations());

        PlanNode placeholder = PlanNode.sourceFromInputs(META, Collections.emptyList());
        assertEquals(0, placeholder.inputs().size(),
                "empty Source list remains the internal placeholder-source representation");
    }

    private static void assertFailure(Runnable action, String... messageParts) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, action::run);
        for (String part : messageParts) {
            assertTrue(failure.getMessage().contains(part),
                    () -> "expected message to contain '" + part + "' but was: " + failure.getMessage());
        }
    }

    private static final class ProbeInput implements Source {
        private static final long serialVersionUID = 1L;
        private final DataSchema meta;
        private final AtomicInteger iterations = new AtomicInteger();

        private ProbeInput(DataSchema meta) {
            this.meta = meta;
        }

        @Override
        public DataSchema getSchema() {
            return meta;
        }

        @Override
        public Iterator<DataRow> iterator() {
            iterations.incrementAndGet();
            return Collections.emptyIterator();
        }

        private int iterations() {
            return iterations.get();
        }
    }
}
