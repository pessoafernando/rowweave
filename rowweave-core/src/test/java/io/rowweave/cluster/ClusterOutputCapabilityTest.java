package io.rowweave.cluster;

import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.PrintSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.local.LocalJobClient;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterOutputCapabilityTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));

    private static DataRow row(long id) {
        DataRow row = new DataRow(1);
        row.set(0, id);
        return row;
    }

    private static PlanGraph graph(Sink output) {
        PlanNode source = PlanNode.source(META, Collections.singletonList(row(7L)));
        PlanNode sink = PlanNode.sink(META, 0, output).build();
        return new PlanGraph(Arrays.asList(source, sink));
    }

    private static PlanGraph duplicateFileDestinationGraph(Path target) {
        PlanNode source = PlanNode.source(META, Collections.singletonList(row(7L)));
        PlanNode first = PlanNode.sink(META, 0, new FileSink(META, target.toString())).build();
        String lexicalAlias = target.getParent() + "/./" + target.getFileName();
        PlanNode duplicate = PlanNode.sink(META, 0, new FileSink(META, lexicalAlias)).build();
        return new PlanGraph(Arrays.asList(source, first, duplicate));
    }

    @Test
    void realClientRejectsDuplicateFileDestinationBeforeMasterLookup(@TempDir Path tmp) {
        AtomicInteger lookups = new AtomicInteger();
        PlanGraph graph = duplicateFileDestinationGraph(tmp.resolve("duplicate.tsv"));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> new RealClusterJobClient(() -> {
                    lookups.incrementAndGet();
                    throw new AssertionError("duplicate destination must not locate or contact a Master");
                }).submit(graph));

        assertEquals(0, lookups.get());
        assertTrue(error.getMessage().contains("duplicate output destination"), error.getMessage());
        assertTrue(error.getMessage().contains("SINK nodes"), error.getMessage());
    }

    @Test
    void masterRejectsRawDuplicateFileDestinationBeforeLookingForWorker(@TempDir Path tmp) {
        PlanGraph graph = duplicateFileDestinationGraph(tmp.resolve("raw-duplicate.tsv"));
        try (MasterServer master = new MasterServer(5_000L)) {
            RpcMessage result = master.handler().apply(RpcMessage.submitJob(graph.toBytes()));

            assertEquals(RpcMessage.Type.JOB_RESULT, result.type());
            assertFalse(result.success());
            assertTrue(result.errorMessage().contains("duplicate output destination"), result.errorMessage());
            assertTrue(result.errorMessage().contains("SINK nodes"), result.errorMessage());
            assertFalse(result.errorMessage().contains("no registered worker"), result.errorMessage());
        }
    }

    @Test
    void realClientRejectsDataSetOutputBeforeMasterLookup() {
        AtomicInteger lookups = new AtomicInteger();
        MasterLocator locator = () -> {
            lookups.incrementAndGet();
            throw new AssertionError("invalid graph must not locate or contact a Master");
        };

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> new RealClusterJobClient(locator).submitToSink(graph(new CollectingSink(META))));

        assertEquals(0, lookups.get());
        assertTrue(error.getMessage().contains("SINK node 1"));
        assertTrue(error.getMessage().contains("CollectingSink"));
        assertTrue(error.getMessage().contains("worker-writable"));
    }

    @Test
    void realClientAcceptsManifestConnectorBeforeMasterLookupWithoutCreatingDomain(@TempDir Path tmp) {
        AtomicInteger lookups = new AtomicInteger();
        ManifestFileSink output = new ManifestFileSink(
                META, tmp.resolve("manifest-domain").toString(), "ledger");

        AssertionError reachedLookup = assertThrows(AssertionError.class,
                () -> new RealClusterJobClient(() -> {
                    lookups.incrementAndGet();
                    throw new AssertionError("manifest connector passed client capability validation");
                }).submit(graph(output)));

        assertEquals(1, lookups.get());
        assertTrue(reachedLookup.getMessage().contains("passed client capability"));
        assertFalse(Files.exists(tmp.resolve("manifest-domain")));
    }

    @Test
    void realClientValidatesEverySinkAtomicallyBeforeMasterLookup() {
        AtomicInteger lookups = new AtomicInteger();
        MasterLocator locator = () -> {
            lookups.incrementAndGet();
            throw new AssertionError("mixed-capability graph must not contact a Master");
        };
        PlanNode source = PlanNode.source(META, Collections.singletonList(row(7L)));
        PlanNode valid = PlanNode.sink(META, 0, new PrintSink(META)).build();
        PlanNode invalid = PlanNode.sink(META, 0, new CollectingSink(META)).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(source, valid, invalid));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> new RealClusterJobClient(locator).submit(graph));

        assertEquals(0, lookups.get());
        assertTrue(error.getMessage().contains("SINK node 2"));
        assertTrue(error.getMessage().contains("CollectingSink"));
    }

    @Test
    void realClientRejectsLegacySinkWithoutOutputBeforeMasterLookup() {
        AtomicInteger lookups = new AtomicInteger();
        PlanNode source = PlanNode.source(META, Collections.emptyList());
        PlanNode legacySink = PlanNode.sink(META, 0, "collecting").build();
        PlanGraph graph = new PlanGraph(Arrays.asList(source, legacySink));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> new RealClusterJobClient(() -> {
                    lookups.incrementAndGet();
                    throw new AssertionError("legacy sink must not contact a Master");
                }).submitToSink(graph));

        assertEquals(0, lookups.get());
        assertTrue(error.getMessage().contains("SINK node 1"));
        assertTrue(error.getMessage().contains("no Sink"));
    }

    @Test
    void realClientRejectsIrreversibleMultiOutputBeforeMasterLookup(@TempDir Path tmp) {
        AtomicInteger lookups = new AtomicInteger();
        PlanNode source = PlanNode.source(META, Collections.singletonList(row(7L)));
        PlanNode file = PlanNode.sink(
                META, 0, new FileSink(META, tmp.resolve("out.tsv").toString())).build();
        PlanNode print = PlanNode.sink(META, 0, new PrintSink(META)).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(source, file, print));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> new RealClusterJobClient(() -> {
                    lookups.incrementAndGet();
                    throw new AssertionError("irreversible graph must not contact a Master");
                }).submit(graph));

        assertEquals(0, lookups.get());
        assertTrue(error.getMessage().contains("coordinated commit"), error.getMessage());
        assertTrue(error.getMessage().contains("PrintSink"), error.getMessage());
    }

    @Test
    void masterRejectsRawIrreversibleMultiOutputBeforeLookingForWorker(@TempDir Path tmp) {
        PlanNode source = PlanNode.source(META, Collections.singletonList(row(7L)));
        PlanNode file = PlanNode.sink(
                META, 0, new FileSink(META, tmp.resolve("raw-out.tsv").toString())).build();
        PlanNode print = PlanNode.sink(META, 0, new PrintSink(META)).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(source, file, print));
        try (MasterServer master = new MasterServer(5_000L)) {
            RpcMessage result = master.handler().apply(RpcMessage.submitJob(graph.toBytes()));

            assertFalse(result.success());
            assertTrue(result.errorMessage().contains("coordinated commit"), result.errorMessage());
            assertFalse(result.errorMessage().contains("no registered worker"), result.errorMessage());
        }
    }

    @Test
    void masterRejectsRawInvalidSubmissionBeforeLookingForWorker() {
        PlanGraph graph = graph(new CollectingSink(META));
        try (MasterServer master = new MasterServer(5_000L)) {
            RpcMessage result = master.handler().apply(RpcMessage.submitJob(graph.toBytes()));

            assertEquals(RpcMessage.Type.JOB_RESULT, result.type());
            assertFalse(result.success());
            assertTrue(result.errorMessage().contains("SINK node 1"));
            assertTrue(result.errorMessage().contains("CollectingSink"));
            assertFalse(result.errorMessage().contains("no registered worker"));
        }
    }

    @Test
    void localJobClientStillCollectsIntoDataSetOutput() {
        CollectingSink output = new CollectingSink(META);

        assertEquals(1L, new LocalJobClient().submitToSink(graph(output)));
        assertEquals(1, output.getRows().size());
        assertEquals(7L, output.getRows().get(0).get(0));
    }

    @Test
    void inProcessClusterAlsoRejectsDataSetOutput(@TempDir File tmp) {
        try (InProcessCluster cluster = new InProcessCluster(1, tmp)) {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> cluster.submitToSink(graph(new CollectingSink(META))));
            assertTrue(error.getMessage().contains("CollectingSink"));
        }
    }

    @Test
    void printOutputDeclaresWorkerWritableCapability() {
        assertTrue(new PrintSink(META).isWorkerWritable());
    }

    @Test
    void discardOutputIsWorkerWritableAndDoesNotMaterializeRows() {
        Sink output = new DiscardSink(META);

        output.collect(row(7L));
        output.collect(row(8L));

        assertNotNull(output.getSchema());
        assertTrue(output.isWorkerWritable());
    }
}

