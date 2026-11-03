package io.rowweave.plan;

import io.rowweave.DataType;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSink;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Durable-sink admission depends on a crash outcome that can be rebuilt, not on a self-reported coordinated flag. */
class OutputCommitValidatorTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void durableRejectsFileAndCustomCoordinatedOutputs(@TempDir Path tmp) {
        IllegalArgumentException file = assertThrows(IllegalArgumentException.class,
                () -> OutputCommitValidator.validateDurable(graph(
                        new FileSink(META, tmp.resolve("plain.tsv").toString()))));
        assertTrue(file.getMessage().startsWith(
                "durable run requires crash-reconcilable output:"), file.getMessage());
        assertTrue(file.getMessage().contains(FileSink.class.getName()), file.getMessage());

        IllegalArgumentException custom = assertThrows(IllegalArgumentException.class,
                () -> OutputCommitValidator.validateDurable(graph(
                        new SelfReportedCoordinatedOutput())));
        assertTrue(custom.getMessage().startsWith(
                "durable run requires crash-reconcilable output:"), custom.getMessage());
        assertTrue(custom.getMessage().contains(
                SelfReportedCoordinatedOutput.class.getName()), custom.getMessage());
    }

    @Test
    void durableAcceptsDiscardAndSingleOrMultiManifest(@TempDir Path tmp) {
        assertDoesNotThrow(() -> OutputCommitValidator.validateDurable(
                graph(new DiscardSink(META))));
        assertDoesNotThrow(() -> OutputCommitValidator.validateDurable(
                graph(new ManifestFileSink(
                        META, tmp.resolve("single").toString(), "ledger"))));
        assertDoesNotThrow(() -> OutputCommitValidator.validateDurable(graph(
                new ManifestFileSink(META, tmp.resolve("multi").toString(), "left"),
                new ManifestFileSink(META, tmp.resolve("multi").toString(), "right"))));
        assertDoesNotThrow(() -> OutputCommitValidator.validateDurable(graph(
                new DiscardSink(META), new DiscardSink(META))));
        IllegalArgumentException mixed = assertThrows(IllegalArgumentException.class,
                () -> OutputCommitValidator.validateDurable(graph(
                        new ManifestFileSink(META,
                                tmp.resolve("mixed-unsafe").toString(), "left"),
                        new DiscardSink(META),
                        new ManifestFileSink(META,
                                tmp.resolve("mixed-unsafe").toString(), "right"))));
        assertTrue(mixed.getMessage().contains("cannot mix"), mixed.getMessage());
    }

    @Test
    void durableDoesNotTrustCustomDescriptorProfile() {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> OutputCommitValidator.validateDurable(
                        graph(new SelfReportedV2Output())));
        assertTrue(failure.getMessage().contains("UNSUPPORTED_DURABLE_SINK_PROVIDER"),
                failure.getMessage());
    }

    @Test
    void durableStillEnforcesManifestDomainAndMemberIdentity(@TempDir Path tmp) {
        IllegalArgumentException domains = assertThrows(IllegalArgumentException.class,
                () -> OutputCommitValidator.validateDurable(graph(
                        new ManifestFileSink(META, tmp.resolve("a").toString(), "left"),
                        new ManifestFileSink(META, tmp.resolve("b").toString(), "right"))));
        assertTrue(domains.getMessage().contains("same manifest commit domain"),
                domains.getMessage());

        IllegalArgumentException members = assertThrows(IllegalArgumentException.class,
                () -> OutputCommitValidator.validateDurable(graph(
                        new ManifestFileSink(META, tmp.resolve("same").toString(), "ledger"),
                        new ManifestFileSink(META, tmp.resolve("same").toString(), "ledger"))));
        assertTrue(members.getMessage().contains("duplicate manifest member"),
                members.getMessage());
    }

    private static PlanGraph graph(Sink... outputs) {
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.source(META, Collections.emptyList()));
        for (Sink output : outputs) {
            nodes.add(PlanNode.sink(META, 0, output).build());
        }
        return new PlanGraph(nodes);
    }

    private static final class SelfReportedCoordinatedOutput implements Sink {
        private static final long serialVersionUID = 1L;
        @Override public DataSchema getSchema() { return META; }
        @Override public void collect(DataRow row) { }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { }
        @Override public void commitPrepared() { }
        @Override public void rollbackCommit() { }
        @Override public void finalizeCommit() { }
    }

    private static final class SelfReportedV2Output implements BatchSink {
        private static final long serialVersionUID = 1L;
        private final BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, "untrusted-manifest", 1,
                io.rowweave.commit.SchemaFingerprint.of(META),
                "untrusted-destination", "untrusted-domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        @Override public BatchSinkDescriptor batchSinkDescriptor() { return descriptor; }
        @Override public DataSchema getSchema() { return META; }
        @Override public void collect(DataRow row) { }
        @Override public String exclusiveDestinationId() { return "untrusted-destination"; }
        @Override public boolean supportsCoordinatedCommit() { return true; }
    }
}
