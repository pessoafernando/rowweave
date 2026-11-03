package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ManifestStageCommandTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));

    @Test
    void manifestCommitContextSurvivesWorkerCommandSerialization(@TempDir Path tmp) throws Exception {
        PlanNode source = PlanNode.source(META, Collections.emptyList());
        PlanGraph fragment = new PlanGraph(Arrays.asList(
                source, PlanNode.sink(META, 0, "placeholder").build()));
        ManifestFileSink output = new ManifestFileSink(
                META, tmp.resolve("domain").toString(), "ledger");
        StageCommand original = StageCommand.sink(META, 64, 1, fragment, output)
                .withCommitGroupId("manifest-group")
                .withManifestCommitContext("attempt-fixed", 73L);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(original);
        }
        StageCommand decoded;
        try (ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            decoded = (StageCommand) in.readObject();
        }

        assertEquals("manifest-group", decoded.commitGroupId());
        assertEquals("attempt-fixed", decoded.manifestAttemptId());
        assertEquals(73L, decoded.manifestGeneration());
    }
}

