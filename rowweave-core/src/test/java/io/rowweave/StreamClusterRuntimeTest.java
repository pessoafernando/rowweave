package io.rowweave;

import io.rowweave.stream.api.StreamClusterConfig;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** combined deployment owns the same public roles in one JVM. */
class StreamClusterRuntimeTest {

    @Test
    void combinedRuntimeStartsWorkerDirectoryAndMaster(@TempDir Path root) {
        Properties properties = new Properties();
        properties.setProperty("rowweave.stream.master.enable", "true");
        properties.setProperty("rowweave.stream.worker.enable", "true");
        properties.setProperty("rowweave.stream.worker.id", "combined-one");
        properties.setProperty("rowweave.stream.state.dir", root.toString());
        StreamClusterConfig config =
                StreamClusterConfig.fromProperties(properties);

        try (StreamClusterRuntime runtime = StreamClusterRuntime.start(config,
                     Collections.emptyList(), Collections.emptyList(),
                     Collections.emptyList(), Collections.emptyList());
             StreamJobClient client = runtime.createLoopbackClient()) {
            assertEquals(1, runtime.workerEndpoints().size());
            StreamControlException missing = assertThrows(
                    StreamControlException.class,
                    () -> client.attach(StreamJobId.of("combined-missing")));
            assertEquals(StreamControlException.Code.JOB_NOT_FOUND,
                    missing.code());
        }
    }
}
