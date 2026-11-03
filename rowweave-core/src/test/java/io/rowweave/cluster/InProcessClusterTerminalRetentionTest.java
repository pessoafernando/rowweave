package io.rowweave.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class InProcessClusterTerminalRetentionTest {

    @Test
    void localPseudoClusterStartsTheMasterJanitor(@TempDir Path temporary) {
        try (InProcessCluster cluster = new InProcessCluster(
                1, temporary.toFile())) {
            assertTrue(cluster.terminalRunJanitorActiveForTest());
        }
    }
}

