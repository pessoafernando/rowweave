package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MasterWholeGraphProgressiveAwaitTest {

    @Test
    void healthyWholeGraphOutlivesStageSetupTimeout() throws Exception {
        List<String> command = io.rowweave.test.ForkedJvm.baseCommand();
        command.add("-Drowweave.cluster.stageTimeoutMs=50");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(WholeGraphProgressiveAwaitProbe.class.getName());
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        boolean finished = process.waitFor(10, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            process.waitFor(2, TimeUnit.SECONDS);
        }
        assertTrue(finished, "the WHOLE_GRAPH probe must end within a bound");
        String output = readAll(process.getInputStream());

        assertEquals(0, process.exitValue(), output);
        assertTrue(output.contains("WHOLE_GRAPH_PROGRESSIVE_OK"), output);
    }

    private static String readAll(InputStream input) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int n;
        while ((n = input.read(buffer)) >= 0) {
            out.write(buffer, 0, n);
        }
        return new String(out.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
    }
}

