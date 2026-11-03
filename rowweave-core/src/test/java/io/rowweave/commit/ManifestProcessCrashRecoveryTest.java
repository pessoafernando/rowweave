package io.rowweave.commit;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** After a real child process halts, only the old generation or the complete new generation is visible. */
class ManifestProcessCrashRecoveryTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));
    private static final List<String> PRE_MANIFEST = Arrays.asList(
            "DATA_WRITTEN", "DATA_FORCED", "PARTIAL_DATA_INSTALLED",
            "ALL_DATA_INSTALLED", "MANIFEST_TEMP_FORCED");
    private static final List<String> POST_MANIFEST = Arrays.asList(
            "MANIFEST_NAME_CREATED", "BEFORE_ACK");

    @Test
    void realProcessCrashMatrixAndRestartRecovery(@TempDir Path tmp) throws Exception {
        for (String point : concat(PRE_MANIFEST, POST_MANIFEST)) {
            Path domain = tmp.resolve(point.toLowerCase());
            commitBaseline(domain);

            ProcessResult child = launch(domain, point);

            assertEquals(41, child.exitCode, point + " output:\n" + child.output);
            assertTrue(child.output.contains("GENERATION=2"), child.output);
            ManifestFileStore store = new ManifestFileStore(domain);
            CommitManifest visible = store.latest();
            assertNotNull(visible);
            if (PRE_MANIFEST.contains(point)) {
                assertEquals(1L, visible.generation(), point);
                assertEquals(1L, readId(domain, "a"));
                assertEquals(2L, readId(domain, "b"));
            } else {
                assertEquals(2L, visible.generation(), point);
                assertEquals(101L, readId(domain, "a"));
                assertEquals(102L, readId(domain, "b"));
            }

            ManifestFileRecovery.Result recovered = new ManifestFileRecovery(domain)
                    .recoverConfirmedDeadAttempt("crash-" + point, 2L, 100);
            if (PRE_MANIFEST.contains(point)) {
                assertEquals(ManifestFileRecovery.Status.ORPHAN_REMOVED, recovered.status(), point);
                assertEquals(1L, store.latest().generation(), point);
                assertFalse(Files.exists(domain.resolve("manifests/2.manifest")), point);
                assertEquals(0L, regularFileCount(domain.resolve("data/2")), point);
            } else {
                assertEquals(ManifestFileRecovery.Status.COMMITTED_PRESERVED, recovered.status(), point);
                assertEquals(2L, store.latest().generation(), point);
                assertTrue(Files.isRegularFile(domain.resolve("data/2/a.data")), point);
                assertTrue(Files.isRegularFile(domain.resolve("data/2/b.data")), point);
            }
            assertEquals(0L, regularFileCount(domain.resolve("staging")), point);
            assertEquals(0L, ownedManifestTemps(domain, 2L, "crash-" + point), point);
        }
    }

    private static void commitBaseline(Path domain) {
        ExecutionEnvironment session = RowWeave.createEnvironment("manifest-crash-baseline");
        session.fromSource(new CollectionSource(META, Collections.singletonList(row(1L))))
                .sinkTo(new ManifestFileSink(META, domain.toString(), "a"));
        session.fromSource(new CollectionSource(META, Collections.singletonList(row(2L))))
                .sinkTo(new ManifestFileSink(META, domain.toString(), "b"));
        session.execute();
    }

    private static long readId(Path domain, String member) {
        try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                META, domain.toString(), member).openReader()) {
            assertTrue(reader.hasNext());
            return (Long) reader.next().get(0);
        }
    }

    private static DataRow row(long id) {
        DataRow row = new DataRow(1);
        row.set(0, id);
        return row;
    }

    private static ProcessResult launch(Path domain, String point) throws Exception {
        Process process = new ProcessBuilder(io.rowweave.test.ForkedJvm.command(
                ManifestCrashProbe.class.getName(), domain.toString(), point))
                .redirectErrorStream(true).start();
        StringBuilder output = new StringBuilder();
        Thread drainer = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append('\n');
                }
            } catch (Exception e) {
                output.append("drain failed: ").append(e).append('\n');
            }
        }, "manifest-crash-probe-output");
        drainer.start();
        assertTrue(process.waitFor(15, TimeUnit.SECONDS), "child did not halt at " + point);
        drainer.join(5_000L);
        return new ProcessResult(process.exitValue(), output.toString());
    }

    private static List<String> concat(List<String> left, List<String> right) {
        java.util.ArrayList<String> values = new java.util.ArrayList<>(left);
        values.addAll(right);
        return values;
    }

    private static long regularFileCount(Path root) throws Exception {
        if (!Files.exists(root)) {
            return 0L;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }

    private static long ownedManifestTemps(Path domain, long generation, String attemptId)
            throws Exception {
        Path manifests = domain.resolve("manifests");
        if (!Files.isDirectory(manifests)) {
            return 0L;
        }
        String prefix = ManifestFileRecovery.manifestTempPrefix(generation, attemptId);
        try (java.util.stream.Stream<Path> paths = Files.list(manifests)) {
            return paths.filter(path -> path.getFileName().toString().startsWith(prefix)).count();
        }
    }

    private static final class ProcessResult {
        private final int exitCode;
        private final String output;

        private ProcessResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
}
