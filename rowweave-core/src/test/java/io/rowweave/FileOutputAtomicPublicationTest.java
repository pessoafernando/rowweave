package io.rowweave;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** One FileSink execution publishes atomically only on a successful close. */
class FileOutputAtomicPublicationTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));
    private static final DataSchema TWO_COLUMNS = new DataSchema(
            new Field("id", DataType.LongType), new Field("label", DataType.StringType));
    private static final byte[] OLD = "previous-committed-result\n".getBytes(StandardCharsets.UTF_8);

    @Test
    void collectKeepsPreviousCommittedFileVisibleUntilClose(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("out.tsv");
        Files.write(target, OLD);
        FileSink output = new FileSink(META, target.toString());

        output.collect(row(7));

        assertArrayEquals(OLD, Files.readAllBytes(target),
                "collect must not truncate or overwrite the last published version");
        assertEquals(1, attemptFiles(tmp, target).size(), "this run writes only to the attempt-private temporary file");
        output.abort();
    }

    @Test
    void abortDeletesOnlyAttemptFileAndPreservesPreviousCommit(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("out.tsv");
        Files.write(target, OLD);
        FileSink output = new FileSink(META, target.toString());
        output.collect(row(8));

        output.abort();

        assertArrayEquals(OLD, Files.readAllBytes(target));
        assertTrue(attemptFiles(tmp, target).isEmpty(), "abort must leave no temporary file for this attempt");
        assertDoesNotThrow(output::abort, "a repeated abort must be idempotent");
        assertThrows(IllegalStateException.class, () -> output.collect(row(9)),
                "an aborted attempt must not be reopened for writing");
    }

    @Test
    void closeReplacesPreviousCommitAndTerminalCallsAreIdempotent(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("out.tsv");
        Files.write(target, OLD);
        FileSink output = new FileSink(META, target.toString());
        output.collect(row(10));
        output.collect(row(11));

        output.close();

        List<DataRow> rows = FileSink.read(META, target.toString());
        assertEquals(2, rows.size());
        assertEquals(10L, rows.get(0).get(0));
        assertEquals(11L, rows.get(1).get(0));
        assertTrue(attemptFiles(tmp, target).isEmpty());
        assertDoesNotThrow(output::close);
        assertDoesNotThrow(output::abort, "abort after commit must not delete the published result");
        assertEquals(2, FileSink.read(META, target.toString()).size());
        assertThrows(IllegalStateException.class, () -> output.collect(row(12)));
    }

    /** A prepare is invisible, and a publish is visible yet still recoverable by compensation until finalize. */
    @Test
    void coordinatedPublishCanRollbackToPreviousCommit(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("coordinated.tsv");
        Files.write(target, OLD);
        FileSink output = new FileSink(META, target.toString());
        assertTrue(output.supportsCoordinatedCommit());
        output.collect(row(61));

        output.prepareCommit();
        assertArrayEquals(OLD, Files.readAllBytes(target), "prepare must not make the result finally visible");

        output.commitPrepared();
        assertEquals(61L, FileSink.read(META, target.toString()).get(0).get(0));

        output.rollbackCommit();
        assertArrayEquals(OLD, Files.readAllBytes(target));
        assertTrue(outputArtifacts(tmp).isEmpty());
        assertDoesNotThrow(output::rollbackCommit, "a repeated rollback must be idempotent");
    }

    @Test
    void coordinatedRollbackRemovesNewTargetWhenNoPreviousCommit(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("new-target.tsv");
        FileSink output = new FileSink(META, target.toString());
        output.collect(row(62));

        output.prepareCommit();
        output.commitPrepared();
        assertTrue(Files.exists(target));

        output.rollbackCommit();
        assertFalse(Files.exists(target));
        assertTrue(outputArtifacts(tmp).isEmpty());
    }

    @Test
    void finalizeMakesCoordinatedCommitTerminal(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("finalized.tsv");
        Files.write(target, OLD);
        FileSink output = new FileSink(META, target.toString());
        output.collect(row(63));

        output.prepareCommit();
        output.commitPrepared();
        output.finalizeCommit();
        output.rollbackCommit();

        assertEquals(63L, FileSink.read(META, target.toString()).get(0).get(0));
        assertTrue(outputArtifacts(tmp).isEmpty());
        assertThrows(IllegalStateException.class, () -> output.collect(row(64)));
    }

    @Test
    void zeroRowCloseReplacesOldNonEmptyFileWithEmptyFile(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("out.tsv");
        Files.write(target, OLD);

        new FileSink(META, target.toString()).close();

        assertTrue(Files.exists(target));
        assertEquals(0L, Files.size(target), "a successful zero-row result must replace the previous non-empty version");
        assertTrue(attemptFiles(tmp, target).isEmpty());
    }

    @Test
    void publishFailurePreservesOldCommitAndClosesAttempt(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("out.tsv");
        Files.write(target, OLD);
        FileSink output = new FileSink(META, target.toString());
        output.collect(row(13));
        Path attempt = onlyAttemptFile(tmp, target);
        Files.delete(attempt);  // deterministically makes the source of an atomic move missing.

        UncheckedIOException error = assertThrows(UncheckedIOException.class, output::close);

        assertTrue(error.getMessage().contains("failed to publish FileSink"));
        assertArrayEquals(OLD, Files.readAllBytes(target));
        assertTrue(attemptFiles(tmp, target).isEmpty());
        assertThrows(IllegalStateException.class, () -> output.collect(row(14)),
                "an attempt whose publish failed must terminate rather than keep writing unknown state");
    }

    @Test
    void serializationDropsActiveAttemptState(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("out.tsv");
        Files.write(target, OLD);
        FileSink original = new FileSink(META, target.toString());
        original.collect(row(21));

        FileSink copy = roundTrip(original);
        copy.collect(row(22));
        assertEquals(2, attemptFiles(tmp, target).size(), "a deserialized copy must establish its own attempt");
        copy.close();
        original.abort();

        List<DataRow> rows = FileSink.read(META, target.toString());
        assertEquals(1, rows.size());
        assertEquals(22L, rows.get(0).get(0));
        assertTrue(attemptFiles(tmp, target).isEmpty());
    }

    @Test
    void exclusiveDestinationIdentityIsStableAcrossAliasesAndSerialization(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("identity.tsv");
        Files.write(target, OLD);
        FileSink direct = new FileSink(META, target.toString());
        FileSink lexicalAlias = new FileSink(
                META, target.getParent() + "/./" + target.getFileName());

        assertEquals(direct.exclusiveDestinationId(), lexicalAlias.exclusiveDestinationId());
        assertEquals(direct.exclusiveDestinationId(), roundTrip(direct).exclusiveDestinationId(),
                "the destination identity must survive Sink descriptor serialization");
        assertNotEquals(direct.exclusiveDestinationId(),
                new FileSink(META, tmp.resolve("other.tsv").toString()).exclusiveDestinationId());
    }

    @Test
    void exclusiveDestinationIdentityResolvesVisibleSymlinkAlias(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("symlink-target.tsv");
        Path alias = tmp.resolve("symlink-alias.tsv");
        Files.write(target, OLD);
        try {
            Files.createSymbolicLink(alias, target);
        } catch (UnsupportedOperationException | java.io.IOException | SecurityException unavailable) {
            org.junit.jupiter.api.Assumptions.assumeTrue(false,
                    "current filesystem cannot create symlinks: " + unavailable.getMessage());
        }

        assertEquals(new FileSink(META, target.toString()).exclusiveDestinationId(),
                new FileSink(META, alias.toString()).exclusiveDestinationId());
    }

    /** The row width a job actually produces has to match the declared DataSchema at the publish boundary. */
    @Test
    void engineRejectsShortRowAndPreservesPreviousCommit(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("schema-guard.tsv");
        Files.write(target, OLD);
        DataRow input = new DataRow(2);
        input.set(0, 31L);
        input.set(1, "source");

        BatchSession session = RowWeave.createBatchSession("short-output-row");
        DataSet malformed = session.fromSource(new CollectionSource(
                TWO_COLUMNS, Collections.singletonList(input))).map(row -> {
            DataRow shortRow = new DataRow(1);
            shortRow.set(0, row.get(0));
            return shortRow;
        });
        malformed.sinkTo(new FileSink(TWO_COLUMNS, target.toString()));

        RuntimeException failure = assertThrows(RuntimeException.class, session::execute,
                "a result declaring two columns but carrying one must fail the Job rather than publish a corrupt file");

        assertTrue(causeMessages(failure).contains("output row width mismatch"), causeMessages(failure));
        assertTrue(causeMessages(failure).contains("expected=2"), causeMessages(failure));
        assertTrue(causeMessages(failure).contains("actual=1"), causeMessages(failure));
        assertArrayEquals(OLD, Files.readAllBytes(target), "a schema failure must preserve the last published version");
        assertTrue(attemptFiles(tmp, target).isEmpty(), "a schema failure must abort and clean up the private attempt");
    }

    /** When a later SINK fails, an earlier FileSink that already finished must leave no partial commit. */
    @Test
    void multiOutputFailureRestoresEarlierFileCommit(@TempDir Path tmp) throws Exception {
        Path firstTarget = tmp.resolve("first.tsv");
        Path failingTarget = tmp.resolve("failing.tsv");
        byte[] firstOld = "first-previous\n".getBytes(StandardCharsets.UTF_8);
        byte[] failingOld = "failing-previous\n".getBytes(StandardCharsets.UTF_8);
        Files.write(firstTarget, firstOld);
        Files.write(failingTarget, failingOld);

        BatchSession session = RowWeave.createBatchSession("multi-output-failure-atomicity");
        session.fromSource(new CollectionSource(META, Collections.singletonList(row(51))))
                .sinkTo(new FileSink(META, firstTarget.toString()));

        DataRow input = new DataRow(2);
        input.set(0, 52L);
        input.set(1, "source");
        session.fromSource(new CollectionSource(TWO_COLUMNS, Collections.singletonList(input)))
                .map(source -> {
                    DataRow shortRow = new DataRow(1);
                    shortRow.set(0, source.get(0));
                    return shortRow;
                })
                .sinkTo(new FileSink(TWO_COLUMNS, failingTarget.toString()));

        RuntimeException failure = assertThrows(RuntimeException.class, session::execute);

        assertTrue(causeMessages(failure).contains("row width"), causeMessages(failure));
        assertTrue(causeMessages(failure).contains("field count 2"), causeMessages(failure));
        assertArrayEquals(firstOld, Files.readAllBytes(firstTarget),
                "a Job failure must compensate by restoring the FileSink that already completed");
        assertArrayEquals(failingOld, Files.readAllBytes(failingTarget));
        List<Path> leftovers = outputArtifacts(tmp);
        assertTrue(leftovers.isEmpty(),
                "failure must leave no attempt or backup behind: " + leftovers);
    }

    /** When the second publish fails, the first published file is restored through its backup. */
    @Test
    void secondPublishFailureRestoresFirstPublishedFile(@TempDir Path tmp) throws Exception {
        Path firstTarget = tmp.resolve("publish-first.tsv");
        Path secondTarget = tmp.resolve("publish-second.tsv");
        byte[] firstOld = "publish-first-old\n".getBytes(StandardCharsets.UTF_8);
        byte[] secondOld = "publish-second-old\n".getBytes(StandardCharsets.UTF_8);
        Files.write(firstTarget, firstOld);
        Files.write(secondTarget, secondOld);

        BatchSession session = RowWeave.createBatchSession("multi-output-publish-failure");
        session.fromSource(new CollectionSource(META, Collections.singletonList(row(71))))
                .sinkTo(new FileSink(META, firstTarget.toString()));
        FileSink second = new FileSink(META, secondTarget.toString());
        session.fromSource(new CollectionSource(META, Collections.singletonList(row(72))))
                .sinkTo(new PublishFailingOutput(second));

        RuntimeException failure = assertThrows(RuntimeException.class, session::execute);

        assertTrue(causeMessages(failure).contains("injected second publish failure"), causeMessages(failure));
        assertArrayEquals(firstOld, Files.readAllBytes(firstTarget));
        assertArrayEquals(secondOld, Files.readAllBytes(secondTarget));
        assertTrue(outputArtifacts(tmp).isEmpty());
    }

    /** A concurrent tee over a shared source prepares only, and a failure on one branch must keep the finished branch unpublished. */
    @Test
    void fanoutFailurePreservesAllPreviousCommits(@TempDir Path tmp) throws Exception {
        Path firstTarget = tmp.resolve("tee-first.tsv");
        Path failingTarget = tmp.resolve("tee-failing.tsv");
        byte[] firstOld = "tee-first-old\n".getBytes(StandardCharsets.UTF_8);
        byte[] failingOld = "tee-failing-old\n".getBytes(StandardCharsets.UTF_8);
        Files.write(firstTarget, firstOld);
        Files.write(failingTarget, failingOld);

        DataRow input = new DataRow(2);
        input.set(0, 81L);
        input.set(1, "source");
        BatchSession session = RowWeave.createBatchSession("fanout-output-failure-atomicity");
        DataSet shared = session.fromSource(
                new CollectionSource(TWO_COLUMNS, Collections.singletonList(input)));
        shared.sinkTo(new FileSink(TWO_COLUMNS, firstTarget.toString()));
        shared.map(source -> {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interrupted);
            }
            DataRow shortRow = new DataRow(1);
            shortRow.set(0, source.get(0));
            return shortRow;
        }).sinkTo(new FileSink(TWO_COLUMNS, failingTarget.toString()));

        assertThrows(RuntimeException.class, session::execute);

        assertArrayEquals(firstOld, Files.readAllBytes(firstTarget));
        assertArrayEquals(failingOld, Files.readAllBytes(failingTarget));
        assertTrue(outputArtifacts(tmp).isEmpty());
    }

    /** A Sink that cannot take part in coordination is rejected before the Source opens, leaving single-Sink compatibility intact. */
    @Test
    void multiOutputPreflightRejectsIrreversibleOutputBeforeInputOpens(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("preflight.tsv");
        Files.write(target, OLD);
        AtomicInteger iteratorCalls = new AtomicInteger();
        Source source = new Source() {
            @Override
            public DataSchema getSchema() {
                return META;
            }

            @Override
            public Iterator<DataRow> iterator() {
                iteratorCalls.incrementAndGet();
                return Collections.singletonList(row(91)).iterator();
            }
        };
        BatchSession session = RowWeave.createBatchSession("irreversible-multi-output");
        DataSet shared = session.fromSource(source);
        shared.sinkTo(new FileSink(META, target.toString()));
        shared.sinkTo(new PrintSink(META));

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, session::execute);

        assertTrue(failure.getMessage().contains("coordinated commit"), failure.getMessage());
        assertEquals(0, iteratorCalls.get());
        assertArrayEquals(OLD, Files.readAllBytes(target));
        assertTrue(outputArtifacts(tmp).isEmpty());
    }

    /** Two SINKs in one whole graph must not contend for the same exclusive file destination. */
    @Test
    void engineRejectsAliasedDuplicateFileDestinationsBeforeInputOpens(@TempDir Path tmp) throws Exception {
        Path target = tmp.resolve("exclusive.tsv");
        Files.write(target, OLD);
        AtomicInteger iteratorCalls = new AtomicInteger();
        Source source = new Source() {
            @Override
            public DataSchema getSchema() {
                return META;
            }

            @Override
            public Iterator<DataRow> iterator() {
                iteratorCalls.incrementAndGet();
                return Collections.singletonList(row(41)).iterator();
            }
        };

        BatchSession session = RowWeave.createBatchSession("duplicate-output-destination");
        DataSet shared = session.fromSource(source);
        shared.sinkTo(new FileSink(META, target.toString()));
        String lexicalAlias = target.getParent() + "/./" + target.getFileName();
        shared.sinkTo(new FileSink(META, lexicalAlias));

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, session::execute,
                "a duplicate exclusive destination must be rejected so the last SINK to close cannot overwrite another result");

        assertTrue(failure.getMessage().contains("duplicate output destination"), failure.getMessage());
        assertTrue(failure.getMessage().contains("SINK nodes"), failure.getMessage());
        assertEquals(0, iteratorCalls.get(), "the whole-graph pre-check must run before any Source is opened");
        assertArrayEquals(OLD, Files.readAllBytes(target), "a rejected misconfiguration must not replace the last published version");
        assertTrue(attemptFiles(tmp, target).isEmpty(), "a failed pre-check must not create FileSink staging");
    }

    private static DataRow row(long id) {
        DataRow row = new DataRow(1);
        row.set(0, id);
        return row;
    }

    private static List<Path> attemptFiles(Path directory, Path target) throws Exception {
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            return files.filter(path -> !path.equals(target)).collect(Collectors.toList());
        }
    }

    private static List<Path> outputArtifacts(Path directory) throws Exception {
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            return files.filter(path -> path.getFileName().toString().startsWith(".rowweave-output-"))
                    .collect(Collectors.toList());
        }
    }


    private static Path onlyAttemptFile(Path directory, Path target) throws Exception {
        List<Path> attempts = attemptFiles(directory, target);
        assertEquals(1, attempts.size(), "expected exactly one private attempt file");
        return attempts.get(0);
    }

    private static FileSink roundTrip(FileSink output) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(output);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (FileSink) in.readObject();
        }
    }

    private static String causeMessages(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current.getMessage() != null) {
                messages.append(current.getMessage()).append('\n');
            }
        }
        return messages.toString();
    }

    private static final class PublishFailingOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private final FileSink delegate;

        private PublishFailingOutput(FileSink delegate) {
            this.delegate = delegate;
        }

        @Override public DataSchema getSchema() { return delegate.getSchema(); }
        @Override public boolean isWorkerWritable() { return true; }
        @Override public String exclusiveDestinationId() { return delegate.exclusiveDestinationId(); }
        @Override public void collect(DataRow row) { delegate.collect(row); }
        @Override public boolean supportsCoordinatedCommit() { return true; }
        @Override public void prepareCommit() { delegate.prepareCommit(); }
        @Override public void commitPrepared() { throw new IllegalStateException("injected second publish failure"); }
        @Override public void rollbackCommit() { delegate.rollbackCommit(); }
        @Override public void finalizeCommit() { delegate.finalizeCommit(); }
    }
}
