package io.rowweave.examples.scenariolab;

import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.util.ArrayList;
import java.util.List;

/**
 * Execution context for one scenario: an BatchSession plus the expected sinks.
 *
 * <p>The scenario body creates sources with {@link #source}, chains operators and registers
 * expectations with {@link #expect} or {@link #expectOrdered}. When the body returns, the
 * framework runs {@code session.execute()}, which submits the whole graph through the genuine
 * InProcessBatchRuntime, reads the FileSink output back with {@link FileSource} and compares it
 * with the oracle. Everything travels over the public production API.
 */
public final class ScenarioLab {

    private final BatchSession session;
    private final String filePathPrefix;
    private final List<Expectation> expectations = new ArrayList<>();
    private int sinkCounter;
    private boolean expectCommitFailure;
    private Class<? extends RuntimeException> expectedCommitException;
    private String expectedCommitMessage;

    public ScenarioLab(String filePathPrefix) {
        this.filePathPrefix = filePathPrefix;
        this.session = io.rowweave.RowWeave.createBatchSession("scenariolab", filePathPrefix);
    }

    public BatchSession session() {
        return session;
    }

    /** Set the job parallelism, which is 1 by default. */
    public void parallelism(int p) {
        session.setParallelism(p);
    }

    /** Set whether the parallel shuffle canonicalizes its tail, false by default. */
    public void canonicalize(boolean b) {
        session.setCanonicalizeOutput(b);
    }

    /** Create a replayable CollectionSource with a generated name. */
    public DataSet source(DataSchema meta, List<DataRow> rows) {
        return source("src" + (sinkCounter++), meta, rows);
    }

    /** Create a replayable source with the given name. */
    public DataSet source(String name, DataSchema meta, List<DataRow> rows) {
        return session.fromSource(new CollectionSource(meta, new ArrayList<>(rows)));
    }

    /** Register an expectation compared as a multiset, since the engine does not promise row order. */
    public void expect(DataSet ds, List<DataRow> expected) {
        expectInternal(ds, expected, false);
    }

    /** Register an ordered expectation, whose position is fixed after orderBy or topN. */
    public void expectOrdered(DataSet ds, List<DataRow> expected) {
        expectInternal(ds, expected, true);
    }

    private void expectInternal(DataSet ds, List<DataRow> expected, boolean ordered) {
        String path = filePathPrefix + "-sink" + (sinkCounter++) + ".tsv";
        DataSchema meta = ds.getSchema();
        ds.sinkTo(new FileSink(meta, path));
        expectations.add(new Expectation(meta, path, new ArrayList<>(expected), ordered));
    }

    /**
     * An expectation with an explicit schema: first check the schema that select derives, then
     * write the rows through a real FileSink.
     */
    public void expectTyped(DataSet ds, DataSchema typedMeta, List<DataRow> expected) {
        requireSameSchema(typedMeta, ds.getSchema());
        expectInternal(ds, expected, false);
    }

    /** The ordered variant of the explicit schema assertion. */
    public void expectTypedOrdered(DataSet ds, DataSchema typedMeta, List<DataRow> expected) {
        requireSameSchema(typedMeta, ds.getSchema());
        expectInternal(ds, expected, true);
    }

    /** Mark this scenario as expecting the commit to fail with the given exception and stable message. */
    public void expectCommitFailure(
            Class<? extends RuntimeException> expectedException, String messageFragment) {
        if (expectedException == null || messageFragment == null || messageFragment.isEmpty()) {
            throw new IllegalArgumentException("expected commit failure requires class and message");
        }
        this.expectCommitFailure = true;
        this.expectedCommitException = expectedException;
        this.expectedCommitMessage = messageFragment;
    }

    boolean isExpectCommitFailure() {
        return expectCommitFailure;
    }

    /** Called by the framework after the scenario body: commit, then read back and compare sink by sink. A mismatch throws AssertionError. */
    void commitAndVerify() {
        if (expectCommitFailure) {
            try {
                session.execute();
            } catch (RuntimeException expected) {
                assertExpectedFailure(expected, expectedCommitException, expectedCommitMessage);
                return;
            }
            throw new AssertionError("expected commit to fail but it succeeded");
        }
        session.execute();
        for (Expectation e : expectations) {
            List<DataRow> actual = readBack(e.meta, e.path);
            RowCompare.Result r = RowCompare.compare(e.meta, e.expected, actual, e.ordered);
            if (!r.pass) {
                throw new AssertionError("sink output mismatch (" + (e.ordered ? "ordered" : "multiset")
                        + "): " + r.message
                        + "\n--- expected (up to 8):\n" + RowCompare.render(e.meta, e.expected, 8)
                        + "--- actual (up to 8):\n" + RowCompare.render(e.meta, actual, 8));
            }
        }
    }

    /** The public immediate commit entry point, which a negative scenario calls from its body to show the failure happens at run time. */
    public void commitNow() {
        commitAndVerify();
    }

    public static void assertExpectedFailure(
            Throwable failure, Class<? extends Throwable> expectedClass, String messageFragment) {
        if (!expectedClass.isInstance(failure)) {
            throw new AssertionError("expected " + expectedClass.getSimpleName()
                    + " but got " + failure.getClass().getSimpleName(), failure);
        }
        Throwable current = failure;
        while (current != null) {
            if (String.valueOf(current.getMessage()).contains(messageFragment)) return;
            current = current.getCause();
        }
        throw new AssertionError("expected failure message containing '" + messageFragment
                + "' but got: " + failure, failure);
    }

    private static void requireSameSchema(DataSchema expected, DataSchema actual) {
        if (expected.getFieldCount() != actual.getFieldCount()) {
            throw new AssertionError("inferred field count mismatch: expected "
                    + expected.getFieldCount() + " but got " + actual.getFieldCount());
        }
        for (int i = 0; i < expected.getFieldCount(); i++) {
            if (expected.getField(i).getDataType() != actual.getField(i).getDataType()) {
                throw new AssertionError("inferred schema mismatch at field " + i + ": expected "
                        + expected.getField(i).getDataType() + " but got "
                        + actual.getField(i).getDataType());
            }
        }
    }

    private static List<DataRow> readBack(DataSchema meta, String path) {
        List<DataRow> rows = new ArrayList<>();
        FileSource in = new FileSource(meta, path);
        try (FileSource.Reader reader = in.openReader()) {
            while (reader.hasNext()) {
                rows.add(reader.next());
            }
        } catch (Exception ex) {
            throw new RuntimeException("read back failed: " + path, ex);
        }
        return rows;
    }

    private static final class Expectation {
        final DataSchema meta;
        final String path;
        final List<DataRow> expected;
        final boolean ordered;

        Expectation(DataSchema meta, String path, List<DataRow> expected, boolean ordered) {
            this.meta = meta;
            this.path = path;
            this.expected = expected;
            this.ordered = ordered;
        }
    }
}
