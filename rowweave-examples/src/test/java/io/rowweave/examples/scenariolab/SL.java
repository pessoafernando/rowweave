package io.rowweave.examples.scenariolab;

import io.rowweave.DataSet;
import io.rowweave.DataSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Static helpers for building scenarios, so that the common shapes fit on one line.
 *
 * <pre>
 * SL.sink("B01-S01", "filter amt&gt;100", META, in,
 * ds -&gt; ds.filter("amt &gt; 100"),   // the DAG, run by the engine
 * in -&gt; SL.filterRows(in, r -&gt; ...), // the oracle, an independent Java implementation
 * false)</pre>
 */
public final class SL {

    private SL() {
    }

    /** A single-sink scenario: source(in), then the dag, with the expectation taken from oracle(in). */
    public static ScenarioDef sink(String id, String title, DataSchema meta, List<io.rowweave.DataRow> in,
                                   java.util.function.Function<DataSet, DataSet> dag,
                                   java.util.function.Function<List<io.rowweave.DataRow>, List<io.rowweave.DataRow>> oracle,
                                   boolean ordered) {
        return ScenarioDef.of(id, title, lab -> {
            DataSet transformed = dag.apply(lab.source(meta, in));
            if (ordered) {
                lab.expectOrdered(transformed, oracle.apply(in));
            } else {
                lab.expect(transformed, oracle.apply(in));
            }
        });
    }

    /**
     * A negative scenario that fails at build or at run time: if the operator throws while the
     * graph is being built, the scenario passes; otherwise an expectation is registered and the
     * commit is required to fail.
     */
    public static ScenarioDef fails(String id, String title, DataSchema meta, List<io.rowweave.DataRow> in,
                                    java.util.function.Function<DataSet, DataSet> op,
                                    List<io.rowweave.DataRow> passThroughExpected,
                                    Class<? extends RuntimeException> expectedClass,
                                    String expectedMessageFragment) {
        return ScenarioDef.of(id, title, lab -> {
            DataSet src = lab.source(meta, in);
            DataSet res;
            try {
                res = op.apply(src);
            } catch (RuntimeException buildRejected) {
                ScenarioLab.assertExpectedFailure(
                        buildRejected, expectedClass, expectedMessageFragment);
                lab.expect(src, in);
                return;
            }
            lab.expect(res, passThroughExpected);
            lab.expectCommitFailure(expectedClass, expectedMessageFragment);
        });
    }

    /** A single-sink scenario with an explicit output schema, written through an identity reduceGroup. */
    public static ScenarioDef sinkTyped(String id, String title, DataSchema meta, List<io.rowweave.DataRow> in,
                                        java.util.function.Function<DataSet, DataSet> dag,
                                        DataSchema typedMeta,
                                        java.util.function.Function<List<io.rowweave.DataRow>, List<io.rowweave.DataRow>> oracle,
                                        boolean ordered) {
        return ScenarioDef.of(id, title, lab -> {
            DataSet transformed = dag.apply(lab.source(meta, in));
            if (ordered) {
                lab.expectTypedOrdered(transformed, typedMeta, oracle.apply(in));
            } else {
                lab.expectTyped(transformed, typedMeta, oracle.apply(in));
            }
        });
    }

    /** A two-source single-sink scenario, for join, union and coGroup. */
    public static ScenarioDef sink2(String id, String title,
                                    DataSchema leftMeta, List<io.rowweave.DataRow> left,
                                    DataSchema rightMeta, List<io.rowweave.DataRow> right,
                                    java.util.function.BiFunction<DataSet, DataSet, DataSet> dag,
                                    java.util.function.BiFunction<List<io.rowweave.DataRow>, List<io.rowweave.DataRow>, List<io.rowweave.DataRow>> oracle,
                                    boolean ordered) {
        return ScenarioDef.of(id, title, lab -> {
            DataSet l = lab.source(leftMeta, left);
            DataSet r = lab.source(rightMeta, right);
            DataSet transformed = dag.apply(l, r);
            if (ordered) {
                lab.expectOrdered(transformed, oracle.apply(left, right));
            } else {
                lab.expect(transformed, oracle.apply(left, right));
            }
        });
    }

    // ---- Shared oracle fragments ----

    /** Filter, as an independent Java implementation. */
    public static List<io.rowweave.DataRow> filterRows(List<io.rowweave.DataRow> in,
                                                     java.util.function.Predicate<io.rowweave.DataRow> p) {
        List<io.rowweave.DataRow> out = new ArrayList<>();
        for (io.rowweave.DataRow r : in) {
            if (p.test(r)) {
                out.add(r);
            }
        }
        return out;
    }

    /** Row-by-row map, as an independent Java implementation. */
    public static List<io.rowweave.DataRow> mapRows(List<io.rowweave.DataRow> in,
                                                  java.util.function.Function<io.rowweave.DataRow, io.rowweave.DataRow> fn) {
        List<io.rowweave.DataRow> out = new ArrayList<>();
        for (io.rowweave.DataRow r : in) {
            io.rowweave.DataRow o = fn.apply(r);
            if (o != null) {
                out.add(o);
            }
        }
        return out;
    }

    /** List concatenation: the union oracle. */
    @SafeVarargs
    public static List<io.rowweave.DataRow> concat(List<io.rowweave.DataRow>... lists) {
        List<io.rowweave.DataRow> out = new ArrayList<>();
        for (List<io.rowweave.DataRow> l : lists) {
            out.addAll(l);
        }
        return out;
    }
}
