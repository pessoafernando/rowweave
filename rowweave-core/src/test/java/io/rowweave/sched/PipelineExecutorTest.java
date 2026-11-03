package io.rowweave.sched;

import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

class PipelineExecutorTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    private static DataRow row(Object v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    private static List<DataRow> rows(Object... vs) {
        List<DataRow> out = new ArrayList<>();
        for (Object v : vs) {
            out.add(row(v));
        }
        return out;
    }

    private static List<Object> collectCol0(List<DataRow> rows) {
        List<Object> out = new ArrayList<>();
        for (DataRow r : rows) {
            out.add(r.get(0));
        }
        return out;
    }

    @Test
    void breakerCompletionFailureIsSuppressedUnderBusinessRoot() {
        RuntimeException business = new RuntimeException("body-root");
        RuntimeException cleanup = new RuntimeException("onDone-cleanup");
        java.util.concurrent.atomic.AtomicInteger doneCalls = new java.util.concurrent.atomic.AtomicInteger();
        PipelineExecutor.Stage stage = PipelineExecutor.streamingBreaker((in, out) -> {
            throw business;
        }, () -> {
            doneCalls.incrementAndGet();
            throw cleanup;
        });

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> stage.run(() -> null, event -> { }));

        assertSame(business, actual);
        assertEquals(1, doneCalls.get());
        assertEquals(1, business.getSuppressed().length);
        assertSame(cleanup, business.getSuppressed()[0]);
    }

    @Test
    void breakerCompletionKeepsSingleFailureSemanticsAndRunsExactlyOnce() {
        java.util.concurrent.atomic.AtomicInteger successDoneCalls = new java.util.concurrent.atomic.AtomicInteger();
        RuntimeException cleanup = new RuntimeException("cleanup-only");
        PipelineExecutor.Stage successfulBody = PipelineExecutor.streamingBreaker(
                (in, out) -> { }, () -> {
                    successDoneCalls.incrementAndGet();
                    throw cleanup;
                });
        assertSame(cleanup, assertThrows(RuntimeException.class,
                () -> successfulBody.run(() -> null, event -> { })));
        assertEquals(1, successDoneCalls.get());

        java.util.concurrent.atomic.AtomicInteger failedDoneCalls = new java.util.concurrent.atomic.AtomicInteger();
        RuntimeException business = new RuntimeException("body-only");
        PipelineExecutor.Stage failedBody = PipelineExecutor.streamingBreaker((in, out) -> {
            throw business;
        }, failedDoneCalls::incrementAndGet);
        assertSame(business, assertThrows(RuntimeException.class,
                () -> failedBody.run(() -> null, event -> { })));
        assertEquals(1, failedDoneCalls.get());
    }

    @Test
    void breakerCompletionAlsoPreservesErrorRoot() {
        AssertionError business = new AssertionError("body-error");
        AssertionError cleanup = new AssertionError("cleanup-error");
        PipelineExecutor.Stage stage = PipelineExecutor.streamingBreaker((in, out) -> {
            throw business;
        }, () -> {
            throw cleanup;
        });

        AssertionError actual = assertThrows(AssertionError.class,
                () -> stage.run(() -> null, event -> { }));

        assertSame(business, actual);
        assertEquals(1, business.getSuppressed().length);
        assertSame(cleanup, business.getSuppressed()[0]);
    }

    @Test
    void binaryTeardownCannotReplaceFrozenBusinessPrimaryWithEarlierTaskFailure() {
        RuntimeException teardownFailure = new RuntimeException("left teardown failed");
        RuntimeException business = new RuntimeException("right business failed");
        CountDownLatch leftStarted = new CountDownLatch(1);
        PipelineExecutor.Stage left = (in, out) -> {
            leftStarted.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw teardownFailure;
            }
        };
        PipelineExecutor.Stage right = (in, out) -> {
            try {
                assertTrue(leftStarted.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(interrupted);
            }
            throw business;
        };
        PipelineExecutor.BinaryStage binary = (l, r, out) -> {
            while (l.next() != null) { }
            while (r.next() != null) { }
        };

        RuntimeException observed = assertThrows(RuntimeException.class, () ->
                new PipelineExecutor().runBinary(
                        Collections.singletonList(left), Collections.singletonList(right),
                        binary, Collections.singletonList((in, out) -> {
                            while (in.next() != null) { }
                        })));

        assertSame(business, observed.getCause());
        assertTrue(Arrays.asList(business.getSuppressed()).contains(teardownFailure),
                "the exact teardown failure remains evidence under the frozen business root");
    }

    @Test
    void threeStageSourcePassThroughToOutputStage() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            CollectingSink out = new CollectingSink(META);
            PipelineExecutor exec = new PipelineExecutor();
            exec.addStage("source", PipelineExecutor.source(rows(1, 2, 3)));
            exec.addStage("plus10", PipelineExecutor.passThroughStream((in, sink) -> {
                StreamEvent e;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        sink.emitRow(row((Integer) e.row().get(0) + 10));
                    }
                }
            }));
            exec.addStage("times2", PipelineExecutor.passThroughStream((in, sink) -> {
                StreamEvent e;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        sink.emitRow(row((Integer) e.row().get(0) * 2));
                    }
                }
            }));
            exec.addStage("output", PipelineExecutor.outputStage(out));
            exec.run(); // no-arg, self-contained
            assertEquals(Arrays.asList(22, 24, 26), collectCol0(out.getRows()));
        });
    }

/** runFanout computes a shared chain once and broadcasts it to two branches, each with its own output and its own correct transformation. */
    @Test
    void fanoutBroadcastsToTwoBranches() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            CollectingSink o1 = new CollectingSink(META);
            CollectingSink o2 = new CollectingSink(META);
            PipelineExecutor exec = new PipelineExecutor();
/** The shared chain: source(1,2,3) then plus ten, giving 11, 12 and 13, computed only once. */
            List<PipelineExecutor.Stage> shared = Arrays.asList(
                    PipelineExecutor.source(rows(1, 2, 3)),
                    PipelineExecutor.passThroughStream((in, sink) -> {
                        StreamEvent e;
                        while ((e = in.next()) != null) if (e.isRecord()) sink.emitRow(row((Integer) e.row().get(0) + 10));
                    }));
/** Branch 1 doubles into o1, giving 22, 24 and 26; branch 2 times a hundred into o2, giving 1100, 1200 and 1300. */
            List<PipelineExecutor.Stage> b1 = Arrays.asList(
                    PipelineExecutor.passThroughStream((in, sink) -> {
                        StreamEvent e;
                        while ((e = in.next()) != null) if (e.isRecord()) sink.emitRow(row((Integer) e.row().get(0) * 2));
                    }),
                    PipelineExecutor.outputStage(o1));
            List<PipelineExecutor.Stage> b2 = Arrays.asList(
                    PipelineExecutor.passThroughStream((in, sink) -> {
                        StreamEvent e;
                        while ((e = in.next()) != null) if (e.isRecord()) sink.emitRow(row((Integer) e.row().get(0) * 100));
                    }),
                    PipelineExecutor.outputStage(o2));
            exec.runFanout(shared, Arrays.asList(b1, b2));
            assertEquals(Arrays.asList(22, 24, 26), collectCol0(o1.getRows()), "branch 1 = shared rows times 2");
            assertEquals(Arrays.asList(1100, 1200, 1300), collectCol0(o2.getRows()), "branch 2 = shared rows times 100");
        });
    }

/** One branch mutating a row in place does not affect the other, because the defensive copy is in effect. */
    @Test
    void fanoutBranchMutationDoesNotBleed() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            CollectingSink o1 = new CollectingSink(META);
            CollectingSink o2 = new CollectingSink(META);
            List<PipelineExecutor.Stage> shared = Collections.singletonList(PipelineExecutor.source(rows(1, 2, 3)));
/** Branch 1 sets column 0 to 999 in place and emits the same reference, which would contaminate branch 2 without a copy. */
            List<PipelineExecutor.Stage> b1 = Arrays.asList(
                    PipelineExecutor.passThroughStream((in, sink) -> {
                        StreamEvent e;
                        while ((e = in.next()) != null) if (e.isRecord()) { DataRow r = e.row(); r.set(0, 999); sink.emitRow(r); }
                    }),
                    PipelineExecutor.outputStage(o1));
/** Branch 2 passes rows through and must see the original 1, 2 and 3, unmutated by branch 1. */
            List<PipelineExecutor.Stage> b2 = Arrays.asList(
                    PipelineExecutor.passThroughStream((in, sink) -> {
                        StreamEvent e;
                        while ((e = in.next()) != null) if (e.isRecord()) sink.emitRow(e.row());
                    }),
                    PipelineExecutor.outputStage(o2));
            exec().runFanout(shared, Arrays.asList(b1, b2));
/** Branch 2 must be unaffected by the in-place mutation in branch 1, which is the defensive copy. Note that the last branch skips the copy, so the mutating branch is not placed last. */
            assertEquals(Arrays.asList(1, 2, 3), collectCol0(o2.getRows()), "branch 2 is not polluted by the mutation branch 1 made to the shared row");
        });
    }

/** A shallow DataRow copy is not enough to isolate a mutable Timestamp, so a fixed schedule checks that no cell reference is shared between branches. */
    @Test
    void fanoutMutableTimestampDoesNotBleedAcrossBranches() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            DataSchema timeMeta = new DataSchema(new Field("ts", DataType.TimestampType));
            Timestamp timestamp = new Timestamp(1_700_000_000_123L);
            timestamp.setNanos(123_456_789);
            int originalNanos = timestamp.getNanos();
            DataRow source = row(timestamp);
            CountDownLatch mutated = new CountDownLatch(1);
            CollectingSink mutatingOut = new CollectingSink(timeMeta);
            CollectingSink observingOut = new CollectingSink(timeMeta);

            List<PipelineExecutor.Stage> mutatingBranch = Arrays.asList(
                    PipelineExecutor.passThroughStream((in, sink) -> {
                        StreamEvent e;
                        while ((e = in.next()) != null) {
                            if (e.isRecord()) {
                                e.row().getTimestamp(0).setNanos(987_654_321);
                                mutated.countDown();
                                sink.emitRow(e.row());
                            }
                        }
                    }),
                    PipelineExecutor.outputStage(mutatingOut));
            List<PipelineExecutor.Stage> observingBranch = Arrays.asList(
                    PipelineExecutor.passThroughStream((in, sink) -> {
                        StreamEvent e;
                        while ((e = in.next()) != null) {
                            if (e.isRecord()) {
                                try {
                                    assertTrue(mutated.await(5, TimeUnit.SECONDS));
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(interrupted);
                                }
                                sink.emitRow(e.row());
                            }
                        }
                    }),
                    PipelineExecutor.outputStage(observingOut));

            exec().runFanout(Collections.singletonList(PipelineExecutor.source(Collections.singletonList(source))),
                    Arrays.asList(mutatingBranch, observingBranch));

            assertEquals(987_654_321, mutatingOut.getRows().get(0).getTimestamp(0).getNanos());
            assertEquals(originalNanos, observingOut.getRows().get(0).getTimestamp(0).getNanos(),
                    "changing a Timestamp in a non-final branch must not leak into the other branch");
        });
    }

    private static PipelineExecutor exec() { return new PipelineExecutor(); }

    @Test
    void outputStageClosesOutput() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            AtomicBoolean closed = new AtomicBoolean(false);
            Sink out = new Sink() {
                @Override
                public DataSchema getSchema() {
                    return META;
                }
                @Override
                public void collect(DataRow row) {
                    // discard
                }
                @Override
                public void close() {
                    closed.set(true);
                }
            };
            PipelineExecutor exec = new PipelineExecutor();
            exec.addStage("source", PipelineExecutor.source(rows(1, 2)));
            exec.addStage("output", PipelineExecutor.outputStage(out));
            exec.run();
            assertTrue(closed.get(), "outputStage must close the Sink after draining");
        });
    }

    @Test
    void emptySourceProducesNothingAndDoesNotHang() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            CollectingSink out = new CollectingSink(META);
            PipelineExecutor exec = new PipelineExecutor();
            exec.addStage("source", PipelineExecutor.source(Collections.emptyList()));
            exec.addStage("identity", PipelineExecutor.passThroughStream((in, sink) -> {
                StreamEvent e;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        sink.emitRow(e.row());
                    }
                }
            }));
            exec.addStage("output", PipelineExecutor.outputStage(out));
            exec.run();
            assertTrue(out.getRows().isEmpty());
        });
    }

    @Test
    void backpressureParksProducerWithSlowOutput() {
        assertTimeoutPreemptively(ofSeconds(15), () -> {
            List<Channel> created = Collections.synchronizedList(new ArrayList<>());
            PipelineExecutor exec = new PipelineExecutor()
                    .withChannelFactory(() -> {
                        Channel c = new LocalChannel(1);
                        created.add(c);
                        return c;
                    });
            // slow Sink stage => upstream channels must park
            List<DataRow> sunk = Collections.synchronizedList(new ArrayList<>());
            Sink slow = new Sink() {
                @Override
                public DataSchema getSchema() {
                    return META;
                }
                @Override
                public void collect(DataRow row) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    sunk.add(row);
                }
            };
            exec.addStage("source", PipelineExecutor.source(rows(1, 2, 3, 4, 5, 6, 7, 8)));
            exec.addStage("identity", PipelineExecutor.passThroughStream((in, sink) -> {
                StreamEvent e;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        sink.emitRow(e.row());
                    }
                }
            }));
            exec.addStage("output", PipelineExecutor.outputStage(slow));
            exec.run();
            assertEquals(8, sunk.size());
            long totalParks = 0;
            for (Channel c : created) {
                totalParks += c.parkCount();
            }
            assertTrue(totalParks > 0, "slow output + cap=1 should cause backpressure parks");
        });
    }

    @Test
    void streamingBreakerConsumesAllThenEmitsSorted() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            CollectingSink out = new CollectingSink(META);
            PipelineExecutor exec = new PipelineExecutor();
            exec.addStage("source", PipelineExecutor.source(rows(3, 1, 2)));
            exec.addStage("sort", PipelineExecutor.streamingBreaker((in, sink) -> {
                List<Integer> buf = new ArrayList<>();
                StreamEvent e;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        buf.add((Integer) e.row().get(0));
                    }
                }
                Collections.sort(buf);
                for (Integer v : buf) {
                    sink.emitRow(row(v));
                }
            }));
            exec.addStage("output", PipelineExecutor.outputStage(out));
            exec.run();
            assertEquals(Arrays.asList(1, 2, 3), collectCol0(out.getRows()));
        });
    }

    @Test
    void failFastSurfacesStageException() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            CollectingSink out = new CollectingSink(META);
            PipelineExecutor exec = new PipelineExecutor();
            exec.addStage("source", PipelineExecutor.source(rows(1, 2)));
            exec.addStage("boom", PipelineExecutor.passThroughStream((in, sink) -> {
                throw new IllegalStateException("boom");
            }));
            exec.addStage("output", PipelineExecutor.outputStage(out));
            RuntimeException ex = assertThrows(RuntimeException.class, exec::run);
            Throwable cause = ex;
            boolean found = false;
            while (cause != null) {
                if (cause instanceof IllegalStateException && "boom".equals(cause.getMessage())) {
                    found = true;
                    break;
                }
                cause = cause.getCause();
            }
            assertTrue(found, "fail-fast must surface the stage's root cause");
        });
    }

    @Test
    void runWithNoStagesIsNoop() {
        assertTimeoutPreemptively(ofSeconds(5), () -> new PipelineExecutor().run());
    }
}
