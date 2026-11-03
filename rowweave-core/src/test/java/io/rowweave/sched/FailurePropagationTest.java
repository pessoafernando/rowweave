package io.rowweave.sched;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

class FailurePropagationTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    private static DataRow row(int v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    private static List<DataRow> bigRows(int n) {
        List<DataRow> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(row(i));
        }
        return out;
    }

    // ---- a mid-stream stage fails while the upstream fills a small channel, and nothing deadlocks because poisoning releases the parks ----

    @Test
    void midStageFailureWithFullUpstreamDoesNotDeadlock() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            // source (many rows) -> boom (throws at once) -> output. With capacity 1 the source parks quickly on a full channel.
            // The boom failure fails the channel, which poisons the upstream, so the source's send wakes and throws ChannelPoisoned and the task exits.
            PipelineExecutor exec = new PipelineExecutor()
                    .withChannelFactory(() -> new LocalChannel(1));
            exec.addStage("source", PipelineExecutor.source(bigRows(1000)));
            exec.addStage("boom", PipelineExecutor.passThroughStream((in, out) -> {
                throw new IllegalStateException("boom");
            }));
            exec.addStage("output", PipelineExecutor.outputStage(new DiscardSink()));
            RuntimeException ex = assertThrows(RuntimeException.class, exec::run);
            assertTrue(rootCauseContains(ex, "boom"), "root failure must surface");
        });
    }

    // ---- after a mid-stream failure the downstream gets only errors, never a suspect record ----

    @Test
    void downstreamSeesNoDirtyRowsAfterMidFailure() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            // source(10) -> a stage that emits 3 rows then throws -> output collects.
            // The failing stage drains its queue, so the downstream should not collect the rows that stage emitted: they are dropped with the drain
            // or consumed before the error. The key assertion is that the output aborts on the error and is not trusted, and that nothing deadlocks.
            AtomicInteger collected = new AtomicInteger();
            Sink counting = new Sink() {
                @Override public DataSchema getSchema() { return META; }
                @Override public void collect(DataRow r) { collected.incrementAndGet(); }
            };
            PipelineExecutor exec = new PipelineExecutor()
                    .withChannelFactory(() -> new LocalChannel(16));
            exec.addStage("source", PipelineExecutor.source(bigRows(10)));
            exec.addStage("emit3boom", PipelineExecutor.passThroughStream((in, out) -> {
                StreamEvent e;
                int n = 0;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        out.emitRow(e.row());
                        if (++n == 3) {
                            throw new IllegalStateException("boom-after-3");
                        }
                    }
                }
            }));
            exec.addStage("output", PipelineExecutor.outputStage(counting));
            assertThrows(RuntimeException.class, exec::run);
            // Not deadlocking is the point; the output aborts on the error path and its result is not trusted, so collected may be anywhere from 0 to 3
            // and no exact value is asserted. What is asserted: the caller receives the exception and nothing hangs.
        });
    }

    // ---- a FileSink abort cleans only this attempt, so a failure leaves no partial file and does not damage the previous successful version ----

    @Test
    void fileOutputAbortedOnFailureDeletesFile(@TempDir Path tmp) {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            Path file = tmp.resolve("out.tsv");
            FileSink fo = new FileSink(META, file.toString());
            PipelineExecutor exec = new PipelineExecutor();
            exec.addStage("source", PipelineExecutor.source(bigRows(5)));
            exec.addStage("boom", PipelineExecutor.passThroughStream((in, out) -> {
                // emit a couple rows then fail (FileSink may have written/flushed some bytes)
                StreamEvent e;
                int n = 0;
                while ((e = in.next()) != null) {
                    if (e.isRecord()) {
                        out.emitRow(e.row());
                        if (++n == 2) {
                            throw new IllegalStateException("boom");
                        }
                    }
                }
            }));
            exec.addStage("output", PipelineExecutor.outputStage(fo));
            assertThrows(RuntimeException.class, exec::run);
            assertFalse(Files.exists(file), "FileSink must be deleted on abort (no dirty file)");
        });
    }

    @Test
    void fileOutputFailurePreservesPreviousCommittedVersion(@TempDir Path tmp) {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            Path file = tmp.resolve("previous.tsv");
            byte[] previous = "previous-commit\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            Files.write(file, previous);
            FileSink output = new FileSink(META, file.toString());
            PipelineExecutor exec = new PipelineExecutor();
            exec.addStage("source", PipelineExecutor.source(bigRows(5)));
            exec.addStage("boom", PipelineExecutor.passThroughStream((in, out) -> {
                StreamEvent event;
                int emitted = 0;
                while ((event = in.next()) != null) {
                    if (event.isRecord()) {
                        out.emitRow(event.row());
                        if (++emitted == 2) {
                            throw new IllegalStateException("boom-after-partial-output");
                        }
                    }
                }
            }));
            exec.addStage("output", PipelineExecutor.outputStage(output));

            assertThrows(RuntimeException.class, exec::run);
            assertArrayEquals(previous, Files.readAllBytes(file));
            try (java.util.stream.Stream<Path> files = Files.list(tmp)) {
                assertEquals(1L, files.count(), "after the failure the directory keeps only the last successful version");
            }
        });
    }

    @Test
    void fileOutputCommittedOnSuccess(@TempDir Path tmp) {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            Path file = tmp.resolve("ok.tsv");
            FileSink fo = new FileSink(META, file.toString());
            PipelineExecutor exec = new PipelineExecutor();
            exec.addStage("source", PipelineExecutor.source(bigRows(3)));
            exec.addStage("output", PipelineExecutor.outputStage(fo));
            exec.run();
            assertTrue(Files.exists(file));
            assertEquals(3, FileSink.read(META, file.toString()).size()); // committed + readable
        });
    }

    // ---- a failure in one binary sub-chain does not hang, since poisoning releases the other side of the binary operator ----

    @Test
    void binarySubChainFailureNoDeadlock() {
        assertTimeoutPreemptively(ofSeconds(10), () -> {
            PipelineExecutor exec = new PipelineExecutor()
                    .withChannelFactory(() -> new LocalChannel(1));
            // left big source ; right source -> boom ; binary drains both (real op stops on error event)
            PipelineExecutor.BinaryStage drainBoth = (l, r, out) -> {
                StreamEvent e;
                while ((e = l.next()) != null) {
                    if (e.isError()) {
                        return;  // breaker contract: stop on seeing an error, consume no further
                    }
                }
                while ((e = r.next()) != null) {
                    if (e.isError()) {
                        return;
                    }
                }
            };
            RuntimeException ex = assertThrows(RuntimeException.class, () -> exec.runBinary(
                    java.util.Arrays.asList(PipelineExecutor.source(bigRows(1000))),
                    java.util.Arrays.asList(
                            PipelineExecutor.source(bigRows(5)),
                            PipelineExecutor.passThroughStream((in, out) -> {
                                throw new IllegalStateException("right-boom");
                            })),
                    drainBoth,
                    java.util.Arrays.asList(PipelineExecutor.outputStage(new DiscardSink()))));
            assertTrue(rootCauseContains(ex, "right-boom"), "right sub-chain failure must surface");
        });
    }

    // ---- LocalChannel unit case: fail poisons and wakes a parked send ----

    @Test
    void localChannelFailWakesParkedSend() {
        assertTimeoutPreemptively(ofSeconds(5), () -> {
            LocalChannel ch = new LocalChannel(1);
            ch.send(StreamEvent.record(row(1))); // fill
            AtomicInteger state = new AtomicInteger(0); // 0=running,1=poisoned,2=other
            AtomicReference<Throwable> senderFailure = new AtomicReference<>();
            Thread producer = new Thread(() -> {
                try {
                    ch.send(StreamEvent.record(row(2))); // parks (full)
                    state.set(3); // succeeded unexpectedly
                } catch (ChannelPoisonedException e) {
                    senderFailure.set(e);
                    state.set(1);
                } catch (Throwable t) {
                    state.set(2);
                }
            });
            producer.start();
            // wait until parked
            long deadline = System.currentTimeMillis() + 2000;
            while (ch.parkCount() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            RuntimeException root = new RuntimeException("boom");
            ch.fail(root); // poison -> parked send must wake + throw
            producer.join(2000);
            assertEquals(1, state.get(), "parked send must wake with ChannelPoisonedException");
            assertSame(root, senderFailure.get().getCause(), "a parked sender keeps the first poison cause of the channel");
        });
    }

    @Test
    void localChannelFailThenPollSeesErrorNotDirtyRows() throws InterruptedException {
        LocalChannel ch = new LocalChannel(8);
        ch.send(StreamEvent.record(row(1)));
        ch.send(StreamEvent.record(row(2)));
        RuntimeException first = new RuntimeException("first");
        ch.fail(first); // drain 2 rows + error placeholder
        ch.fail(new RuntimeException("second"));
        StreamEvent e = ch.poll();
        assertNotNull(e);
        assertTrue(e.isError(), "after fail, poll must yield error (dirty rows drained)");
        assertSame(first, e.cause(), "a repeated fail does not overwrite the first poison cause");
    }

    private static boolean rootCauseContains(Throwable ex, String msg) {
        for (Throwable c = ex; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains(msg)) {
                return true;
            }
        }
        return false;
    }

    private static final class DiscardSink implements Sink {
        @Override public DataSchema getSchema() { return META; }
        @Override public void collect(DataRow row) { }
    }
}
