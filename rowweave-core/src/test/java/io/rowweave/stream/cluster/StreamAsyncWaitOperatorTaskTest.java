package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.StreamAsyncFunction;
import io.rowweave.StreamAsyncRequest;
import io.rowweave.StreamCollector;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamAsyncOrdering;
import io.rowweave.stream.api.StreamAsyncReplaySpec;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** completions re-enter a bounded operator mailbox. */
class StreamAsyncWaitOperatorTaskTest {

    @Test
    void exactlyOnceRequiresReplayConformanceForTheDeclaredAuthority() {
        StreamTestGraph plan =
                StreamAsyncWaitProductAdmissionTest.plan();
        ControlledLookup lookup = new ControlledLookup();

        UnsupportedOperationException rejected = assertThrows(
                UnsupportedOperationException.class,
                () -> StreamOperatorTaskFactory.create(
                        plan.onlyNode(StreamOperatorCode.ASYNC_WAIT),
                        plan.onlyNode(StreamOperatorCode.SOURCE).outputSchema(),
                        ignored -> lookup, plan.product.plan.maxParallelism(),
                        PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE));
        assertTrue(rejected.getMessage().contains("replay conformance"));

        StreamFunctionResolver conformed = new StreamFunctionResolver() {
            @Override public java.io.Serializable resolve(
                    io.rowweave.portable.FunctionDeploymentReference ignored) {
                return lookup;
            }

            @Override public boolean supportsAsyncReplay(
                    io.rowweave.portable.FunctionDeploymentReference reference,
                    StreamAsyncReplaySpec replay) {
                return reference.equals(
                        StreamAsyncWaitProductAdmissionTest.function())
                        && replay.equals(StreamAsyncReplaySpec.identityKeyed(
                        "request-id-v1",
                        StreamAsyncWaitProductAdmissionTest.digest("replay")));
            }
        };
        StreamOperatorTask admitted = StreamOperatorTaskFactory.create(
                plan.onlyNode(StreamOperatorCode.ASYNC_WAIT),
                plan.onlyNode(StreamOperatorCode.SOURCE).outputSchema(),
                conformed, plan.product.plan.maxParallelism(),
                PortableStreamPlan.DeliveryGuarantee.EXACTLY_ONCE);
        admitted.close();
    }

    @Test
    void orderedWaitDoesNotEmitPastAnIncompleteRequest() throws Exception {
        ControlledLookup lookup = new ControlledLookup();
        StreamOperatorTask task = task(StreamAsyncOrdering.ORDERED, lookup);
        Rows output = new Rows();
        try {
            task.start(System.nanoTime());
            task.process(row(1L), 101L, output);
            task.process(row(2L), 102L, output);
            assertTrue(lookup.invoked.await(2L, TimeUnit.SECONDS));
            eventually(() -> lookup.secondCompleted);
            StreamTaskResourceUsage usage = task.resourceUsage();
            assertTrue(usage.complete());
            assertTrue(usage.managedStateBytes() > 0L);

            task.onIdleTimer(System.nanoTime(), output);
            assertEquals(0, output.rows.size());

            lookup.releaseFirst.countDown();
            eventually(() -> lookup.firstCompleted);
            eventually(() -> {
                task.onIdleTimer(System.nanoTime(), output);
                return output.rows.size() == 2;
            });
            assertEquals(java.util.Arrays.asList(1L, 2L), output.ids());
            assertEquals(java.util.Arrays.asList(101L, 102L),
                    output.timestamps);
        } finally {
            task.close();
        }
    }

    @Test
    void unorderedWaitEmitsReadyCompletionWithoutWaitingForEarlierInput()
            throws Exception {
        ControlledLookup lookup = new ControlledLookup();
        StreamOperatorTask task = task(StreamAsyncOrdering.UNORDERED, lookup);
        Rows output = new Rows();
        try {
            task.start(System.nanoTime());
            task.process(row(1L), 101L, output);
            task.process(row(2L), 102L, output);
            assertTrue(lookup.invoked.await(2L, TimeUnit.SECONDS));
            eventually(() -> lookup.secondCompleted);

            eventually(() -> {
                task.onIdleTimer(System.nanoTime(), output);
                return output.rows.size() == 1;
            });
            assertEquals(java.util.Collections.singletonList(2L),
                    output.ids());

            lookup.releaseFirst.countDown();
            eventually(() -> lookup.firstCompleted);
            eventually(() -> {
                task.onIdleTimer(System.nanoTime(), output);
                return output.rows.size() == 2;
            });
            assertEquals(java.util.Arrays.asList(2L, 1L), output.ids());
        } finally {
            task.close();
        }
    }

    @Test
    void unorderedWaitDoesNotCrossWatermarkOrStatusFrontiers()
            throws Exception {
        ControlLookup lookup = new ControlLookup();
        StreamOperatorTask task = task(StreamAsyncOrdering.UNORDERED, lookup);
        Rows output = new Rows();
        try {
            task.start(System.nanoTime());
            task.process(row(1L), 101L, output);
            task.process(row(2L), 102L, output);
            assertTrue(lookup.twoInvoked.await(2L, TimeUnit.SECONDS));
            eventually(() -> lookup.completed.contains(2L));

            assertFalse(task.onWatermark(200L, output));
            assertFalse(task.onWatermarkStatus(
                    StreamEnvelope.WatermarkStatus.IDLE, 7L, output));
            task.process(row(3L), 103L, output);
            eventually(() -> lookup.completed.contains(3L));
            task.onIdleTimer(System.nanoTime(), output);
            assertEquals(java.util.Collections.singletonList("R2"),
                    output.events);

            lookup.releaseFirst.countDown();
            eventually(() -> lookup.completed.contains(1L));
            eventually(() -> {
                task.onIdleTimer(System.nanoTime(), output);
                return output.events.size() == 5;
            });
            assertEquals(java.util.Arrays.asList(
                    "R2", "R1", "W200", "S:IDLE:7", "R3"),
                    output.events);
        } finally {
            task.close();
        }
    }

    @Test
    void maximumConcurrencyBackpressuresUntilAMailboxCompletionFreesCapacity()
            throws Exception {
        CapacityLookup lookup = new CapacityLookup();
        StreamOperatorTask task = task(StreamAsyncOrdering.UNORDERED, lookup);
        Rows output = new Rows();
        try {
            task.start(System.nanoTime());
            for (long id = 1L; id <= 4L; id++) {
                task.process(row(id), null, output);
            }
            assertTrue(lookup.fourInvoked.await(2L, TimeUnit.SECONDS));
            CompletableFuture<Void> fifth = CompletableFuture.runAsync(
                    () -> task.process(row(5L), null, output));
            Thread.sleep(50L);
            assertTrue(!fifth.isDone(),
                    "capacity must backpressure the fifth input");

            lookup.releaseOne.countDown();
            fifth.get(2L, TimeUnit.SECONDS);
            assertEquals(1, output.rows.size());
            assertTrue(lookup.fifthInvoked.await(2L, TimeUnit.SECONDS));
        } finally {
            lookup.releaseAll.countDown();
            task.close();
        }
    }

    @Test
    void retryKeepsLogicalIdentityAndCanonicalRequest() throws Exception {
        RetryingLookup lookup = new RetryingLookup();
        StreamOperatorTask task = task(StreamAsyncOrdering.ORDERED, lookup);
        Rows output = new Rows();
        try {
            task.start(System.nanoTime());
            task.process(row(7L), 707L, output);
            eventually(() -> lookup.requests.size() == 3);
            eventually(() -> {
                task.onIdleTimer(System.nanoTime(), output);
                return output.rows.size() == 1;
            });

            assertEquals(java.util.Arrays.asList(1, 2, 3),
                    lookup.attempts());
            assertEquals(1L, lookup.identities().stream().distinct().count());
            assertEquals(1L, lookup.frames().stream().distinct().count());
            assertEquals(7L, output.rows.get(0).getLong(0));
            assertEquals(Long.valueOf(707L), output.timestamps.get(0));
        } finally {
            task.close();
        }
    }

    @Test
    void timeoutInterruptsEachAttemptAndExhaustionFailsTheOperator()
            throws Exception {
        TimeoutLookup lookup = new TimeoutLookup();
        StreamTestGraph plan =
                StreamAsyncWaitProductAdmissionTest.plan(
                        StreamAsyncOrdering.ORDERED, 20L, 3, 1L, 2L);
        StreamOperatorTask task = StreamOperatorTaskFactory.create(
                plan.onlyNode(StreamOperatorCode.ASYNC_WAIT),
                plan.onlyNode(StreamOperatorCode.SOURCE).outputSchema(),
                ignored -> lookup, plan.product.plan.maxParallelism());
        Rows output = new Rows();
        try {
            task.start(System.nanoTime());
            task.process(row(9L), null, output);
            assertTrue(lookup.threeInvoked.await(2L, TimeUnit.SECONDS));

            Throwable failure = awaitMailboxFailure(task, output);
            assertTrue(failure.getMessage().contains("timed out"));
            assertEquals(java.util.Arrays.asList(1, 2, 3),
                    lookup.attempts());
            assertEquals(1L,
                    lookup.identities().stream().distinct().count());
            assertEquals(0, output.rows.size());
        } finally {
            task.close();
        }
    }

    @Test
    void escapedCompletionCollectorPoisonsTheAttempt() throws Exception {
        EscapingLookup lookup = new EscapingLookup();
        StreamOperatorTask task = task(StreamAsyncOrdering.ORDERED, lookup);
        Rows output = new Rows();
        try {
            task.start(System.nanoTime());
            task.process(row(8L), null, output);
            eventually(() -> {
                task.onIdleTimer(System.nanoTime(), output);
                return lookup.escaped != null;
            });
            assertEquals(java.util.Collections.singletonList(8L),
                    output.ids());

            CompletableFuture<Void> escaped = CompletableFuture.runAsync(
                    () -> lookup.escaped.collect(row(88L)));
            assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> escaped.get(2L, TimeUnit.SECONDS));
            eventually(() -> output.failure != null);
            assertTrue(output.failure.getMessage().contains(
                    "escaped its mailbox callback"));
        } finally {
            task.close();
        }
    }

    @Test
    void resultRowAndByteLimitsFailInsideTheMailbox() throws Exception {
        StreamOperatorTask rows = task(
                StreamEmissionLimit.of(1, 8_192L),
                new EmittingLookup(2, "ok"));
        Rows rowOutput = new Rows();
        try {
            rows.start(System.nanoTime());
            rows.process(row(10L), null, rowOutput);
            Throwable failure = awaitMailboxFailure(rows, rowOutput);
            assertTrue(failure.getMessage().contains("emission limit"));
            assertEquals(1, rowOutput.rows.size());
        } finally {
            rows.close();
        }

        StreamOperatorTask bytes = task(
                StreamEmissionLimit.of(1, 8L),
                new EmittingLookup(1,
                        "a-value-that-cannot-fit-in-eight-bytes"));
        Rows byteOutput = new Rows();
        try {
            bytes.start(System.nanoTime());
            bytes.process(row(11L), null, byteOutput);
            Throwable failure = awaitMailboxFailure(bytes, byteOutput);
            assertTrue(failure.getMessage().contains("emission limit"));
            assertEquals(0, byteOutput.rows.size());
        } finally {
            bytes.close();
        }
    }

    @Test
    void completionCannotChangeAnIdentityStableKeyValue() throws Exception {
        StreamOperatorTask task = task(StreamAsyncOrdering.ORDERED,
                new KeyChangingLookup());
        Rows output = new Rows();
        try {
            task.start(System.nanoTime());
            task.process(row(12L), null, output);

            Throwable failure = awaitMailboxFailure(task, output);
            assertTrue(failure.getMessage().contains(
                    "changed its stable output key"));
            assertEquals(0, output.rows.size());
        } finally {
            task.close();
        }
    }

    @Test
    void checkpointRestoresSameInflightIdentityAndCompletedOrderedResponse(
            @TempDir Path root) throws Exception {
        StreamTestGraph plan =
                StreamAsyncWaitProductAdmissionTest.plan();
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 65_536L);
        CheckpointLookup beforeLookup = new CheckpointLookup(true);
        StreamOperatorTask before = StreamOperatorTaskFactory.create(
                plan.onlyNode(StreamOperatorCode.ASYNC_WAIT),
                plan.onlyNode(StreamOperatorCode.SOURCE).outputSchema(),
                ignored -> beforeLookup, plan.product.plan.maxParallelism());
        Rows held = new Rows();
        StreamCheckpointMember member;
        String inflightIdentity;
        try {
            before.start(System.nanoTime());
            before.process(row(1L), 101L, held);
            before.process(row(2L), 102L, held);
            assertTrue(beforeLookup.twoInvoked.await(2L, TimeUnit.SECONDS));
            eventually(() -> beforeLookup.secondCompleted);
            assertFalse(before.onWatermark(200L, held));
            assertEquals(0, held.rows.size());
            inflightIdentity = beforeLookup.identityFor(1L);

            member = before.snapshot(checkpoint(plan), payloads);
            assertEquals(StreamCheckpointMember.Kind.STATE, member.kind());
        } finally {
            before.close();
        }

        CheckpointLookup afterLookup = new CheckpointLookup(false);
        StreamOperatorTask restored = StreamOperatorTaskFactory.create(
                plan.onlyNode(StreamOperatorCode.ASYNC_WAIT),
                plan.onlyNode(StreamOperatorCode.SOURCE).outputSchema(),
                ignored -> afterLookup, plan.product.plan.maxParallelism());
        Rows output = new Rows();
        try {
            restored.restore(member, payloads);
            restored.start(System.nanoTime());
            eventually(() -> {
                restored.onIdleTimer(System.nanoTime(), output);
                return output.rows.size() == 2;
            });

            assertEquals(java.util.Arrays.asList(1L, 2L), output.ids());
            assertEquals(java.util.Arrays.asList(
                    "R1", "R2", "W200"), output.events);
            assertEquals(java.util.Collections.singletonList(1L),
                    afterLookup.ids());
            assertEquals(inflightIdentity, afterLookup.identityFor(1L));
        } finally {
            restored.close();
        }
    }

    @Test
    void endOfInputWaitsForEveryPendingCompletion() throws Exception {
        ControlledLookup lookup = new ControlledLookup();
        StreamOperatorTask task = task(StreamAsyncOrdering.ORDERED, lookup);
        Rows output = new Rows();
        try {
            task.start(System.nanoTime());
            task.process(row(1L), null, output);
            task.process(row(2L), null, output);
            assertTrue(lookup.invoked.await(2L, TimeUnit.SECONDS));
            assertFalse(task.onEnd(output));
            assertEquals(0, output.ends);

            lookup.releaseFirst.countDown();
            eventually(() -> {
                task.onIdleTimer(System.nanoTime(), output);
                return output.ends == 1;
            });
            assertEquals(java.util.Arrays.asList(1L, 2L), output.ids());
        } finally {
            task.close();
        }
    }

    private static StreamOperatorTask task(StreamAsyncOrdering ordering,
            StreamAsyncFunction lookup) {
        StreamTestGraph plan =
                StreamAsyncWaitProductAdmissionTest.plan(ordering);
        PortableRowSchema input = plan.onlyNode(
                StreamOperatorCode.SOURCE).outputSchema();
        return StreamOperatorTaskFactory.create(
                plan.onlyNode(StreamOperatorCode.ASYNC_WAIT), input,
                ignored -> lookup, plan.product.plan.maxParallelism());
    }

    private static StreamOperatorTask task(StreamEmissionLimit emission,
            StreamAsyncFunction lookup) {
        StreamTestGraph plan =
                StreamAsyncWaitProductAdmissionTest.plan(
                        StreamAsyncOrdering.ORDERED,
                        1_000L, 1, 0L, 0L, emission);
        return StreamOperatorTaskFactory.create(
                plan.onlyNode(StreamOperatorCode.ASYNC_WAIT),
                plan.onlyNode(StreamOperatorCode.SOURCE).outputSchema(),
                ignored -> lookup, plan.product.plan.maxParallelism());
    }

    private static DataRow row(long id) {
        DataRow row = new DataRow(1);
        row.set(0, id);
        return row;
    }

    private static final class ControlledLookup
            implements StreamAsyncFunction {
        private final CountDownLatch invoked = new CountDownLatch(2);
        private final CountDownLatch releaseFirst = new CountDownLatch(1);
        private volatile boolean firstCompleted;
        private volatile boolean secondCompleted;

        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }

        @Override public byte[] invoke(StreamAsyncRequest request)
                throws Exception {
            long id = ByteBuffer.wrap(request.requestFrame()).getLong();
            invoked.countDown();
            if (id == 1L) releaseFirst.await();
            if (id == 1L) firstCompleted = true;
            else secondCompleted = true;
            return request.requestFrame();
        }

        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            DataRow result = new DataRow(2);
            result.set(0, input.getLong(0));
            result.set(1, "customer-" +
                    ByteBuffer.wrap(responseFrame).getLong());
            output.collect(result);
        }
    }

    private static final class CapacityLookup
            implements StreamAsyncFunction {
        private final CountDownLatch fourInvoked = new CountDownLatch(4);
        private final CountDownLatch fifthInvoked = new CountDownLatch(1);
        private final CountDownLatch releaseOne = new CountDownLatch(1);
        private final CountDownLatch releaseAll = new CountDownLatch(1);

        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }

        @Override public byte[] invoke(StreamAsyncRequest request)
                throws Exception {
            long id = ByteBuffer.wrap(request.requestFrame()).getLong();
            if (id <= 4L) fourInvoked.countDown();
            else fifthInvoked.countDown();
            if (id == 1L) releaseOne.await();
            else releaseAll.await();
            return request.requestFrame();
        }

        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            DataRow result = new DataRow(2);
            result.set(0, input.getLong(0));
            result.set(1, "done");
            output.collect(result);
        }
    }

    private static final class ControlLookup
            implements StreamAsyncFunction {
        private final CountDownLatch twoInvoked = new CountDownLatch(2);
        private final CountDownLatch releaseFirst = new CountDownLatch(1);
        private final java.util.concurrent.CopyOnWriteArrayList<Long>
                completed = new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }

        @Override public byte[] invoke(StreamAsyncRequest request)
                throws Exception {
            long id = ByteBuffer.wrap(request.requestFrame()).getLong();
            twoInvoked.countDown();
            if (id == 1L) releaseFirst.await();
            completed.add(id);
            return request.requestFrame();
        }

        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            DataRow result = new DataRow(2);
            result.set(0, input.getLong(0));
            result.set(1, "control");
            output.collect(result);
        }
    }

    private static final class RetryingLookup
            implements StreamAsyncFunction {
        private final java.util.concurrent.CopyOnWriteArrayList<
                StreamAsyncRequest> requests =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }

        @Override public byte[] invoke(StreamAsyncRequest request) {
            requests.add(request);
            if (request.attempt() < 3) {
                throw new IllegalStateException("retry me");
            }
            return request.requestFrame();
        }

        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            DataRow result = new DataRow(2);
            result.set(0, input.getLong(0));
            result.set(1, "retried");
            output.collect(result);
        }

        private List<Integer> attempts() {
            List<Integer> result = new ArrayList<>();
            for (StreamAsyncRequest request : requests) {
                result.add(request.attempt());
            }
            return result;
        }

        private List<String> identities() {
            List<String> result = new ArrayList<>();
            for (StreamAsyncRequest request : requests) {
                result.add(request.operationIdentity());
            }
            return result;
        }

        private List<String> frames() {
            List<String> result = new ArrayList<>();
            for (StreamAsyncRequest request : requests) {
                result.add(java.util.Arrays.toString(
                        request.requestFrame()));
            }
            return result;
        }
    }

    private static final class TimeoutLookup
            implements StreamAsyncFunction {
        private final CountDownLatch threeInvoked = new CountDownLatch(3);
        private final java.util.concurrent.CopyOnWriteArrayList<
                StreamAsyncRequest> requests =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }

        @Override public byte[] invoke(StreamAsyncRequest request)
                throws Exception {
            requests.add(request);
            threeInvoked.countDown();
            Thread.sleep(10_000L);
            return request.requestFrame();
        }

        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            throw new AssertionError("timed-out lookup must not complete");
        }

        private List<Integer> attempts() {
            List<Integer> result = new ArrayList<>();
            for (StreamAsyncRequest request : requests) {
                result.add(request.attempt());
            }
            return result;
        }

        private List<String> identities() {
            List<String> result = new ArrayList<>();
            for (StreamAsyncRequest request : requests) {
                result.add(request.operationIdentity());
            }
            return result;
        }
    }

    private static final class EscapingLookup
            implements StreamAsyncFunction {
        private volatile StreamCollector escaped;

        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }

        @Override public byte[] invoke(StreamAsyncRequest request) {
            return request.requestFrame();
        }

        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            escaped = output;
            DataRow result = new DataRow(2);
            result.set(0, input.getLong(0));
            result.set(1, "escaped");
            output.collect(result);
        }
    }

    private static final class EmittingLookup
            implements StreamAsyncFunction {
        private final int outputs;
        private final String value;

        private EmittingLookup(int outputs, String value) {
            this.outputs = outputs;
            this.value = value;
        }

        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }

        @Override public byte[] invoke(StreamAsyncRequest request) {
            return request.requestFrame();
        }

        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            for (int index = 0; index < outputs; index++) {
                DataRow row = new DataRow(2);
                row.set(0, input.getLong(0));
                row.set(1, value);
                output.collect(row);
            }
        }
    }

    private static final class KeyChangingLookup
            implements StreamAsyncFunction {
        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }

        @Override public byte[] invoke(StreamAsyncRequest request) {
            return request.requestFrame();
        }

        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            DataRow changed = new DataRow(2);
            changed.set(0, input.getLong(0) + 1L);
            changed.set(1, "changed");
            output.collect(changed);
        }
    }

    private static final class CheckpointLookup
            implements StreamAsyncFunction {
        private final boolean blockFirst;
        private final CountDownLatch twoInvoked;
        private final CountDownLatch releaseFirst = new CountDownLatch(1);
        private final java.util.concurrent.CopyOnWriteArrayList<
                StreamAsyncRequest> requests =
                new java.util.concurrent.CopyOnWriteArrayList<>();
        private volatile boolean secondCompleted;

        private CheckpointLookup(boolean blockFirst) {
            this.blockFirst = blockFirst;
            this.twoInvoked = new CountDownLatch(blockFirst ? 2 : 1);
        }

        @Override public byte[] createRequest(DataRow input) {
            return ByteBuffer.allocate(Long.BYTES)
                    .putLong(input.getLong(0)).array();
        }

        @Override public byte[] invoke(StreamAsyncRequest request)
                throws Exception {
            requests.add(request);
            twoInvoked.countDown();
            long id = ByteBuffer.wrap(request.requestFrame()).getLong();
            if (blockFirst && id == 1L) releaseFirst.await();
            if (id == 2L) secondCompleted = true;
            return request.requestFrame();
        }

        @Override public void complete(DataRow input, byte[] responseFrame,
                StreamCollector output) {
            DataRow result = new DataRow(2);
            result.set(0, input.getLong(0));
            result.set(1, "restored");
            output.collect(result);
        }

        private String identityFor(long id) {
            for (StreamAsyncRequest request : requests) {
                if (ByteBuffer.wrap(request.requestFrame()).getLong() == id) {
                    return request.operationIdentity();
                }
            }
            throw new AssertionError("request was not invoked: " + id);
        }

        private List<Long> ids() {
            List<Long> result = new ArrayList<>();
            for (StreamAsyncRequest request : requests) {
                result.add(ByteBuffer.wrap(
                        request.requestFrame()).getLong());
            }
            return result;
        }
    }

    private static final class Rows implements StreamOperatorTask.Sink {
        private final List<DataRow> rows = new ArrayList<>();
        private final List<Long> timestamps = new ArrayList<>();
        private final List<String> events = new ArrayList<>();
        private volatile Throwable failure;
        private int ends;
        @Override public void emit(DataRow row, Long eventTimestamp) {
            rows.add(row);
            timestamps.add(eventTimestamp);
            events.add("R" + row.getLong(0));
        }
        @Override public void emitWatermark(long timestamp) {
            events.add("W" + timestamp);
        }
        @Override public void emitWatermarkStatus(
                StreamEnvelope.WatermarkStatus status, long generation) {
            events.add("S:" + status + ':' + generation);
        }
        @Override public void fail(Throwable cause) { failure = cause; }
        @Override public void emitEnd() { ends++; }
        private List<Long> ids() {
            List<Long> ids = new ArrayList<>();
            for (DataRow row : rows) ids.add(row.getLong(0));
            return ids;
        }
    }

    private static void eventually(Check condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (!condition.value() && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(condition.value(), "condition did not become true");
    }

    private static Throwable awaitMailboxFailure(
            StreamOperatorTask task, Rows output) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
        while (System.nanoTime() < deadline) {
            try {
                task.onIdleTimer(System.nanoTime(), output);
            } catch (Throwable failure) {
                return failure;
            }
            Thread.sleep(5L);
        }
        throw new AssertionError("async failure did not reach the mailbox");
    }

    private static CheckpointIdentity checkpoint(
            StreamTestGraph plan) {
        return new CheckpointIdentity(
                plan.product.artifact.definitionId(),
                plan.product.artifact.artifactId(),
                "run", "attempt", 1L, 1L, "coordinator", 1L,
                plan.product.commandDigest,
                plan.product.artifact.artifactId(), 1, 1);
    }

    private interface Check { boolean value(); }
}
