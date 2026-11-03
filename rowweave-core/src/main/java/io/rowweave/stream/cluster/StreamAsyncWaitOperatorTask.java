package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.StreamAsyncFunction;
import io.rowweave.StreamAsyncRequest;
import io.rowweave.StreamCollector;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamAsyncBinding;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.UUID;

/** Engine-owned async lookup executor whose completions re-enter the mailbox. */
final class StreamAsyncWaitOperatorTask implements StreamOperatorTask {
    private static final byte[] STATE_MAGIC =
            "RWXXAS01".getBytes(StandardCharsets.US_ASCII);
    private static final int STATE_VERSION = 3;
    private static final String STATE_NAME = "async-wait-inflight";
    private static final String STATE_CODEC =
            "rowweave.stream.async-wait-state.v3";
    private static final int LEGACY_STATE_VERSION = 2;
    private static final String LEGACY_STATE_CODEC =
            "rowweave.stream.async-wait-state.v2";

    private final PortableStreamNode node;
    private final StreamAsyncBinding binding;
    private final StreamAsyncFunction function;
    private final RowSerializer inputSerializer;
    private final RowSerializer outputSerializer;
    private final RowSerializer inputKeySerializer;
    private final RowSerializer outputKeySerializer;
    private final int[] inputKeyIndices;
    private final int[] outputKeyIndices;
    private final ExecutorService executor;
    private final ScheduledExecutorService timeoutExecutor;
    private final int subtaskIndex;
    private final int keyGroupStart;
    private final int keyGroupEnd;
    private final Map<Long, Pending> pending = new LinkedHashMap<>();
    private final ArrayDeque<Completion> completions = new ArrayDeque<>();
    private final ArrayDeque<Control> controls = new ArrayDeque<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private long inputSequence;
    private long lastEmittedSequence;
    private long pendingBytes;
    private long maximumPendingBytesObserved;
    private int maximumCompletionEventsObserved;
    private String operationPrefix = UUID.randomUUID().toString();
    private boolean endRequested;
    private boolean endEmitted;

    StreamAsyncWaitOperatorTask(PortableStreamNode node,
            PortableRowSchema inputSchema, StreamAsyncFunction function,
            int maximumParallelism, int subtaskIndex,
            int keyGroupStart, int keyGroupEnd) {
        if (node == null || inputSchema == null || function == null
                || !(node.binding() instanceof StreamAsyncBinding)
                || maximumParallelism <= 0 || subtaskIndex < 0
                || keyGroupStart < 0 || keyGroupEnd < keyGroupStart
                || keyGroupEnd >= maximumParallelism) {
            throw new IllegalArgumentException(
                    "Stream async wait runtime is incomplete");
        }
        this.node = node;
        this.binding = (StreamAsyncBinding) node.binding();
        this.function = function;
        this.subtaskIndex = subtaskIndex;
        this.keyGroupStart = keyGroupStart;
        this.keyGroupEnd = keyGroupEnd;
        this.inputSerializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(inputSchema));
        this.outputSerializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(node.outputSchema()));
        this.inputKeyIndices = identityKeyIndices(
                inputSchema, node.outputSchema(), node.stableKeyFields());
        this.outputKeyIndices = inputKeyIndices == null ? null
                : keyIndices(node.outputSchema(), node.stableKeyFields());
        this.inputKeySerializer = inputKeyIndices == null ? null
                : new RowSerializer(PortablePlanCompiler.toDataSchema(
                keySchema(inputSchema, inputKeyIndices)));
        this.outputKeySerializer = outputKeyIndices == null ? null
                : new RowSerializer(PortablePlanCompiler.toDataSchema(
                keySchema(node.outputSchema(), outputKeyIndices)));
        this.executor = Executors.newFixedThreadPool(
                binding.resources().maximumConcurrency(),
                new AsyncThreadFactory(node.operatorUid()));
        this.timeoutExecutor = Executors.newSingleThreadScheduledExecutor(
                new AsyncThreadFactory(node.operatorUid() + "-timeout"));
    }

    @Override public String operatorUid() { return node.operatorUid(); }

    @Override public synchronized void process(DataRow input,
            Long eventTimestamp, Sink output) {
        requireOpen();
        drain(output);
        byte[] inputFrame = inputSerializer.serialize(input);
        byte[] requestFrame = function.createRequest(input.copy());
        if (requestFrame == null || requestFrame.length == 0) {
            throw new IllegalArgumentException(
                    "Stream async function returned an empty request");
        }
        long retained = Math.addExact(inputFrame.length, requestFrame.length);
        if (retained > binding.resources().maximumPendingBytes()) {
            throw new IllegalStateException(
                    "Stream async request exceeds the pending byte limit");
        }
        while (pending.size() >= binding.resources().maximumConcurrency()
                || Math.addExact(pendingBytes, retained)
                > binding.resources().maximumPendingBytes()) {
            awaitCapacity(output, retained);
        }
        long nextPendingBytes = Math.addExact(pendingBytes, retained);
        long sequence = Math.addExact(inputSequence, 1L);
        String operationIdentity = operationPrefix + ':' + sequence;
        Pending operation = new Pending(sequence, operationIdentity,
                inputFrame, requestFrame.clone(), eventTimestamp, retained);
        pending.put(sequence, operation);
        inputSequence = sequence;
        pendingBytes = nextPendingBytes;
        observeResources();
        submit(operation);
    }

    @Override public boolean hasIdleTimer() { return true; }

    @Override public synchronized StreamTaskResourceUsage resourceUsage() {
        return StreamTaskResourceUsage.complete(
                maximumCompletionEventsObserved,
                maximumPendingBytesObserved, 0, 0);
    }

    @Override public synchronized long idleDelayMillis(long ignoredNowNanos) {
        return pending.isEmpty() ? 100L : 1L;
    }

    @Override public synchronized void onIdleTimer(long ignoredNowNanos,
            Sink output) {
        if (!closed.get()) drain(output);
    }

    @Override public synchronized boolean onWatermark(long timestamp,
            Sink output) {
        requireOpen();
        awaitControlCapacity(output);
        controls.addLast(Control.watermark(inputSequence, timestamp));
        drain(output);
        return false;
    }

    @Override public synchronized boolean onWatermarkStatus(
            StreamEnvelope.WatermarkStatus status, long generation,
            Sink output) {
        requireOpen();
        if (status == null || generation <= 0L) {
            throw new IllegalArgumentException(
                    "Stream async watermark status is invalid");
        }
        awaitControlCapacity(output);
        controls.addLast(Control.status(
                inputSequence, status, generation));
        drain(output);
        return false;
    }

    @Override public synchronized boolean onEnd(Sink output) {
        requireOpen();
        drain(output);
        endRequested = true;
        if (!pending.isEmpty() || !controls.isEmpty()) return false;
        endEmitted = true;
        return true;
    }

    @Override public boolean hasCheckpointState() { return true; }

    @Override public synchronized StreamCheckpointMember snapshot(
            CheckpointIdentity checkpoint,
            FileCheckpointPayloadStore payloadStore) {
        if (checkpoint == null || payloadStore == null) {
            throw new IllegalArgumentException(
                    "Stream async checkpoint runtime is unavailable");
        }
        Path staged = null;
        Throwable primary = null;
        try {
            staged = Files.createTempFile(
                    "rowweave-stream-async-", ".snapshot");
            try (DataOutputStream output = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(staged,
                            StandardOpenOption.TRUNCATE_EXISTING)))) {
                output.write(STATE_MAGIC);
                output.writeInt(STATE_VERSION);
                output.writeUTF(operationPrefix);
                output.writeLong(inputSequence);
                output.writeLong(lastEmittedSequence);
                output.writeInt(pending.size());
                for (Pending operation : pending.values()) {
                    Completion completion = completion(operation);
                    if (completion != null && completion.failure != null) {
                        throw invocationFailure(completion.failure);
                    }
                    output.writeLong(operation.sequence);
                    output.writeUTF(operation.operationIdentity);
                    output.writeInt(operation.attempt);
                    output.writeBoolean(operation.eventTimestamp != null);
                    if (operation.eventTimestamp != null) {
                        output.writeLong(operation.eventTimestamp);
                    }
                    writeFrame(output, operation.inputFrame);
                    writeFrame(output, operation.requestFrame);
                    if (completion == null) output.writeInt(-1);
                    else writeFrame(output, completion.responseFrame);
                }
                output.writeInt(controls.size());
                for (Control control : controls) {
                    output.writeInt(control.kind.wireId);
                    output.writeLong(control.frontier);
                    output.writeLong(control.value);
                    output.writeLong(control.generation);
                }
            }
            FileCheckpointPayloadStore.StoredPayload stored =
                    payloadStore.store(staged);
            return StreamCheckpointMember.stateHandle(
                    node.operatorUid(), subtaskIndex,
                    keyGroupStart, keyGroupEnd,
                    STATE_NAME, STATE_CODEC, STATE_VERSION,
                    stored.digest(), stored.contentBytes());
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException failure) {
            primary = failure;
            throw new IllegalStateException(
                    "Stream async checkpoint staging failed", failure);
        } finally {
            delete(staged, primary);
        }
    }

    @Override public synchronized void restore(StreamCheckpointMember member,
            FileCheckpointPayloadStore payloadStore) {
        validateRestoreMember(member, payloadStore);
        if (!pending.isEmpty() || inputSequence != 0L) {
            throw new IllegalStateException(
                    "Stream async state was already initialized");
        }
        Path staged = null;
        Throwable primary = null;
        try {
            staged = Files.createTempFile("rowweave-stream-async-", ".restore");
            payloadStore.restore(
                    new FileCheckpointPayloadStore.StoredPayload(
                            member.contentDigest(), member.contentBytes()),
                    staged);
            try (DataInputStream input = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(staged)))) {
                byte[] magic = new byte[STATE_MAGIC.length];
                input.readFully(magic);
                int restoredVersion = input.readInt();
                if (!java.util.Arrays.equals(STATE_MAGIC, magic)
                        || restoredVersion != STATE_VERSION
                        && restoredVersion != LEGACY_STATE_VERSION) {
                    throw invalidState();
                }
                String restoredPrefix = input.readUTF();
                long restoredInputSequence = input.readLong();
                long restoredFrontier = input.readLong();
                int count = input.readInt();
                if (!restoredPrefix.matches(
                        "[A-Za-z0-9][A-Za-z0-9._:-]{0,255}")
                        || restoredInputSequence < 0L
                        || restoredFrontier < 0L
                        || restoredFrontier > restoredInputSequence
                        || count < 0 || count > binding.resources()
                        .maximumConcurrency()) {
                    throw invalidState();
                }
                operationPrefix = restoredPrefix;
                inputSequence = restoredInputSequence;
                lastEmittedSequence = restoredFrontier;
                long previous = restoredFrontier;
                for (int index = 0; index < count; index++) {
                    long sequence = input.readLong();
                    String identity = input.readUTF();
                    int attempt = input.readInt();
                    Long eventTimestamp = input.readBoolean()
                            ? Long.valueOf(input.readLong()) : null;
                    byte[] inputFrame = readFrame(input, false);
                    byte[] requestFrame = readFrame(input, false);
                    int responseSize = input.readInt();
                    byte[] response = responseSize == -1 ? null
                            : readFrame(input, responseSize, true);
                    StreamAsyncRequest.of(identity, attempt, requestFrame);
                    if (sequence <= previous
                            || sequence > restoredInputSequence
                            || restoredVersion == LEGACY_STATE_VERSION
                            && !identity.equals(
                            operationPrefix + ':' + sequence)) {
                        throw invalidState();
                    }
                    long retained = Math.addExact(inputFrame.length,
                            requestFrame.length);
                    if (response != null) retained = Math.addExact(
                            retained, response.length);
                    pendingBytes = Math.addExact(pendingBytes, retained);
                    if (pendingBytes
                            > binding.resources().maximumPendingBytes()) {
                        throw invalidState();
                    }
                    Pending operation = new Pending(sequence, identity,
                            inputFrame, requestFrame, eventTimestamp,
                            retained);
                    operation.attempt = attempt;
                    if (response != null) {
                        operation.completion = Completion.success(
                                sequence, response);
                        if (binding.ordering()
                                == StreamAsyncBinding.Ordering.UNORDERED) {
                            completions.addLast(operation.completion);
                        }
                    }
                pending.put(sequence, operation);
                    observeResources();
                    previous = sequence;
                }
                int controlCount = input.readInt();
                if (controlCount < 0 || controlCount > maximumControls()) {
                    throw invalidState();
                }
                long previousFrontier = 0L;
                for (int index = 0; index < controlCount; index++) {
                    Control control = Control.restore(
                            input.readInt(), input.readLong(),
                            input.readLong(), input.readLong());
                    if (control.frontier < previousFrontier
                            || control.frontier > restoredInputSequence) {
                        throw invalidState();
                    }
                    controls.addLast(control);
                    previousFrontier = control.frontier;
                }
                if (input.read() != -1) throw invalidState();
            }
            for (Pending operation : pending.values()) {
                if (operation.completion == null) submit(operation);
            }
            observeResources();
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } catch (IOException failure) {
            primary = failure;
            throw new IllegalArgumentException(
                    "Stream async checkpoint state is invalid", failure);
        } finally {
            delete(staged, primary);
        }
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        timeoutExecutor.shutdownNow();
        executor.shutdownNow();
        synchronized (this) {
            pending.clear();
            completions.clear();
            controls.clear();
            pendingBytes = 0L;
        }
    }

    private void submit(Pending operation) {
        executor.execute(() -> {
            Completion completion;
            while (true) {
                try {
                    StreamAsyncRequest request = StreamAsyncRequest.of(
                            operation.operationIdentity, operation.attempt,
                            operation.requestFrame);
                    byte[] response = invokeOnce(request);
                    if (response == null) {
                        throw new IllegalArgumentException(
                                "Stream async function returned a null response");
                    }
                    completion = Completion.success(
                            operation.sequence, response);
                    break;
                } catch (Throwable failure) {
                    if (closed.get()) return;
                    if (operation.attempt
                            >= binding.resources().maximumAttempts()) {
                        completion = Completion.failure(
                                operation.sequence, failure);
                        break;
                    }
                    operation.attempt = Math.addExact(
                            operation.attempt, 1);
                    try {
                        long delay = retryDelay(operation.attempt - 1);
                        if (delay > 0L) Thread.sleep(delay);
                    } catch (InterruptedException interrupted) {
                        if (closed.get()) return;
                        Thread.interrupted();
                    }
                }
            }
            synchronized (StreamAsyncWaitOperatorTask.this) {
                if (!closed.get() && pending.containsKey(
                        operation.sequence)) {
                    if (completion.failure == null) {
                        long nextBytes;
                        try {
                            nextBytes = Math.addExact(pendingBytes,
                                    completion.responseFrame.length);
                        } catch (ArithmeticException overflow) {
                            nextBytes = Long.MAX_VALUE;
                        }
                        if (nextBytes
                                > binding.resources().maximumPendingBytes()) {
                            completion = Completion.failure(
                                    operation.sequence,
                                    new IllegalStateException(
                                    "Stream async pending byte limit exceeded"));
                        } else {
                            pendingBytes = nextBytes;
                            operation.retainedBytes = Math.addExact(
                                    operation.retainedBytes,
                                    completion.responseFrame.length);
                        }
                    }
                    completions.addLast(completion);
                    observeResources();
                    StreamAsyncWaitOperatorTask.this.notifyAll();
                }
            }
        });
    }

    private byte[] invokeOnce(StreamAsyncRequest request) throws Exception {
        Thread invocationThread = Thread.currentThread();
        AtomicBoolean timedOut = new AtomicBoolean();
        ScheduledFuture<?> timeout = timeoutExecutor.schedule(() -> {
            timedOut.set(true);
            invocationThread.interrupt();
        }, binding.resources().timeoutMillis(), TimeUnit.MILLISECONDS);
        try {
            byte[] response;
            try {
                response = function.invoke(request);
            } catch (Exception failure) {
                if (timedOut.get()) throw timeoutFailure(failure);
                throw failure;
            }
            if (timedOut.get()) throw timeoutFailure(null);
            return response;
        } finally {
            timeout.cancel(false);
            if (timedOut.get()) Thread.interrupted();
        }
    }

    private static IllegalStateException timeoutFailure(Throwable cause) {
        return new IllegalStateException(
                "Stream async invocation timed out", cause);
    }

    private void awaitCapacity(Sink output, long retained) {
        drain(output);
        if (pending.size() < binding.resources().maximumConcurrency()
                && Math.addExact(pendingBytes, retained)
                <= binding.resources().maximumPendingBytes()) {
            return;
        }
        try {
            wait(100L);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Stream async backpressure wait was interrupted",
                    interrupted);
        }
        requireOpen();
    }

    private void awaitControlCapacity(Sink output) {
        drain(output);
        while (controls.size() >= maximumControls()) {
            try {
                wait(100L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Stream async control wait was interrupted",
                        interrupted);
            }
            requireOpen();
            drain(output);
        }
    }

    private int maximumControls() {
        return binding.resources().maximumConcurrency();
    }

    private long retryDelay(int completedAttempt) {
        long delay = binding.resources().initialBackoffMillis();
        long maximum = binding.resources().maximumBackoffMillis();
        for (int index = 1; index < completedAttempt; index++) {
            if (delay >= maximum) return maximum;
            delay = delay > maximum / 2L ? maximum : delay * 2L;
        }
        return Math.min(delay, maximum);
    }

    private void drain(Sink output) {
        if (binding.ordering() == StreamAsyncBinding.Ordering.ORDERED) {
            attachCompletions();
            Iterator<Map.Entry<Long, Pending>> iterator =
                    pending.entrySet().iterator();
            while (iterator.hasNext()) {
                Pending operation = iterator.next().getValue();
                if (operation.completion == null) break;
                emit(operation, operation.completion, output);
                lastEmittedSequence = operation.sequence;
                pendingBytes -= operation.retainedBytes;
                iterator.remove();
            }
            emitReadyControls(output);
            emitDeferredEnd(output);
            return;
        }
        boolean advanced;
        do {
            advanced = drainUnorderedSegment(output);
            if (emitReadyControls(output)) advanced = true;
        } while (advanced && (!completions.isEmpty()
                || !controls.isEmpty()));
        emitDeferredEnd(output);
    }

    private void observeResources() {
        maximumPendingBytesObserved = Math.max(
                maximumPendingBytesObserved, pendingBytes);
        maximumCompletionEventsObserved = Math.max(
                maximumCompletionEventsObserved, completions.size());
    }

    private boolean drainUnorderedSegment(Sink output) {
        long frontier = controls.isEmpty()
                ? Long.MAX_VALUE : controls.peekFirst().frontier;
        boolean emitted = false;
        Iterator<Completion> iterator = completions.iterator();
        while (iterator.hasNext()) {
            Completion completion = iterator.next();
            if (completion.sequence > frontier) continue;
            iterator.remove();
            Pending operation = pending.remove(completion.sequence);
            if (operation == null) continue;
            emit(operation, completion, output);
            pendingBytes -= operation.retainedBytes;
            emitted = true;
        }
        return emitted;
    }

    private boolean emitReadyControls(Sink output) {
        boolean emitted = false;
        while (!controls.isEmpty()
                && !hasPendingAtOrBefore(controls.peekFirst().frontier)) {
            controls.removeFirst().emit(output);
            emitted = true;
        }
        if (emitted) notifyAll();
        return emitted;
    }

    private boolean hasPendingAtOrBefore(long frontier) {
        for (Long sequence : pending.keySet()) {
            if (sequence <= frontier) return true;
            break;
        }
        return false;
    }

    private void emitDeferredEnd(Sink output) {
        if (endRequested && !endEmitted && pending.isEmpty()
                && controls.isEmpty()) {
            endEmitted = true;
            output.emitEnd();
        }
    }

    private void attachCompletions() {
        Completion completion;
        while ((completion = completions.pollFirst()) != null) {
            Pending operation = pending.get(completion.sequence);
            if (operation != null) operation.completion = completion;
        }
    }

    private void emit(Pending operation, Completion completion,
            Sink output) {
        if (completion.failure != null) {
            output.fail(completion.failure);
            if (completion.failure instanceof RuntimeException) {
                throw (RuntimeException) completion.failure;
            }
            if (completion.failure instanceof Error) {
                throw (Error) completion.failure;
            }
            throw new IllegalStateException(
                    "Stream async invocation failed", completion.failure);
        }
        DataRow input = inputSerializer.deserialize(operation.inputFrame);
        QuotaCollector collector = new QuotaCollector(
                output, operation.eventTimestamp,
                inputKeySerializer == null ? null
                        : inputKeySerializer.serializeProjected(
                        input, inputKeyIndices));
        try {
            function.complete(input, completion.responseFrame.clone(),
                    collector);
        } finally {
            collector.close();
        }
        collector.requireHealthy();
    }

    private Completion completion(Pending operation) {
        if (operation.completion != null) return operation.completion;
        for (Completion value : completions) {
            if (value.sequence == operation.sequence) return value;
        }
        return null;
    }

    private static RuntimeException invocationFailure(Throwable failure) {
        if (failure instanceof RuntimeException) {
            return (RuntimeException) failure;
        }
        return new IllegalStateException(
                "Stream async invocation failed before checkpoint", failure);
    }

    private void validateRestoreMember(StreamCheckpointMember member,
            FileCheckpointPayloadStore payloadStore) {
        if (member == null || payloadStore == null
                || member.kind() != StreamCheckpointMember.Kind.STATE
                || !node.operatorUid().equals(member.operatorUid())
                || member.subtaskIndex() != subtaskIndex
                || member.keyGroupStart() != keyGroupStart
                || member.keyGroupEnd() != keyGroupEnd
                || !STATE_NAME.equals(member.subjectId())
                || !(STATE_CODEC.equals(member.codecId())
                && member.schemaVersion() == STATE_VERSION
                || LEGACY_STATE_CODEC.equals(member.codecId())
                && member.schemaVersion() == LEGACY_STATE_VERSION)) {
            throw new IllegalArgumentException(
                    "Stream async restore member is incompatible");
        }
    }

    private byte[] readFrame(DataInputStream input, boolean emptyAllowed)
            throws IOException {
        return readFrame(input, input.readInt(), emptyAllowed);
    }

    private byte[] readFrame(DataInputStream input, int size,
            boolean emptyAllowed) throws IOException {
        if (size < (emptyAllowed ? 0 : 1)
                || size > binding.resources().maximumPendingBytes()
                || size > Integer.MAX_VALUE - 8) {
            throw invalidState();
        }
        byte[] frame = new byte[size];
        input.readFully(frame);
        return frame;
    }

    private static void writeFrame(DataOutputStream output, byte[] frame)
            throws IOException {
        output.writeInt(frame.length);
        output.write(frame);
    }

    private static IllegalArgumentException invalidState() {
        return new IllegalArgumentException(
                "Stream async checkpoint state is invalid");
    }

    private static void delete(Path path, Throwable primary) {
        if (path == null) return;
        try {
            Files.deleteIfExists(path);
        } catch (IOException failure) {
            if (primary != null) primary.addSuppressed(failure);
            else throw new IllegalStateException(
                    "Stream async checkpoint staging cleanup failed",
                    failure);
        }
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException(
                    "Stream async wait task is closed");
        }
        if (endRequested) {
            throw new IllegalStateException(
                    "Stream async wait received input after end-of-input");
        }
    }

    private final class QuotaCollector implements StreamCollector {
        private final Sink output;
        private final Long eventTimestamp;
        private final Thread owner = Thread.currentThread();
        private final byte[] expectedStableKey;
        private final AtomicBoolean active = new AtomicBoolean(true);
        private final AtomicReference<RuntimeException> poison =
                new AtomicReference<>();
        private int rows;
        private long bytes;

        private QuotaCollector(Sink output, Long eventTimestamp,
                byte[] expectedStableKey) {
            this.output = output;
            this.eventTimestamp = eventTimestamp;
            this.expectedStableKey = expectedStableKey;
        }

        @Override public synchronized void collect(DataRow row) {
            if (!active.get() || Thread.currentThread() != owner) {
                throw rejectEscapedEmission();
            }
            RuntimeException prior = poison.get();
            if (prior != null) throw prior;
            try {
                if (row == null) {
                    throw new IllegalArgumentException(
                            "Stream async completion returned a null row");
                }
                if (expectedStableKey != null) {
                    byte[] actual = outputKeySerializer.serializeProjected(
                            row, outputKeyIndices);
                    if (!MessageDigest.isEqual(expectedStableKey, actual)) {
                        throw new IllegalArgumentException(
                                "Stream async wait changed its stable output key");
                    }
                }
                int nextRows = Math.addExact(rows, 1);
                long nextBytes = Math.addExact(bytes,
                        outputSerializer.serializedSize(row));
                if (nextRows > binding.resources().maximumResults()
                        || nextBytes
                        > binding.resources().maximumResultBytes()) {
                    throw new IllegalStateException(
                            "Stream async result emission limit exceeded");
                }
                rows = nextRows;
                bytes = nextBytes;
                output.emit(row, eventTimestamp);
            } catch (RuntimeException failure) {
                poison.compareAndSet(null, failure);
                throw failure;
            }
        }

        @Override public void collect(StreamOutputTag tag, DataRow row) {
            throw new UnsupportedOperationException(
                    "Stream async wait has no tagged output");
        }

        private void close() { active.set(false); }

        private void requireHealthy() {
            RuntimeException failure = poison.get();
            if (failure != null) throw failure;
        }

        private IllegalStateException rejectEscapedEmission() {
            IllegalStateException failure = new IllegalStateException(
                    "Stream async collector escaped its mailbox callback");
            poison.compareAndSet(null, failure);
            try {
                output.fail(failure);
            } catch (RuntimeException | Error outputFailure) {
                if (outputFailure != failure) {
                    failure.addSuppressed(outputFailure);
                }
            }
            return failure;
        }
    }

    private static int[] identityKeyIndices(PortableRowSchema input,
            PortableRowSchema output, List<String> stableKeys) {
        if (stableKeys.isEmpty()) return null;
        int[] inputIndices = keyIndices(input, stableKeys);
        int[] outputIndices = keyIndices(output, stableKeys);
        if (inputIndices == null || outputIndices == null) return null;
        for (int index = 0; index < inputIndices.length; index++) {
            if (!input.field(inputIndices[index]).equals(
                    output.field(outputIndices[index]))) return null;
        }
        return inputIndices;
    }

    private static int[] keyIndices(PortableRowSchema schema,
            List<String> stableKeys) {
        int[] result = new int[stableKeys.size()];
        for (int offset = 0; offset < stableKeys.size(); offset++) {
            result[offset] = -1;
            for (int index = 0; index < schema.fieldCount(); index++) {
                if (schema.field(index).fieldId().equals(
                        stableKeys.get(offset))) {
                    result[offset] = index;
                    break;
                }
            }
            if (result[offset] < 0) return null;
        }
        return result;
    }

    private static PortableRowSchema keySchema(PortableRowSchema schema,
            int[] indices) {
        List<io.rowweave.portable.PortableField> fields =
                new ArrayList<>(indices.length);
        for (int index : indices) fields.add(schema.field(index));
        return new PortableRowSchema(fields);
    }

    private static final class Pending {
        final long sequence;
        final String operationIdentity;
        final byte[] inputFrame;
        final byte[] requestFrame;
        final Long eventTimestamp;
        long retainedBytes;
        volatile int attempt = 1;
        Completion completion;

        Pending(long sequence, String operationIdentity, byte[] inputFrame,
                byte[] requestFrame, Long eventTimestamp,
                long retainedBytes) {
            this.sequence = sequence;
            this.operationIdentity = operationIdentity;
            this.inputFrame = inputFrame;
            this.requestFrame = requestFrame;
            this.eventTimestamp = eventTimestamp;
            this.retainedBytes = retainedBytes;
        }
    }

    private static final class Completion {
        final long sequence;
        final byte[] responseFrame;
        final Throwable failure;

        private Completion(long sequence, byte[] responseFrame,
                Throwable failure) {
            this.sequence = sequence;
            this.responseFrame = responseFrame;
            this.failure = failure;
        }

        static Completion success(long sequence, byte[] responseFrame) {
            return new Completion(sequence, responseFrame.clone(), null);
        }

        static Completion failure(long sequence, Throwable failure) {
            return new Completion(sequence, null, failure);
        }
    }

    private static final class Control {
        private final ControlKind kind;
        private final long frontier;
        private final long value;
        private final long generation;

        private Control(ControlKind kind, long frontier,
                long value, long generation) {
            this.kind = kind;
            this.frontier = frontier;
            this.value = value;
            this.generation = generation;
        }

        static Control watermark(long frontier, long timestamp) {
            return new Control(ControlKind.WATERMARK,
                    frontier, timestamp, 0L);
        }

        static Control status(long frontier,
                StreamEnvelope.WatermarkStatus status, long generation) {
            return new Control(status == StreamEnvelope.WatermarkStatus.IDLE
                    ? ControlKind.IDLE : ControlKind.ACTIVE,
                    frontier, 0L, generation);
        }

        static Control restore(int wireId, long frontier,
                long value, long generation) {
            ControlKind kind = ControlKind.fromWireId(wireId);
            if (frontier < 0L
                    || kind == ControlKind.WATERMARK && generation != 0L
                    || kind != ControlKind.WATERMARK
                    && (value != 0L || generation <= 0L)) {
                throw invalidState();
            }
            return new Control(kind, frontier, value, generation);
        }

        void emit(Sink output) {
            if (kind == ControlKind.WATERMARK) {
                output.emitWatermark(value);
            } else {
                output.emitWatermarkStatus(
                        kind == ControlKind.IDLE
                                ? StreamEnvelope.WatermarkStatus.IDLE
                                : StreamEnvelope.WatermarkStatus.ACTIVE,
                        generation);
            }
        }
    }

    private enum ControlKind {
        WATERMARK(1), IDLE(2), ACTIVE(3);
        private final int wireId;
        ControlKind(int wireId) { this.wireId = wireId; }
        static ControlKind fromWireId(int wireId) {
            for (ControlKind kind : values()) {
                if (kind.wireId == wireId) return kind;
            }
            throw invalidState();
        }
    }

    private static final class AsyncThreadFactory implements ThreadFactory {
        private final String operatorUid;
        private final AtomicInteger sequence = new AtomicInteger();

        private AsyncThreadFactory(String operatorUid) {
            this.operatorUid = operatorUid;
        }

        @Override public Thread newThread(Runnable action) {
            Thread thread = new Thread(action,
                    "rowweave-stream-async-" + operatorUid + '-'
                            + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
