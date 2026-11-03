package io.rowweave.run;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * Durable Run authority on one local file system, where each accepted mutation is one WAL frame that has been forced.
 * This is not a multi-host consensus backend, so it cannot be used to claim Master HA.
 */
public final class FileRunStateStore implements RunStateStore, RunLifecycleCatalog,
        RunRetentionAuthority, RunCancellationAuthority, RunDeadlineAuthority {
    private static final long MAX_JOURNAL_BYTES = 128L * 1024L * 1024L;
    private static final int FRAME_HEADER_BYTES = 12;
    private static final int FRAME_CHECKSUM_BYTES = 32;
    private static final int STRIPE_COUNT = 256;
    private static final ReentrantLock[] JVM_LOCKS = new ReentrantLock[STRIPE_COUNT];
    private static final ReentrantLock[] LIFECYCLE_JVM_LOCKS =
            new ReentrantLock[RunLifecycleCatalogKernel.SHARD_COUNT];

    static {
        for (int i = 0; i < JVM_LOCKS.length; i++) JVM_LOCKS[i] = new ReentrantLock();
        for (int i = 0; i < LIFECYCLE_JVM_LOCKS.length; i++) {
            LIFECYCLE_JVM_LOCKS[i] = new ReentrantLock();
        }
    }

    private final Path root;
    private final Path lifecycleRoot;
    private final Runnable afterForce;
    private final int maxLifecyclePerShard;
    private final LongSupplier authorityClock;
    private final PurgeObserver purgeObserver;
    private final AtomicLong lastAuthorityTime = new AtomicLong();

    public FileRunStateStore(Path baseDirectory) {
        this(baseDirectory, () -> { },
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                System::currentTimeMillis);
    }

    /** Injection point for the same-package crash probe only; not part of the published production API. */
    FileRunStateStore(Path baseDirectory, Runnable afterForce) {
        this(baseDirectory, afterForce,
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                System::currentTimeMillis);
    }

    FileRunStateStore(Path baseDirectory, Runnable afterForce,
                      int maxLifecyclePerShard, LongSupplier authorityClock) {
        this(baseDirectory, afterForce, maxLifecyclePerShard,
                authorityClock, PurgeObserver.NOOP);
    }

    FileRunStateStore(Path baseDirectory, Runnable afterForce,
                      int maxLifecyclePerShard, LongSupplier authorityClock,
                      PurgeObserver purgeObserver) {
        if (baseDirectory == null) throw new IllegalArgumentException("baseDirectory is required");
        if (afterForce == null) throw new IllegalArgumentException("afterForce is required");
        if (maxLifecyclePerShard < 1
                || maxLifecyclePerShard > RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD) {
            throw new IllegalArgumentException("maxLifecyclePerShard out of bounds");
        }
        if (authorityClock == null) throw new IllegalArgumentException("authorityClock is required");
        if (purgeObserver == null) throw new IllegalArgumentException("purgeObserver is required");
        this.root = baseDirectory.toAbsolutePath().normalize().resolve("runs-v1");
        this.lifecycleRoot = root.resolve("lifecycle-v1");
        this.afterForce = afterForce;
        this.maxLifecyclePerShard = maxLifecyclePerShard;
        this.authorityClock = authorityClock;
        this.purgeObserver = purgeObserver;
        try {
            boolean existed = Files.exists(root);
            Files.createDirectories(root);
            requireDirectory(root, "run state root");
            if (!existed) forceDirectory(root.getParent());
            boolean lifecycleExisted = Files.exists(lifecycleRoot);
            Files.createDirectories(lifecycleRoot);
            requireDirectory(lifecycleRoot, "lifecycle root");
            if (!lifecycleExisted) forceDirectory(root);
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to initialize run state directory", failure);
        }
    }

    @Override
    public RunDecision apply(RunCommand command) {
        if (command == null) throw new IllegalArgumentException("command is required");
        return locked(command.runId, true, loaded -> {
            RunLifecycleRef authoritative = readLifecycle(command.runId, false);
            if (authoritative != null
                    && authoritative.state() == RunLifecycleState.DELETING) {
                return RunDecision.rejected(
                        RunDecision.Code.RETENTION_IN_PROGRESS, loaded.snapshot);
            }
            if ((loaded.snapshot == null) != (authoritative == null)) {
                throw lifecycleCorruption(command.runId,
                        "journal/lifecycle presence mismatch");
            }
            StoredCommand prior = loaded.commands.get(command.commandId);
            if (prior != null) {
                return MessageDigest.isEqual(prior.digest, command.commandDigestUnsafe())
                        ? RunDecision.replayed(loaded.snapshot, prior.event)
                        : RunDecision.rejected(RunDecision.Code.COMMAND_ID_CONFLICT, loaded.snapshot);
            }
            return applyLoaded(command, loaded);
        });
    }

    @Override
    public RunCancellationResult requestUserCancellation(
            String runId, String requestId) {
        runId = RunCancellationAuthorityKernel.validateRunId(runId);
        requestId = RunCancellationAuthorityKernel.validateRequestId(requestId);
        Path directory = runDirectory(runId);
        if (!Files.exists(directory)) {
            return new RunCancellationResult(runId,
                    RunCancellationResult.Disposition.NOT_FOUND, null, 0L);
        }
        final String validatedRunId = runId;
        final String commandId = RunCancellationAuthorityKernel.commandId(runId, requestId);
        return locked(runId, false, loaded -> {
            RunLifecycleRef authoritative = readLifecycle(validatedRunId, false);
            if (authoritative != null
                    && authoritative.state() == RunLifecycleState.DELETING) {
                return RunCancellationAuthorityKernel.retiring(validatedRunId);
            }
            if ((loaded.snapshot == null) != (authoritative == null)) {
                throw lifecycleCorruption(validatedRunId,
                        "journal/lifecycle presence mismatch");
            }
            if (loaded.snapshot == null) {
                return new RunCancellationResult(validatedRunId,
                        RunCancellationResult.Disposition.NOT_FOUND, null, 0L);
            }
            StoredCommand prior = loaded.commands.get(commandId);
            if (prior != null) {
                return RunCancellationAuthorityKernel.replayed(
                        validatedRunId, loaded.snapshot, prior.event, commandId);
            }
            RunCancellationAuthorityKernel.Plan plan = RunCancellationAuthorityKernel.plan(
                    validatedRunId, commandId, loaded.snapshot);
            if (plan.result() != null) return plan.result();
            RunDecision decision = applyLoaded(plan.command(), loaded);
            if (decision.code() != RunDecision.Code.ACCEPTED) {
                throw new IllegalStateException(
                        "atomic public cancellation rejected: " + decision.code());
            }
            return RunCancellationAuthorityKernel.accepted(decision.snapshot());
        });
    }

    @Override
    public RunDeadlineResult evaluateDeadline(String runId) {
        runId = RunDeadlineAuthorityKernel.validateRunId(runId);
        Path directory = runDirectory(runId);
        if (!Files.exists(directory)) return RunDeadlineAuthorityKernel.missing(runId);
        final String validatedRunId = runId;
        return locked(runId, false, loaded -> {
            RunLifecycleRef authoritative = readLifecycle(validatedRunId, false);
            if (authoritative != null
                    && authoritative.state() == RunLifecycleState.DELETING) {
                return RunDeadlineAuthorityKernel.retiring(validatedRunId);
            }
            if ((loaded.snapshot == null) != (authoritative == null)) {
                throw lifecycleCorruption(validatedRunId,
                        "journal/lifecycle presence mismatch");
            }
            if (loaded.snapshot == null) {
                return RunDeadlineAuthorityKernel.missing(validatedRunId);
            }
            RunSnapshot current = loaded.snapshot;
            if (current.deadlineEpochMillis() == 0L) {
                return RunDeadlineAuthorityKernel.plan(null, current, 0L).result();
            }
            long observed = authorityClock.getAsLong();
            if (observed <= 0L) {
                throw new IllegalStateException("authority clock must be > 0");
            }
            String commandId = RunDeadlineAuthorityKernel.commandId(current);
            StoredCommand prior = loaded.commands.get(commandId);
            if (prior != null) {
                return RunDeadlineAuthorityKernel.replayed(
                        current, prior.event, commandId, observed);
            }
            RunDeadlineAuthorityKernel.Plan plan =
                    RunDeadlineAuthorityKernel.plan(commandId, current, observed);
            if (plan.result() != null) return plan.result();
            RunDecision decision = applyLoaded(plan.command(), loaded);
            if (decision.code() != RunDecision.Code.ACCEPTED) {
                throw new IllegalStateException(
                        "atomic deadline mutation rejected: " + decision.code());
            }
            return RunDeadlineAuthorityKernel.accepted(
                    decision.snapshot(), observed);
        });
    }

    private RunDecision applyLoaded(RunCommand command, Loaded loaded) {
        RunDecision decision = RunStateMachine.apply(loaded.snapshot, command);
        if (!decision.accepted()) return decision;
        byte[] frame = RunStateCodec.encodeRecord(
                new RunJournalRecord(command, decision.snapshot(), decision.event()));
        if (loaded.journalLength + frame.length > MAX_JOURNAL_BYTES) {
            return RunDecision.rejected(RunDecision.Code.LIMIT_EXCEEDED, loaded.snapshot);
        }
        RunLifecycleRef nextLifecycle = null;
        if (loaded.snapshot == null) {
            nextLifecycle = RunLifecycleRef.active(decision.snapshot().runId(),
                    decision.snapshot().revision(), decision.snapshot().status(),
                    decision.snapshot().artifactId());
            if (!reserveLifecycle(nextLifecycle)) {
                return RunDecision.rejected(RunDecision.Code.LIMIT_EXCEEDED, null);
            }
        }
        appendAndForce(loaded.journal, loaded.journalLength, frame);
        if (nextLifecycle == null) nextLifecycle = lifecycleFor(decision.snapshot());
        writeLifecycle(nextLifecycle);
        afterForce.run();
        return decision;
    }

    @Override
    public RunSnapshot get(String runId) {
        validateRunId(runId);
        Path directory = runDirectory(runId);
        if (!Files.exists(directory)) return null;
        return locked(runId, false, loaded -> loaded.snapshot);
    }

    @Override
    public List<ExecutionEvent> events(String runId, long afterSequence, int limit) {
        validateRunId(runId);
        if (afterSequence < 0L) throw new IllegalArgumentException("afterSequence must be >= 0");
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("limit must be in [1,1000]");
        return locked(runId, false, loaded -> {
            RunLifecycleRef ref = readLifecycle(runId, loaded.snapshot != null);
            if (ref != null && ref.state() == RunLifecycleState.DELETING) {
                throw new RunHistoryRetiringException(runId);
            }
            List<ExecutionEvent> page = new ArrayList<>(Math.min(limit, loaded.events.size()));
            for (ExecutionEvent event : loaded.events) {
                if (event.sequence() > afterSequence) {
                    page.add(event);
                    if (page.size() == limit) break;
                }
            }
            return Collections.unmodifiableList(page);
        });
    }

    @Override
    public RunLifecycleRef lifecycle(String runId) {
        validateRunId(runId);
        RunLifecycleRef ref = readLifecycle(runId, false);
        if (ref != null && ref.terminalAtMillis() > 0L) {
            observeAuthorityTime(ref.terminalAtMillis());
        }
        return ref;
    }

    @Override
    public RunLifecyclePage scanLifecycle(String cursor, int limit) {
        RunLifecycleCatalogKernel.Cursor position =
                RunLifecycleCatalogKernel.decodeCursor(cursor);
        List<RunLifecycleRef> refs = readLifecycleShard(position.shard());
        List<RunLifecycleRef> terminal = new ArrayList<>();
        for (RunLifecycleRef ref : refs) {
            if (ref.state() != RunLifecycleState.ACTIVE) {
                terminal.add(ref);
                observeAuthorityTime(ref.terminalAtMillis());
            }
        }
        return RunLifecycleCatalogKernel.page(cursor, limit, terminal);
    }

    @Override
    public RunRetentionClaim claim(
            String runId, long expectedTerminalRevision, long cutoffMillis,
            RetentionOwner owner, long observedOwnershipEpoch) {
        validateRunId(runId);
        String digest = RunLifecycleCatalogKernel.digestHex(runId);
        int shard = RunLifecycleCatalogKernel.shard(digest);
        return locked(runId, false, loaded -> withLifecycleShardLock(shard, () -> {
            RunLifecycleRef ref = readLifecycle(runId, loaded.snapshot != null);
            RunRetentionAuthorityKernel.Transition transition =
                    RunRetentionAuthorityKernel.evaluate(
                            runId, loaded.snapshot, ref, false,
                            expectedTerminalRevision, cutoffMillis, owner,
                            observedOwnershipEpoch);
            if (transition.nextLifecycle() != null) {
                writeLifecycle(transition.nextLifecycle());
                afterForce.run();
            }
            return transition.claim();
        }));
    }

    @Override
    public boolean current(RetentionToken token) {
        if (token == null) throw new IllegalArgumentException("token is required");
        return RunRetentionAuthorityKernel.current(
                readLifecycle(token.runId(), false), token);
    }

    @Override
    public RunPurgeProgress purge(RetentionToken token, int maxDeletes) {
        validatePurge(token, maxDeletes);
        String runId = token.runId();
        String digest = RunLifecycleCatalogKernel.digestHex(runId);
        int shard = RunLifecycleCatalogKernel.shard(digest);
        return withRunStripeLock(runId, () -> withLifecycleShardLock(shard, () -> {
            RunLifecycleRef ref = readLifecycle(runId, false);
            Path live = runDirectory(runId);
            Path trash = trashDirectory(runId);
            if (ref == null) {
                if (Files.exists(live, LinkOption.NOFOLLOW_LINKS)) {
                    return RunPurgeProgress.staleOwner();
                }
                try {
                    if (Files.exists(trash, LinkOption.NOFOLLOW_LINKS)) {
                        if (!validPublishedTrash(runId, trash, token)) {
                            return RunPurgeProgress.corrupt();
                        }
                        deleteTrashBestEffort(trash);
                    }
                    return RunPurgeProgress.purged();
                } catch (IOException failure) {
                    throw new UncheckedIOException(
                            "failed to reconcile purged run " + runId, failure);
                }
            }
            if (!RunRetentionAuthorityKernel.current(ref, token)) {
                return RunPurgeProgress.staleOwner();
            }
            if (Files.exists(live, LinkOption.NOFOLLOW_LINKS)
                    && Files.exists(trash, LinkOption.NOFOLLOW_LINKS)) {
                return RunPurgeProgress.corrupt();
            }
            try {
                if (Files.exists(live, LinkOption.NOFOLLOW_LINKS)) {
                    requireDirectory(live, "run directory");
                    Loaded loaded = load(runId, live.resolve("journal.axr"));
                    if (!matchesTerminalToken(loaded.snapshot, token)) {
                        return RunPurgeProgress.corrupt();
                    }
                    Path trashRoot = trash.getParent();
                    boolean trashRootExisted = Files.exists(
                            trashRoot, LinkOption.NOFOLLOW_LINKS);
                    Files.createDirectories(trashRoot);
                    requireDirectory(trashRoot, "run trash root");
                    if (!trashRootExisted) forceDirectory(root);
                    try {
                        Files.move(live, trash, StandardCopyOption.ATOMIC_MOVE);
                    } catch (AtomicMoveNotSupportedException unsupported) {
                        throw new IllegalStateException(
                                "run purge requires atomic move: " + live, unsupported);
                    }
                    forceDirectory(root);
                    forceDirectory(trashRoot);
                    purgeObserver.afterMove();
                } else if (!Files.exists(trash, LinkOption.NOFOLLOW_LINKS)) {
                    return RunPurgeProgress.corrupt();
                }
                if (!validTrash(runId, trash, token)) {
                    return RunPurgeProgress.corrupt();
                }
                deleteLifecycle(ref);
                purgeObserver.afterLifecycleDelete();
                deleteTrashBestEffort(trash);
                return RunPurgeProgress.purged();
            } catch (IOException failure) {
                throw new UncheckedIOException(
                        "failed to purge run state for " + runId, failure);
            }
        }));
    }

    Path journalPath(String runId) {
        validateRunId(runId);
        return runDirectory(runId).resolve("journal.axr");
    }

    Path lifecyclePathForTest(String runId) {
        validateRunId(runId);
        return lifecyclePath(runId);
    }

    Path trashPathForTest(String runId) {
        validateRunId(runId);
        return trashDirectory(runId);
    }

    Path runStripeLockPathForTest(String runId) {
        validateRunId(runId);
        int stripe = RunLifecycleCatalogKernel.shard(
                RunLifecycleCatalogKernel.digestHex(runId));
        return root.resolve("locks-v1").resolve(String.format(
                java.util.Locale.ROOT, "%02x.lock", stripe));
    }

    private <T> T locked(String runId, boolean create, Operation<T> operation) {
        validateRunId(runId);
        return withRunStripeLock(runId, () -> {
            Path directory = runDirectory(runId);
            try {
                if (!create && !Files.exists(directory)) {
                    return operation.apply(Loaded.empty(directory.resolve("journal.axr")));
                }
                boolean directoryExisted = Files.exists(directory);
                if (create && !directoryExisted) {
                    RunLifecycleRef reserved = readLifecycle(runId, false);
                    if (reserved != null) {
                        return operation.apply(Loaded.empty(
                                directory.resolve("journal.axr")));
                    }
                    cleanupPurgedTrashForReuse(runId);
                }
                Files.createDirectories(directory);
                requireDirectory(directory, "run directory");
                if (!directoryExisted) forceDirectory(root);
                Loaded loaded = load(runId, directory.resolve("journal.axr"));
                if (loaded.snapshot != null) reconcileLifecycle(loaded.snapshot);
                return operation.apply(loaded);
            } catch (IOException failure) {
                throw new UncheckedIOException(
                        "run state file operation failed for " + runId, failure);
            }
        });
    }

    private static Loaded load(String expectedRunId, Path journal) throws IOException {
        if (!Files.exists(journal)) return Loaded.empty(journal);
        long size = Files.size(journal);
        if (size > MAX_JOURNAL_BYTES || size > Integer.MAX_VALUE) {
            throw new IllegalStateException("run journal exceeds limit: " + size);
        }
        byte[] bytes = Files.readAllBytes(journal);
        Loaded loaded = Loaded.empty(journal);
        int offset = 0;
        while (offset < bytes.length) {
            int remaining = bytes.length - offset;
            if (remaining < FRAME_HEADER_BYTES) {
                truncateTornTail(journal, offset);
                loaded.journalLength = offset;
                return loaded;
            }
            int bodyLength = readInt(bytes, offset + 8);
            if (bodyLength < 0 || bodyLength > RunStateCodec.MAX_FRAME_BYTES) {
                throw corruption(expectedRunId, offset, "invalid frame body length " + bodyLength, null);
            }
            long totalLong = (long) FRAME_HEADER_BYTES + bodyLength + FRAME_CHECKSUM_BYTES;
            if (totalLong > RunStateCodec.MAX_FRAME_BYTES) {
                throw corruption(expectedRunId, offset, "frame exceeds limit", null);
            }
            int total = (int) totalLong;
            if (remaining < total) {
                truncateTornTail(journal, offset);
                loaded.journalLength = offset;
                return loaded;
            }
            byte[] frame = Arrays.copyOfRange(bytes, offset, offset + total);
            final RunJournalRecord record;
            try {
                record = RunStateCodec.decodeRecord(frame);
            } catch (IllegalArgumentException malformed) {
                throw corruption(expectedRunId, offset, "invalid complete frame", malformed);
            }
            verifyAndApply(expectedRunId, loaded, record, offset);
            offset += total;
            loaded.journalLength = offset;
        }
        return loaded;
    }

    private static void verifyAndApply(String expectedRunId, Loaded loaded,
                                       RunJournalRecord record, int offset) {
        RunCommand command = record.command();
        if (!expectedRunId.equals(command.runId)) {
            throw corruption(expectedRunId, offset, "run identity/hash path mismatch", null);
        }
        if (loaded.commands.containsKey(command.commandId)) {
            throw corruption(expectedRunId, offset, "duplicate commandId in journal", null);
        }
        RunDecision recomputed;
        try {
            recomputed = RunStateMachine.apply(loaded.snapshot, command);
        } catch (RuntimeException invalid) {
            throw corruption(expectedRunId, offset, "state machine rejected journal record", invalid);
        }
        if (!recomputed.accepted()
                || !Arrays.equals(RunStateCodec.encodeSnapshot(recomputed.snapshot()),
                RunStateCodec.encodeSnapshot(record.snapshot()))
                || !Arrays.equals(RunStateCodec.encodeEvent(recomputed.event()),
                RunStateCodec.encodeEvent(record.event()))) {
            throw corruption(expectedRunId, offset, "journal record does not match state machine", null);
        }
        loaded.snapshot = record.snapshot();
        loaded.events.add(record.event());
        loaded.commands.put(command.commandId,
                new StoredCommand(command.commandDigestUnsafe(), record.event()));
    }

    private static void appendAndForce(Path journal, long expectedLength, byte[] frame) {
        try {
            boolean existed = Files.exists(journal);
            try (FileChannel channel = FileChannel.open(journal,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                if (channel.size() != expectedLength) {
                    throw new IllegalStateException("run journal changed while locked");
                }
                channel.position(expectedLength);
                ByteBuffer buffer = ByteBuffer.wrap(frame);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            if (!existed) forceDirectory(journal.getParent());
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to append durable run record", failure);
        }
    }

    private static void truncateTornTail(Path journal, long length) throws IOException {
        try (FileChannel channel = FileChannel.open(journal, StandardOpenOption.WRITE)) {
            channel.truncate(length);
            channel.force(true);
        }
    }

    private static void forceDirectory(Path directory) throws IOException {
        if (directory == null) return;
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private RunLifecycleRef lifecycleFor(RunSnapshot snapshot) {
        if (snapshot.status().isTerminal()) {
            return RunLifecycleRef.terminal(snapshot.runId(), snapshot.revision(),
                    snapshot.status(), snapshot.artifactId(), nextAuthorityTime());
        }
        return RunLifecycleRef.active(snapshot.runId(), snapshot.revision(),
                snapshot.status(), snapshot.artifactId());
    }

    private long nextAuthorityTime() {
        long observed = authorityClock.getAsLong();
        if (observed <= 0L) {
            throw new IllegalStateException("authority clock must be > 0");
        }
        return observeAuthorityTime(observed);
    }

    private long observeAuthorityTime(long observed) {
        if (observed <= 0L) {
            throw new IllegalStateException("authority time must be > 0");
        }
        for (;;) {
            long previous = lastAuthorityTime.get();
            long next = Math.max(previous, observed);
            if (previous == next || lastAuthorityTime.compareAndSet(previous, next)) {
                return next;
            }
        }
    }

    private void reconcileLifecycle(RunSnapshot snapshot) {
        RunLifecycleRef ref = readLifecycle(snapshot.runId(), true);
        if (!ref.artifactId().equals(snapshot.artifactId())
                || ref.revision() > snapshot.revision()) {
            throw lifecycleCorruption(snapshot.runId(), "lifecycle is ahead or has another artifact");
        }
        if (ref.revision() == snapshot.revision()) {
            boolean expectedState = snapshot.status().isTerminal()
                    ? ref.state() == RunLifecycleState.TERMINAL
                    || ref.state() == RunLifecycleState.DELETING
                    : ref.state() == RunLifecycleState.ACTIVE;
            if (ref.status() != snapshot.status() || !expectedState) {
                throw lifecycleCorruption(snapshot.runId(), "lifecycle snapshot mismatch");
            }
            if (ref.terminalAtMillis() > 0L) observeAuthorityTime(ref.terminalAtMillis());
            return;
        }
        if (ref.state() != RunLifecycleState.ACTIVE) {
            throw lifecycleCorruption(snapshot.runId(), "terminal lifecycle is behind journal");
        }
        writeLifecycle(lifecycleFor(snapshot));
    }

    private boolean reserveLifecycle(RunLifecycleRef intended) {
        String digest = RunLifecycleCatalogKernel.digestHex(intended.runId());
        int shard = RunLifecycleCatalogKernel.shard(digest);
        return withLifecycleShardLock(shard, () -> {
            RunLifecycleRef existing = readLifecycle(intended.runId(), false);
            if (existing != null) {
                if (sameLifecycle(existing, intended)) return true;
                throw lifecycleCorruption(intended.runId(), "lifecycle reservation conflict");
            }
            List<RunLifecycleRef> entries = readLifecycleShard(shard);
            if (entries.size() >= maxLifecyclePerShard) return false;
            writeLifecycle(intended);
            return true;
        });
    }

    private static boolean sameLifecycle(RunLifecycleRef left, RunLifecycleRef right) {
        return left.runId().equals(right.runId())
                && left.revision() == right.revision()
                && left.status() == right.status()
                && left.artifactId().equals(right.artifactId())
                && left.state() == right.state()
                && left.terminalAtMillis() == right.terminalAtMillis();
    }

    private RunLifecycleRef readLifecycle(String runId, boolean required) {
        Path path = lifecyclePath(runId);
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            if (required) throw lifecycleCorruption(runId, "lifecycle entry is missing");
            return null;
        }
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw lifecycleCorruption(runId, "lifecycle entry is not a regular file");
        }
        try {
            byte[] bytes = Files.readAllBytes(path);
            RunLifecycleRef ref = RunRetentionCodec.decodeRef(bytes);
            if (!runId.equals(ref.runId())
                    || !path.equals(lifecyclePath(ref.runId()))) {
                throw lifecycleCorruption(runId, "lifecycle identity/hash mismatch");
            }
            return ref;
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to read lifecycle for " + runId, failure);
        } catch (IllegalArgumentException malformed) {
            throw lifecycleCorruption(runId, "invalid lifecycle entry", malformed);
        }
    }

    private List<RunLifecycleRef> readLifecycleShard(int shard) {
        Path directory = lifecycleEntriesDirectory(shard);
        if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
            return Collections.emptyList();
        }
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("lifecycle shard is not a directory: " + directory);
        }
        List<RunLifecycleRef> refs = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.axl")) {
            for (Path path : stream) {
                if (refs.size() >= maxLifecyclePerShard) {
                    throw new IllegalStateException("lifecycle shard exceeds configured capacity");
                }
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IllegalStateException("lifecycle entry is not a regular file: " + path);
                }
                final RunLifecycleRef ref;
                try {
                    ref = RunRetentionCodec.decodeRef(Files.readAllBytes(path));
                } catch (IllegalArgumentException malformed) {
                    throw lifecycleCorruption(path.getFileName().toString(),
                            "invalid lifecycle entry", malformed);
                }
                if (!path.equals(lifecyclePath(ref.runId()))
                        || RunLifecycleCatalogKernel.shard(
                        RunLifecycleCatalogKernel.digestHex(ref.runId())) != shard) {
                    throw lifecycleCorruption(ref.runId(), "lifecycle path identity mismatch");
                }
                refs.add(ref);
            }
            return refs;
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to scan lifecycle shard " + shard, failure);
        }
    }

    private void writeLifecycle(RunLifecycleRef ref) {
        Path path = lifecyclePath(ref.runId());
        Path directory = path.getParent();
        Path temp = directory.resolve(path.getFileName().toString() + ".tmp");
        byte[] bytes = RunRetentionCodec.encodeRef(ref);
        try {
            Path entriesRoot = lifecycleRoot.resolve("entries");
            boolean entriesExisted = Files.exists(entriesRoot, LinkOption.NOFOLLOW_LINKS);
            boolean directoryExisted = Files.exists(directory, LinkOption.NOFOLLOW_LINKS);
            Files.createDirectories(directory);
            requireDirectory(entriesRoot, "lifecycle entries root");
            requireDirectory(directory, "lifecycle shard");
            if (!entriesExisted) forceDirectory(lifecycleRoot);
            if (!directoryExisted) forceDirectory(entriesRoot);
            if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)
                    && !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                throw lifecycleCorruption(ref.runId(),
                        "lifecycle target is not a regular file");
            }
            if (Files.exists(temp, LinkOption.NOFOLLOW_LINKS)
                    && !Files.isRegularFile(temp, LinkOption.NOFOLLOW_LINKS)) {
                throw lifecycleCorruption(ref.runId(),
                        "lifecycle temp is not a regular file");
            }
            try (FileChannel channel = FileChannel.open(temp,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING, LinkOption.NOFOLLOW_LINKS)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            try {
                Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IllegalStateException(
                        "run lifecycle requires atomic move: " + path, unsupported);
            }
            forceDirectory(directory);
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to publish lifecycle for " + ref.runId(), failure);
        }
    }

    private void deleteLifecycle(RunLifecycleRef expected) throws IOException {
        Path path = lifecyclePath(expected.runId());
        RunLifecycleRef current = readLifecycle(expected.runId(), true);
        if (!sameLifecycle(current, expected)
                || !java.util.Objects.equals(
                        current.retentionToken(), expected.retentionToken())) {
            throw lifecycleCorruption(expected.runId(),
                    "lifecycle changed before final purge");
        }
        Files.delete(path);
        forceDirectory(path.getParent());
    }

    private static boolean matchesTerminalToken(
            RunSnapshot snapshot, RetentionToken token) {
        return snapshot != null && snapshot.status().isTerminal()
                && snapshot.runId().equals(token.runId())
                && snapshot.revision() == token.terminalRevision();
    }

    private static boolean validTrash(
            String runId, Path trash, RetentionToken token) throws IOException {
        if (!validTrashShape(trash)) return false;
        Loaded loaded = load(runId, trash.resolve("journal.axr"));
        return matchesTerminalToken(loaded.snapshot, token);
    }

    private static boolean validPublishedTrash(
            String runId, Path trash, RetentionToken token) throws IOException {
        if (!validTrashShape(trash)) return false;
        Path journal = trash.resolve("journal.axr");
        if (!Files.exists(journal, LinkOption.NOFOLLOW_LINKS)) return true;
        Loaded loaded = load(runId, journal);
        return matchesTerminalToken(loaded.snapshot, token);
    }

    private static boolean validTrashShape(Path trash) throws IOException {
        if (!Files.isDirectory(trash, LinkOption.NOFOLLOW_LINKS)) return false;
        try (DirectoryStream<Path> children = Files.newDirectoryStream(trash)) {
            int count = 0;
            for (Path child : children) {
                if (++count > 2 || (!child.getFileName().toString().equals("journal.axr")
                        && !child.getFileName().toString().equals("writer.lock"))
                        || !Files.isRegularFile(child, LinkOption.NOFOLLOW_LINKS)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static void deleteTrashBestEffort(Path trash) {
        try {
            Files.deleteIfExists(trash.resolve("journal.axr"));
            Files.deleteIfExists(trash.resolve("writer.lock"));
            Files.deleteIfExists(trash);
            forceDirectory(trash.getParent());
        } catch (IOException ignored) {
            // Publication already completed by live-dir move + lifecycle removal.
        }
    }

    private void cleanupPurgedTrashForReuse(String runId) throws IOException {
        Path trash = trashDirectory(runId);
        if (!Files.exists(trash, LinkOption.NOFOLLOW_LINKS)) return;
        if (!validTrashIdentity(runId, trash)) {
            throw lifecycleCorruption(runId,
                    "unrecognized quarantine blocks identity reuse");
        }
        Files.deleteIfExists(trash.resolve("journal.axr"));
        Files.deleteIfExists(trash.resolve("writer.lock"));
        Files.delete(trash);
        forceDirectory(trash.getParent());
    }

    private static boolean validTrashIdentity(String runId, Path trash)
            throws IOException {
        if (!validTrashShape(trash)) return false;
        Path journal = trash.resolve("journal.axr");
        if (!Files.exists(journal, LinkOption.NOFOLLOW_LINKS)) return true;
        Loaded loaded = load(runId, journal);
        return loaded.snapshot != null
                && loaded.snapshot.status().isTerminal()
                && runId.equals(loaded.snapshot.runId());
    }

    private static void validatePurge(RetentionToken token, int maxDeletes) {
        if (token == null) throw new IllegalArgumentException("token is required");
        if (maxDeletes < 1 || maxDeletes > 1000) {
            throw new IllegalArgumentException("maxDeletes must be in [1,1000]");
        }
    }

    private <T> T withRunStripeLock(String runId, LifecycleOperation<T> operation) {
        String digest = RunLifecycleCatalogKernel.digestHex(runId);
        int stripe = RunLifecycleCatalogKernel.shard(digest);
        ReentrantLock jvm = JVM_LOCKS[stripe];
        jvm.lock();
        try {
            Path lockDirectory = root.resolve("locks-v1");
            boolean directoryExisted = Files.exists(
                    lockDirectory, LinkOption.NOFOLLOW_LINKS);
            Files.createDirectories(lockDirectory);
            requireDirectory(lockDirectory, "run stripe lock directory");
            if (!directoryExisted) forceDirectory(root);
            Path lockPath = lockDirectory.resolve(String.format(
                    java.util.Locale.ROOT, "%02x.lock", stripe));
            boolean existed = Files.exists(lockPath, LinkOption.NOFOLLOW_LINKS);
            try (FileChannel channel = FileChannel.open(lockPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    LinkOption.NOFOLLOW_LINKS);
                 FileLock ignored = channel.lock()) {
                if (!existed) forceDirectory(lockDirectory);
                return operation.apply();
            }
        } catch (IOException failure) {
            throw new UncheckedIOException(
                    "failed to lock run stripe for " + runId, failure);
        } finally {
            jvm.unlock();
        }
    }

    private <T> T withLifecycleShardLock(int shard, LifecycleOperation<T> operation) {
        ReentrantLock jvm = LIFECYCLE_JVM_LOCKS[shard];
        jvm.lock();
        try {
            Path lockDirectory = lifecycleRoot.resolve("locks");
            boolean directoryExisted = Files.exists(
                    lockDirectory, LinkOption.NOFOLLOW_LINKS);
            Files.createDirectories(lockDirectory);
            requireDirectory(lockDirectory, "lifecycle lock directory");
            if (!directoryExisted) forceDirectory(lifecycleRoot);
            Path lockPath = lockDirectory.resolve(String.format(java.util.Locale.ROOT,
                    "%02x.lock", shard));
            boolean existed = Files.exists(lockPath, LinkOption.NOFOLLOW_LINKS);
            try (FileChannel channel = FileChannel.open(lockPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    LinkOption.NOFOLLOW_LINKS);
                 FileLock ignored = channel.lock()) {
                if (!existed) forceDirectory(lockDirectory);
                return operation.apply();
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("failed to lock lifecycle shard " + shard, failure);
        } finally {
            jvm.unlock();
        }
    }

    private Path lifecyclePath(String runId) {
        String digest = RunLifecycleCatalogKernel.digestHex(runId);
        int shard = RunLifecycleCatalogKernel.shard(digest);
        return lifecycleEntriesDirectory(shard).resolve(digest + ".axl");
    }

    private Path lifecycleEntriesDirectory(int shard) {
        return lifecycleRoot.resolve("entries").resolve(
                String.format(java.util.Locale.ROOT, "%02x", shard));
    }

    private static void requireDirectory(Path directory, String kind) {
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException(kind + " is not a directory: " + directory);
        }
    }

    private Path runDirectory(String runId) {
        return root.resolve(hex(sha256(runId.getBytes(java.nio.charset.StandardCharsets.UTF_8))));
    }

    private Path trashDirectory(String runId) {
        return root.resolve("trash-v1").resolve(
                hex(sha256(runId.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                        + ".deleting");
    }

    private static void validateRunId(String runId) {
        RunText.required(runId, "runId", RunText.MAX_ID_BYTES);
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) | ((bytes[offset + 1] & 0xff) << 16)
                | ((bytes[offset + 2] & 0xff) << 8) | (bytes[offset + 3] & 0xff);
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String hex(byte[] bytes) {
        return RunCommandCanonical.hex(bytes);
    }

    private static IllegalStateException corruption(String runId, long offset,
                                                     String message, Throwable cause) {
        String detail = "corrupt run journal run=" + runId + " offset=" + offset + ": " + message;
        return cause == null ? new IllegalStateException(detail) : new IllegalStateException(detail, cause);
    }

    private static IllegalStateException lifecycleCorruption(String runId, String message) {
        return lifecycleCorruption(runId, message, null);
    }

    private static IllegalStateException lifecycleCorruption(
            String runId, String message, Throwable cause) {
        String detail = "corrupt run lifecycle run=" + runId + ": " + message;
        return cause == null ? new IllegalStateException(detail)
                : new IllegalStateException(detail, cause);
    }

    private interface Operation<T> {
        T apply(Loaded loaded);
    }

    private interface LifecycleOperation<T> {
        T apply();
    }

    interface PurgeObserver {
        PurgeObserver NOOP = new PurgeObserver() {
            @Override
            public void afterMove() { }

            @Override
            public void afterLifecycleDelete() { }
        };

        void afterMove();
        void afterLifecycleDelete();
    }

    private static final class Loaded {
        final Path journal;
        RunSnapshot snapshot;
        final List<ExecutionEvent> events = new ArrayList<>();
        final Map<String, StoredCommand> commands = new LinkedHashMap<>();
        long journalLength;

        private Loaded(Path journal) { this.journal = journal; }
        static Loaded empty(Path journal) { return new Loaded(journal); }
    }

    private static final class StoredCommand {
        final byte[] digest;
        final ExecutionEvent event;

        StoredCommand(byte[] digest, ExecutionEvent event) {
            this.digest = digest.clone();
            this.event = event;
        }
    }
}
