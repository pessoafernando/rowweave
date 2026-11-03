package io.rowweave.incremental;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Pure, bounded state model. This is deliberately test-only, not production authority. */
final class IncrementalSafetyReferenceModel {
    private static final int HARD_AUTHORITY_WORDS = 80;
    private static final int SHARD_COUNT = 256;
    private static final int ENTRIES_PER_SHARD = 16;
    private static final int MAX_FEEDS = SHARD_COUNT * ENTRIES_PER_SHARD;

    enum Property {
        MONOTONIC_FRONTIER,
        NO_GAP,
        NO_FRONTIER_AHEAD,
        SINGLE_ACTIVE_INTERVAL,
        FROZEN_TARGET,
        FENCE_SAFETY,
        INTENT_MONOTONICITY,
        BACKFILL_ISOLATION,
        BOUNDED_AUTHORITY
    }

    enum Action {
        CAPTURE,
        CLAIM,
        READ,
        PREPARE,
        INTENT,
        PUBLISH,
        QUERY,
        FRONTIER_CAS,
        ACK_LOSS,
        ABORT,
        CANCEL,
        RETRY,
        LEADER_REPLACEMENT
    }

    enum UnsafeOrder {
        FRONTIER_BEFORE_PUBLISH,
        INTENT_ONLY_VISIBLE_PROOF,
        STALE_QUERY_PROOF_AFTER_CRASH,
        SPLIT_FRONTIER_RUN_SUCCESS,
        NEW_ATTEMPT_AFTER_INTENT,
        FAILED_THEN_CLEAR_ACTIVE,
        CLEAR_ACTIVE_THEN_RETARGET,
        STAGE_SUCCESS_AS_COMMIT_PROOF,
        UNKNOWN_QUERY_GUESSED_COMMITTED,
        TIMESTAMP_WITHOUT_TIE_BREAK,
        LATE_LEFT_RECORD,
        OLD_BACKFILL_REPLAY,
        SAME_GENERATION_DIFFERENT_DIGEST,
        FEED_PURGE_RECREATE_ABA,
        LOCAL_LEADER_TOCTOU,
        RUN_ID_DIFFERENT_BINDING,
        SELF_REFERENTIAL_HASH
    }

    private enum Mode { NORMAL, BACKFILL }
    private enum Credential { CURRENT, STALE_FENCE, STALE_LEADER, STALE_INCARNATION }
    private enum QueryObservation { NONE, EXACT, UNKNOWN, CONFLICT }
    private enum RunTerminal { SUCCEEDED, FAILED }
    private enum AckPoint { NONE, CLAIM, PUBLISH, TERMINAL }

    static Exploration exhaustive(int maximumDepth) {
        if (maximumDepth < 1 || maximumDepth > 12) {
            throw new IllegalArgumentException("reference depth must be in 1..12");
        }
        MutableReport report = new MutableReport();
        Map<State, List<String>> frontier = new HashMap<>();
        State initial = State.initial();
        frontier.put(initial, Collections.singletonList("INITIALIZE"));
        Set<State> visited = new HashSet<>();
        visited.add(initial);
        for (int depth = 0; depth < maximumDepth && !frontier.isEmpty(); depth++) {
            Map<State, List<String>> next = new HashMap<>();
            for (Map.Entry<State, List<String>> entry : frontier.entrySet()) {
                for (Command command : commands(entry.getKey())) {
                    Transition transition = apply(entry.getKey(), command);
                    report.transitionCount++;
                    report.observedActions.add(command.action);
                    List<String> trace = append(entry.getValue(), command.describe(transition.applied));
                    checkAll(entry.getKey(), command, transition, trace, report);
                    report.observe(entry.getKey(), command, transition);
                    if (visited.add(transition.after)) next.put(transition.after, trace);
                }
            }
            frontier = next;
        }
        report.reachableStateCount = visited.size();
        return report.freeze();
    }

    static BackfillReplayProfile backfillThenNormalThenReplay() {
        State state = State.initial();
        long committedBefore = state.committed;
        state = complete(state, captureBackfill(state, state.lastBackfillOrdinal + 1));
        long committedAfterBackfill = state.committed;
        int publicationsAfterBackfill = state.publicationCount;
        long ordinalBeforeReplay = state.lastBackfillOrdinal;
        state = complete(state, captureNormal(state, state.committed + 2));
        long committedAfterNormal = state.committed;
        int publicationsBeforeReplay = state.publicationCount;

        Command staleReplay = Command.capture(
                Mode.BACKFILL, state.committed, state.committed + 1,
                ordinalBeforeReplay, state.nextTriggerId);
        Transition replay = apply(state, staleReplay);
        boolean rejectedBeforeArtifact = !replay.applied
                && replay.after.publicationCount == publicationsBeforeReplay;
        return new BackfillReplayProfile(
                committedBefore, committedAfterBackfill, committedAfterNormal,
                publicationsBeforeReplay, replay.after.publicationCount,
                ordinalBeforeReplay, replay.after.lastBackfillOrdinal,
                rejectedBeforeArtifact,
                !publicationIdentity(state.lastNormalReceipt).equals(
                        publicationIdentity(state.lastBackfillReceipt)),
                publicationsAfterBackfill);
    }

    static HistoryProfile boundedHistory(int cycles) {
        if (cycles < 1) throw new IllegalArgumentException("cycles must be positive");
        State state = State.initial();
        int initialWords = state.authorityWords();
        int maximumWords = initialWords;
        int maximumActive = 0;
        int normalReceipts = 0;
        int backfillReceipts = 0;
        int transitions = 0;
        for (int cycle = 0; cycle < cycles; cycle++) {
            Command capture = (cycle & 1) == 0
                    ? captureNormal(state, state.committed + 1)
                    : captureBackfill(state, state.lastBackfillOrdinal + 1);
            state = requireApplied(state, capture);
            transitions++;
            maximumWords = Math.max(maximumWords, state.authorityWords());
            maximumActive = Math.max(maximumActive, state.activeCount());
            Action[] actions = {Action.CLAIM, Action.READ, Action.PREPARE, Action.INTENT,
                    Action.PUBLISH, Action.QUERY, Action.FRONTIER_CAS};
            for (Action action : actions) {
                Command command = action == Action.QUERY
                        ? Command.query(true, state) : Command.current(action, state);
                state = requireApplied(state, command);
                transitions++;
                maximumWords = Math.max(maximumWords, state.authorityWords());
                maximumActive = Math.max(maximumActive, state.activeCount());
            }
            normalReceipts = state.lastNormalReceipt == null ? 0 : 1;
            backfillReceipts = state.lastBackfillReceipt == null ? 0 : 1;
        }
        return new HistoryProfile(
                initialWords, maximumWords, HARD_AUTHORITY_WORDS, transitions,
                maximumActive, normalReceipts, backfillReceipts);
    }

    static CatalogProfile catalogProfile(int initializeAttempts) {
        if (initializeAttempts < 0) throw new IllegalArgumentException("attempts cannot be negative");
        GuardCatalog catalog = new GuardCatalog();
        int accepted = 0;
        int rejected = 0;
        int candidate = 0;
        String firstAccepted = null;
        while (accepted < Math.min(initializeAttempts, MAX_FEEDS)) {
            String feedId = "feed-" + candidate++;
            if (!catalog.shardHasCapacity(feedId)) continue;
            if (!catalog.initialize(feedId)) throw new AssertionError("capacity probe drift");
            if (firstAccepted == null) firstAccepted = feedId;
            accepted++;
        }
        while (accepted + rejected < initializeAttempts) {
            String feedId = "overflow-" + candidate++;
            if (catalog.initialize(feedId)) {
                throw new AssertionError("full guard catalog accepted a new feed");
            }
            rejected++;
        }
        int guardsBeforePurge = catalog.guardEntryCount();
        long incarnationBefore = firstAccepted == null ? 0 : catalog.incarnation(firstAccepted);
        catalog.purgeAllLiveFeeds();
        boolean guardsSurvivePurge = catalog.guardEntryCount() == guardsBeforePurge
                && catalog.liveFeedCount() == 0;
        boolean incarnationAdvanced = firstAccepted == null
                || catalog.initialize(firstAccepted)
                && catalog.incarnation(firstAccepted) == incarnationBefore + 1;
        if (accepted > MAX_FEEDS) throw new AssertionError("catalog exceeded hard feed limit");
        return new CatalogProfile(
                accepted, rejected, SHARD_COUNT, catalog.maximumEntriesPerShard(),
                catalog.maximumCanonicalFrameBytes(), catalog.canonicalFramesMeasured(),
                guardsSurvivePurge, incarnationAdvanced);
    }

    static Counterexample counterexample(UnsafeOrder order) {
        if (order == null) throw new IllegalArgumentException("unsafe order is required");
        UnsafeReplay replay = UnsafeReplay.run(unsafeSteps(order));
        UnsafeEvidence evidence = replay.evidence();
        String violatedRule = detectUnsafe(evidence);
        return new Counterexample(
                violatedRule != null, violatedRule == null ? "" : violatedRule,
                replay.trace(), replay.transitionCount());
    }

    private static State complete(State initial, Command capture) {
        State state = requireApplied(initial, capture);
        state = requireApplied(state, Command.current(Action.CLAIM, state));
        state = requireApplied(state, Command.current(Action.READ, state));
        state = requireApplied(state, Command.current(Action.PREPARE, state));
        state = requireApplied(state, Command.current(Action.INTENT, state));
        state = requireApplied(state, Command.current(Action.PUBLISH, state));
        state = requireApplied(state, Command.query(true, state));
        return requireApplied(state, Command.current(Action.FRONTIER_CAS, state));
    }

    private static State requireApplied(State state, Command command) {
        Transition transition = apply(state, command);
        if (!transition.applied) throw new AssertionError("reference scenario rejected " + command.action);
        return transition.after;
    }

    private static List<Command> commands(State state) {
        List<Command> commands = new ArrayList<>();
        commands.add(captureNormal(state, state.committed + 1));
        commands.add(captureNormal(state, state.committed + 2));
        commands.add(captureBackfill(state, state.lastBackfillOrdinal + 1));
        if (state.lastClosedCandidate != null) {
            commands.add(Command.capture(
                    Mode.NORMAL, state.committed, state.committed + 2, 0,
                    state.lastClosedCandidate.triggerId));
        }
        commands.add(Command.current(Action.CLAIM, state));
        commands.add(Command.withCredential(Action.CLAIM, state, Credential.STALE_INCARNATION));
        commands.add(Command.current(Action.READ, state));
        commands.add(Command.withCredential(Action.READ, state, Credential.STALE_FENCE));
        commands.add(Command.current(Action.PREPARE, state));
        commands.add(Command.withCredential(Action.PREPARE, state, Credential.STALE_FENCE));
        commands.add(Command.current(Action.INTENT, state));
        commands.add(Command.current(Action.PUBLISH, state));
        commands.add(Command.query(true, state));
        commands.add(Command.query(false, state));
        commands.add(Command.queryConflict(state));
        commands.add(Command.current(Action.FRONTIER_CAS, state));
        commands.add(Command.withCredential(Action.FRONTIER_CAS, state, Credential.STALE_FENCE));
        commands.add(Command.withCredential(Action.FRONTIER_CAS, state, Credential.STALE_LEADER));
        commands.add(Command.withCredential(Action.FRONTIER_CAS, state, Credential.STALE_INCARNATION));
        commands.add(Command.ack(state, false));
        commands.add(Command.ack(state, true));
        commands.add(Command.current(Action.ABORT, state));
        commands.add(Command.current(Action.CANCEL, state));
        commands.add(Command.withCredential(Action.CANCEL, state, Credential.STALE_FENCE));
        commands.add(Command.withCredential(Action.CANCEL, state, Credential.STALE_LEADER));
        commands.add(Command.withCredential(Action.CANCEL, state, Credential.STALE_INCARNATION));
        commands.add(Command.current(Action.RETRY, state));
        commands.add(Command.current(Action.LEADER_REPLACEMENT, state));
        return commands;
    }

    private static Command captureNormal(State state, long target) {
        return Command.capture(
                Mode.NORMAL, state.committed, target, 0, state.nextTriggerId);
    }

    private static Command captureBackfill(State state, long ordinal) {
        return Command.capture(
                Mode.BACKFILL, state.committed, state.committed + 1,
                ordinal, state.nextTriggerId);
    }

    private static Transition apply(State before, Command command) {
        if (before.conflict && command.action != Action.ACK_LOSS) {
            return Transition.rejected(before, command);
        }
        switch (command.action) {
            case CAPTURE:
                if (before.conflict || before.pending != null || before.active != null
                        || command.target <= command.from
                        || command.triggerId != before.nextTriggerId
                        || (command.mode == Mode.NORMAL && command.from != before.committed)
                        || (command.mode == Mode.BACKFILL
                        && command.ordinal != before.lastBackfillOrdinal + 1)) {
                    return Transition.rejected(before, command);
                }
                Candidate candidate = new Candidate(
                        command.mode, command.from, command.target, command.ordinal,
                        command.triggerId,
                        intervalIdentity(command.mode, before.feedIncarnation,
                                command.from, command.target, command.ordinal,
                                command.triggerId));
                return Transition.applied(before.withPending(candidate), command);
            case CLAIM:
                if (before.pending == null || before.active != null
                        || !credentialsMatch(before, command, 1)) {
                    return Transition.rejected(before, command);
                }
                return Transition.applied(before.claim(), command);
            case READ:
                if (!activeCredentialsMatch(before, command)) return Transition.rejected(before, command);
                return Transition.applied(before.withActive(before.active.withRead()), command);
            case PREPARE:
                if (!activeCredentialsMatch(before, command) || !before.active.read) {
                    return Transition.rejected(before, command);
                }
                return Transition.applied(before.withActive(before.active.withPrepared()), command);
            case INTENT:
                if (!activeCredentialsMatch(before, command) || !before.active.prepared
                        || before.active.abortClosed) {
                    return Transition.rejected(before, command);
                }
                return Transition.applied(before.withActive(before.active.withIntent()), command);
            case PUBLISH:
                if (!activeCredentialsMatch(before, command) || !before.active.intent) {
                    return Transition.rejected(before, command);
                }
                if (before.visiblePublication != null
                        && before.visiblePublication.equals(Publication.forActive(before.active))) {
                    return Transition.applied(before, command);
                }
                return Transition.applied(before.publish(), command);
            case QUERY:
                if (!activeCredentialsMatch(before, command)
                        || before.visiblePublication == null
                        || command.queryObservation == QueryObservation.UNKNOWN) {
                    return Transition.rejected(before, command);
                }
                if (command.queryObservation == QueryObservation.CONFLICT) {
                    return Transition.applied(before.withConflict(), command);
                }
                if (command.queryObservation != QueryObservation.EXACT) {
                    return Transition.rejected(before, command);
                }
                if (!before.visiblePublication.equals(Publication.forActive(before.active))) {
                    return Transition.rejected(before, command);
                }
                return Transition.applied(
                        before.withActive(before.active.withProof(before.ownerTerm)), command);
            case FRONTIER_CAS:
                if (!activeCredentialsMatch(before, command)
                        || before.active.proofOwnerTerm != before.ownerTerm) {
                    return Transition.rejected(before, command);
                }
                if (before.active.candidate.mode == Mode.NORMAL) {
                    if (before.active.candidate.from != before.committed
                            || before.active.candidate.target <= before.committed) {
                        return Transition.rejected(before, command);
                    }
                    return Transition.applied(before.commitNormal(), command);
                }
                if (before.active.candidate.ordinal != before.lastBackfillOrdinal + 1) {
                    return Transition.rejected(before, command);
                }
                return Transition.applied(before.commitBackfill(), command);
            case ACK_LOSS:
                if (!ackReceiptMatches(before, command.expectedAck)) {
                    return Transition.rejected(before, command);
                }
                return Transition.applied(before, command);
            case ABORT:
                if (!activeCredentialsMatch(before, command) || before.active.intent
                        || !before.active.prepared || before.active.abortClosed) {
                    return Transition.rejected(before, command);
                }
                return Transition.applied(
                        before.withActive(before.active.withAbortClosed()), command);
            case CANCEL:
                if (before.active != null) {
                    if (before.active.intent || !activeCredentialsMatch(before, command)
                            || before.active.prepared && !before.active.abortClosed) {
                        return Transition.rejected(before, command);
                    }
                    return Transition.applied(before.clearActive(), command);
                }
                if (before.pending != null) {
                    if (!credentialsMatch(before, command, 1)) {
                        return Transition.rejected(before, command);
                    }
                    return Transition.applied(before.clearPending(), command);
                }
                return Transition.rejected(before, command);
            case RETRY:
                if (!activeCredentialsMatch(before, command)
                        || before.active.intent || before.active.attempt >= 3
                        || before.active.prepared && !before.active.abortClosed) {
                    return Transition.rejected(before, command);
                }
                return Transition.applied(before.retryActive(), command);
            case LEADER_REPLACEMENT:
                if (before.ownerTerm >= 3) return Transition.rejected(before, command);
                return Transition.applied(before.replaceLeader(), command);
            default:
                throw new AssertionError(command.action);
        }
    }

    private static boolean activeCredentialsMatch(State state, Command command) {
        return state.active != null && credentialsMatch(state, command, state.active.attempt);
    }

    private static boolean credentialsMatch(State state, Command command, int attempt) {
        return command.feedIncarnation == state.feedIncarnation
                && command.ownerTerm == state.ownerTerm
                && command.attempt == attempt;
    }

    private static boolean ackReceiptMatches(State state, AckReceipt expected) {
        if (expected == null || !expected.equals(state.ackReceipt)) return false;
        if (expected.point == AckPoint.CLAIM) {
            return state.active != null && state.ownerTerm == expected.ownerTerm
                    && state.active.attempt == expected.attempt
                    && state.active.candidate.intervalId.equals(expected.intervalId);
        }
        if (expected.point == AckPoint.PUBLISH) {
            return state.active != null && state.ownerTerm == expected.ownerTerm
                    && state.active.attempt == expected.attempt
                    && state.visiblePublication != null
                    && state.visiblePublication.publicationId.equals(expected.publicationId);
        }
        if (expected.point == AckPoint.TERMINAL) {
            RunOutcome terminal = state.lastOutcome == null ? null
                    : state.lastOutcome.mode == Mode.NORMAL
                    ? state.lastRunOutcome : state.lastBackfillRunOutcome;
            return state.active == null && state.lastOutcome != null
                    && terminal != null && terminal.terminal == RunTerminal.SUCCEEDED
                    && state.lastOutcome.intervalId.equals(expected.intervalId)
                    && state.lastOutcome.attempt == expected.attempt
                    && state.lastOutcome.publicationId.equals(expected.publicationId)
                    && state.committed == expected.committed;
        }
        return false;
    }

    private static void checkAll(
            State before, Command command, Transition transition,
            List<String> trace, MutableReport report) {
        State after = transition.after;
        if (after.committed < before.committed
                || ((!transition.applied || command.action == Action.CANCEL)
                && after.committed != before.committed)) {
            report.violate(Property.MONOTONIC_FRONTIER, trace);
        }
        if (after.active != null && after.active.candidate.mode == Mode.NORMAL
                && after.active.candidate.from != after.committed) {
            report.violate(Property.NO_GAP, trace);
        }
        if (transition.applied && command.action == Action.FRONTIER_CAS) {
            RunOutcome terminal = before.active == null ? null
                    : before.active.candidate.mode == Mode.NORMAL
                    ? after.lastRunOutcome : after.lastBackfillRunOutcome;
            if (before.active == null || after.lastOutcome == null
                    || after.lastOutcome.from != before.active.candidate.from
                    || after.lastOutcome.target != before.active.candidate.target
                    || after.visiblePublication == null
                    || !after.lastOutcome.publicationId.equals(
                    after.visiblePublication.publicationId)
                    || terminal == null
                    || terminal.terminal != RunTerminal.SUCCEEDED
                    || !terminal.intervalId.equals(
                    before.active.candidate.intervalId)
                    || terminal.attempt != before.active.attempt) {
                report.violate(Property.NO_GAP, trace);
            }
        }
        if (after.committed > 0 && (after.lastNormalReceipt == null
                || after.lastNormalReceipt.target != after.committed
                || after.lastRunOutcome == null
                || after.lastRunOutcome.terminal != RunTerminal.SUCCEEDED
                || !after.lastNormalReceipt.intervalId.equals(
                after.lastRunOutcome.intervalId)
                || after.lastNormalReceipt.attempt != after.lastRunOutcome.attempt
                || after.lastCommittedPublication == null
                || !after.lastNormalReceipt.publicationId.equals(
                after.lastCommittedPublication.publicationId))) {
            report.violate(Property.NO_FRONTIER_AHEAD, trace);
        }
        if (after.activeCount() > 1) report.violate(Property.SINGLE_ACTIVE_INTERVAL, trace);
        if (before.active != null && after.active != null
                && (!before.active.candidate.intervalId.equals(after.active.candidate.intervalId)
                || before.active.candidate.from != after.active.candidate.from
                || before.active.candidate.target != after.active.candidate.target
                || before.active.candidate.triggerId != after.active.candidate.triggerId)) {
            report.violate(Property.FROZEN_TARGET, trace);
        }
        if (!command.credentialsAreCurrent(before) && transition.applied
                && command.requiresAuthority()) {
            report.violate(Property.FENCE_SAFETY, trace);
        }
        if (!transition.applied && !before.equals(after)) {
            report.violate(Property.FENCE_SAFETY, trace);
        }
        if (before.active != null && before.active.intent) {
            boolean jointCommit = transition.applied && command.action == Action.FRONTIER_CAS;
            if (!jointCommit && (after.active == null
                    || after.active.attempt != before.active.attempt
                    || !after.active.intent)) {
                report.violate(Property.INTENT_MONOTONICITY, trace);
            }
        }
        if (before.active != null && before.active.candidate.mode == Mode.BACKFILL
                && command.action == Action.FRONTIER_CAS
                && after.committed != before.committed) {
            report.violate(Property.BACKFILL_ISOLATION, trace);
        }
        if (after.lastBackfillReceipt != null && after.lastNormalReceipt != null
                && publicationIdentity(after.lastBackfillReceipt)
                .equals(publicationIdentity(after.lastNormalReceipt))) {
            report.violate(Property.BACKFILL_ISOLATION, trace);
        }
        if (after.authorityWords() > HARD_AUTHORITY_WORDS || after.activeCount() > 1) {
            report.violate(Property.BOUNDED_AUTHORITY, trace);
        }
    }

    private static String detectUnsafe(UnsafeEvidence evidence) {
        if (evidence.committedAdvanced
                && (!evidence.exactPublicationProof || !evidence.runSucceeded)) {
            return Property.NO_FRONTIER_AHEAD.name();
        }
        if (evidence.proofLeaderTerm != evidence.currentLeaderTerm
                || evidence.feedIncarnation != evidence.currentFeedIncarnation
                || evidence.localLeaderCheckOnly) {
            return Property.FENCE_SAFETY.name();
        }
        if (evidence.intent && (evidence.attemptChanged || evidence.activeCleared)) {
            return Property.INTENT_MONOTONICITY.name();
        }
        if (evidence.activeCleared && evidence.targetChanged) {
            return Property.FROZEN_TARGET.name();
        }
        if (evidence.readFrom != evidence.proofFrom || evidence.lateLeftAccepted
                || !evidence.totalRecordOrder || evidence.runBindingChanged) {
            return Property.NO_GAP.name();
        }
        if (evidence.backfillRepublished || evidence.namespaceCollision) {
            return Property.BACKFILL_ISOLATION.name();
        }
        if (evidence.sameGenerationDifferentDigestAccepted) {
            return "PUBLICATION_IDENTITY";
        }
        if (evidence.hashDependencyCycle) return "CANONICAL_HASH_DAG";
        return null;
    }

    private static String intervalIdentity(
            Mode mode, long incarnation, long from, long target,
            long ordinal, long triggerId) {
        return (mode == Mode.NORMAL ? "N:" : "B:") + incarnation + ":"
                + (mode == Mode.NORMAL ? triggerId : ordinal)
                + ":" + from + ":" + target;
    }

    private static String publicationIdentity(Receipt receipt) {
        return receipt == null ? "" : receipt.intervalId;
    }

    private static final class GuardCatalog {
        private final List<Map<String, Long>> shards = new ArrayList<>(SHARD_COUNT);
        private final Set<String> liveFeeds = new HashSet<>();
        private boolean framesMeasured;

        private GuardCatalog() {
            for (int shard = 0; shard < SHARD_COUNT; shard++) shards.add(new HashMap<>());
        }

        private boolean shardHasCapacity(String feedId) {
            String digest = digestHex(feedId);
            Map<String, Long> shard = shards.get(shardIndex(feedId));
            return shard.containsKey(digest) || shard.size() < ENTRIES_PER_SHARD;
        }

        private boolean initialize(String feedId) {
            String digest = digestHex(feedId);
            Map<String, Long> shard = shards.get(shardIndex(feedId));
            Long old = shard.get(digest);
            if (old == null && shard.size() == ENTRIES_PER_SHARD) return false;
            shard.put(digest, old == null ? 1L : old + 1L);
            liveFeeds.add(digest);
            return true;
        }

        private long incarnation(String feedId) {
            Long value = shards.get(shardIndex(feedId)).get(digestHex(feedId));
            if (value == null) throw new AssertionError("missing guard entry");
            return value;
        }

        private void purgeAllLiveFeeds() { liveFeeds.clear(); }
        private int liveFeedCount() { return liveFeeds.size(); }

        private int guardEntryCount() {
            int total = 0;
            for (Map<String, Long> shard : shards) total += shard.size();
            return total;
        }

        private int maximumEntriesPerShard() {
            int maximum = 0;
            for (Map<String, Long> shard : shards) maximum = Math.max(maximum, shard.size());
            return maximum;
        }

        private int maximumCanonicalFrameBytes() {
            int maximum = 0;
            for (Map<String, Long> shard : shards) {
                maximum = Math.max(maximum, encodeShard(shard).length);
            }
            framesMeasured = true;
            return maximum;
        }

        private boolean canonicalFramesMeasured() { return framesMeasured; }

        private static byte[] encodeShard(Map<String, Long> shard) {
            List<String> ordered = new ArrayList<>(shard.keySet());
            Collections.sort(ordered);
            try {
                ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
                DataOutputStream body = new DataOutputStream(bodyBytes);
                body.writeInt(0x52574744);
                body.writeInt(ordered.size());
                for (String digest : ordered) {
                    byte[] encoded = digest.getBytes(StandardCharsets.US_ASCII);
                    body.writeInt(encoded.length);
                    body.write(encoded);
                    body.writeLong(shard.get(digest));
                }
                body.flush();
                byte[] canonicalBody = bodyBytes.toByteArray();
                ByteArrayOutputStream frame = new ByteArrayOutputStream(
                        canonicalBody.length + 32);
                frame.write(canonicalBody);
                frame.write(sha256(canonicalBody));
                return frame.toByteArray();
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
        }

        private static int shardIndex(String feedId) {
            return sha256(feedId.getBytes(StandardCharsets.UTF_8))[0] & 0xff;
        }

        private static String digestHex(String feedId) {
            byte[] digest = sha256(feedId.getBytes(StandardCharsets.UTF_8));
            StringBuilder value = new StringBuilder(64);
            for (byte current : digest) {
                int unsigned = current & 0xff;
                value.append(Character.forDigit(unsigned >>> 4, 16));
                value.append(Character.forDigit(unsigned & 0xf, 16));
            }
            return value.toString();
        }

        private static byte[] sha256(byte[] value) {
            try {
                return MessageDigest.getInstance("SHA-256").digest(value);
            } catch (NoSuchAlgorithmException impossible) {
                throw new AssertionError(impossible);
            }
        }
    }

    private static List<String> append(List<String> trace, String step) {
        List<String> copy = new ArrayList<>(trace.size() + 1);
        copy.addAll(trace);
        copy.add(step);
        return Collections.unmodifiableList(copy);
    }

    static final class Exploration {
        private final int reachableStateCount;
        private final int transitionCount;
        private final EnumSet<Action> observedActions;
        private final EnumSet<Property> checkedProperties;
        private final EnumMap<Property, List<List<String>>> violations;
        private final int completedNormalIntervals;
        private final int completedBackfills;
        private final int completedAfterLeaderReplacement;
        private final int completedAfterRetry;
        private final int ackLossReplays;
        private final int rejectedStaleFenceCommands;
        private final int rejectedStaleLeaderCommands;
        private final int rejectedStaleIncarnationCommands;
        private final int rejectedRetargetCommands;
        private final int rejectedIntentRollbackCommands;
        private final int conflictStates;
        private final int rejectedAfterConflictCommands;
        private final int durableAbortClosures;
        private final int rejectedMissingAbortClosureCommands;
        private final int rejectedStaleCloseCommands;
        private final int rejectedSameRunRetargetCommands;
        private final int claimAckLossReplays;
        private final int publishAckLossReplays;
        private final int terminalAckLossReplays;
        private final int rejectedCorruptAckReplays;
        private final int retryClaimAckLossReplays;
        private final int rejectedPrematureQueries;

        private Exploration(MutableReport mutable) {
            this.reachableStateCount = mutable.reachableStateCount;
            this.transitionCount = mutable.transitionCount;
            this.observedActions = EnumSet.copyOf(mutable.observedActions);
            this.checkedProperties = EnumSet.allOf(Property.class);
            this.violations = new EnumMap<>(Property.class);
            for (Property property : Property.values()) {
                this.violations.put(property, Collections.unmodifiableList(
                        new ArrayList<>(mutable.violations.get(property))));
            }
            this.completedNormalIntervals = mutable.completedNormalIntervals;
            this.completedBackfills = mutable.completedBackfills;
            this.completedAfterLeaderReplacement = mutable.completedAfterLeaderReplacement;
            this.completedAfterRetry = mutable.completedAfterRetry;
            this.ackLossReplays = mutable.ackLossReplays;
            this.rejectedStaleFenceCommands = mutable.rejectedStaleFenceCommands;
            this.rejectedStaleLeaderCommands = mutable.rejectedStaleLeaderCommands;
            this.rejectedStaleIncarnationCommands = mutable.rejectedStaleIncarnationCommands;
            this.rejectedRetargetCommands = mutable.rejectedRetargetCommands;
            this.rejectedIntentRollbackCommands = mutable.rejectedIntentRollbackCommands;
            this.conflictStates = mutable.conflictStates;
            this.rejectedAfterConflictCommands = mutable.rejectedAfterConflictCommands;
            this.durableAbortClosures = mutable.durableAbortClosures;
            this.rejectedMissingAbortClosureCommands =
                    mutable.rejectedMissingAbortClosureCommands;
            this.rejectedStaleCloseCommands = mutable.rejectedStaleCloseCommands;
            this.rejectedSameRunRetargetCommands = mutable.rejectedSameRunRetargetCommands;
            this.claimAckLossReplays = mutable.claimAckLossReplays;
            this.publishAckLossReplays = mutable.publishAckLossReplays;
            this.terminalAckLossReplays = mutable.terminalAckLossReplays;
            this.rejectedCorruptAckReplays = mutable.rejectedCorruptAckReplays;
            this.retryClaimAckLossReplays = mutable.retryClaimAckLossReplays;
            this.rejectedPrematureQueries = mutable.rejectedPrematureQueries;
        }

        int reachableStateCount() { return reachableStateCount; }
        int transitionCount() { return transitionCount; }
        EnumSet<Action> observedActions() { return EnumSet.copyOf(observedActions); }
        EnumSet<Property> checkedProperties() { return EnumSet.copyOf(checkedProperties); }
        List<List<String>> violations(Property property) { return violations.get(property); }
        int completedNormalIntervals() { return completedNormalIntervals; }
        int completedBackfills() { return completedBackfills; }
        int completedAfterLeaderReplacement() { return completedAfterLeaderReplacement; }
        int completedAfterRetry() { return completedAfterRetry; }
        int ackLossReplays() { return ackLossReplays; }
        int rejectedStaleFenceCommands() { return rejectedStaleFenceCommands; }
        int rejectedStaleLeaderCommands() { return rejectedStaleLeaderCommands; }
        int rejectedStaleIncarnationCommands() { return rejectedStaleIncarnationCommands; }
        int rejectedRetargetCommands() { return rejectedRetargetCommands; }
        int rejectedIntentRollbackCommands() { return rejectedIntentRollbackCommands; }
        int conflictStates() { return conflictStates; }
        int rejectedAfterConflictCommands() { return rejectedAfterConflictCommands; }
        int durableAbortClosures() { return durableAbortClosures; }
        int rejectedMissingAbortClosureCommands() {
            return rejectedMissingAbortClosureCommands;
        }
        int rejectedStaleCloseCommands() { return rejectedStaleCloseCommands; }
        int rejectedSameRunRetargetCommands() { return rejectedSameRunRetargetCommands; }
        int claimAckLossReplays() { return claimAckLossReplays; }
        int publishAckLossReplays() { return publishAckLossReplays; }
        int terminalAckLossReplays() { return terminalAckLossReplays; }
        int rejectedCorruptAckReplays() { return rejectedCorruptAckReplays; }
        int retryClaimAckLossReplays() { return retryClaimAckLossReplays; }
        int rejectedPrematureQueries() { return rejectedPrematureQueries; }
    }

    static final class BackfillReplayProfile {
        private final long committedBeforeBackfill;
        private final long committedAfterBackfill;
        private final long committedAfterNormal;
        private final int publicationsBeforeReplay;
        private final int publicationsAfterReplay;
        private final long lastBackfillOrdinalBeforeReplay;
        private final long lastBackfillOrdinalAfterReplay;
        private final boolean oldReplayRejectedBeforeArtifact;
        private final boolean namespacesDistinct;
        private final int publicationsAfterBackfill;

        private BackfillReplayProfile(
                long committedBeforeBackfill, long committedAfterBackfill,
                long committedAfterNormal, int publicationsBeforeReplay,
                int publicationsAfterReplay, long lastBackfillOrdinalBeforeReplay,
                long lastBackfillOrdinalAfterReplay, boolean oldReplayRejectedBeforeArtifact,
                boolean namespacesDistinct, int publicationsAfterBackfill) {
            this.committedBeforeBackfill = committedBeforeBackfill;
            this.committedAfterBackfill = committedAfterBackfill;
            this.committedAfterNormal = committedAfterNormal;
            this.publicationsBeforeReplay = publicationsBeforeReplay;
            this.publicationsAfterReplay = publicationsAfterReplay;
            this.lastBackfillOrdinalBeforeReplay = lastBackfillOrdinalBeforeReplay;
            this.lastBackfillOrdinalAfterReplay = lastBackfillOrdinalAfterReplay;
            this.oldReplayRejectedBeforeArtifact = oldReplayRejectedBeforeArtifact;
            this.namespacesDistinct = namespacesDistinct;
            this.publicationsAfterBackfill = publicationsAfterBackfill;
        }

        long committedBeforeBackfill() { return committedBeforeBackfill; }
        long committedAfterBackfill() { return committedAfterBackfill; }
        long committedAfterNormal() { return committedAfterNormal; }
        int publicationsBeforeReplay() { return publicationsBeforeReplay; }
        int publicationsAfterReplay() { return publicationsAfterReplay; }
        int publicationsAfterBackfill() { return publicationsAfterBackfill; }
        long lastBackfillOrdinalBeforeReplay() { return lastBackfillOrdinalBeforeReplay; }
        long lastBackfillOrdinalAfterReplay() { return lastBackfillOrdinalAfterReplay; }
        boolean oldReplayRejectedBeforeArtifact() { return oldReplayRejectedBeforeArtifact; }
        boolean normalAndBackfillNamespacesAreDistinct() { return namespacesDistinct; }
    }

    static final class HistoryProfile {
        private final int initialAuthorityWords;
        private final int maximumAuthorityWords;
        private final int hardAuthorityWordLimit;
        private final int transitionsApplied;
        private final int maxActiveIntervals;
        private final int maxRetainedNormalReceipts;
        private final int maxRetainedBackfillReceipts;

        private HistoryProfile(
                int initialAuthorityWords, int maximumAuthorityWords,
                int hardAuthorityWordLimit, int transitionsApplied,
                int maxActiveIntervals, int maxRetainedNormalReceipts,
                int maxRetainedBackfillReceipts) {
            this.initialAuthorityWords = initialAuthorityWords;
            this.maximumAuthorityWords = maximumAuthorityWords;
            this.hardAuthorityWordLimit = hardAuthorityWordLimit;
            this.transitionsApplied = transitionsApplied;
            this.maxActiveIntervals = maxActiveIntervals;
            this.maxRetainedNormalReceipts = maxRetainedNormalReceipts;
            this.maxRetainedBackfillReceipts = maxRetainedBackfillReceipts;
        }

        int initialAuthorityWords() { return initialAuthorityWords; }
        int maximumAuthorityWords() { return maximumAuthorityWords; }
        int hardAuthorityWordLimit() { return hardAuthorityWordLimit; }
        int transitionsApplied() { return transitionsApplied; }
        int maxActiveIntervals() { return maxActiveIntervals; }
        int maxRetainedNormalReceipts() { return maxRetainedNormalReceipts; }
        int maxRetainedBackfillReceipts() { return maxRetainedBackfillReceipts; }
    }

    static final class CatalogProfile {
        private final int acceptedFeeds;
        private final int rejectedFeeds;
        private final int shardCount;
        private final int maximumEntriesPerShard;
        private final int maximumShardFrameBytes;
        private final boolean canonicalFramesMeasured;
        private final boolean guardEntriesSurvivePurge;
        private final boolean incarnationAdvancedAfterPurge;

        private CatalogProfile(
                int acceptedFeeds, int rejectedFeeds, int shardCount,
                int maximumEntriesPerShard, int maximumShardFrameBytes,
                boolean canonicalFramesMeasured, boolean guardEntriesSurvivePurge,
                boolean incarnationAdvancedAfterPurge) {
            this.acceptedFeeds = acceptedFeeds;
            this.rejectedFeeds = rejectedFeeds;
            this.shardCount = shardCount;
            this.maximumEntriesPerShard = maximumEntriesPerShard;
            this.maximumShardFrameBytes = maximumShardFrameBytes;
            this.canonicalFramesMeasured = canonicalFramesMeasured;
            this.guardEntriesSurvivePurge = guardEntriesSurvivePurge;
            this.incarnationAdvancedAfterPurge = incarnationAdvancedAfterPurge;
        }

        int acceptedFeeds() { return acceptedFeeds; }
        int rejectedFeeds() { return rejectedFeeds; }
        int shardCount() { return shardCount; }
        int maximumEntriesPerShard() { return maximumEntriesPerShard; }
        int maximumShardFrameBytes() { return maximumShardFrameBytes; }
        boolean canonicalFramesMeasured() { return canonicalFramesMeasured; }
        boolean guardEntriesSurvivePurge() { return guardEntriesSurvivePurge; }
        boolean incarnationAdvancedAfterPurge() { return incarnationAdvancedAfterPurge; }
    }

    static final class Counterexample {
        private final boolean detected;
        private final String violatedRule;
        private final List<String> trace;
        private final int replayedTransitions;

        private Counterexample(
                boolean detected, String violatedRule, List<String> trace,
                int replayedTransitions) {
            this.detected = detected;
            this.violatedRule = violatedRule;
            this.trace = Collections.unmodifiableList(new ArrayList<>(trace));
            this.replayedTransitions = replayedTransitions;
        }

        boolean detected() { return detected; }
        String violatedRule() { return violatedRule; }
        List<String> trace() { return trace; }
        int replayedTransitions() { return replayedTransitions; }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Counterexample)) return false;
            Counterexample that = (Counterexample) other;
            return detected == that.detected
                    && violatedRule.equals(that.violatedRule) && trace.equals(that.trace);
        }

        @Override
        public int hashCode() { return Objects.hash(detected, violatedRule, trace); }
    }

    private static final class MutableReport {
        private int reachableStateCount;
        private int transitionCount;
        private final EnumSet<Action> observedActions = EnumSet.noneOf(Action.class);
        private final EnumMap<Property, List<List<String>>> violations =
                new EnumMap<>(Property.class);
        private int completedNormalIntervals;
        private int completedBackfills;
        private int completedAfterLeaderReplacement;
        private int completedAfterRetry;
        private int ackLossReplays;
        private int rejectedStaleFenceCommands;
        private int rejectedStaleLeaderCommands;
        private int rejectedStaleIncarnationCommands;
        private int rejectedRetargetCommands;
        private int rejectedIntentRollbackCommands;
        private int conflictStates;
        private int rejectedAfterConflictCommands;
        private int durableAbortClosures;
        private int rejectedMissingAbortClosureCommands;
        private int rejectedStaleCloseCommands;
        private int rejectedSameRunRetargetCommands;
        private int claimAckLossReplays;
        private int publishAckLossReplays;
        private int terminalAckLossReplays;
        private int rejectedCorruptAckReplays;
        private int retryClaimAckLossReplays;
        private int rejectedPrematureQueries;

        private MutableReport() {
            for (Property property : Property.values()) violations.put(property, new ArrayList<>());
        }

        private void violate(Property property, List<String> trace) {
            if (violations.get(property).size() < 8) violations.get(property).add(trace);
        }

        private void observe(State before, Command command, Transition transition) {
            if (transition.applied && command.action == Action.FRONTIER_CAS && before.active != null) {
                if (before.active.candidate.mode == Mode.NORMAL) completedNormalIntervals++;
                else completedBackfills++;
                if (before.active.replacementSeen) completedAfterLeaderReplacement++;
                if (before.active.retrySeen) completedAfterRetry++;
            }
            if (transition.applied && command.action == Action.ACK_LOSS) {
                ackLossReplays++;
                if (command.expectedAck.point == AckPoint.CLAIM) claimAckLossReplays++;
                if (command.expectedAck.point == AckPoint.PUBLISH) publishAckLossReplays++;
                if (command.expectedAck.point == AckPoint.TERMINAL) terminalAckLossReplays++;
                if (command.expectedAck.point == AckPoint.CLAIM
                        && before.active != null && before.active.retrySeen) {
                    retryClaimAckLossReplays++;
                }
            }
            if (!transition.applied && command.credential == Credential.STALE_FENCE) {
                rejectedStaleFenceCommands++;
            }
            if (!transition.applied && command.credential == Credential.STALE_LEADER) {
                rejectedStaleLeaderCommands++;
            }
            if (!transition.applied && command.credential == Credential.STALE_INCARNATION) {
                rejectedStaleIncarnationCommands++;
            }
            if (!transition.applied && command.action == Action.CAPTURE
                    && (before.pending != null || before.active != null)) {
                rejectedRetargetCommands++;
            }
            if (!transition.applied && before.active != null && before.active.intent
                    && (command.action == Action.CANCEL || command.action == Action.RETRY)) {
                rejectedIntentRollbackCommands++;
            }
            if (!before.conflict && transition.after.conflict) conflictStates++;
            if (before.conflict && !transition.applied) rejectedAfterConflictCommands++;
            if (transition.applied && command.action == Action.ABORT) durableAbortClosures++;
            if (!transition.applied && before.active != null && before.active.prepared
                    && !before.active.abortClosed
                    && (command.action == Action.CANCEL || command.action == Action.RETRY)) {
                rejectedMissingAbortClosureCommands++;
            }
            if (!transition.applied && command.action == Action.CANCEL
                    && command.credential != Credential.CURRENT) rejectedStaleCloseCommands++;
            if (!transition.applied && command.action == Action.ACK_LOSS
                    && command.expectedAck != null
                    && !command.expectedAck.equals(before.ackReceipt)) {
                rejectedCorruptAckReplays++;
            }
            if (!transition.applied && command.action == Action.QUERY
                    && command.queryObservation == QueryObservation.EXACT
                    && before.active != null
                    && (before.visiblePublication == null
                    || !before.visiblePublication.equals(Publication.forActive(before.active)))) {
                rejectedPrematureQueries++;
            }
            if (!transition.applied && command.action == Action.CAPTURE
                    && before.lastClosedCandidate != null
                    && command.triggerId == before.lastClosedCandidate.triggerId) {
                rejectedSameRunRetargetCommands++;
            }
        }

        private Exploration freeze() { return new Exploration(this); }
    }

    private static final class Command {
        private final Action action;
        private final Credential credential;
        private final long feedIncarnation;
        private final long ownerTerm;
        private final int attempt;
        private final Mode mode;
        private final long from;
        private final long target;
        private final long ordinal;
        private final QueryObservation queryObservation;
        private final AckReceipt expectedAck;
        private final long triggerId;

        private Command(
                Action action, Credential credential, long feedIncarnation,
                long ownerTerm, int attempt, Mode mode, long from,
                long target, long ordinal, QueryObservation queryObservation,
                AckReceipt expectedAck, long triggerId) {
            this.action = action;
            this.credential = credential;
            this.feedIncarnation = feedIncarnation;
            this.ownerTerm = ownerTerm;
            this.attempt = attempt;
            this.mode = mode;
            this.from = from;
            this.target = target;
            this.ordinal = ordinal;
            this.queryObservation = queryObservation;
            this.expectedAck = expectedAck;
            this.triggerId = triggerId;
        }

        private static Command capture(
                Mode mode, long from, long target, long ordinal, long triggerId) {
            return new Command(Action.CAPTURE, Credential.CURRENT,
                    0, 0, 0, mode, from, target, ordinal,
                    QueryObservation.NONE, null, triggerId);
        }

        private static Command current(Action action, State state) {
            return withCredential(action, state, Credential.CURRENT);
        }

        private static Command query(boolean exact, State state) {
            Command current = current(Action.QUERY, state);
            return new Command(current.action, current.credential, current.feedIncarnation,
                    current.ownerTerm, current.attempt, null, 0, 0, 0,
                    exact ? QueryObservation.EXACT : QueryObservation.UNKNOWN, null, 0);
        }

        private static Command queryConflict(State state) {
            Command current = current(Action.QUERY, state);
            return new Command(current.action, current.credential, current.feedIncarnation,
                    current.ownerTerm, current.attempt, null, 0, 0, 0,
                    QueryObservation.CONFLICT, null, 0);
        }

        private static Command ack(State state, boolean corrupt) {
            AckReceipt receipt = state.ackReceipt;
            if (receipt != null && corrupt) receipt = receipt.corrupt();
            return new Command(Action.ACK_LOSS, Credential.CURRENT,
                    state.feedIncarnation, state.ownerTerm,
                    state.active == null ? 1 : state.active.attempt,
                    null, 0, 0, 0, QueryObservation.NONE, receipt, 0);
        }

        private static Command withCredential(
                Action action, State state, Credential credential) {
            long incarnation = state.feedIncarnation;
            long term = state.ownerTerm;
            int attempt = state.active == null ? 1 : state.active.attempt;
            if (credential == Credential.STALE_INCARNATION) incarnation--;
            if (credential == Credential.STALE_LEADER) term--;
            if (credential == Credential.STALE_FENCE) attempt--;
            return new Command(action, credential, incarnation, term, attempt,
                    null, 0, 0, 0, QueryObservation.NONE, null, 0);
        }

        private boolean credentialsAreCurrent(State state) {
            if (!requiresAuthority()) return true;
            int expectedAttempt = state.active == null ? 1 : state.active.attempt;
            return feedIncarnation == state.feedIncarnation
                    && ownerTerm == state.ownerTerm && attempt == expectedAttempt;
        }

        private boolean requiresAuthority() {
            return action != Action.CAPTURE && action != Action.ACK_LOSS
                    && action != Action.LEADER_REPLACEMENT;
        }

        private String describe(boolean applied) {
            return action + "[" + credential + "]=" + (applied ? "APPLIED" : "REJECTED");
        }
    }

    private static final class Transition {
        private final State after;
        private final boolean applied;

        private Transition(State after, boolean applied) {
            this.after = after;
            this.applied = applied;
        }

        private static Transition applied(State after, Command ignored) {
            return new Transition(after, true);
        }

        private static Transition rejected(State before, Command ignored) {
            return new Transition(before, false);
        }
    }

    private static final class Candidate {
        private final Mode mode;
        private final long from;
        private final long target;
        private final long ordinal;
        private final long triggerId;
        private final String intervalId;

        private Candidate(
                Mode mode, long from, long target, long ordinal,
                long triggerId, String intervalId) {
            this.mode = mode;
            this.from = from;
            this.target = target;
            this.ordinal = ordinal;
            this.triggerId = triggerId;
            this.intervalId = intervalId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Candidate)) return false;
            Candidate that = (Candidate) other;
            return from == that.from && target == that.target && ordinal == that.ordinal
                    && triggerId == that.triggerId
                    && mode == that.mode && intervalId.equals(that.intervalId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mode, from, target, ordinal, triggerId, intervalId);
        }
    }

    private static final class Active {
        private final Candidate candidate;
        private final int attempt;
        private final boolean read;
        private final boolean prepared;
        private final boolean intent;
        private final long proofOwnerTerm;
        private final boolean abortClosed;
        private final boolean replacementSeen;
        private final boolean retrySeen;

        private Active(
                Candidate candidate, int attempt, boolean read, boolean prepared,
                boolean intent, long proofOwnerTerm, boolean abortClosed,
                boolean replacementSeen, boolean retrySeen) {
            this.candidate = candidate;
            this.attempt = attempt;
            this.read = read;
            this.prepared = prepared;
            this.intent = intent;
            this.proofOwnerTerm = proofOwnerTerm;
            this.abortClosed = abortClosed;
            this.replacementSeen = replacementSeen;
            this.retrySeen = retrySeen;
        }

        private static Active first(Candidate candidate) {
            return new Active(candidate, 1, false, false, false,
                    0L, false, false, false);
        }

        private Active withRead() {
            return new Active(candidate, attempt, true, prepared, intent,
                    proofOwnerTerm, abortClosed, replacementSeen, retrySeen);
        }

        private Active withPrepared() {
            return new Active(candidate, attempt, read, true, intent,
                    proofOwnerTerm, abortClosed, replacementSeen, retrySeen);
        }

        private Active withIntent() {
            return new Active(candidate, attempt, read, prepared, true,
                    proofOwnerTerm, abortClosed, replacementSeen, retrySeen);
        }

        private Active withProof(long ownerTerm) {
            return new Active(candidate, attempt, read, prepared, intent,
                    ownerTerm, abortClosed, replacementSeen, retrySeen);
        }

        private Active withAbortClosed() {
            return new Active(candidate, attempt, read, prepared, intent,
                    proofOwnerTerm, true, replacementSeen, retrySeen);
        }

        private Active retry() {
            return new Active(candidate, attempt + 1, false, false, false,
                    0, false, replacementSeen, true);
        }

        private Active withReplacement() {
            return new Active(candidate, attempt, read, prepared, intent,
                    proofOwnerTerm, abortClosed, true, retrySeen);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Active)) return false;
            Active that = (Active) other;
            return attempt == that.attempt && read == that.read && prepared == that.prepared
                    && intent == that.intent && abortClosed == that.abortClosed
                    && proofOwnerTerm == that.proofOwnerTerm
                    && replacementSeen == that.replacementSeen && retrySeen == that.retrySeen
                    && candidate.equals(that.candidate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(candidate, attempt, read, prepared, intent,
                    proofOwnerTerm, abortClosed, replacementSeen, retrySeen);
        }
    }

    private static final class Publication {
        private final String intervalId;
        private final int attempt;
        private final String publicationId;

        private Publication(String intervalId, int attempt, String publicationId) {
            this.intervalId = intervalId;
            this.attempt = attempt;
            this.publicationId = publicationId;
        }

        private static Publication forActive(Active active) {
            return new Publication(active.candidate.intervalId, active.attempt,
                    "publication:" + active.candidate.intervalId + ":" + active.attempt);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Publication)) return false;
            Publication that = (Publication) other;
            return attempt == that.attempt && intervalId.equals(that.intervalId)
                    && publicationId.equals(that.publicationId);
        }

        @Override
        public int hashCode() { return Objects.hash(intervalId, attempt, publicationId); }
    }

    private static final class Receipt {
        private final Mode mode;
        private final long from;
        private final long target;
        private final long ordinal;
        private final String intervalId;
        private final String publicationId;
        private final int attempt;
        private final long proofOwnerTerm;

        private Receipt(Active active, Publication publication, long currentOwnerTerm) {
            this.mode = active.candidate.mode;
            this.from = active.candidate.from;
            this.target = active.candidate.target;
            this.ordinal = active.candidate.ordinal;
            this.intervalId = active.candidate.intervalId;
            this.publicationId = publication.publicationId;
            this.attempt = active.attempt;
            this.proofOwnerTerm = currentOwnerTerm;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Receipt)) return false;
            Receipt that = (Receipt) other;
            return from == that.from && target == that.target && ordinal == that.ordinal
                    && attempt == that.attempt && proofOwnerTerm == that.proofOwnerTerm
                    && mode == that.mode && intervalId.equals(that.intervalId)
                    && publicationId.equals(that.publicationId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mode, from, target, ordinal, intervalId,
                    publicationId, attempt, proofOwnerTerm);
        }
    }

    private static final class RunOutcome {
        private final String intervalId;
        private final int attempt;
        private final RunTerminal terminal;

        private RunOutcome(String intervalId, int attempt, RunTerminal terminal) {
            this.intervalId = intervalId;
            this.attempt = attempt;
            this.terminal = terminal;
        }

        private static RunOutcome succeeded(Active active) {
            return new RunOutcome(
                    active.candidate.intervalId, active.attempt, RunTerminal.SUCCEEDED);
        }

        private static RunOutcome failed(Active active) {
            return new RunOutcome(
                    active.candidate.intervalId, active.attempt, RunTerminal.FAILED);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof RunOutcome)) return false;
            RunOutcome that = (RunOutcome) other;
            return attempt == that.attempt && intervalId.equals(that.intervalId)
                    && terminal == that.terminal;
        }

        @Override
        public int hashCode() { return Objects.hash(intervalId, attempt, terminal); }
    }

    private static final class AckReceipt {
        private final AckPoint point;
        private final String intervalId;
        private final int attempt;
        private final long ownerTerm;
        private final long committed;
        private final String publicationId;

        private AckReceipt(
                AckPoint point, String intervalId, int attempt, long ownerTerm,
                long committed, String publicationId) {
            this.point = point;
            this.intervalId = intervalId;
            this.attempt = attempt;
            this.ownerTerm = ownerTerm;
            this.committed = committed;
            this.publicationId = publicationId;
        }

        private AckReceipt corrupt() {
            return new AckReceipt(point, intervalId + "-corrupt", attempt,
                    ownerTerm, committed, publicationId);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof AckReceipt)) return false;
            AckReceipt that = (AckReceipt) other;
            return attempt == that.attempt && ownerTerm == that.ownerTerm
                    && committed == that.committed && point == that.point
                    && intervalId.equals(that.intervalId)
                    && Objects.equals(publicationId, that.publicationId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(point, intervalId, attempt, ownerTerm,
                    committed, publicationId);
        }
    }

    private static final class State {
        private final long feedIncarnation;
        private final long ownerTerm;
        private final long revision;
        private final long committed;
        private final Candidate pending;
        private final Active active;
        private final Receipt lastNormalReceipt;
        private final long lastBackfillOrdinal;
        private final Receipt lastBackfillReceipt;
        private final Receipt lastOutcome;
        private final Publication visiblePublication;
        private final RunOutcome lastRunOutcome;
        private final AckReceipt ackReceipt;
        private final boolean conflict;
        private final int publicationCount;
        private final long nextTriggerId;
        private final Candidate lastClosedCandidate;
        private final RunOutcome lastFailedRunOutcome;
        private final Publication lastCommittedPublication;
        private final RunOutcome lastBackfillRunOutcome;

        private State(
                long feedIncarnation, long ownerTerm, long revision, long committed,
                Candidate pending, Active active, Receipt lastNormalReceipt,
                long lastBackfillOrdinal, Receipt lastBackfillReceipt,
                Receipt lastOutcome, Publication visiblePublication,
                RunOutcome lastRunOutcome, AckReceipt ackReceipt,
                boolean conflict, int publicationCount, long nextTriggerId,
                Candidate lastClosedCandidate, RunOutcome lastFailedRunOutcome,
                Publication lastCommittedPublication,
                RunOutcome lastBackfillRunOutcome) {
            this.feedIncarnation = feedIncarnation;
            this.ownerTerm = ownerTerm;
            this.revision = revision;
            this.committed = committed;
            this.pending = pending;
            this.active = active;
            this.lastNormalReceipt = lastNormalReceipt;
            this.lastBackfillOrdinal = lastBackfillOrdinal;
            this.lastBackfillReceipt = lastBackfillReceipt;
            this.lastOutcome = lastOutcome;
            this.visiblePublication = visiblePublication;
            this.lastRunOutcome = lastRunOutcome;
            this.ackReceipt = ackReceipt;
            this.conflict = conflict;
            this.publicationCount = publicationCount;
            this.nextTriggerId = nextTriggerId;
            this.lastClosedCandidate = lastClosedCandidate;
            this.lastFailedRunOutcome = lastFailedRunOutcome;
            this.lastCommittedPublication = lastCommittedPublication;
            this.lastBackfillRunOutcome = lastBackfillRunOutcome;
        }

        private static State initial() {
            return new State(1, 1, 0, 0, null, null,
                    null, 0, null, null, null, null, null,
                    false, 0, 1, null, null, null, null);
        }

        private State withPending(Candidate value) {
            return copy(value, active, ownerTerm, revision, committed,
                    lastNormalReceipt, lastBackfillOrdinal, lastBackfillReceipt,
                    lastOutcome, publicationCount);
        }

        private State clearPending() {
            return new State(feedIncarnation, ownerTerm, revision + 1, committed,
                    null, active, lastNormalReceipt, lastBackfillOrdinal,
                    lastBackfillReceipt, lastOutcome, visiblePublication,
                    lastRunOutcome, ackReceipt, conflict, publicationCount,
                    nextTriggerId + 1, pending, lastFailedRunOutcome,
                    lastCommittedPublication, lastBackfillRunOutcome);
        }

        private State claim() {
            Active claimed = Active.first(pending);
            AckReceipt claimReceipt = new AckReceipt(
                    AckPoint.CLAIM, pending.intervalId, 1, ownerTerm, committed, null);
            return new State(feedIncarnation, ownerTerm, revision + 1, committed,
                    null, claimed, lastNormalReceipt, lastBackfillOrdinal,
                    lastBackfillReceipt, lastOutcome, visiblePublication,
                    lastRunOutcome, claimReceipt, conflict, publicationCount,
                    nextTriggerId, lastClosedCandidate, lastFailedRunOutcome,
                    lastCommittedPublication, lastBackfillRunOutcome);
        }

        private State withActive(Active value) {
            return copy(pending, value, ownerTerm, revision, committed,
                    lastNormalReceipt, lastBackfillOrdinal, lastBackfillReceipt,
                    lastOutcome, publicationCount);
        }

        private State publish() {
            Publication publication = Publication.forActive(active);
            AckReceipt publishReceipt = new AckReceipt(
                    AckPoint.PUBLISH, active.candidate.intervalId, active.attempt,
                    ownerTerm, committed, publication.publicationId);
            return new State(feedIncarnation, ownerTerm, revision, committed,
                    pending, active, lastNormalReceipt, lastBackfillOrdinal,
                    lastBackfillReceipt, lastOutcome, publication,
                    lastRunOutcome, publishReceipt, conflict, publicationCount + 1,
                    nextTriggerId, lastClosedCandidate, lastFailedRunOutcome,
                    lastCommittedPublication, lastBackfillRunOutcome);
        }

        private State commitNormal() {
            Receipt receipt = new Receipt(active, visiblePublication, ownerTerm);
            RunOutcome runOutcome = RunOutcome.succeeded(active);
            AckReceipt terminalReceipt = new AckReceipt(
                    AckPoint.TERMINAL, active.candidate.intervalId, active.attempt,
                    ownerTerm, active.candidate.target, visiblePublication.publicationId);
            return new State(feedIncarnation, ownerTerm, revision + 1,
                    active.candidate.target, null, null, receipt,
                    lastBackfillOrdinal, lastBackfillReceipt, receipt,
                    visiblePublication, runOutcome, terminalReceipt,
                    conflict, publicationCount, nextTriggerId + 1, active.candidate,
                    lastFailedRunOutcome, visiblePublication, lastBackfillRunOutcome);
        }

        private State commitBackfill() {
            Receipt receipt = new Receipt(active, visiblePublication, ownerTerm);
            RunOutcome runOutcome = RunOutcome.succeeded(active);
            AckReceipt terminalReceipt = new AckReceipt(
                    AckPoint.TERMINAL, active.candidate.intervalId, active.attempt,
                    ownerTerm, committed, visiblePublication.publicationId);
            return new State(feedIncarnation, ownerTerm, revision + 1, committed,
                    null, null, lastNormalReceipt, active.candidate.ordinal,
                    receipt, receipt, visiblePublication, lastRunOutcome,
                    terminalReceipt, conflict, publicationCount,
                    nextTriggerId + 1, active.candidate, lastFailedRunOutcome,
                    lastCommittedPublication, runOutcome);
        }

        private State clearActive() {
            RunOutcome failed = active == null ? lastFailedRunOutcome : RunOutcome.failed(active);
            return new State(feedIncarnation, ownerTerm, revision + 1, committed,
                    null, null, lastNormalReceipt, lastBackfillOrdinal,
                    lastBackfillReceipt, lastOutcome, visiblePublication,
                    lastRunOutcome, ackReceipt, conflict, publicationCount,
                    nextTriggerId + 1, active == null ? lastClosedCandidate : active.candidate,
                    failed, lastCommittedPublication, lastBackfillRunOutcome);
        }

        private State retryActive() {
            Active retried = active.retry();
            AckReceipt retryClaimReceipt = new AckReceipt(
                    AckPoint.CLAIM, retried.candidate.intervalId, retried.attempt,
                    ownerTerm, committed, null);
            return new State(feedIncarnation, ownerTerm, revision + 1, committed,
                    pending, retried, lastNormalReceipt, lastBackfillOrdinal,
                    lastBackfillReceipt, lastOutcome, visiblePublication,
                    lastRunOutcome, retryClaimReceipt, conflict, publicationCount,
                    nextTriggerId, lastClosedCandidate, RunOutcome.failed(active),
                    lastCommittedPublication, lastBackfillRunOutcome);
        }

        private State replaceLeader() {
            Active replacement = active == null ? null : active.withReplacement();
            return copy(pending, replacement, ownerTerm + 1, revision + 1, committed,
                    lastNormalReceipt, lastBackfillOrdinal, lastBackfillReceipt,
                    lastOutcome, publicationCount);
        }

        private State withConflict() {
            return new State(feedIncarnation, ownerTerm, revision + 1, committed,
                    pending, active, lastNormalReceipt, lastBackfillOrdinal,
                    lastBackfillReceipt, lastOutcome, visiblePublication,
                    lastRunOutcome, ackReceipt, true, publicationCount,
                    nextTriggerId, lastClosedCandidate, lastFailedRunOutcome,
                    lastCommittedPublication, lastBackfillRunOutcome);
        }

        private State copy(
                Candidate newPending, Active newActive, long newTerm, long newRevision,
                long newCommitted, Receipt newNormal, long newBackfillOrdinal,
                Receipt newBackfill, Receipt newOutcome, int newPublicationCount) {
            return new State(feedIncarnation, newTerm, newRevision, newCommitted,
                    newPending, newActive, newNormal, newBackfillOrdinal,
                    newBackfill, newOutcome, visiblePublication,
                    lastRunOutcome, ackReceipt, conflict, newPublicationCount,
                    nextTriggerId, lastClosedCandidate, lastFailedRunOutcome,
                    lastCommittedPublication, lastBackfillRunOutcome);
        }

        private int activeCount() { return active == null ? 0 : 1; }
        private int authorityWords() {
            int words = 8;
            if (pending != null) words += 6;
            if (active != null) words += 12;
            if (lastNormalReceipt != null) words += 9;
            if (lastBackfillReceipt != null) words += 9;
            if (lastRunOutcome != null) words += 3;
            if (lastFailedRunOutcome != null) words += 3;
            if (lastCommittedPublication != null) words += 3;
            if (lastBackfillRunOutcome != null) words += 3;
            if (ackReceipt != null) words += 6;
            if (lastClosedCandidate != null) words += 6;
            return words;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof State)) return false;
            State that = (State) other;
            return feedIncarnation == that.feedIncarnation && ownerTerm == that.ownerTerm
                    && revision == that.revision && committed == that.committed
                    && lastBackfillOrdinal == that.lastBackfillOrdinal
                    && conflict == that.conflict && publicationCount == that.publicationCount
                    && nextTriggerId == that.nextTriggerId
                    && Objects.equals(pending, that.pending) && Objects.equals(active, that.active)
                    && Objects.equals(lastNormalReceipt, that.lastNormalReceipt)
                    && Objects.equals(lastBackfillReceipt, that.lastBackfillReceipt)
                    && Objects.equals(lastOutcome, that.lastOutcome)
                    && Objects.equals(visiblePublication, that.visiblePublication)
                    && Objects.equals(lastRunOutcome, that.lastRunOutcome)
                    && Objects.equals(ackReceipt, that.ackReceipt)
                    && Objects.equals(lastClosedCandidate, that.lastClosedCandidate)
                    && Objects.equals(lastFailedRunOutcome, that.lastFailedRunOutcome)
                    && Objects.equals(lastCommittedPublication, that.lastCommittedPublication)
                    && Objects.equals(lastBackfillRunOutcome, that.lastBackfillRunOutcome);
        }

        @Override
        public int hashCode() {
            return Objects.hash(feedIncarnation, ownerTerm, revision, committed,
                    pending, active, lastNormalReceipt, lastBackfillOrdinal,
                    lastBackfillReceipt, lastOutcome, visiblePublication,
                    lastRunOutcome, ackReceipt, conflict, publicationCount,
                    nextTriggerId, lastClosedCandidate, lastFailedRunOutcome,
                    lastCommittedPublication, lastBackfillRunOutcome);
        }
    }

    private enum UnsafeStep {
        WRITE_FRONTIER,
        WRITE_INTENT,
        TRUST_INTENT_AS_PROOF,
        PUBLISH,
        QUERY_EXACT,
        QUERY_UNKNOWN,
        GUESS_UNKNOWN_COMMITTED,
        MARK_RUN_SUCCEEDED,
        REPLACE_LEADER,
        CREATE_NEW_ATTEMPT,
        MARK_RUN_FAILED,
        CLEAR_ACTIVE,
        RETARGET_SAME_RUN,
        MARK_STAGE_SUCCEEDED,
        ACCEPT_EQUAL_TIMESTAMP_WITHOUT_TIE_BREAK,
        CLAIM_INTERVAL,
        ACCEPT_LATE_LEFT_RECORD,
        COMPLETE_BACKFILL,
        COMMIT_NORMAL,
        REPLAY_OLD_BACKFILL,
        PUBLISH_GENERATION_A,
        REPLACE_SAME_GENERATION_WITH_DIGEST_B,
        PURGE_FEED,
        RECREATE_WITH_OLD_INCARNATION,
        LOCAL_LEADER_CHECK,
        BIND_RUN_TO_ARTIFACT_A,
        REBIND_RUN_TO_ARTIFACT_B,
        HASH_MANIFEST_FROM_CONTROL,
        HASH_CONTROL_FROM_MANIFEST
    }

    private static List<UnsafeStep> unsafeSteps(UnsafeOrder order) {
        switch (order) {
            case FRONTIER_BEFORE_PUBLISH:
                return Arrays.asList(UnsafeStep.WRITE_FRONTIER, UnsafeStep.PUBLISH);
            case INTENT_ONLY_VISIBLE_PROOF:
                return Arrays.asList(UnsafeStep.WRITE_INTENT, UnsafeStep.TRUST_INTENT_AS_PROOF);
            case STALE_QUERY_PROOF_AFTER_CRASH:
                return Arrays.asList(
                        UnsafeStep.PUBLISH, UnsafeStep.QUERY_EXACT,
                        UnsafeStep.MARK_RUN_SUCCEEDED, UnsafeStep.REPLACE_LEADER,
                        UnsafeStep.WRITE_FRONTIER);
            case SPLIT_FRONTIER_RUN_SUCCESS:
                return Arrays.asList(
                        UnsafeStep.PUBLISH, UnsafeStep.QUERY_EXACT,
                        UnsafeStep.WRITE_FRONTIER);
            case NEW_ATTEMPT_AFTER_INTENT:
                return Arrays.asList(UnsafeStep.WRITE_INTENT, UnsafeStep.CREATE_NEW_ATTEMPT);
            case FAILED_THEN_CLEAR_ACTIVE:
                return Arrays.asList(
                        UnsafeStep.WRITE_INTENT, UnsafeStep.MARK_RUN_FAILED,
                        UnsafeStep.CLEAR_ACTIVE);
            case CLEAR_ACTIVE_THEN_RETARGET:
                return Arrays.asList(UnsafeStep.CLEAR_ACTIVE, UnsafeStep.RETARGET_SAME_RUN);
            case STAGE_SUCCESS_AS_COMMIT_PROOF:
                return Arrays.asList(
                        UnsafeStep.MARK_STAGE_SUCCEEDED, UnsafeStep.WRITE_FRONTIER);
            case UNKNOWN_QUERY_GUESSED_COMMITTED:
                return Arrays.asList(
                        UnsafeStep.QUERY_UNKNOWN, UnsafeStep.GUESS_UNKNOWN_COMMITTED);
            case TIMESTAMP_WITHOUT_TIE_BREAK:
                return Arrays.asList(
                        UnsafeStep.CLAIM_INTERVAL,
                        UnsafeStep.ACCEPT_EQUAL_TIMESTAMP_WITHOUT_TIE_BREAK);
            case LATE_LEFT_RECORD:
                return Arrays.asList(
                        UnsafeStep.CLAIM_INTERVAL, UnsafeStep.ACCEPT_LATE_LEFT_RECORD);
            case OLD_BACKFILL_REPLAY:
                return Arrays.asList(
                        UnsafeStep.COMPLETE_BACKFILL, UnsafeStep.COMMIT_NORMAL,
                        UnsafeStep.REPLAY_OLD_BACKFILL);
            case SAME_GENERATION_DIFFERENT_DIGEST:
                return Arrays.asList(
                        UnsafeStep.PUBLISH_GENERATION_A,
                        UnsafeStep.REPLACE_SAME_GENERATION_WITH_DIGEST_B);
            case FEED_PURGE_RECREATE_ABA:
                return Arrays.asList(
                        UnsafeStep.PURGE_FEED, UnsafeStep.RECREATE_WITH_OLD_INCARNATION);
            case LOCAL_LEADER_TOCTOU:
                return Arrays.asList(
                        UnsafeStep.PUBLISH, UnsafeStep.QUERY_EXACT,
                        UnsafeStep.MARK_RUN_SUCCEEDED, UnsafeStep.LOCAL_LEADER_CHECK,
                        UnsafeStep.REPLACE_LEADER, UnsafeStep.WRITE_FRONTIER);
            case RUN_ID_DIFFERENT_BINDING:
                return Arrays.asList(
                        UnsafeStep.BIND_RUN_TO_ARTIFACT_A,
                        UnsafeStep.REBIND_RUN_TO_ARTIFACT_B);
            case SELF_REFERENTIAL_HASH:
                return Arrays.asList(
                        UnsafeStep.HASH_MANIFEST_FROM_CONTROL,
                        UnsafeStep.HASH_CONTROL_FROM_MANIFEST);
            default:
                throw new AssertionError(order);
        }
    }

    private static final class UnsafeReplay {
        private boolean committedAdvanced;
        private boolean publicationVisible;
        private boolean exactPublicationProof;
        private boolean runSucceeded;
        private long proofLeaderTerm = 1;
        private long currentLeaderTerm = 1;
        private long feedIncarnation = 1;
        private long currentFeedIncarnation = 1;
        private boolean localLeaderCheckOnly;
        private boolean intent;
        private boolean attemptChanged;
        private boolean activeCleared;
        private boolean targetChanged;
        private long readFrom = 10;
        private long proofFrom = 10;
        private boolean lateLeftAccepted;
        private boolean totalRecordOrder = true;
        private boolean runBindingChanged;
        private boolean backfillRepublished;
        private boolean namespaceCollision;
        private boolean sameGenerationDifferentDigestAccepted;
        private boolean hashDependencyCycle;
        private boolean manifestDependsOnControl;
        private final List<String> trace = new ArrayList<>();

        private static UnsafeReplay run(List<UnsafeStep> steps) {
            UnsafeReplay replay = new UnsafeReplay();
            for (UnsafeStep step : steps) replay.apply(step);
            return replay;
        }

        private void apply(UnsafeStep step) {
            trace.add(step.name());
            switch (step) {
                case WRITE_FRONTIER: committedAdvanced = true; break;
                case WRITE_INTENT: intent = true; break;
                case TRUST_INTENT_AS_PROOF:
                    committedAdvanced = true; runSucceeded = true; break;
                case PUBLISH: publicationVisible = true; break;
                case QUERY_EXACT:
                    exactPublicationProof = publicationVisible;
                    proofLeaderTerm = currentLeaderTerm; break;
                case QUERY_UNKNOWN: exactPublicationProof = false; break;
                case GUESS_UNKNOWN_COMMITTED:
                    committedAdvanced = true; runSucceeded = true; break;
                case MARK_RUN_SUCCEEDED: runSucceeded = true; break;
                case REPLACE_LEADER: currentLeaderTerm++; break;
                case CREATE_NEW_ATTEMPT: attemptChanged = true; break;
                case MARK_RUN_FAILED: runSucceeded = false; break;
                case CLEAR_ACTIVE: activeCleared = true; break;
                case RETARGET_SAME_RUN: targetChanged = true; break;
                case MARK_STAGE_SUCCEEDED: break;
                case ACCEPT_EQUAL_TIMESTAMP_WITHOUT_TIE_BREAK:
                    totalRecordOrder = false; break;
                case CLAIM_INTERVAL: readFrom = proofFrom = 10; break;
                case ACCEPT_LATE_LEFT_RECORD: lateLeftAccepted = true; break;
                case COMPLETE_BACKFILL: break;
                case COMMIT_NORMAL: break;
                case REPLAY_OLD_BACKFILL: backfillRepublished = true; break;
                case PUBLISH_GENERATION_A: break;
                case REPLACE_SAME_GENERATION_WITH_DIGEST_B:
                    sameGenerationDifferentDigestAccepted = true; break;
                case PURGE_FEED: currentFeedIncarnation++; break;
                case RECREATE_WITH_OLD_INCARNATION: break;
                case LOCAL_LEADER_CHECK: localLeaderCheckOnly = true; break;
                case BIND_RUN_TO_ARTIFACT_A: break;
                case REBIND_RUN_TO_ARTIFACT_B: runBindingChanged = true; break;
                case HASH_MANIFEST_FROM_CONTROL: manifestDependsOnControl = true; break;
                case HASH_CONTROL_FROM_MANIFEST:
                    hashDependencyCycle = manifestDependsOnControl; break;
                default: throw new AssertionError(step);
            }
        }

        private UnsafeEvidence evidence() {
            return new UnsafeEvidence(
                    committedAdvanced, exactPublicationProof, runSucceeded,
                    proofLeaderTerm, currentLeaderTerm, feedIncarnation,
                    currentFeedIncarnation, localLeaderCheckOnly, intent,
                    attemptChanged, activeCleared, targetChanged, readFrom, proofFrom,
                    lateLeftAccepted, totalRecordOrder, runBindingChanged,
                    backfillRepublished, namespaceCollision,
                    sameGenerationDifferentDigestAccepted, hashDependencyCycle);
        }

        private List<String> trace() {
            return Collections.unmodifiableList(new ArrayList<>(trace));
        }

        private int transitionCount() { return trace.size(); }
    }

    private static final class UnsafeEvidence {
        private final boolean committedAdvanced;
        private final boolean exactPublicationProof;
        private final boolean runSucceeded;
        private final long proofLeaderTerm;
        private final long currentLeaderTerm;
        private final long feedIncarnation;
        private final long currentFeedIncarnation;
        private final boolean localLeaderCheckOnly;
        private final boolean intent;
        private final boolean attemptChanged;
        private final boolean activeCleared;
        private final boolean targetChanged;
        private final long readFrom;
        private final long proofFrom;
        private final boolean lateLeftAccepted;
        private final boolean totalRecordOrder;
        private final boolean runBindingChanged;
        private final boolean backfillRepublished;
        private final boolean namespaceCollision;
        private final boolean sameGenerationDifferentDigestAccepted;
        private final boolean hashDependencyCycle;
        private UnsafeEvidence(
                boolean committedAdvanced, boolean exactPublicationProof, boolean runSucceeded,
                long proofLeaderTerm, long currentLeaderTerm, long feedIncarnation,
                long currentFeedIncarnation, boolean localLeaderCheckOnly,
                boolean intent, boolean attemptChanged, boolean activeCleared,
                boolean targetChanged, long readFrom, long proofFrom,
                boolean lateLeftAccepted, boolean totalRecordOrder,
                boolean runBindingChanged, boolean backfillRepublished,
                boolean namespaceCollision, boolean sameGenerationDifferentDigestAccepted,
                boolean hashDependencyCycle) {
            this.committedAdvanced = committedAdvanced;
            this.exactPublicationProof = exactPublicationProof;
            this.runSucceeded = runSucceeded;
            this.proofLeaderTerm = proofLeaderTerm;
            this.currentLeaderTerm = currentLeaderTerm;
            this.feedIncarnation = feedIncarnation;
            this.currentFeedIncarnation = currentFeedIncarnation;
            this.localLeaderCheckOnly = localLeaderCheckOnly;
            this.intent = intent;
            this.attemptChanged = attemptChanged;
            this.activeCleared = activeCleared;
            this.targetChanged = targetChanged;
            this.readFrom = readFrom;
            this.proofFrom = proofFrom;
            this.lateLeftAccepted = lateLeftAccepted;
            this.totalRecordOrder = totalRecordOrder;
            this.runBindingChanged = runBindingChanged;
            this.backfillRepublished = backfillRepublished;
            this.namespaceCollision = namespaceCollision;
            this.sameGenerationDifferentDigestAccepted = sameGenerationDifferentDigestAccepted;
            this.hashDependencyCycle = hashDependencyCycle;
        }
    }
}
