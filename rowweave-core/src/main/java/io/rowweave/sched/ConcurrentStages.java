package io.rowweave.sched;

import java.util.List;

/**
 * The general primitive for orchestrating stages concurrently. A multi-stage concurrent graph such as a keyed
 * shuffle or a binary join is run the same way: <b>start them all, poll-join, poison every channel at once if
 * any fails so the parked threads are released, then fail fast</b>. One thread per stage, since {@link Task} is
 * a dedicated thread, and never a fixed-size pool, which under pure push is a deadlock trap.
 *
 * <p><b>Disambiguation</b>: the first failure that is not a {@link ChannelPoisonedException} is the real cause
 * and is thrown directly; when only poison exceptions are present, which are the collateral failures, the cause
 * behind one of them is thrown. Used by the local PushDagExecutor keyed shuffle and by the in-process cluster
 * alike, so the production path does not depend on cluster.
 */
public final class ConcurrentStages {

    private ConcurrentStages() {
    }

    /**
     * Starts every task, poll-joins them, and fails fast, polling every 50ms and poisoning allChannels at the
     * first failure so nothing stays parked.
     *
     * @param tasks the stages to run concurrently, one thread each
     * @param names the stage names in the same order as tasks, used in the failure message
     * @param allChannels every channel to poison on failure, releasing all sends and polls parked on them
     */
    public static void runFailFast(List<Task> tasks, List<String> names, List<Channel> allChannels) {
        for (Task t : tasks) {
            t.start();
        }
        boolean poisoned = false;
        Throwable canonicalPrimary = null;
        int canonicalIndex = -1;
        try {
            for (Task t : tasks) {
                while (!t.isCompleted()) {
                    t.awaitCompletion(50);
                    if (!poisoned && anyFailed(tasks)) {
                        canonicalPrimary = firstFailure(tasks);
                        canonicalIndex = indexOfFailure(tasks, canonicalPrimary);
                        ChannelPoisonedException poison = new ChannelPoisonedException(
                                "concurrent job failing; poisoning channels");
                        for (Channel ch : allChannels) {
                            try {
                                ch.fail(poison);
                            } catch (Throwable cleanupFailure) {
                                appendSuppressedDistinct(canonicalPrimary, cleanupFailure);
                            }
                        }
                        for (Task task : tasks) {
                            try {
                                task.cancel(poison);
                            } catch (Throwable cleanupFailure) {
                                appendSuppressedDistinct(canonicalPrimary, cleanupFailure);
                            }
                            task.interrupt();
                        }
                        poisoned = true;
                    }
                }
            }
        } catch (InterruptedException ie) {
            RuntimeException failure = new RuntimeException(
                    "concurrent job interrupted while awaiting completion", ie);
            ChannelPoisonedException poison = new ChannelPoisonedException(
                    "concurrent job owner interrupted; poisoning channels");
            for (Channel channel : allChannels) {
                try {
                    channel.fail(poison);
                } catch (Throwable cleanupFailure) {
                    appendSuppressedDistinct(failure, cleanupFailure);
                }
            }
            for (Task task : tasks) {
                try {
                    task.cancel(poison);
                } catch (Throwable cleanupFailure) {
                    appendSuppressedDistinct(failure, cleanupFailure);
                }
                task.interrupt();
            }
            boolean interruptedAgain = false;
            for (Task task : tasks) {
                for (;;) {
                    try {
                        task.awaitCompletion();
                        break;
                    } catch (InterruptedException repeated) {
                        interruptedAgain = true;
                    }
                }
            }
            if (interruptedAgain) {
                failure.addSuppressed(
                        new InterruptedException("interrupted again during concurrent stage teardown"));
            }
            for (Task task : tasks) {
                Throwable childFailure = task.failure();
                appendSuppressedDistinct(failure, childFailure);
            }
            Thread.currentThread().interrupt();
            throw failure;
        }
        if (canonicalPrimary != null) {
            appendOtherTaskFailures(canonicalPrimary, tasks);
            throw new RuntimeException("stage " + names.get(canonicalIndex) + " failed",
                    canonicalPrimary);
        }
        Throwable connected = null;
        for (int i = 0; i < tasks.size(); i++) {
            Throwable f = tasks.get(i).failure();
            if (f == null) {
                continue;
            }
            if (f instanceof ChannelPoisonedException) {
                if (connected == null) {
                    connected = f;
                }
                continue;
            }
            throw new RuntimeException("stage " + names.get(i) + " failed", f);
        }
        if (connected != null) {
            throw new RuntimeException("concurrent job failed (channel poisoned)", connected);
        }
    }

    private static boolean anyFailed(List<Task> tasks) {
        for (Task t : tasks) {
            if (t.isCompleted() && t.failure() != null) {
                return true;
            }
        }
        return false;
    }

    private static Throwable firstFailure(List<Task> tasks) {
        for (Task task : tasks) {
            Throwable failure = task.failure();
            if (failure != null) return failure;
        }
        return null;
    }

    private static int indexOfFailure(List<Task> tasks, Throwable failure) {
        for (int i = 0; i < tasks.size(); i++) {
            if (tasks.get(i).failure() == failure) return i;
        }
        return -1;
    }

    private static void appendOtherTaskFailures(Throwable primary, List<Task> tasks) {
        for (Task task : tasks) {
            Throwable child = task.failure();
            if (child != null && child != primary
                    && !containsThrowable(primary, child)
                    && !containsThrowable(child, primary)) {
                appendSuppressedDistinct(primary, child);
            }
        }
    }

    private static boolean containsThrowable(Throwable root, Throwable target) {
        if (root == null || target == null) return false;
        java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(
                new java.util.IdentityHashMap<>());
        java.util.ArrayDeque<Throwable> pending = new java.util.ArrayDeque<>();
        pending.push(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.pop();
            if (current == target) return true;
            if (!seen.add(current)) continue;
            if (current.getCause() != null) pending.push(current.getCause());
            for (Throwable suppressed : current.getSuppressed()) pending.push(suppressed);
        }
        return false;
    }

    private static void appendSuppressedDistinct(Throwable primary, Throwable candidate) {
        if (primary != null && candidate != null && primary != candidate
                && !containsThrowable(primary, candidate)
                && !containsThrowable(candidate, primary)) {
            primary.addSuppressed(candidate);
        }
    }
}
