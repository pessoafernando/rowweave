package io.rowweave.mem;

import java.util.concurrent.locks.ReentrantLock;

/** Dynamic merge fan-in and final k-way cursor credit conversion, shared by every external sort. */
final class ExternalSortMergePlanner {

    private ExternalSortMergePlanner() {
    }

    /**
     * Converts the single-head escrow the caller already holds into the real final k-way credit and returns the new ownership.
     * On failure the original head ownership is unchanged, so the caller can still reclaim the previous value on close.
     */
    static long prepareFinalRuns(SortedRunStore result, MemoryManager memoryManager,
                                 int maxFrameSize, long currentHeadCredit) {
        ReentrantLock lock = memoryManager.externalSortPhaseLock();
        boolean acquired = false;
        long additionalCredit = 0;
        boolean additionalOwned = false;
        Throwable primary = null;
        try {
            lock.lockInterruptibly();
            acquired = true;
            if (result.acceptedRunCount() == 0) {
                if (currentHeadCredit > 0) memoryManager.releaseExact(currentHeadCredit);
                return 0;
            }

            long frame = ManagedMemoryLayout.arrayBytes(
                    Math.max(ExternalSortMemory.FRAME_HEADER_BYTES, maxFrameSize));
            long perCursor = Math.addExact(ExternalSortMemory.READER_BUFFER_CREDIT,
                    Math.addExact(Math.multiplyExact(3L, frame), ExternalSortMemory.CURSOR_METADATA_CREDIT));
            long mergeFixed = Math.addExact(ExternalSortMemory.READER_BUFFER_CREDIT,
                    Math.addExact(frame, ExternalSortMemory.CURSOR_METADATA_CREDIT));
            long finalFixed = frame;
            long acceptedRuns = result.acceptedRunCount();
            long preferredRuns = Math.min((long) SortedRunStore.MERGE_FANIN, acceptedRuns);
            long minimumTotal = Math.addExact(finalFixed, perCursor);
            long preferredFinal = Math.addExact(finalFixed,
                    Math.multiplyExact(perCursor, preferredRuns));
            long preferredTotal = acceptedRuns > 1
                    ? Math.max(Math.addExact(mergeFixed,
                    Math.multiplyExact(perCursor, preferredRuns)), preferredFinal)
                    : preferredFinal;
            long minimumAdditional = Math.max(0L, minimumTotal - currentHeadCredit);
            long preferredAdditional = Math.max(minimumAdditional,
                    Math.max(0L, preferredTotal - currentHeadCredit));
            additionalCredit = memoryManager.reserveAvailableUpTo(
                    minimumAdditional, preferredAdditional);
            if (additionalCredit < minimumAdditional) {
                throw new MemoryBudgetExceededException("cannot reserve sort merge credit: minimum="
                        + minimumAdditional + " available=" + memoryManager.availableBytes()
                        + " reusableHead=" + currentHeadCredit);
            }
            additionalOwned = additionalCredit > 0;
            long heldCredit = Math.addExact(currentHeadCredit, additionalCredit);
            long byBudget = heldCredit <= mergeFixed ? 0 : (heldCredit - mergeFixed) / perCursor;
            int fanIn = (int) Math.min(Math.min((long) SortedRunStore.MERGE_FANIN, byBudget),
                    result.acceptedRunCount());
            if (result.acceptedRunCount() > 1 && fanIn < 2) {
                throw new MemoryBudgetExceededException("cannot reserve two-way sort merge: held="
                        + heldCredit + " reusableHead=" + currentHeadCredit
                        + " fixed=" + mergeFixed + " perCursor=" + perCursor);
            }

            long maxFinalByBudget = heldCredit <= finalFixed
                    ? 0 : (heldCredit - finalFixed) / perCursor;
            int targetRuns = (int) Math.min(result.acceptedRunCount(),
                    Math.min((long) SortedRunStore.MERGE_FANIN, maxFinalByBudget));
            if (targetRuns < 1) {
                throw new MemoryBudgetExceededException("cannot reserve final sort cursor: held="
                        + heldCredit + " reusableHead=" + currentHeadCredit
                        + " fixed=" + finalFixed + " perCursor=" + perCursor);
            }

            if (result.acceptedRunCount() > targetRuns) {
                result.mergeAcceptedRunsToAtMost(targetRuns, fanIn);
            }

            long targetCredit = Math.addExact(finalFixed,
                    Math.multiplyExact(perCursor, result.acceptedRunCount()));
            if (targetCredit > heldCredit) {
                throw new IllegalStateException("sort merge retained more runs than reserved credit: target="
                        + targetCredit + " held=" + heldCredit);
            }
            long excess = heldCredit - targetCredit;
            if (excess > 0) memoryManager.releaseExact(excess);
            additionalOwned = false;
            return targetCredit;
        } catch (InterruptedException interrupted) {
            RuntimeException root = new RuntimeException("interrupted waiting for external sort phase", interrupted);
            Thread.currentThread().interrupt();
            primary = root;
            throw root;
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            Throwable cleanupFailure = null;
            if (additionalOwned) {
                try {
                    memoryManager.releaseExact(additionalCredit);
                } catch (RuntimeException | Error cleanup) {
                    cleanupFailure = cleanup;
                }
            }
            if (acquired) {
                try {
                    lock.unlock();
                } catch (RuntimeException | Error cleanup) {
                    cleanupFailure = appendFailure(cleanupFailure, cleanup);
                }
            }
            if (cleanupFailure != null) {
                if (primary != null) {
                    primary.addSuppressed(cleanupFailure);
                } else {
                    throwFailure(cleanupFailure);
                }
            }
        }
    }

    private static Throwable appendFailure(Throwable primary, Throwable cleanup) {
        if (primary == null) {
            return cleanup;
        }
        if (primary != cleanup) {
            primary.addSuppressed(cleanup);
        }
        return primary;
    }

    private static void throwFailure(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
    }
}
