package io.rowweave.incremental;

import io.rowweave.cluster.registry.zk.ZkIncrementalFrontierStore;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.BatchSinkAbortRecordCodec;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStoreKernel;
import io.rowweave.run.StageAssignmentManifestCodec;

/** Package authority bridge that mints abort proofs only from live ZK evidence. */
public final class ZkIncrementalAbortProofAuthority {
    private ZkIncrementalAbortProofAuthority() { }

    public static VerifiedIncrementalAbortClosureProof verify(
            ZkIncrementalFrontierStore store,
            FrontierRecoveryToken frontierToken, RecoveryToken runToken,
            RunAttemptFence fence, AttemptStatus terminalStatus) {
        if (store == null || frontierToken == null || runToken == null
                || fence == null || (terminalStatus != AttemptStatus.FAILED
                && terminalStatus != AttemptStatus.CANCELED
                && terminalStatus != AttemptStatus.TIMED_OUT)
                || !store.currentlyOwns(frontierToken, runToken)) {
            throw new IllegalArgumentException(
                    "current dual ownership and supported terminal are required");
        }
        IncrementalAuthorityRecord record = store.record(frontierToken.feedId());
        IncrementalActiveInterval active = record == null
                ? null : record.activeInterval();
        RunSnapshot run = store.runSnapshot(fence.runId());
        BatchSinkAbortRecord abort = store.batchSinkAbort(fence);
        ManifestCommitJournalSnapshot journal =
                store.manifestCommitJournal(fence);
        if (record == null
                || record.state() != IncrementalFrontierAuthorityState.ACTIVE
                || active == null || !active.fence().equals(fence)
                || run == null || abort == null
                || !abortIdentityMatches(record, active, run, abort)
                || abort.state() != BatchSinkAbortRecord.State.ABORTED
                || journal == null || journal.intent() != null) {
            throw new IllegalArgumentException(
                    "current abort closure and no-intent evidence are required");
        }
        RunCommand terminal = abortTerminalCommand(
                "verify-abort-" + active.identity().intervalId(), run,
                active, terminalStatus, abort.reasonCode());
        if (RunStateStoreKernel.evaluate(run, terminal).code()
                != RunDecision.Code.ACCEPTED) {
            throw new IllegalArgumentException(
                    "abort terminal does not match current Run state");
        }
        byte[] abortDigest = IncrementalAuthorityValue.sha256(
                BatchSinkAbortRecordCodec.encode(abort));
        byte[] noIntentDigest = IncrementalProofDigest.noSinkIntent(
                active, StageAssignmentManifestCodec.encodeRoot(
                        journal.assignments().manifest()));
        return VerifiedIncrementalAbortClosureProof.authorityVerified(
                record, active, terminalStatus, abortDigest, noIntentDigest,
                frontierToken, runToken);
    }

    private static boolean abortIdentityMatches(
            IncrementalAuthorityRecord record, IncrementalActiveInterval active,
            RunSnapshot run, BatchSinkAbortRecord abort) {
        return run.runId().equals(active.fence().runId())
                && run.artifactId().equals(active.artifactId())
                && run.definitionId().equals(hex(record.definitionDigest()))
                && run.currentAttempt() != null
                && run.currentAttempt().attemptNumber() == active.attemptOrdinal()
                && run.currentAttempt().attemptId().equals(
                active.fence().attemptId())
                && run.currentAttempt().fenceToken()
                == active.fence().fenceToken()
                && run.currentAttempt().publicationId().equals(
                abort.publicationId());
    }

    private static RunCommand abortTerminalCommand(
            String commandId, RunSnapshot run, IncrementalActiveInterval active,
            AttemptStatus terminalStatus, String reasonCode) {
        long eventTime = Math.addExact(run.updatedAtMillis(), 1L);
        return RunCommand.verifiedAbortClosureTerminal(
                commandId, run.runId(), run.revision(),
                active.fence().attemptId(), active.fence().fenceToken(),
                terminalStatus, eventTime, reasonCode);
    }

    private static String hex(byte[] value) {
        char[] output = new char[value.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < value.length; i++) {
            int next = value[i] & 0xff;
            output[i * 2] = digits[next >>> 4];
            output[i * 2 + 1] = digits[next & 0x0f];
        }
        return new String(output);
    }
}
