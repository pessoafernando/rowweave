package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RecoveryToken;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Authority-only proof that no Sink intent exists and all prepared output is durably closed. */
public final class VerifiedIncrementalAbortClosureProof {
    private final String feedId;
    private final long feedIncarnation;
    private final String intervalId;
    private final RunAttemptFence fence;
    private final AttemptStatus terminalStatus;
    private final byte[] abortClosureDigest;
    private final byte[] noSinkIntentDigest;
    private final byte[] frontierRecoveryTokenDigest;
    private final byte[] runRecoveryTokenDigest;
    private final byte[] proofDigest;

    private VerifiedIncrementalAbortClosureProof(
            IncrementalAuthorityRecord record, IncrementalActiveInterval active,
            AttemptStatus terminalStatus, byte[] abortClosureDigest,
            byte[] noSinkIntentDigest, FrontierRecoveryToken frontierToken,
            RecoveryToken runToken) {
        if (record == null || active == null || frontierToken == null
                || runToken == null || (terminalStatus != AttemptStatus.FAILED
                && terminalStatus != AttemptStatus.CANCELED
                && terminalStatus != AttemptStatus.TIMED_OUT)) {
            throw new IllegalArgumentException(
                    "abort proof requires current authority evidence and a supported terminal");
        }
        this.feedId = record.feedId();
        this.feedIncarnation = record.feedIncarnation();
        this.intervalId = active.identity().intervalId();
        this.fence = active.fence();
        this.terminalStatus = terminalStatus;
        this.abortClosureDigest = IncrementalAuthorityValue.digest(
                abortClosureDigest, "abort closure digest");
        this.noSinkIntentDigest = IncrementalAuthorityValue.digest(
                noSinkIntentDigest, "no-Sink-intent digest");
        this.frontierRecoveryTokenDigest = IncrementalAuthorityValue.digest(
                frontierToken.tokenDigest(), "frontier recovery token digest");
        this.runRecoveryTokenDigest = IncrementalAuthorityValue.digest(
                runToken.tokenDigest(), "Run recovery token digest");
        this.proofDigest = proofDigest(
                record, active, terminalStatus.name(), this.abortClosureDigest,
                this.noSinkIntentDigest, this.frontierRecoveryTokenDigest,
                this.runRecoveryTokenDigest);
    }

    static VerifiedIncrementalAbortClosureProof authorityVerified(
            IncrementalAuthorityRecord record, IncrementalActiveInterval active,
            AttemptStatus terminalStatus, byte[] abortClosureDigest,
            byte[] noSinkIntentDigest, FrontierRecoveryToken frontierToken,
            RecoveryToken runToken) {
        return new VerifiedIncrementalAbortClosureProof(
                record, active, terminalStatus, abortClosureDigest,
                noSinkIntentDigest, frontierToken, runToken);
    }

    public String feedId() { return feedId; }
    public long feedIncarnation() { return feedIncarnation; }
    public String intervalId() { return intervalId; }
    public RunAttemptFence fence() { return fence; }
    public AttemptStatus terminalStatus() { return terminalStatus; }
    public byte[] abortClosureDigest() { return abortClosureDigest.clone(); }
    public byte[] noSinkIntentDigest() { return noSinkIntentDigest.clone(); }
    public byte[] frontierRecoveryTokenDigest() {
        return frontierRecoveryTokenDigest.clone();
    }
    public byte[] runRecoveryTokenDigest() { return runRecoveryTokenDigest.clone(); }
    public byte[] proofDigest() { return proofDigest.clone(); }

    private static byte[] proofDigest(
            IncrementalAuthorityRecord record, IncrementalActiveInterval active,
            String terminalStatus, byte[] abortClosureDigest,
            byte[] noSinkIntentDigest, byte[] frontierRecoveryTokenDigest,
            byte[] runRecoveryTokenDigest) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeText(
                    out, "rowweave-cdc-abort-proof-v2");
            IncrementalAuthorityCodecSupport.writeText(out, record.feedId());
            out.writeLong(record.feedIncarnation());
            IncrementalAuthorityCodecSupport.writeText(
                    out, active.identity().intervalId());
            IncrementalAuthorityCodecSupport.writeText(out, active.fence().runId());
            IncrementalAuthorityCodecSupport.writeText(out, active.fence().attemptId());
            out.writeLong(active.fence().fenceToken());
            IncrementalAuthorityCodecSupport.writeText(out, terminalStatus);
            out.write(abortClosureDigest);
            out.write(noSinkIntentDigest);
            out.write(frontierRecoveryTokenDigest);
            out.write(runRecoveryTokenDigest);
            out.flush();
            return IncrementalAuthorityValue.sha256(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory abort proof failed", impossible);
        }
    }
}
