package io.rowweave.stream.runtime;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.PortableStreamPlanArtifact;
import io.rowweave.stream.contract.PortableStreamPlanCodec;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.StreamSavepoint;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Fail-before-state-access admission for exact savepoint restores. */
public final class StreamSavepointRestoreValidator {
    private StreamSavepointRestoreValidator() { }

    public static PortableStreamPlan validateExact(
            StreamSavepoint savepoint, PlanArtifact targetArtifact) {
        if (savepoint == null || targetArtifact == null) {
            throw failure("savepoint and target artifact are required");
        }
        PortableStreamPlan target;
        try {
            target = targetArtifact.payloadFormat()
                    == PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1
                    ? PortableStreamJobArtifact.decodeValidated(
                            targetArtifact).plan()
                    : PortableStreamPlanArtifact.decodeValidated(
                            targetArtifact);
        } catch (RuntimeException invalidArtifact) {
            throw failure("target Stream artifact is invalid", invalidArtifact);
        }
        CheckpointIdentity source = savepoint.checkpointManifest()
                .checkpointIdentity();
        String targetPlanDigest = hex(sha256(
                PortableStreamPlanCodec.encode(target)));
        if (!source.definitionId().equals(targetArtifact.definitionId())
                || !source.artifactId().equals(targetArtifact.artifactId())
                || !source.planDigest().equals(targetPlanDigest)
                || source.stateFormatVersion() != 1) {
            throw failure("savepoint does not exactly match the target artifact");
        }
        return target;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String hex(byte[] value) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            int current = value[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(result);
    }

    private static StreamCheckpointException failure(String message) {
        return new StreamCheckpointException(
                StreamCheckpointException.Code.SAVEPOINT_INCOMPATIBLE,
                message);
    }

    private static StreamCheckpointException failure(
            String message, Throwable cause) {
        return new StreamCheckpointException(
                StreamCheckpointException.Code.SAVEPOINT_INCOMPATIBLE,
                message, cause);
    }
}
