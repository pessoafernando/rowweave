package io.rowweave.stream.cluster;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamRecoveryPointId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** Canonical bounded restore/offline-rescale submission payload. */
final class StreamRecoverySubmissionCodec {
    static final int MAX_FRAME_BYTES = PlanArtifactCodec.MAX_FRAME_BYTES + 4096;
    private static final int VERSION = 1;

    private StreamRecoverySubmissionCodec() { }

    static byte[] encode(PlanArtifact artifact,
            StreamProductRecovery recovery) {
        if (artifact == null || recovery == null) {
            throw new IllegalArgumentException(
                    "Stream recovery Artifact and provenance are required");
        }
        byte[] artifactFrame = PlanArtifactCodec.encode(artifact);
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(VERSION);
                writeText(output, recovery.sourceJobId().value());
                output.writeInt(recovery.recoveryPointId().kind().ordinal());
                writeText(output, recovery.recoveryPointId().value());
                output.writeInt(recovery.targetParallelism());
                output.writeInt(recovery.targetMaxParallelism());
                output.writeBoolean(recovery.offlineRescale());
                output.writeInt(artifactFrame.length);
                output.write(artifactFrame);
            }
            byte[] result = bytes.toByteArray();
            if (result.length > MAX_FRAME_BYTES) throw invalid();
            return result;
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static Decoded decode(byte[] frame, boolean expectedOfflineRescale) {
        if (frame == null || frame.length <= 0
                || frame.length > MAX_FRAME_BYTES) throw invalid();
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            if (input.readInt() != VERSION) throw invalid();
            StreamJobId sourceJobId = StreamJobId.of(readText(input));
            int kind = input.readInt();
            String pointValue = readText(input);
            StreamRecoveryPointId point = kind == 0
                    ? StreamRecoveryPointId.checkpoint(pointValue)
                    : kind == 1
                    ? StreamRecoveryPointId.savepoint(pointValue) : null;
            if (point == null) throw invalid();
            int parallelism = input.readInt();
            int maxParallelism = input.readInt();
            boolean offlineRescale = input.readBoolean();
            if (offlineRescale != expectedOfflineRescale) throw invalid();
            int artifactLength = input.readInt();
            if (artifactLength <= 0
                    || artifactLength > PlanArtifactCodec.MAX_FRAME_BYTES
                    || artifactLength != input.available()) throw invalid();
            byte[] artifact = new byte[artifactLength];
            input.readFully(artifact);
            return new Decoded(PlanArtifactCodec.decode(artifact),
                    new StreamProductRecovery(sourceJobId, point,
                            parallelism, maxParallelism, offlineRescale));
        } catch (StreamProductControlException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw invalid();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        } catch (RuntimeException malformed) {
            throw invalid();
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= 0 || bytes.length > 1024) throw invalid();
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > 1024 || length > input.available()) {
            throw invalid();
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes)).toString();
        } catch (java.nio.charset.CharacterCodingException malformed) {
            throw invalid();
        }
    }

    private static StreamProductControlException invalid() {
        return new StreamProductControlException(
                StreamProductControlException.Code.INVALID_FRAME,
                "Stream recovery submission is malformed");
    }

    static final class Decoded {
        final PlanArtifact artifact;
        final StreamProductRecovery recovery;
        private Decoded(PlanArtifact artifact,
                StreamProductRecovery recovery) {
            this.artifact = artifact;
            this.recovery = recovery;
        }
    }
}
