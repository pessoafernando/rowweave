package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointDecisionCodec;
import io.rowweave.stream.contract.CheckpointManifest;
import io.rowweave.stream.contract.CheckpointManifestCodec;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamCheckpointMemberCodec;
import io.rowweave.stream.runtime.InMemoryStreamCheckpointAuthority;
import io.rowweave.stream.runtime.StreamCheckpointAuthority;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Bounded commit proof and member metadata transferred over the Worker lane. */
final class StreamCheckpointTransferBundleCodec {
    static final int MAX_FRAME_BYTES = 768 * 1024;
    private static final byte[] MAGIC =
            "RWXXCPB1".getBytes(StandardCharsets.US_ASCII);
    private static final int COMMIT_VERSION = 1;
    private static final int COMPLETED_VERSION = 2;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_MEMBERS = 256;
    private static final int MAX_MEMBER_BYTES = 16 * 1024;

    static final class Bundle {
        private final CheckpointDecision decision;
        private final CheckpointDecision completion;
        private final CheckpointManifest manifest;
        private final List<StreamCheckpointMember> members;

        private Bundle(CheckpointDecision decision,
                CheckpointDecision completion,
                CheckpointManifest manifest,
                List<StreamCheckpointMember> members) {
            this.decision = decision;
            this.completion = completion;
            this.manifest = manifest;
            this.members = Collections.unmodifiableList(
                    new ArrayList<>(members));
        }

        CheckpointDecision decision() { return decision; }
        CheckpointManifest manifest() { return manifest; }
        List<StreamCheckpointMember> members() { return members; }

        StreamCheckpointAuthority authority() {
            InMemoryStreamCheckpointAuthority authority =
                    new InMemoryStreamCheckpointAuthority();
            authority.publishDecision(decision, manifest);
            if (completion != null) authority.complete(completion);
            return authority;
        }
    }

    private StreamCheckpointTransferBundleCodec() { }

    static byte[] encode(CheckpointDecision decision,
            CheckpointManifest manifest,
            List<StreamCheckpointMember> members) {
        return encode(COMMIT_VERSION, decision, null, manifest, members);
    }

    static byte[] encodeCompleted(CheckpointDecision decision,
            CheckpointDecision completion, CheckpointManifest manifest,
            List<StreamCheckpointMember> members) {
        if (completion == null
                || completion.status()
                != CheckpointDecision.Status.COMPLETED
                || decision == null
                || !decision.checkpointIdentity().equals(
                completion.checkpointIdentity())
                || !decision.manifestDigest().equals(
                completion.manifestDigest())) {
            throw new IllegalArgumentException(
                    "exact completed checkpoint transfer proof is required");
        }
        return encode(COMPLETED_VERSION, decision, completion,
                manifest, members);
    }

    private static byte[] encode(int version,
            CheckpointDecision decision, CheckpointDecision completion,
            CheckpointManifest manifest,
            List<StreamCheckpointMember> members) {
        if (decision == null
                || decision.status()
                != CheckpointDecision.Status.COMMIT_DECIDED
                || manifest == null
                || !decision.checkpointIdentity().equals(
                manifest.checkpointIdentity())
                || !decision.manifestDigest().equals(
                manifest.manifestDigest())
                || members == null || members.size() > MAX_MEMBERS) {
            throw new IllegalArgumentException(
                    "exact bounded checkpoint transfer proof is required");
        }
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream() {
                @Override public synchronized void write(int value) {
                    requireRoom(1);
                    super.write(value);
                }

                @Override public synchronized void write(byte[] value,
                        int offset, int length) {
                    requireRoom(length);
                    super.write(value, offset, length);
                }

                private void requireRoom(int length) {
                    if (length < 0 || count + (long) length
                            > MAX_FRAME_BYTES - MAGIC.length - 8
                            - DIGEST_BYTES) {
                        throw new IllegalArgumentException(
                                "checkpoint transfer proof exceeds its bound");
                    }
                }
            };
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                writeBytes(body, CheckpointDecisionCodec.encode(decision),
                        MAX_FRAME_BYTES);
                if (version == COMPLETED_VERSION) {
                    writeBytes(body,
                            CheckpointDecisionCodec.encode(completion),
                            MAX_FRAME_BYTES);
                }
                writeBytes(body, CheckpointManifestCodec.encode(manifest),
                        MAX_FRAME_BYTES);
                body.writeInt(members.size());
                for (StreamCheckpointMember member : members) {
                    writeBytes(body, StreamCheckpointMemberCodec.encode(member),
                            MAX_MEMBER_BYTES);
                }
            }
            byte[] body = bodyBytes.toByteArray();
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
            try (DataOutputStream frame = new DataOutputStream(frameBytes)) {
                frame.write(MAGIC);
                frame.writeInt(version);
                frame.writeInt(body.length);
                frame.write(body);
                frame.write(sha256(body));
            }
            byte[] frame = frameBytes.toByteArray();
            if (frame.length > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "checkpoint transfer proof exceeds its bound");
            }
            return frame;
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static Bundle decode(byte[] frame) {
        if (frame == null || frame.length < MAGIC.length + 8 + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "checkpoint transfer proof exceeds its bound");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            int version = input.readInt();
            int bodyLength = input.readInt();
            if (!MessageDigest.isEqual(MAGIC, magic)
                    || (version != COMMIT_VERSION
                    && version != COMPLETED_VERSION)
                    || bodyLength < 0 || MAGIC.length + 8L + bodyLength
                    + DIGEST_BYTES != frame.length) {
                throw new IllegalArgumentException(
                        "checkpoint transfer proof header is invalid");
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] expected = new byte[DIGEST_BYTES];
            input.readFully(expected);
            if (!MessageDigest.isEqual(expected, sha256(body))) {
                throw new IllegalArgumentException(
                        "checkpoint transfer proof checksum differs");
            }
            try (DataInputStream value = new DataInputStream(
                    new ByteArrayInputStream(body))) {
                CheckpointDecision decision = CheckpointDecisionCodec.decode(
                        readBytes(value, MAX_FRAME_BYTES));
                CheckpointDecision completion = version == COMPLETED_VERSION
                        ? CheckpointDecisionCodec.decode(
                        readBytes(value, MAX_FRAME_BYTES)) : null;
                CheckpointManifest manifest = CheckpointManifestCodec.decode(
                        readBytes(value, MAX_FRAME_BYTES));
                int count = value.readInt();
                if (count < 0 || count > MAX_MEMBERS) {
                    throw new IllegalArgumentException(
                            "checkpoint transfer member count is invalid");
                }
                List<StreamCheckpointMember> members =
                        new ArrayList<>(count);
                for (int index = 0; index < count; index++) {
                    members.add(StreamCheckpointMemberCodec.decode(
                            readBytes(value, MAX_MEMBER_BYTES)));
                }
                if (value.available() != 0
                        || decision.status()
                        != CheckpointDecision.Status.COMMIT_DECIDED
                        || (version == COMPLETED_VERSION
                        && (completion.status()
                        != CheckpointDecision.Status.COMPLETED
                        || !decision.checkpointIdentity().equals(
                        completion.checkpointIdentity())
                        || !decision.manifestDigest().equals(
                        completion.manifestDigest())))
                        || !decision.checkpointIdentity().equals(
                        manifest.checkpointIdentity())
                        || !decision.manifestDigest().equals(
                        manifest.manifestDigest())) {
                    throw new IllegalArgumentException(
                            "checkpoint transfer proof is inconsistent");
                }
                return new Bundle(decision, completion, manifest, members);
            }
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "checkpoint transfer proof is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "checkpoint transfer proof is malformed", malformed);
        }
    }

    private static void writeBytes(DataOutputStream output, byte[] bytes,
            int maximum) throws IOException {
        if (bytes == null || bytes.length < 1 || bytes.length > maximum) {
            throw new IllegalArgumentException(
                    "checkpoint transfer field exceeds its bound");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static byte[] readBytes(DataInputStream input, int maximum)
            throws IOException {
        int length = input.readInt();
        if (length < 1 || length > maximum || length > input.available()) {
            throw new IllegalArgumentException(
                    "checkpoint transfer field exceeds its bound");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return bytes;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
