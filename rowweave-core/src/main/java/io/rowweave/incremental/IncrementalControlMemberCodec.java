package io.rowweave.incremental;

import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.run.RunAttemptFence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Canonical frame for the reserved Manifest control member. */
public final class IncrementalControlMemberCodec {
    public static final String MEMBER_NAME = "__rowweave_incremental_v1__";
    public static final String SCHEMA_FINGERPRINT =
            "rowweave.incremental-control.v1";
    public static final int MAX_FRAME_BYTES = 64 * 1024;

    private static final int MAGIC = 0x52574943; // RWIC
    private static final int VERSION = 1;

    private IncrementalControlMemberCodec() {
    }

    public static byte[] encodeForPublication(
            String feedId, byte[] definitionDigest,
            IncrementalActiveInterval active,
            IncrementalPlanDescriptor descriptor,
            String publicationId, long generation,
            List<CommitManifest.Member> dataMembers) {
        return encode(create(feedId, definitionDigest, active, descriptor,
                publicationId, generation, dataMembers));
    }

    static IncrementalControlMember create(
            String feedId, byte[] definitionDigest,
            IncrementalActiveInterval active,
            IncrementalPlanDescriptor descriptor,
            String publicationId, long generation,
            List<CommitManifest.Member> dataMembers) {
        if (active == null || descriptor == null || dataMembers == null
                || dataMembers.isEmpty()) {
            throw new IllegalArgumentException(
                    "incremental control member inputs are required");
        }
        byte[] membersDigest = dataMembersDigest(dataMembers);
        byte[] decisionInput = decisionInputDigest(
                active, publicationId, generation, membersDigest);
        return new IncrementalControlMember(
                feedId, active.capturedTarget().feedIncarnation(),
                definitionDigest, active.identity().intervalId(),
                active.capturedTarget().mode(),
                CapturedTargetReceiptCodec.encode(active.capturedTarget()),
                active.artifactId(), active.fence(), publicationId, generation,
                IncrementalPlanDescriptorCodec.encode(descriptor),
                membersDigest, decisionInput);
    }

    public static byte[] encode(IncrementalControlMember member) {
        if (member == null) {
            throw new IllegalArgumentException(
                    "incremental control member is required");
        }
        try {
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(payloadBytes);
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            IncrementalAuthorityCodecSupport.writeText(out, member.feedId());
            out.writeLong(member.feedIncarnation());
            out.write(member.definitionDigest());
            IncrementalAuthorityCodecSupport.writeText(out, member.intervalId());
            out.writeByte(member.mode().wireId());
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, member.capturedTargetFrame());
            IncrementalAuthorityCodecSupport.writeText(out, member.artifactId());
            writeFence(out, member.fence());
            IncrementalAuthorityCodecSupport.writeText(
                    out, member.publicationId());
            out.writeLong(member.generation());
            IncrementalAuthorityCodecSupport.writeBytes(
                    out, member.planDescriptorFrame());
            out.write(member.dataMembersDigest());
            out.write(member.decisionInputDigest());
            out.flush();
            byte[] payload = payloadBytes.toByteArray();
            ByteArrayOutputStream frame = new ByteArrayOutputStream(
                    payload.length + IncrementalAuthorityValue.DIGEST_BYTES);
            frame.write(payload);
            frame.write(sha256(payload));
            byte[] encoded = frame.toByteArray();
            if (encoded.length > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "incremental control member exceeds limit");
            }
            return encoded;
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory incremental control encoding failed", impossible);
        }
    }

    public static IncrementalControlMember decode(byte[] frame) {
        if (frame == null || frame.length < 96
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental control member frame length is invalid");
        }
        int payloadLength = frame.length - IncrementalAuthorityValue.DIGEST_BYTES;
        byte[] payload = java.util.Arrays.copyOf(frame, payloadLength);
        byte[] expected = java.util.Arrays.copyOfRange(
                frame, payloadLength, frame.length);
        if (!MessageDigest.isEqual(sha256(payload), expected)) {
            throw new IllegalArgumentException(
                    "incremental control member digest mismatch");
        }
        try {
            DataInputStream in = new DataInputStream(
                    new ByteArrayInputStream(payload));
            if (in.readInt() != MAGIC || in.readInt() != VERSION) {
                throw new IllegalArgumentException(
                        "incremental control member header is invalid");
            }
            String feedId = IncrementalAuthorityCodecSupport.readText(
                    in, "feed id");
            long incarnation = in.readLong();
            byte[] definition = IncrementalAuthorityCodecSupport.readDigest(
                    in, "definition digest");
            String intervalId = IncrementalAuthorityCodecSupport.readText(
                    in, "interval id");
            IncrementalRunMode mode = IncrementalRunMode.fromWire(
                    in.readUnsignedByte());
            byte[] capture = IncrementalAuthorityCodecSupport.readBytes(
                    in, CapturedTargetReceiptCodec.MAX_FRAME_BYTES,
                    "captured target");
            String artifactId = IncrementalAuthorityCodecSupport.readText(
                    in, "artifact id");
            RunAttemptFence fence = readFence(in);
            String publicationId = IncrementalAuthorityCodecSupport.readText(
                    in, "publication id");
            long generation = in.readLong();
            byte[] descriptor = IncrementalAuthorityCodecSupport.readBytes(
                    in, IncrementalPlanDescriptorCodec.MAX_FRAME_BYTES,
                    "plan descriptor");
            byte[] membersDigest = IncrementalAuthorityCodecSupport.readDigest(
                    in, "data members digest");
            byte[] decisionDigest = IncrementalAuthorityCodecSupport.readDigest(
                    in, "decision input digest");
            if (in.available() != 0) {
                throw new IllegalArgumentException(
                        "incremental control member has trailing bytes");
            }
            IncrementalControlMember decoded = new IncrementalControlMember(
                    feedId, incarnation, definition, intervalId, mode, capture,
                    artifactId, fence, publicationId, generation, descriptor,
                    membersDigest, decisionDigest);
            if (!java.util.Arrays.equals(frame, encode(decoded))) {
                throw new IllegalArgumentException(
                        "incremental control member is not canonical");
            }
            return decoded;
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "incremental control member is malformed", malformed);
        }
    }

    public static void requirePublicationManifest(
            byte[] controlFrame, CommitManifest manifest) {
        IncrementalControlMember control = decode(controlFrame);
        if (manifest == null
                || manifest.generation() != control.generation()
                || !manifest.attemptId().equals(control.publicationId())) {
            throw new IllegalArgumentException(
                    "incremental publication Manifest identity differs");
        }
        CommitManifest.Member expectedControl = new CommitManifest.Member(
                MEMBER_NAME,
                "data/" + control.generation() + "/" + MEMBER_NAME + ".data",
                SCHEMA_FINGERPRINT, controlFrame.length,
                sha256(controlFrame));
        List<CommitManifest.Member> dataMembers = new ArrayList<>();
        boolean foundControl = false;
        for (CommitManifest.Member member : manifest.members()) {
            if (MEMBER_NAME.equals(member.name())) {
                if (foundControl || !expectedControl.equals(member)) {
                    throw new IllegalArgumentException(
                            "incremental publication control member differs");
                }
                foundControl = true;
            } else {
                dataMembers.add(member);
            }
        }
        if (!foundControl
                || !MessageDigest.isEqual(
                dataMembersDigest(dataMembers), control.dataMembersDigest())
                || !MessageDigest.isEqual(
                decisionInputDigest(
                        control.intervalId(), control.fence(),
                        control.publicationId(), control.generation(),
                        control.dataMembersDigest()),
                control.decisionInputDigest())) {
            throw new IllegalArgumentException(
                    "incremental publication Manifest closure differs");
        }
    }

    private static byte[] dataMembersDigest(
            List<CommitManifest.Member> members) {
        List<CommitManifest.Member> ordered = new ArrayList<>(members);
        ordered.sort(Comparator.comparing(CommitManifest.Member::name));
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(ordered.size());
            for (CommitManifest.Member member : ordered) {
                if (member == null || MEMBER_NAME.equals(member.name())) {
                    throw new IllegalArgumentException(
                            "reserved/null data member is invalid");
                }
                IncrementalAuthorityCodecSupport.writeBytes(
                        out, ManifestCommitWire.encodeMember(member));
            }
            out.flush();
            return sha256(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory member digest failed", impossible);
        }
    }

    private static byte[] decisionInputDigest(
            IncrementalActiveInterval active, String publicationId,
            long generation, byte[] membersDigest) {
        return decisionInputDigest(
                active.identity().intervalId(), active.fence(), publicationId,
                generation, membersDigest);
    }

    private static byte[] decisionInputDigest(
            String intervalId, RunAttemptFence fence, String publicationId,
            long generation, byte[] membersDigest) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            IncrementalAuthorityCodecSupport.writeText(
                    out, intervalId);
            writeFence(out, fence);
            IncrementalAuthorityCodecSupport.writeText(out, publicationId);
            out.writeLong(generation);
            out.write(membersDigest);
            out.flush();
            return sha256(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory decision digest failed", impossible);
        }
    }

    private static void writeFence(
            DataOutputStream out, RunAttemptFence fence) throws IOException {
        IncrementalAuthorityCodecSupport.writeText(out, fence.runId());
        IncrementalAuthorityCodecSupport.writeText(out, fence.attemptId());
        out.writeLong(fence.fenceToken());
    }

    private static RunAttemptFence readFence(DataInputStream in)
            throws IOException {
        return new RunAttemptFence(
                IncrementalAuthorityCodecSupport.readText(in, "run id"),
                IncrementalAuthorityCodecSupport.readText(in, "attempt id"),
                in.readLong());
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
