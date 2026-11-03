package io.rowweave.incremental;

import io.rowweave.run.RecoveryOwner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/** Canonical wire and digest preimage for frontier recovery ownership. */
public final class FrontierRecoveryTokenCodec {
    public static final int MAX_FRAME_BYTES = 2048;

    private static final byte[] MAGIC = {'A', 'X', 'F', 'T'};
    private static final int VERSION = 1;

    private FrontierRecoveryTokenCodec() {}

    public static byte[] encode(FrontierRecoveryToken token) {
        if (token == null) throw new IllegalArgumentException("frontier recovery token is required");
        try {
            byte[] fields = fields(
                    token.feedId(), token.owner(), token.ownershipEpoch(),
                    token.claimedRevision(), token.feedIncarnation(),
                    token.guardShardObservation(), token.feedRootCzxid());
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(fields.length + 32);
            bytes.write(fields);
            bytes.write(token.tokenDigest());
            byte[] frame = bytes.toByteArray();
            if (frame.length > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("frontier recovery token exceeds limit");
            }
            return frame;
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory frontier token encoding failed", impossible);
        }
    }

    public static FrontierRecoveryToken decode(byte[] frame) {
        if (frame == null || frame.length < 4 + 1 + 32 || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("frontier recovery token frame length is invalid");
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(frame));
            byte[] magic = new byte[MAGIC.length];
            in.readFully(magic);
            if (!Arrays.equals(magic, MAGIC)) {
                throw new IllegalArgumentException("frontier recovery token magic is invalid");
            }
            if (in.readUnsignedByte() != VERSION) {
                throw new IllegalArgumentException("unknown frontier recovery token version");
            }
            String feedId = IncrementalAuthorityCodecSupport.readText(in, "token feed id");
            RecoveryOwner owner = new RecoveryOwner(
                    IncrementalAuthorityCodecSupport.readText(in, "token owner id"),
                    in.readLong());
            long ownershipEpoch = in.readLong();
            long claimedRevision = in.readLong();
            long incarnation = in.readLong();
            int shardIndex = in.readUnsignedShort();
            long shardRevision = in.readLong();
            byte[] shardDigest = IncrementalAuthorityCodecSupport.readDigest(in, "token guard digest");
            long rootCzxid = in.readLong();
            byte[] tokenDigest = IncrementalAuthorityCodecSupport.readDigest(in, "token digest");
            if (in.read() != -1) throw new IllegalArgumentException("frontier token trailing bytes");
            FrontierRecoveryToken token = FrontierRecoveryToken.decoded(
                    feedId, owner, ownershipEpoch, claimedRevision, incarnation,
                    new FeedGuardShardObservation(shardIndex, shardRevision, shardDigest),
                    rootCzxid, tokenDigest);
            if (!Arrays.equals(frame, encode(token))) {
                throw new IllegalArgumentException("frontier recovery token is not canonical");
            }
            return token;
        } catch (IOException malformed) {
            throw new IllegalArgumentException("frontier recovery token is malformed", malformed);
        }
    }

    static byte[] digestFields(
            String feedId, RecoveryOwner owner, long ownershipEpoch,
            long claimedRevision, long feedIncarnation,
            FeedGuardShardObservation guard, long feedRootCzxid) {
        return IncrementalAuthorityValue.sha256(fields(
                feedId, owner, ownershipEpoch, claimedRevision,
                feedIncarnation, guard, feedRootCzxid));
    }

    private static byte[] fields(
            String feedId, RecoveryOwner owner, long ownershipEpoch,
            long claimedRevision, long feedIncarnation,
            FeedGuardShardObservation guard, long feedRootCzxid) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.write(MAGIC);
            out.writeByte(VERSION);
            IncrementalAuthorityCodecSupport.writeText(out, feedId);
            IncrementalAuthorityCodecSupport.writeText(out, owner.ownerId());
            out.writeLong(owner.leaderTermToken());
            out.writeLong(ownershipEpoch);
            out.writeLong(claimedRevision);
            out.writeLong(feedIncarnation);
            out.writeShort(guard.shardIndex());
            out.writeLong(guard.shardRevision());
            out.write(guard.shardDigest());
            out.writeLong(feedRootCzxid);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory frontier token encoding failed", impossible);
        }
    }
}
