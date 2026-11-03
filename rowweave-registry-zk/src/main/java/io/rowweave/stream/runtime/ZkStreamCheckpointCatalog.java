package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointEvidence;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamCheckpointMemberCodec;
import io.rowweave.stream.contract.StreamSavepoint;
import io.rowweave.stream.contract.StreamSavepointCodec;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** ZooKeeper catalog for bounded member proofs and savepoint metadata only. */
public final class ZkStreamCheckpointCatalog
        implements StreamCheckpointCatalog {
    private static final int MAX_MEMBER_BYTES = 8 * 1024;
    private static final int MAX_SAVEPOINT_BYTES = 512 * 1024;
    private static final int MAX_CATALOG_ENTRIES = 16_384;

    private final CuratorFramework client;
    private final String membersRoot;
    private final String savepointsRoot;

    public ZkStreamCheckpointCatalog(CuratorFramework client, String root) {
        if (client == null || root == null || !root.startsWith("/")
                || root.length() < 2 || root.endsWith("/")
                || root.contains("//")) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "valid ZooKeeper checkpoint catalog root is required");
        }
        this.client = client;
        this.membersRoot = root + "/members";
        this.savepointsRoot = root + "/savepoints";
        ensurePath(membersRoot);
        ensurePath(savepointsRoot);
    }

    @Override public void publish(StreamCheckpointMember member) {
        if (member == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint member is required");
        }
        byte[] frame = bounded(StreamCheckpointMemberCodec.encode(member),
                MAX_MEMBER_BYTES,
                StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                "checkpoint member frame exceeds its bound");
        publishImmutable(memberPath(member.toEvidence()), frame,
                StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                "checkpoint member identity was reused");
    }

    @Override public StreamCheckpointMember member(
            CheckpointEvidence evidence) {
        if (evidence == null) {
            throw failure(
                    StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "checkpoint evidence is required");
        }
        byte[] frame = read(memberPath(evidence), MAX_MEMBER_BYTES,
                StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                "checkpoint member frame is missing or corrupt");
        try {
            StreamCheckpointMember member =
                    StreamCheckpointMemberCodec.decode(frame);
            if (!evidence.equals(member.toEvidence())) {
                throw failure(StreamCheckpointException.Code
                                .CORRUPT_MEMBER_CATALOG,
                        "checkpoint member evidence does not match its path");
            }
            return member;
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (RuntimeException malformed) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "checkpoint member frame is corrupt", malformed);
        }
    }

    @Override public StreamSavepoint publishSavepoint(
            StreamSavepoint savepoint) {
        if (savepoint == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "savepoint is required");
        }
        byte[] frame = bounded(StreamSavepointCodec.encode(savepoint),
                MAX_SAVEPOINT_BYTES,
                StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                "savepoint metadata exceeds its bound");
        String path = savepointPath(savepoint.savepointId());
        publishImmutable(path, frame,
                StreamCheckpointException.Code.SAVEPOINT_CONFLICT,
                "savepoint name cannot be rebound to another checkpoint");
        return decodeSavepoint(read(path, MAX_SAVEPOINT_BYTES,
                StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                "savepoint metadata is missing or corrupt"),
                savepoint.savepointId());
    }

    @Override public StreamSavepoint get(String savepointId) {
        String canonical = StreamSavepoint.canonicalId(savepointId);
        byte[] frame = readOptional(savepointPath(canonical),
                MAX_SAVEPOINT_BYTES,
                StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                "savepoint metadata is corrupt");
        return frame == null ? null : decodeSavepoint(frame, canonical);
    }

    @Override public List<StreamSavepoint> savepoints() {
        try {
            List<String> children = client.getChildren()
                    .forPath(savepointsRoot);
            requireBoundedCatalog(children);
            List<StreamSavepoint> result = new ArrayList<>();
            for (String child : children) {
                byte[] frame = read(savepointsRoot + '/' + child,
                        MAX_SAVEPOINT_BYTES,
                        StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                        "savepoint metadata is missing or corrupt");
                StreamSavepoint savepoint = decodeSavepoint(frame, null);
                if (!(savepointsRoot + '/' + child).equals(
                        savepointPath(savepoint.savepointId()))) {
                    throw failure(
                            StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                            "savepoint identity does not match its path");
                }
                result.add(savepoint);
            }
            result.sort((left, right) -> left.savepointId().compareTo(
                    right.savepointId()));
            return Collections.unmodifiableList(result);
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "ZooKeeper savepoint catalog read failed", failure);
        }
    }

    @Override public void removeCheckpointReference(String checkpointId) {
        String canonical = StreamSavepoint.canonicalId(checkpointId);
        if (!canonical.startsWith("stream-checkpoint:")) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "only retained checkpoint references may be removed");
        }
        deleteOptional(savepointPath(canonical),
                "ZooKeeper checkpoint reference removal failed");
    }

    @Override public void retainOnlyMembers(
            Collection<StreamCheckpointMember> retained) {
        if (retained == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "retained checkpoint members are required");
        }
        Set<String> protectedPaths = new HashSet<>();
        for (StreamCheckpointMember member : retained) {
            if (member == null) {
                throw failure(
                        StreamCheckpointException.Code.INVALID_CONFIGURATION,
                        "retained checkpoint member is required");
            }
            protectedPaths.add(memberPath(member.toEvidence()));
        }
        try {
            List<String> children = client.getChildren().forPath(membersRoot);
            requireBoundedCatalog(children);
            for (String child : children) {
                String path = membersRoot + '/' + child;
                if (!protectedPaths.contains(path)) {
                    deleteOptional(path,
                            "ZooKeeper checkpoint member removal failed");
                }
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "ZooKeeper checkpoint member catalog is unavailable",
                    failure);
        }
    }

    private StreamSavepoint decodeSavepoint(byte[] frame, String expectedId) {
        try {
            StreamSavepoint decoded = StreamSavepointCodec.decode(frame);
            if (expectedId != null
                    && !expectedId.equals(decoded.savepointId())) {
                throw failure(StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                        "savepoint identity does not match its path");
            }
            return decoded;
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (RuntimeException malformed) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.CORRUPT_SAVEPOINT,
                    "savepoint metadata is corrupt", malformed);
        }
    }

    private void publishImmutable(String path, byte[] frame,
            StreamCheckpointException.Code conflictCode,
            String conflictMessage) {
        try {
            client.create().forPath(path, frame);
        } catch (KeeperException.NodeExistsException race) {
            byte[] existing = read(path, Math.max(MAX_MEMBER_BYTES,
                    MAX_SAVEPOINT_BYTES), conflictCode, conflictMessage);
            if (!Arrays.equals(existing, frame)) {
                throw failure(conflictCode, conflictMessage);
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (Exception ioFailure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "ZooKeeper checkpoint catalog publication failed",
                    ioFailure);
        }
    }

    private byte[] read(String path, int maximum,
            StreamCheckpointException.Code code, String message) {
        byte[] value = readOptional(path, maximum, code, message);
        if (value == null) throw failure(code, message);
        return value;
    }

    private byte[] readOptional(String path, int maximum,
            StreamCheckpointException.Code code, String message) {
        try {
            return bounded(client.getData().forPath(path), maximum,
                    code, message);
        } catch (KeeperException.NoNodeException absent) {
            return null;
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (Exception ioFailure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "ZooKeeper checkpoint catalog read failed", ioFailure);
        }
    }

    private String memberPath(CheckpointEvidence evidence) {
        return membersRoot + '/' + digest(evidence.memberId() + ':'
                + evidence.contentDigest() + ':'
                + Long.toUnsignedString(evidence.contentBytes()));
    }

    private String savepointPath(String savepointId) {
        return savepointsRoot + '/' + digest(
                StreamSavepoint.canonicalId(savepointId));
    }

    private void ensurePath(String path) {
        try {
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            }
        } catch (KeeperException.NodeExistsException race) {
            // Another Master established the same immutable namespace.
        } catch (Exception failure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    "ZooKeeper checkpoint catalog initialization failed",
                    failure);
        }
    }

    private void deleteOptional(String path, String message) {
        try {
            client.delete().forPath(path);
        } catch (KeeperException.NoNodeException absent) {
            // Idempotent retention may replay after a coordinator takeover.
        } catch (Exception failure) {
            throw new StreamCheckpointException(
                    StreamCheckpointException.Code.IO_FAILURE,
                    message, failure);
        }
    }

    private static void requireBoundedCatalog(List<String> children) {
        if (children.size() > MAX_CATALOG_ENTRIES) {
            throw failure(StreamCheckpointException.Code.CORRUPT_MEMBER_CATALOG,
                    "ZooKeeper checkpoint catalog exceeds its bound");
        }
    }

    private static byte[] bounded(byte[] frame, int maximum,
            StreamCheckpointException.Code code, String message) {
        if (frame == null || frame.length < 1 || frame.length > maximum) {
            throw failure(code, message);
        }
        return frame;
    }

    private static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(bytes.length * 2);
            for (byte item : bytes) {
                result.append(Character.forDigit((item >>> 4) & 0x0f, 16));
                result.append(Character.forDigit(item & 0x0f, 16));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }
}
