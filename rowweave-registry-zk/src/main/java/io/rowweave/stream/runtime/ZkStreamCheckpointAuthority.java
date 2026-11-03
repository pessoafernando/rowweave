package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.CheckpointDecisionCodec;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointManifest;
import io.rowweave.stream.contract.CheckpointManifestCodec;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.zookeeper.KeeperException;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

/** ZooKeeper first-decision authority containing bounded metadata only. */
public final class ZkStreamCheckpointAuthority
        implements StreamCheckpointAuthority {
    private static final int MAX_ZNODE_BYTES = 512 * 1024;
    private static final int MAX_CHECKPOINTS = 16_384;

    private final CuratorFramework client;
    private final String manifestsRoot;
    private final String checkpointsRoot;

    public ZkStreamCheckpointAuthority(CuratorFramework client, String root) {
        if (client == null || root == null || !root.startsWith("/")
                || root.length() < 2 || root.endsWith("/")
                || root.contains("//")) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "valid ZooKeeper checkpoint root is required");
        }
        this.client = client;
        this.manifestsRoot = root + "/manifests";
        this.checkpointsRoot = root + "/checkpoints";
        ensurePath(manifestsRoot);
        ensurePath(checkpointsRoot);
    }

    @Override
    public CheckpointDecision publishDecision(
            CheckpointDecision proposed, CheckpointManifest manifest) {
        validateFirstDecision(proposed, manifest);
        CheckpointIdentity identity = proposed.checkpointIdentity();
        ensurePath(checkpointPath(identity));
        CheckpointDecision existing = readDecision(decisionPath(identity), identity);
        if (existing != null) {
            CheckpointDecision exact = requireExact(existing, proposed);
            if (exact.status() == CheckpointDecision.Status.COMMIT_DECIDED) {
                requireManifest(exact);
            }
            return exact;
        }

        byte[] decisionBytes = bounded(
                CheckpointDecisionCodec.encode(proposed));
        List<CuratorOp> operations = new ArrayList<>();
        if (proposed.status() == CheckpointDecision.Status.COMMIT_DECIDED) {
            byte[] manifestBytes = bounded(
                    CheckpointManifestCodec.encode(manifest));
            CheckpointManifest durable = manifest(proposed.manifestDigest());
            if (durable == null) {
                operations.add(createOp(
                        manifestPath(proposed.manifestDigest()), manifestBytes));
            } else if (!durable.equals(manifest)) {
                throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                        "content-addressed checkpoint manifest drifted");
            }
        }
        operations.add(createOp(decisionPath(identity), decisionBytes));
        try {
            client.transaction().forOperations(operations);
        } catch (KeeperException.NodeExistsException race) {
            return requirePublished(proposed, manifest);
        } catch (Exception failure) {
            throw io("ZooKeeper checkpoint decision publication failed", failure);
        }
        return requirePublished(proposed, manifest);
    }

    @Override
    public CheckpointDecision complete(CheckpointDecision completed) {
        if (completed == null
                || completed.status() != CheckpointDecision.Status.COMPLETED) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "completion marker must carry COMPLETED status");
        }
        CheckpointIdentity identity = completed.checkpointIdentity();
        ensurePath(checkpointPath(identity));
        CheckpointDecision committed = readDecision(decisionPath(identity), identity);
        if (committed == null
                || committed.status() != CheckpointDecision.Status.COMMIT_DECIDED
                || !committed.manifestDigest().equals(
                completed.manifestDigest())) {
            throw failure(StreamCheckpointException.Code.FIRST_DECISION_CONFLICT,
                    "completion requires the exact durable commit decision");
        }
        requireManifest(committed);
        String path = completionPath(identity);
        CheckpointDecision existing = readDecision(path, identity);
        if (existing != null) return requireExact(existing, completed);
        try {
            client.create().forPath(path, bounded(
                    CheckpointDecisionCodec.encode(completed)));
        } catch (KeeperException.NodeExistsException race) {
            return requireExact(readDecision(path, identity), completed);
        } catch (Exception failure) {
            throw io("ZooKeeper checkpoint completion failed", failure);
        }
        return requireExact(readDecision(path, identity), completed);
    }

    @Override
    public CheckpointDecision decision(CheckpointIdentity identity) {
        requireIdentity(identity);
        CheckpointDecision first = firstDecision(identity);
        CheckpointDecision completion = readDecision(
                completionPath(identity), identity);
        if (completion == null) return first;
        if (first == null
                || first.status() != CheckpointDecision.Status.COMMIT_DECIDED
                || completion.status() != CheckpointDecision.Status.COMPLETED
                || !first.manifestDigest().equals(completion.manifestDigest())) {
            throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                    "checkpoint completion is detached from its decision");
        }
        return completion;
    }

    @Override
    public CheckpointDecision firstDecision(CheckpointIdentity identity) {
        requireIdentity(identity);
        CheckpointDecision first = readDecision(decisionPath(identity), identity);
        if (first != null
                && first.status() == CheckpointDecision.Status.COMMIT_DECIDED) {
            requireManifest(first);
        }
        return first;
    }

    @Override
    public CheckpointManifest manifest(String digest) {
        requireDigest(digest);
        byte[] bytes = read(manifestPath(digest));
        if (bytes == null) return null;
        try {
            CheckpointManifest manifest = CheckpointManifestCodec.decode(bytes);
            if (!digest.equals(manifest.manifestDigest())) {
                throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                        "checkpoint manifest digest does not match its path");
            }
            return manifest;
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (RuntimeException malformed) {
            throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                    "ZooKeeper checkpoint manifest is corrupt", malformed);
        }
    }

    @Override
    public List<CheckpointDecision> decisionsForRun(String runId) {
        if (runId == null || runId.isEmpty()) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint run identity is required");
        }
        try {
            List<String> children = client.getChildren()
                    .forPath(checkpointsRoot);
            if (children.size() > MAX_CHECKPOINTS) {
                throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                        "checkpoint decision catalog exceeds its bound");
            }
            List<CheckpointDecision> result = new ArrayList<>();
            for (String child : children) {
                String path = checkpointsRoot + '/' + child;
                byte[] encoded = read(path + "/decision");
                if (encoded == null) continue;
                CheckpointDecision first;
                try {
                    first = CheckpointDecisionCodec.decode(encoded);
                } catch (RuntimeException malformed) {
                    throw failure(
                            StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                            "ZooKeeper checkpoint decision is corrupt",
                            malformed);
                }
                CheckpointIdentity identity = first.checkpointIdentity();
                if (!child.equals(identityDigest(identity))) {
                    throw failure(
                            StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                            "checkpoint decision identity does not match its path");
                }
                if (runId.equals(identity.runId())) {
                    result.add(decision(identity));
                }
            }
            result.sort(Comparator.comparingLong(value ->
                    value.checkpointIdentity().checkpointSequence()));
            return Collections.unmodifiableList(result);
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (Exception failure) {
            throw io("ZooKeeper checkpoint decision catalog is unavailable",
                    failure);
        }
    }

    @Override public void retainOnly(Set<CheckpointIdentity> identities,
            Set<String> manifestDigests) {
        if (identities == null || manifestDigests == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "protected checkpoint manifests are required");
        }
        Set<String> protectedCheckpoints = new HashSet<>();
        for (CheckpointIdentity identity : identities) {
            requireIdentity(identity);
            protectedCheckpoints.add(identityDigest(identity));
        }
        Set<String> protectedManifests = new HashSet<>();
        for (String digest : manifestDigests) {
            requireDigest(digest);
            protectedManifests.add(digest);
        }
        try {
            List<String> checkpoints = client.getChildren()
                    .forPath(checkpointsRoot);
            List<String> manifests = client.getChildren()
                    .forPath(manifestsRoot);
            if (checkpoints.size() > MAX_CHECKPOINTS
                    || manifests.size() > MAX_CHECKPOINTS) {
                throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                        "checkpoint authority catalog exceeds its bound");
            }
            for (String child : checkpoints) {
                if (!protectedCheckpoints.contains(child)) {
                    deleteTree(checkpointsRoot + '/' + child);
                }
            }
            for (String child : manifests) {
                if (!protectedManifests.contains(child)) {
                    deleteOptional(manifestsRoot + '/' + child);
                }
            }
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (Exception failure) {
            throw io("ZooKeeper checkpoint retention failed", failure);
        }
    }

    private CheckpointDecision requirePublished(
            CheckpointDecision proposed, CheckpointManifest manifest) {
        CheckpointDecision published = readDecision(
                decisionPath(proposed.checkpointIdentity()),
                proposed.checkpointIdentity());
        CheckpointDecision exact = requireExact(published, proposed);
        if (exact.status() == CheckpointDecision.Status.COMMIT_DECIDED) {
            CheckpointManifest durable = requireManifest(exact);
            if (!durable.equals(manifest)) {
                throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                        "durable checkpoint manifest does not match proposal");
            }
        }
        return exact;
    }

    private CheckpointManifest requireManifest(CheckpointDecision decision) {
        CheckpointManifest manifest = manifest(decision.manifestDigest());
        if (manifest == null || !decision.checkpointIdentity().equals(
                manifest.checkpointIdentity())) {
            throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                    "commit decision has no exact durable manifest");
        }
        return manifest;
    }

    private CheckpointDecision readDecision(
            String path, CheckpointIdentity expected) {
        byte[] bytes = read(path);
        if (bytes == null) return null;
        try {
            CheckpointDecision decision = CheckpointDecisionCodec.decode(bytes);
            if (!expected.equals(decision.checkpointIdentity())) {
                throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                        "checkpoint decision identity does not match its path");
            }
            return decision;
        } catch (StreamCheckpointException failure) {
            throw failure;
        } catch (RuntimeException malformed) {
            throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                    "ZooKeeper checkpoint decision is corrupt", malformed);
        }
    }

    private byte[] read(String path) {
        try {
            byte[] data = client.getData().forPath(path);
            return bounded(data);
        } catch (KeeperException.NoNodeException absent) {
            return null;
        } catch (StreamCheckpointException corruption) {
            throw corruption;
        } catch (Exception failure) {
            throw io("ZooKeeper checkpoint metadata read failed", failure);
        }
    }

    private CuratorOp createOp(String path, byte[] data) {
        try {
            return client.transactionOp().create().forPath(path, data);
        } catch (Exception failure) {
            throw io("ZooKeeper checkpoint transaction build failed", failure);
        }
    }

    private static CheckpointDecision requireExact(
            CheckpointDecision existing, CheckpointDecision proposed) {
        if (existing == null || !existing.equals(proposed)) {
            throw failure(StreamCheckpointException.Code.FIRST_DECISION_CONFLICT,
                    "checkpoint first decision cannot be rewritten");
        }
        return existing;
    }

    private static void validateFirstDecision(
            CheckpointDecision proposed, CheckpointManifest manifest) {
        if (proposed == null
                || (proposed.status() != CheckpointDecision.Status.ABORTED
                && proposed.status() != CheckpointDecision.Status.COMMIT_DECIDED)) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "first decision must be ABORTED or COMMIT_DECIDED");
        }
        if (proposed.status() == CheckpointDecision.Status.ABORTED) {
            if (manifest != null) {
                throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                        "aborted checkpoint cannot publish a manifest");
            }
            return;
        }
        if (manifest == null
                || !proposed.checkpointIdentity().equals(
                manifest.checkpointIdentity())
                || !proposed.manifestDigest().equals(
                manifest.manifestDigest())) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "commit decision requires its exact canonical manifest");
        }
    }

    private static byte[] bounded(byte[] data) {
        if (data == null || data.length < 1 || data.length > MAX_ZNODE_BYTES) {
            throw failure(StreamCheckpointException.Code.CORRUPT_AUTHORITY,
                    "ZooKeeper checkpoint metadata exceeds its bound");
        }
        return data;
    }

    private String decisionPath(CheckpointIdentity identity) {
        return checkpointPath(identity) + "/decision";
    }

    private String completionPath(CheckpointIdentity identity) {
        return checkpointPath(identity) + "/completed";
    }

    private String checkpointPath(CheckpointIdentity identity) {
        requireIdentity(identity);
        return checkpointsRoot + "/" + identityDigest(identity);
    }

    private void ensurePath(String path) {
        try {
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            }
        } catch (KeeperException.NodeExistsException race) {
            // Another coordinator created the same durable container.
        } catch (Exception failure) {
            throw io("ZooKeeper checkpoint path initialization failed", failure);
        }
    }

    private void deleteTree(String path) {
        try {
            client.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (KeeperException.NoNodeException absent) {
            // Idempotent retention may replay after a coordinator takeover.
        } catch (Exception failure) {
            throw io("ZooKeeper checkpoint removal failed", failure);
        }
    }

    private void deleteOptional(String path) {
        try {
            client.delete().forPath(path);
        } catch (KeeperException.NoNodeException absent) {
            // Idempotent retention may replay after a coordinator takeover.
        } catch (Exception failure) {
            throw io("ZooKeeper checkpoint manifest removal failed", failure);
        }
    }

    private static String identityDigest(CheckpointIdentity identity) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeText(output, identity.definitionId());
                writeText(output, identity.artifactId());
                writeText(output, identity.runId());
                writeText(output, identity.attemptId());
                output.writeLong(identity.fenceToken());
                output.writeLong(identity.coordinatorTerm());
                writeText(output, identity.coordinatorIncarnation());
                output.writeLong(identity.checkpointSequence());
                writeText(output, identity.planDigest());
                writeText(output, identity.deploymentDigest());
                output.writeInt(identity.dataProtocolVersion());
                output.writeInt(identity.stateFormatVersion());
            }
            return hex(sha256(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory checkpoint identity encoding failed", impossible);
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
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

    private static void requireIdentity(CheckpointIdentity identity) {
        if (identity == null) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "checkpoint identity is required");
        }
    }

    private static void requireDigest(String digest) {
        if (digest == null || !digest.matches("[0-9a-f]{64}")) {
            throw failure(StreamCheckpointException.Code.INVALID_CONFIGURATION,
                    "canonical checkpoint digest is required");
        }
    }

    private static StreamCheckpointException io(
            String message, Throwable cause) {
        return failure(StreamCheckpointException.Code.IO_FAILURE,
                message, cause);
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message) {
        return new StreamCheckpointException(code, message);
    }

    private static StreamCheckpointException failure(
            StreamCheckpointException.Code code, String message,
            Throwable cause) {
        return new StreamCheckpointException(code, message, cause);
    }

    String manifestPath(String digest) {
        requireDigest(digest);
        return manifestsRoot + "/" + digest;
    }
}
