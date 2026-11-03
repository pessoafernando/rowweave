package io.rowweave.cluster.registry.zk;

import io.rowweave.upgrade.NamespaceVerification;
import io.rowweave.upgrade.StoreFormatHeader;
import io.rowweave.upgrade.StoreFormatHeaderCodec;
import io.rowweave.upgrade.UpgradeAuthorityKernel;
import io.rowweave.upgrade.UpgradeAuthorityRecord;
import io.rowweave.upgrade.UpgradeAuthorityRecordCodec;
import io.rowweave.upgrade.UpgradeAuthorityStore;
import io.rowweave.upgrade.UpgradeCommand;
import io.rowweave.upgrade.UpgradeCommandEvidence;
import io.rowweave.upgrade.UpgradeObjectEvidence;
import io.rowweave.upgrade.UpgradeObjectEvidenceCodec;
import io.rowweave.upgrade.UpgradeOwner;
import io.rowweave.upgrade.UpgradePhase;
import io.rowweave.upgrade.UpgradeTransition;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** ZooKeeper StoreFormatHeader and UpgradeAuthority with one-version CAS cutover. */
public final class ZkUpgradeAuthority implements UpgradeAuthorityStore {
    @FunctionalInterface
    interface CommitObserver { void afterCommit() throws Exception; }

    private static final int MAX_RETRIES = 16;
    private static final int MAX_OBJECTS = 100_000;
    private static final int MAX_CHILDREN = 4096;
    private static final int MAX_OBJECT_BYTES = 1024 * 1024;

    private final CuratorFramework client;
    private final String root;
    private final String namespaces;
    private final String headerPath;
    private final String authorityPath;
    private final String copyEvidenceRoot;
    private final long rootCzxid;
    private final CommitObserver observer;

    public ZkUpgradeAuthority(CuratorFramework client, String root) {
        this(client, root, () -> { });
    }

    ZkUpgradeAuthority(
            CuratorFramework client, String root, CommitObserver observer) {
        if (client == null || observer == null) {
            throw new IllegalArgumentException("client and observer are required");
        }
        validateRoot(root);
        this.client = client;
        this.root = root;
        this.namespaces = root + "/namespaces";
        this.headerPath = root + "/store-format-header-v1";
        this.authorityPath = root + "/upgrade-authority-v1";
        this.copyEvidenceRoot = root + "/copy-evidence-v1";
        this.observer = observer;
        try {
            ensurePersistent(root);
            ensurePersistent(namespaces);
            ensurePersistent(copyEvidenceRoot);
            Stat stat = client.checkExists().forPath(root);
            if (stat == null) throw new IllegalStateException("upgrade root disappeared");
            this.rootCzxid = stat.getCzxid();
        } catch (Exception failure) {
            throw unavailable("initialize ZK upgrade authority", failure);
        }
    }

    public UpgradeAuthorityRecord bootstrap(
            StoreFormatHeader sourceHeader, StoreFormatHeader targetHeader,
            long generation, String sourceNamespace, String shadowNamespace,
            UpgradeOwner owner, long rollbackDeadlineEpochMillis) {
        validateNamespace(sourceNamespace);
        validateNamespace(shadowNamespace);
        if (sourceNamespace.equals(shadowNamespace)) {
            throw new IllegalArgumentException("source and shadow namespace must differ");
        }
        try {
            checkRoot();
            Scan source = scan(namespacePath(sourceNamespace));
            String zero = zeroDigest();
            UpgradeAuthorityRecord initial = new UpgradeAuthorityRecord(
                    1, sourceHeader, targetHeader, generation, UpgradePhase.IDLE,
                    sourceNamespace, source.digest, shadowNamespace, source.digest,
                    owner, 0L, sourceNamespace, 0L, 1L, 0L, 0, zero, zero,
                    rollbackDeadlineEpochMillis, true, false,
                    Collections.<UpgradeCommandEvidence>emptyList());
            List<CuratorOp> operations = new ArrayList<CuratorOp>(2);
            operations.add(client.transactionOp().create().forPath(
                    headerPath, StoreFormatHeaderCodec.encode(sourceHeader)));
            operations.add(client.transactionOp().create().forPath(
                    authorityPath, UpgradeAuthorityRecordCodec.encode(initial)));
            client.transaction().forOperations(operations);
            return initial;
        } catch (KeeperException.NodeExistsException existing) {
            UpgradeAuthorityRecord current = read();
            requireBootstrapIdentity(current, sourceHeader, targetHeader,
                    generation, sourceNamespace, shadowNamespace, owner);
            return current;
        } catch (Exception failure) {
            throw unavailable("bootstrap ZK upgrade authority", failure);
        }
    }

    @Override
    public UpgradeAuthorityRecord read() {
        try {
            checkRoot();
            byte[] frame = client.getData().forPath(authorityPath);
            if (frame.length > UpgradeAuthorityRecordCodec.MAX_FRAME_BYTES) {
                throw new IllegalStateException("ZK upgrade authority is oversized");
            }
            return UpgradeAuthorityRecordCodec.decode(frame);
        } catch (Exception failure) {
            throw unavailable("read ZK upgrade authority", failure);
        }
    }

    @Override
    public UpgradeTransition apply(
            UpgradeCommand command, long activeRunCount,
            long activeIntervalCount, long authorityTimeMillis) {
        Throwable lastRace = null;
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                checkRoot();
                Stat stat = new Stat();
                byte[] frame = client.getData().storingStatIn(stat)
                        .forPath(authorityPath);
                UpgradeAuthorityRecord current =
                        UpgradeAuthorityRecordCodec.decode(frame);
                UpgradeTransition transition = UpgradeAuthorityKernel.apply(
                        current, command, activeRunCount, activeIntervalCount,
                        authorityTimeMillis);
                if (transition.code() != UpgradeTransition.Code.ACCEPTED) {
                    return transition;
                }
                client.setData().withVersion(stat.getVersion()).forPath(
                        authorityPath, UpgradeAuthorityRecordCodec.encode(
                                transition.record()));
                observer.afterCommit();
                return transition;
            } catch (KeeperException.BadVersionException
                    | KeeperException.ConnectionLossException race) {
                lastRace = race;
            } catch (Exception failure) {
                throw unavailable("apply ZK upgrade command", failure);
            }
        }
        try {
            UpgradeAuthorityRecord current = read();
            UpgradeTransition reconciled = UpgradeAuthorityKernel.apply(
                    current, command, activeRunCount, activeIntervalCount,
                    authorityTimeMillis);
            if (reconciled.code() == UpgradeTransition.Code.REPLAYED) {
                return reconciled;
            }
        } catch (RuntimeException ignored) {
            // Preserve the original bounded-race failure below.
        }
        throw unavailable("apply ZK upgrade command after retry exhaustion", lastRace);
    }

    public NamespaceVerification copyAndVerifyShadow(UpgradeCommand command) {
        if (command == null || command.kind() != UpgradeCommand.Kind.BEGIN_COPY) {
            throw new IllegalArgumentException("accepted BEGIN_COPY command is required");
        }
        UpgradeAuthorityRecord current = read();
        UpgradeTransition replay = UpgradeAuthorityKernel.apply(
                current, command, 0L, 0L, 0L);
        if (current.phase() != UpgradePhase.COPYING
                || replay.code() != UpgradeTransition.Code.REPLAYED) {
            throw new IllegalStateException("ZK shadow copy is not authorized");
        }
        try {
            String sourcePath = namespacePath(current.sourceNamespace());
            String targetPath = namespacePath(current.shadowNamespace());
            Scan before = scan(sourcePath);
            if (!before.digest.equals(current.sourceDigest())) {
                throw new IllegalStateException("ZK source namespace digest drift");
            }
            ensurePersistent(targetPath);
            long evidenceCount = 0L;
            for (Node object : before.nodes) {
                String target = targetPath + "/" + object.relativePath;
                ensurePersistent(parent(target));
                Stat existing = client.checkExists().forPath(target);
                if (existing == null) {
                    client.create().forPath(target, object.data);
                } else {
                    client.setData().forPath(target, object.data);
                }
                UpgradeObjectEvidence evidence = new UpgradeObjectEvidence(
                        UpgradeObjectEvidence.VERSION_1, command.generation(),
                        object.relativePath, digest(object.data),
                        digest(client.getData().forPath(target)),
                        command.commandId(), command.commandDigest());
                writeCopyEvidence(command, evidence);
                if (!evidence.equals(copyEvidenceUnsafe(
                        command, object.relativePath))) {
                    throw new IllegalStateException("ZK shadow copy evidence drift");
                }
                evidenceCount = Math.addExact(evidenceCount, 1L);
            }
            Scan sourceAfter = scan(sourcePath);
            Scan target = scan(targetPath);
            if (!before.digest.equals(sourceAfter.digest)
                    || !target.digest.equals(current.shadowDigest())
                    || sourceAfter.objectCount != target.objectCount) {
                throw new IllegalStateException("ZK shadow semantic verification failed"
                        + " sourceBefore=" + before.digest
                        + " sourceAfter=" + sourceAfter.digest
                        + " expectedTarget=" + current.shadowDigest()
                        + " actualTarget=" + target.digest
                        + " sourceObjects=" + sourceAfter.objectCount
                        + " targetObjects=" + target.objectCount
                        + " targetPaths=" + paths(target));
            }
            return new NamespaceVerification(sourceAfter.objectCount,
                    sourceAfter.shardCount, evidenceCount,
                    sourceAfter.digest, target.digest);
        } catch (Exception failure) {
            throw unavailable("copy and verify ZK shadow namespace", failure);
        }
    }

    public UpgradeObjectEvidence copyEvidence(
            UpgradeCommand command, String relativePath) {
        try {
            UpgradeAuthorityRecord current = read();
            requireCopyCommand(command, current);
            return copyEvidenceUnsafe(command, relativePath);
        } catch (Exception failure) {
            throw unavailable("read ZK shadow copy evidence", failure);
        }
    }

    private UpgradeObjectEvidence copyEvidenceUnsafe(
            UpgradeCommand command, String relativePath) throws Exception {
        String path = copyEvidencePath(command, relativePath);
        Stat stat = client.checkExists().forPath(path);
        if (stat == null) {
            throw new IllegalStateException("ZK shadow copy evidence is absent");
        }
        try {
            UpgradeObjectEvidence evidence = UpgradeObjectEvidenceCodec.decode(
                    client.getData().forPath(path));
            if (evidence.generation() != command.generation()
                    || !evidence.relativePath().equals(relativePath)
                    || !evidence.commandId().equals(command.commandId())
                    || !evidence.commandDigest().equals(command.commandDigest())) {
                throw new IllegalStateException("ZK shadow copy evidence differs");
            }
            return evidence;
        } catch (IllegalArgumentException corrupt) {
            throw new IllegalStateException(
                    "ZK shadow copy evidence is corrupt", corrupt);
        }
    }

    private void writeCopyEvidence(
            UpgradeCommand command, UpgradeObjectEvidence evidence)
            throws Exception {
        String path = copyEvidencePath(command, evidence.relativePath());
        ensurePersistent(parent(path));
        byte[] frame = UpgradeObjectEvidenceCodec.encode(evidence);
        if (client.checkExists().forPath(path) == null) {
            try {
                client.create().forPath(path, frame);
                return;
            } catch (KeeperException.NodeExistsException race) {
                // An uncertain/replayed copy converges through the set below.
            }
        }
        client.setData().forPath(path, frame);
    }

    private String copyEvidencePath(
            UpgradeCommand command, String relativePath) {
        validateRelativePath(relativePath);
        String identity = digest(
                relativePath.getBytes(StandardCharsets.UTF_8));
        return copyEvidenceRoot + "/" + command.generation()
                + "/" + command.commandDigest()
                + "/" + identity.substring(0, 2)
                + "/" + identity.substring(2, 4)
                + "/" + identity;
    }

    private static void requireCopyCommand(
            UpgradeCommand command, UpgradeAuthorityRecord current) {
        if (command == null || command.kind() != UpgradeCommand.Kind.BEGIN_COPY
                || UpgradeAuthorityKernel.apply(current, command, 0L, 0L, 0L)
                .code() != UpgradeTransition.Code.REPLAYED) {
            throw new IllegalStateException("accepted BEGIN_COPY command is required");
        }
    }

    private static void validateRelativePath(String value) {
        if (value == null || value.isEmpty() || value.length() > 1024
                || value.startsWith("/") || value.endsWith("/")
                || value.contains("\\") || value.contains("//")
                || value.contains("/../") || value.startsWith("../")
                || value.equals("..") || value.contains("/./")
                || value.startsWith("./") || value.equals(".")) {
            throw new IllegalArgumentException("object relative path is invalid");
        }
    }

    /** Absolute ZK path selected by the durable active pointer. */
    public String activeNamespacePath() {
        return namespacePath(read().activeNamespace());
    }

    private Scan scan(String namespacePath) throws Exception {
        if (client.checkExists().forPath(namespacePath) == null) {
            throw new IllegalStateException("ZK namespace is absent: " + namespacePath);
        }
        List<Node> nodes = new ArrayList<Node>();
        int[] shardCount = {0};
        collect(namespacePath, namespacePath, nodes, shardCount);
        Collections.sort(nodes, Comparator.comparing(node -> node.relativePath));
        MessageDigest digest = sha256();
        for (Node node : nodes) {
            byte[] path = node.relativePath.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(4).putInt(path.length).array());
            digest.update(path);
            digest.update(ByteBuffer.allocate(4).putInt(node.data.length).array());
            digest.update(node.data);
        }
        return new Scan(nodes, nodes.size(), shardCount[0], hex(digest.digest()));
    }

    private void collect(String base, String path, List<Node> nodes,
                         int[] shardCount) throws Exception {
        List<String> children = client.getChildren().forPath(path);
        collect(base, path, children, nodes, shardCount);
    }

    private void collect(String base, String path, List<String> children,
                         List<Node> nodes, int[] shardCount) throws Exception {
        if (children.size() > MAX_CHILDREN) {
            throw new IllegalStateException("ZK namespace child limit exceeded");
        }
        Collections.sort(children);
        if (!path.equals(base) && parent(path).equals(base)
                && !children.isEmpty()) {
            shardCount[0] = Math.addExact(shardCount[0], 1);
        }
        for (String child : children) {
            String childPath = path + "/" + child;
            List<String> childChildren = client.getChildren().forPath(childPath);
            byte[] data = client.getData().forPath(childPath);
            if (data.length > MAX_OBJECT_BYTES) {
                throw new IllegalStateException("ZK namespace object exceeds limit");
            }
            if (data.length != 0 || childChildren.isEmpty()) {
                if (nodes.size() == MAX_OBJECTS) {
                    throw new IllegalStateException("ZK namespace object limit exceeded");
                }
                nodes.add(new Node(childPath.substring(base.length() + 1), data));
            }
            collect(base, childPath, childChildren, nodes, shardCount);
        }
    }

    private void checkRoot() throws Exception {
        Stat stat = client.checkExists().forPath(root);
        if (stat == null || stat.getCzxid() != rootCzxid) {
            throw new IllegalStateException("ZK upgrade root incarnation changed");
        }
    }

    private void requireBootstrapIdentity(
            UpgradeAuthorityRecord current, StoreFormatHeader sourceHeader,
            StoreFormatHeader targetHeader, long generation,
            String sourceNamespace, String shadowNamespace,
            UpgradeOwner owner) {
        if (!current.sourceHeader().equals(sourceHeader)
                || !current.targetHeader().equals(targetHeader)
                || current.generation() != generation
                || !current.sourceNamespace().equals(sourceNamespace)
                || !current.shadowNamespace().equals(shadowNamespace)
                || !current.owner().equals(owner)) {
            throw new IllegalStateException("existing ZK upgrade bootstrap differs");
        }
    }

    private String namespacePath(String name) {
        validateNamespace(name);
        return namespaces + "/" + name;
    }

    private void ensurePersistent(String path) throws Exception {
        if (client.checkExists().forPath(path) != null) return;
        try {
            client.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
        } catch (KeeperException.NodeExistsException race) {
            // Concurrent creator won with the same persistent shape.
        }
    }

    private static void validateRoot(String root) {
        if (root == null || root.length() < 2 || root.length() > 512
                || root.charAt(0) != '/' || root.endsWith("/")
                || root.contains("//") || root.contains("/../")
                || root.endsWith("/..") || root.contains("/./")) {
            throw new IllegalArgumentException("ZK upgrade root is invalid");
        }
    }

    private static void validateNamespace(String name) {
        if (name == null || name.isEmpty() || name.length() > 255
                || name.equals(".") || name.equals("..")
                || name.indexOf('/') >= 0 || name.indexOf('\\') >= 0) {
            throw new IllegalArgumentException("ZK namespace name is invalid");
        }
    }

    private static String parent(String path) {
        int separator = path.lastIndexOf('/');
        if (separator <= 0) return "/";
        return path.substring(0, separator);
    }

    private static String paths(Scan scan) {
        StringBuilder value = new StringBuilder();
        for (Node node : scan.nodes) {
            if (value.length() != 0) value.append(',');
            value.append(node.relativePath).append(':').append(node.data.length);
        }
        return value.toString();
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String digest(byte[] bytes) {
        return hex(sha256().digest(bytes));
    }

    private static String hex(byte[] bytes) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] encoded = new char[bytes.length * 2];
        for (int index = 0; index < bytes.length; index++) {
            int value = bytes[index] & 0xff;
            encoded[index * 2] = alphabet[value >>> 4];
            encoded[index * 2 + 1] = alphabet[value & 0x0f];
        }
        return new String(encoded);
    }

    private static String zeroDigest() {
        return "0000000000000000000000000000000000000000000000000000000000000000";
    }

    private static IllegalStateException unavailable(
            String operation, Throwable failure) {
        if (failure instanceof IllegalStateException) {
            return (IllegalStateException) failure;
        }
        return new IllegalStateException(operation + " failed", failure);
    }

    private static final class Node {
        private final String relativePath;
        private final byte[] data;

        private Node(String relativePath, byte[] data) {
            this.relativePath = relativePath;
            this.data = data.clone();
        }
    }

    private static final class Scan {
        private final List<Node> nodes;
        private final long objectCount;
        private final int shardCount;
        private final String digest;

        private Scan(List<Node> nodes, long objectCount, int shardCount,
                     String digest) {
            this.nodes = nodes;
            this.objectCount = objectCount;
            this.shardCount = shardCount;
            this.digest = digest;
        }
    }
}
