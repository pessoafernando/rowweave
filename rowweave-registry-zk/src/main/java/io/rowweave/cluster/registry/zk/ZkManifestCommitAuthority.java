package io.rowweave.cluster.registry.zk;

import io.rowweave.commit.FileCommitGenerationAuthority;
import io.rowweave.commit.ManifestCommitAuthority;
import io.rowweave.commit.ManifestFileStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicBoolean;

/** The commit generation kept in a persistent ZK node, together with the domain writer's lease in an ephemeral one. */
final class ZkManifestCommitAuthority implements ManifestCommitAuthority {

    private static final String ROOT = "/rowweave/manifest-commit-v1/domains";
    private static final int ATTEMPT_MAGIC = 0x5a4d4131; // ZMA1

    private final CuratorFramework client;
    private final FileCommitGenerationAuthority auditClaims = new FileCommitGenerationAuthority();

    ZkManifestCommitAuthority(CuratorFramework client) {
        this.client = client;
    }

    @Override
    public Lease tryAcquire(Path domain, String attemptId) {
        validateAttempt(attemptId);
        Domain key = domain(domain);
        ensureDomain(key);
        String leasePath = key.path + "/lease";
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(
                    leasePath, attemptId.getBytes(StandardCharsets.UTF_8));
            return new ZkLease(leasePath);
        } catch (KeeperException.NodeExistsException occupied) {
            return null;
        } catch (Exception failure) {
            throw new RuntimeException("ZK manifest lease acquire failed", failure);
        }
    }

    @Override
    public long reserve(Path domain, String attemptId) {
        validateAttempt(attemptId);
        Domain key = domain(domain);
        ensureDomain(key);
        String attemptPath = key.path + "/attempts/" + sha256Hex(attemptId);
        Long existing = readAttempt(attemptPath, attemptId);
        if (existing != null) {
            auditClaims.recordClaim(domain, attemptId, existing);
            return existing;
        }
        String counterPath = key.path + "/counter";
        for (int retries = 0; retries < 10_000; retries++) {
            try {
                Stat stat = new Stat();
                byte[] bytes = client.getData().storingStatIn(stat).forPath(counterPath);
                long current = decodeCounter(bytes);
                if (current == Long.MAX_VALUE) {
                    throw new IllegalStateException("manifest generation exhausted");
                }
                long next = current + 1L;
                client.setData().withVersion(stat.getVersion())
                        .forPath(counterPath, encodeCounter(next));
                try {
                    client.create().withMode(CreateMode.PERSISTENT)
                            .forPath(attemptPath, encodeAttempt(next, attemptId));
                    auditClaims.recordClaim(domain, attemptId, next);
                    return next;
                } catch (KeeperException.NodeExistsException racedAttempt) {
                    long winner = readRequiredAttempt(attemptPath, attemptId);
                    auditClaims.recordClaim(domain, attemptId, winner);
                    return winner;
                }
            } catch (KeeperException.BadVersionException racedCounter) {
                // CAS loser: reread counter and retry; allocated values are never reused.
            } catch (Exception failure) {
                throw new RuntimeException("ZK manifest generation reserve failed", failure);
            }
        }
        throw new IllegalStateException("ZK manifest generation CAS did not converge");
    }

    @Override
    public Long find(Path domain, String attemptId) {
        validateAttempt(attemptId);
        Domain key = domain(domain);
        String attemptPath = key.path + "/attempts/" + sha256Hex(attemptId);
        Long generation = readAttempt(attemptPath, attemptId);
        if (generation != null) {
            auditClaims.recordClaim(domain, attemptId, generation);
        }
        return generation;
    }

    private void ensureDomain(Domain domain) {
        try {
            ensurePersistent(domain.path, new byte[0]);
            ensurePersistent(domain.path + "/identity", domain.identity.getBytes(StandardCharsets.UTF_8));
            byte[] identity = client.getData().forPath(domain.path + "/identity");
            if (!MessageDigest.isEqual(identity, domain.identity.getBytes(StandardCharsets.UTF_8))) {
                throw new IllegalStateException("manifest domain hash identity conflict");
            }
            ensurePersistent(domain.path + "/attempts", new byte[0]);
            ensurePersistent(domain.path + "/counter", encodeCounter(0L));
        } catch (Exception failure) {
            throw new RuntimeException("ZK manifest domain initialization failed", failure);
        }
    }

    private void ensurePersistent(String path, byte[] data) throws Exception {
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                    .forPath(path, data);
        } catch (KeeperException.NodeExistsException ignore) {
            // idempotent initialization; identity/counter are verified/read separately.
        }
    }

    private Long readAttempt(String path, String expectedAttempt) {
        try {
            return decodeAttempt(client.getData().forPath(path), expectedAttempt);
        } catch (KeeperException.NoNodeException missing) {
            return null;
        } catch (Exception failure) {
            throw new RuntimeException("ZK manifest attempt lookup failed", failure);
        }
    }

    private long readRequiredAttempt(String path, String expectedAttempt) {
        Long generation = readAttempt(path, expectedAttempt);
        if (generation == null) {
            throw new IllegalStateException("raced manifest attempt mapping disappeared");
        }
        return generation;
    }

    private Domain domain(Path domain) {
        ManifestFileStore store = new ManifestFileStore(domain);
        String identity = store.domainId();
        return new Domain(identity, ROOT + "/" + sha256Hex(identity));
    }

    private static void validateAttempt(String attemptId) {
        if (attemptId == null || attemptId.isEmpty() || attemptId.indexOf('\0') >= 0
                || attemptId.getBytes(StandardCharsets.UTF_8).length > 1024) {
            throw new IllegalArgumentException("invalid manifest attemptId");
        }
    }

    private static byte[] encodeCounter(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    private static long decodeCounter(byte[] bytes) {
        if (bytes == null || bytes.length != Long.BYTES) {
            throw new IllegalStateException("invalid ZK manifest counter");
        }
        long value = ByteBuffer.wrap(bytes).getLong();
        if (value < 0L) {
            throw new IllegalStateException("negative ZK manifest counter");
        }
        return value;
    }

    private static byte[] encodeAttempt(long generation, String attemptId) throws Exception {
        byte[] attempt = attemptId.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(ATTEMPT_MAGIC);
            out.writeLong(generation);
            out.writeInt(attempt.length);
            out.write(attempt);
        }
        return bytes.toByteArray();
    }

    private static long decodeAttempt(byte[] encoded, String expectedAttempt) throws Exception {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
            if (in.readInt() != ATTEMPT_MAGIC) {
                throw new IllegalStateException("invalid ZK manifest attempt magic");
            }
            long generation = in.readLong();
            int length = in.readInt();
            if (generation <= 0L || length < 1 || length > 1024) {
                throw new IllegalStateException("invalid ZK manifest attempt mapping");
            }
            byte[] attempt = new byte[length];
            in.readFully(attempt);
            if (in.available() != 0 || !expectedAttempt.equals(new String(attempt, StandardCharsets.UTF_8))) {
                throw new IllegalStateException("ZK manifest attempt hash collision/conflict");
            }
            return generation;
        }
    }

    private static String sha256Hex(String text) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder(hash.length * 2);
            for (byte value : hash) {
                out.append(String.format(java.util.Locale.ROOT, "%02x", value & 0xff));
            }
            return out.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private final class ZkLease implements Lease {
        private final String path;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ZkLease(String path) {
            this.path = path;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                Stat stat = client.checkExists().forPath(path);
                if (stat == null) {
                    return;
                }
                long sessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
                if (stat.getEphemeralOwner() == sessionId) {
                    client.delete().withVersion(stat.getVersion()).forPath(path);
                }
            } catch (KeeperException.NoNodeException ignore) {
                // session close/another terminal cleanup already released it.
            } catch (Exception failure) {
                throw new RuntimeException("ZK manifest lease release failed", failure);
            }
        }
    }

    private static final class Domain {
        final String identity;
        final String path;

        Domain(String identity, String path) {
            this.identity = identity;
            this.path = path;
        }
    }
}
