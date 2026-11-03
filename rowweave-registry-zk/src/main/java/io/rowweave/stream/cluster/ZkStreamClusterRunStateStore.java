package io.rowweave.stream.cluster;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;

/** ZooKeeper version-CAS authority for Stream run placement and attempt fences. */
public final class ZkStreamClusterRunStateStore
        implements StreamClusterRunStateStore {
    private static final int MAX_RUNS = 4_096;

    private final CuratorFramework client;
    private final String root;
    private final String runsRoot;
    private final String termPath;

    public ZkStreamClusterRunStateStore(CuratorFramework client, String root) {
        if (client == null || root == null || !root.startsWith("/")
                || root.length() < 2 || root.endsWith("/")
                || root.contains("//")) {
            throw new IllegalArgumentException(
                    "valid ZooKeeper Stream run store configuration is required");
        }
        this.client = client;
        this.root = root;
        this.runsRoot = root + "/runs";
        this.termPath = root + "/coordinator-term";
        ensurePath(runsRoot, new byte[0]);
        ensurePath(termPath, longFrame(0L));
    }

    @Override
    public StreamClusterRunState createIfAbsent(
            StreamClusterRunState initial) {
        if (initial == null) {
            throw new IllegalArgumentException("initial Stream run is required");
        }
        byte[] encoded = StreamClusterRunStateCodec.encode(initial);
        try {
            if (client.getChildren().forPath(runsRoot).size() >= MAX_RUNS) {
                throw new IllegalStateException(
                        "ZooKeeper Stream run capacity is exhausted");
            }
            client.create().withMode(CreateMode.PERSISTENT)
                    .forPath(path(initial.runId()), encoded);
            return StreamClusterRunStateCodec.decode(encoded);
        } catch (KeeperException.NodeExistsException race) {
            return current(initial.runId());
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw io("ZooKeeper Stream run publication failed", failure);
        }
    }

    @Override
    public StreamClusterRunState current(String runId) {
        requireRunId(runId);
        try {
            StreamClusterRunState state = StreamClusterRunStateCodec.decode(
                    client.getData().forPath(path(runId)));
            requireIdentity(runId, state);
            return state;
        } catch (KeeperException.NoNodeException absent) {
            return null;
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw io("ZooKeeper Stream run read failed", failure);
        }
    }

    @Override
    public List<StreamClusterRunState> all() {
        try {
            List<String> children = client.getChildren().forPath(runsRoot);
            if (children.size() > MAX_RUNS) {
                throw new IllegalStateException(
                        "ZooKeeper Stream run catalog exceeds its bound");
            }
            List<StreamClusterRunState> result = new ArrayList<>();
            for (String child : children) {
                StreamClusterRunState state = StreamClusterRunStateCodec.decode(
                        client.getData().forPath(runsRoot + '/' + child));
                if (!child.equals(key(state.runId()))) {
                    throw new IllegalStateException(
                            "ZooKeeper Stream run key is noncanonical");
                }
                result.add(state);
            }
            result.sort((left, right) ->
                    left.runId().compareTo(right.runId()));
            return Collections.unmodifiableList(result);
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw io("ZooKeeper Stream run catalog read failed", failure);
        }
    }

    @Override
    public StreamClusterRunState update(String runId,
            UnaryOperator<StreamClusterRunState> mutation) {
        requireRunId(runId);
        if (mutation == null) {
            throw new IllegalArgumentException("Stream run mutation is required");
        }
        String path = path(runId);
        while (true) {
            try {
                Stat version = new Stat();
                StreamClusterRunState current =
                        StreamClusterRunStateCodec.decode(
                                client.getData().storingStatIn(version)
                                        .forPath(path));
                requireIdentity(runId, current);
                StreamClusterRunState next = mutation.apply(current);
                requireIdentity(runId, next);
                if (next.equals(current)) return current;
                try {
                    client.setData().withVersion(version.getVersion())
                            .forPath(path,
                                    StreamClusterRunStateCodec.encode(next));
                    return next;
                } catch (KeeperException.BadVersionException race) {
                    // Re-evaluate the domain mutation against the latest fence.
                }
            } catch (KeeperException.NoNodeException absent) {
                return null;
            } catch (RuntimeException failure) {
                throw failure;
            } catch (Exception failure) {
                throw io("ZooKeeper Stream run mutation failed", failure);
            }
        }
    }

    @Override
    public StreamClusterRunState delete(String runId,
            UnaryOperator<StreamClusterRunState> validation) {
        requireRunId(runId);
        if (validation == null) {
            throw new IllegalArgumentException(
                    "Stream run deletion validation is required");
        }
        String path = path(runId);
        while (true) {
            try {
                Stat version = new Stat();
                StreamClusterRunState current =
                        StreamClusterRunStateCodec.decode(
                                client.getData().storingStatIn(version)
                                        .forPath(path));
                requireIdentity(runId, current);
                StreamClusterRunState validated = validation.apply(current);
                requireIdentity(runId, validated);
                try {
                    client.delete().withVersion(version.getVersion())
                            .forPath(path);
                    return validated;
                } catch (KeeperException.BadVersionException race) {
                    // Re-check the deletion fence against the latest version.
                }
            } catch (KeeperException.NoNodeException absent) {
                return null;
            } catch (RuntimeException failure) {
                throw failure;
            } catch (Exception failure) {
                throw io("ZooKeeper Stream run deletion failed", failure);
            }
        }
    }

    @Override
    public long nextCoordinatorTerm() {
        while (true) {
            try {
                Stat version = new Stat();
                long current = readLong(client.getData()
                        .storingStatIn(version).forPath(termPath));
                long next = Math.addExact(current, 1L);
                try {
                    client.setData().withVersion(version.getVersion())
                            .forPath(termPath, longFrame(next));
                    return next;
                } catch (KeeperException.BadVersionException race) {
                    // Another Master reserved the term; retry from its value.
                }
            } catch (RuntimeException failure) {
                throw failure;
            } catch (Exception failure) {
                throw io("ZooKeeper Stream coordinator term failed", failure);
            }
        }
    }

    private String path(String runId) {
        return runsRoot + '/' + key(runId);
    }

    private static String key(String runId) {
        requireRunId(runId);
        return sha256Hex(runId.getBytes(StandardCharsets.UTF_8));
    }

    private static void requireRunId(String runId) {
        if (runId == null || runId.isEmpty()) {
            throw new IllegalArgumentException("Stream run ID is required");
        }
    }

    private static void requireIdentity(String runId,
            StreamClusterRunState state) {
        if (state == null || !runId.equals(state.runId())) {
            throw new IllegalStateException(
                    "Stream run mutation changed its identity");
        }
    }

    private void ensurePath(String path, byte[] data) {
        try {
            if (client.checkExists().forPath(path) == null) {
                try {
                    client.create().creatingParentsIfNeeded()
                            .forPath(path, data);
                } catch (KeeperException.NodeExistsException race) {
                    // Concurrent constructor established the same path.
                }
            }
        } catch (Exception failure) {
            throw io("ZooKeeper Stream run root is unavailable", failure);
        }
    }

    private static byte[] longFrame(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    private static long readLong(byte[] value) {
        if (value == null || value.length != 8) {
            throw new IllegalStateException(
                    "ZooKeeper Stream coordinator term is malformed");
        }
        long term = ByteBuffer.wrap(value).getLong();
        if (term < 0L) {
            throw new IllegalStateException(
                    "ZooKeeper Stream coordinator term is invalid");
        }
        return term;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String sha256Hex(byte[] value) {
        byte[] digest = sha256(value);
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[digest.length * 2];
        for (int index = 0; index < digest.length; index++) {
            int current = digest[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0xf];
        }
        return new String(result);
    }

    private static IllegalStateException io(String message, Throwable cause) {
        return new IllegalStateException(message, cause);
    }
}
