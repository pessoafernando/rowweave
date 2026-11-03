package io.rowweave.cluster.registry.zk;

import io.rowweave.run.RunRetentionAuthorityContract;
import io.rowweave.run.RunStateStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;

import java.util.ArrayList;
import java.util.List;

class ZkRunRetentionAuthorityContractTest extends RunRetentionAuthorityContract {
    private final List<AutoCloseable> resources = new ArrayList<>();

    @Override
    protected RunStateStore newStore() {
        try {
            TestingServer server = new TestingServer(true);
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString(server.getConnectString())
                    .sessionTimeoutMs(1_000)
                    .connectionTimeoutMs(250)
                    .retryPolicy(new ExponentialBackoffRetry(10, 1))
                    .build();
            client.start();
            client.blockUntilConnected();
            resources.add(client);
            resources.add(server);
            return new ZkRunStateStore(client);
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    @AfterEach
    void closeResources() throws Exception {
        for (int i = resources.size() - 1; i >= 0; i--) resources.get(i).close();
        resources.clear();
    }
}
