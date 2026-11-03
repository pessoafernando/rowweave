package io.rowweave.cluster.registry.zk;

import io.rowweave.run.RecoverableRunCatalogContract;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreProvider;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.List;

class ZkRecoverableRunCatalogContractTest extends RecoverableRunCatalogContract {
    private TestingServer server;
    private final List<ZkServiceRegistry> registries = new ArrayList<>();

    @BeforeEach
    void startServer() throws Exception {
        server = new TestingServer(true);
    }

    @AfterEach
    void closeServer() throws Exception {
        for (ZkServiceRegistry registry : registries) registry.close();
        server.close();
    }

    @Override
    protected RunStateStore newStore() {
        ZkServiceRegistry registry = new ZkServiceRegistry(server.getConnectString());
        registries.add(registry);
        return ((RunStateStoreProvider) registry).runStateStore();
    }
}
