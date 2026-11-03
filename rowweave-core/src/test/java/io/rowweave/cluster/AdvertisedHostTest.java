package io.rowweave.cluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AdvertisedHostTest {

    @AfterEach
    void clear() {
        System.clearProperty("rowweave.worker.host");
        System.clearProperty("rowweave.control.host");
        System.clearProperty("rowweave.control.securityProfile");
    }

    @Test
    void defaultsToLoopbackControlHost() throws Exception {
        System.clearProperty("rowweave.worker.host");
        java.io.File tmp = new java.io.File(System.getProperty("java.io.tmpdir"),
                "rowweave-adv-host-def-" + System.nanoTime());
        tmp.mkdirs();
        try (MasterServer master = new MasterServer(5_000L)) {
            try (WorkerServer worker = WorkerServer.newUnguarded(
                    "wDEF", 4, "127.0.0.1", master.port(), tmp)) {
                assertEquals("127.0.0.1", master.worker("wDEF").host(),
                        "LOOPBACK profile must publish a loopback endpoint");
            }
        } finally {
            deleteRec(tmp);
        }
    }

    @Test
    void legacyWorkerHostCannotBroadenLoopbackControlEndpoint() throws Exception {
        System.setProperty("rowweave.worker.host", "10.20.30.40");
        java.io.File tmp = new java.io.File(System.getProperty("java.io.tmpdir"),
                "rowweave-adv-host-" + System.nanoTime());
        tmp.mkdirs();
        try (MasterServer master = new MasterServer(5_000L)) {
            try (WorkerServer worker = WorkerServer.newUnguarded(
                    "wADV", 4, "127.0.0.1", master.port(), tmp)) {
                MasterServer.WorkerInfo wi = master.worker("wADV");
                assertNotNull(wi, "workers registered");
                assertEquals("127.0.0.1", wi.host(),
                        "legacy data host must not expose the plaintext control endpoint");
            }
        } finally {
            deleteRec(tmp);
        }
    }

    private static void deleteRec(java.io.File f) {
        java.io.File[] cs = f.listFiles();
        if (cs != null) for (java.io.File c : cs) deleteRec(c);
        f.delete();
    }
}

