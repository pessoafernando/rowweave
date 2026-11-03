package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class WorkerServerSingletonTest {

    private MasterServer master;
    private final List<WorkerServer> toClose = new ArrayList<>();

    private WorkerServer guarded(String id, File dir) {
        return new WorkerServer(id, 4, "127.0.0.1", master.port(), dir);
    }

    @AfterEach
    void tearDown() {
        for (WorkerServer w : toClose) {
            try { w.close(); } catch (RuntimeException ignored) { }
        }
        toClose.clear();
        if (master != null) { master.close(); master = null; }
    }

    @Test
    void secondConcurrentWorkerThrows(@TempDir Path tmp) {
        master = new MasterServer(10000, 0, new InProcessRegistry());
        WorkerServer w0 = guarded("w0", new File(tmp.toFile(), "w0"));
        toClose.add(w0);
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> guarded("w1", new File(tmp.toFile(), "w1")));
        assertTrue(ex.getMessage() != null && ex.getMessage().toLowerCase().contains("worker"),
                "the message names the worker already present in this JVM: " + ex.getMessage());
    }

    @Test
    void canStartNewWorkerAfterClose(@TempDir Path tmp) {
        master = new MasterServer(10000, 0, new InProcessRegistry());
        WorkerServer w0 = guarded("w0", new File(tmp.toFile(), "w0"));
        w0.close();
        WorkerServer w1 = assertDoesNotThrow(() -> guarded("w1", new File(tmp.toFile(), "w1")));
        toClose.add(w1);
    }

    @Test
    void closeIsIdempotent(@TempDir Path tmp) {
        master = new MasterServer(10000, 0, new InProcessRegistry());
        WorkerServer w0 = guarded("w0", new File(tmp.toFile(), "w0"));
        w0.close();
        assertDoesNotThrow(w0::close);
        WorkerServer w1 = guarded("w1", new File(tmp.toFile(), "w1"));
        toClose.add(w1);
        assertDoesNotThrow(w0::close);
        assertThrows(IllegalStateException.class,
                () -> guarded("w2", new File(tmp.toFile(), "w2")));
    }

    @Test
    void newUnguardedBypassesGuard(@TempDir Path tmp) {
        master = new MasterServer(10000, 0, new InProcessRegistry());
        WorkerServer w0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), new File(tmp.toFile(), "w0"));
        toClose.add(w0);
        WorkerServer w1 = assertDoesNotThrow(() -> WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), new File(tmp.toFile(), "w1")));
        toClose.add(w1);
    }
}

