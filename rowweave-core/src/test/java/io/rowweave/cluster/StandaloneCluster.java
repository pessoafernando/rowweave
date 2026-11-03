package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.monitor.MonitorBootstrap;
import io.rowweave.monitor.MonitorLauncher;

import java.io.File;
import java.nio.file.Files;

public final class StandaloneCluster {

    public static void main(String[] args) throws Exception {
        int masterPort = Integer.getInteger("cluster.master.port", 19000);
        int monitorPort = Integer.getInteger("cluster.monitor.port", 18080);

        File dir = Files.createTempDirectory("standalone-cluster").toFile();
        File w0 = new File(dir, "w0"); w0.mkdirs();
        File w1 = new File(dir, "w1"); w1.mkdirs();

        InMemoryJobMonitor monitor = new InMemoryJobMonitor(200);
        MasterServer master = new MasterServer(10000, masterPort, new InProcessRegistry(), monitor);
        WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
        WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1);
        a0.startHeartbeat(1000);
        a1.startHeartbeat(1000);

        MonitorLauncher launcher = MonitorBootstrap.discover();
        int mport = -1;
        if (launcher != null) {
            mport = launcher.start(monitor, monitorPort, null);
        }

        Thread refresher = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                master.refreshWorkerMonitor();
                try { Thread.sleep(2000); } catch (InterruptedException e) { return; }
            }
        });
        refresher.setDaemon(true);
        refresher.start();

        System.out.println("CLUSTER_READY");
        System.out.println("MASTER_PORT=" + master.port());
        System.out.println("MONITOR_URL=http://127.0.0.1:" + (mport > 0 ? mport : monitorPort) + "/");
        System.out.println("Workers: w0, w1 (32 slots each)");
        System.out.println("point a client at 127.0.0.1:" + master.port() + " to submit jobs; the monitor is in the browser. Ctrl-C to stop.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (launcher != null) launcher.stop();
            a0.close(); a1.close(); master.close();
            System.out.println("CLUSTER_STOPPED");
        }));

        Thread.currentThread().join();
    }

    private StandaloneCluster() { }
}

