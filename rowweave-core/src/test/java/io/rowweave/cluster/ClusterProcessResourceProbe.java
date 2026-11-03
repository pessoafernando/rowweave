package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.monitor.JobMonitorRegistry;

import java.io.File;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;

/** Fresh-process role probe; stdout is a machine-readable certification record. */
public final class ClusterProcessResourceProbe {

    private ClusterProcessResourceProbe() {
    }

    public static void main(String[] arguments) throws Exception {
        String mode = arguments.length == 0 ? "combined" : arguments[0];
        boolean masterEnabled = "master".equals(mode) || "combined".equals(mode);
        boolean workerEnabled = "worker".equals(mode) || "combined".equals(mode);
        if (!masterEnabled && !workerEnabled) {
            throw new IllegalArgumentException("unknown role profile " + mode);
        }
        System.setProperty("rowweave.master.enable", Boolean.toString(masterEnabled));
        System.setProperty("rowweave.worker.enable", Boolean.toString(workerEnabled));

        ProcessBootstrapEnvelope process = TransportSecurityBootstrap.bootstrapProcess();
        ClusterConfig config = ClusterConfig.fromSystemProperties(process);
        File temporary = Files.createTempDirectory("rowweave-role-profile").toFile();
        RpcServer fakeMaster = null;
        MasterServer master = null;
        WorkerServer worker = null;
        try (DeploymentRuntime runtime = config.loadDeploymentRuntime(
                process.tlsEnvelope())) {
            if (masterEnabled) {
                master = new MasterServer(5_000L, 0, new InProcessRegistry(),
                        JobMonitorRegistry.NOOP, null, runtime);
                master.startFixedLeadershipServices();
            } else {
                fakeMaster = new RpcServer(message ->
                        message.type() == RpcMessage.Type.HEARTBEAT
                                ? null : RpcMessage.ack());
            }
            if (workerEnabled) {
                int masterPort = master == null ? fakeMaster.port() : master.port();
                worker = new WorkerServer("profile-worker", config.workerSlots(),
                        new FixedMasterLocator(new Endpoint("127.0.0.1", masterPort)),
                        temporary, runtime, config.workerMaxStageThreads());
                worker.dataPort();
            }
            Thread.sleep(300L);
            printProfile(mode, config, process.capacityEnvelope());
            System.out.flush();
            System.in.read();
        } finally {
            if (worker != null) worker.close();
            if (master != null) master.close();
            if (fakeMaster != null) fakeMaster.close();
        }
    }

    private static void printProfile(String mode, ClusterConfig config,
                                     JvmCapacityEnvelope capacity) {
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        MemoryUsage nonHeap = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        long metaspace = 0L;
        long code = 0L;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            String name = pool.getName();
            long used = pool.getUsage() == null ? 0L : pool.getUsage().getUsed();
            if (name.contains("Metaspace") || name.contains("Compressed Class")) {
                metaspace += used;
            }
            if (pool.getType() == MemoryType.NON_HEAP
                    && !name.contains("Metaspace")
                    && !name.contains("Compressed Class")) {
                code += used;
            }
        }
        long direct = 0L;
        long mapped = 0L;
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(
                BufferPoolMXBean.class)) {
            if ("direct".equals(pool.getName())) direct = pool.getMemoryUsed();
            if ("mapped".equals(pool.getName())) mapped = pool.getMemoryUsed();
        }
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        int rowweaveThreads = 0;
        for (ThreadInfo info : threads.getThreadInfo(threads.getAllThreadIds())) {
            if (info != null && info.getThreadName().startsWith("rowweave-")) {
                rowweaveThreads++;
            }
        }
        long upper = config.javaThreadUpperBound();
        System.out.println("ROLE_PROFILE mode=" + mode
                + " heapUsed=" + heap.getUsed()
                + " heapCommitted=" + heap.getCommitted()
                + " nonHeapUsed=" + nonHeap.getUsed()
                + " metaspaceUsed=" + metaspace
                + " codeUsed=" + code
                + " directUsed=" + direct
                + " mappedUsed=" + mapped
                + " liveThreads=" + threads.getThreadCount()
                + " peakThreads=" + threads.getPeakThreadCount()
                + " rowweaveThreads=" + rowweaveThreads
                + " threadUpper=" + upper
                + " rssRequired=" + capacity.rssRequiredBytes(upper)
                + " processLimit=" + capacity.processMemoryLimitBytes()
                + " heapCap=" + capacity.maxHeapBytes()
                + " directCap=" + capacity.maxDirectMemoryBytes()
                + " metaspaceCap=" + capacity.maxMetaspaceBytes()
                + " codeCap=" + capacity.reservedCodeCacheBytes());
    }
}
