package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("profile")
class RpcConnectionCostProbeTest {

    private static final int WARMUP = Integer.getInteger(
            "rowweave.rpc.connectionProbe.warmup", 20);
    private static final int ITERS = Integer.getInteger(
            "rowweave.rpc.connectionProbe.iterations", 200);

    private static RpcServer echoServer() {
        return new RpcServer(req -> RpcMessage.ack());
    }

    @Test
    void measureAssignConnectionCost() {
        try (RpcServer server = echoServer()) {
            int port = server.port();
            RpcMessage assignLike = RpcMessage.assignStage("bench-worker", 1L, new byte[0]);

            for (int i = 0; i < WARMUP; i++) {
                try (RpcClient c = new RpcClient("127.0.0.1", port)) { c.call(assignLike); }
            }
            long t0 = System.nanoTime();
            for (int i = 0; i < ITERS; i++) {
                try (RpcClient c = new RpcClient("127.0.0.1", port)) { c.call(assignLike); }
            }
            long shortConnNs = System.nanoTime() - t0;

            for (int i = 0; i < WARMUP; i++) {
                new RpcClient("127.0.0.1", port).close();
            }
            long t1 = System.nanoTime();
            for (int i = 0; i < ITERS; i++) {
                new RpcClient("127.0.0.1", port).close();
            }
            long connectOnlyNs = System.nanoTime() - t1;

            try (RpcClient reuse = new RpcClient("127.0.0.1", port)) {
                for (int i = 0; i < WARMUP; i++) reuse.call(assignLike);
                long t2 = System.nanoTime();
                for (int i = 0; i < ITERS; i++) reuse.call(assignLike);
                long reuseNs = System.nanoTime() - t2;

                double shortUs = shortConnNs / 1000.0 / ITERS;
                double connUs = connectOnlyNs / 1000.0 / ITERS;
                double reuseUs = reuseNs / 1000.0 / ITERS;

                System.out.println("\n=== per-assign connection cost measured on loopback, " + ITERS + " run average)===");
                System.out.printf("A) short connection today (new RpcClient+call+close): %8.1f us/op%n", shortUs);
                System.out.printf("B) connect and close only (connect+handshake+close) : %8.1f us/op%n", connUs);
                System.out.printf("C) reused long connection (call only, no connect)   : %8.1f us/op%n", reuseUs);
                System.out.printf("-> connection cost paid on every assign A-C is about %.1f us; the connect itself B is about %.1f us%n",
                        shortUs - reuseUs, connUs);
                System.out.printf("-> a 4-stage job (about 12 short RPC calls) pays about %.2f ms purely for scheduling, independent of data volume%n",
                        (shortUs - reuseUs) * 12 / 1000.0);
                System.out.println("================================================\n");
            }
        }
    }
}

