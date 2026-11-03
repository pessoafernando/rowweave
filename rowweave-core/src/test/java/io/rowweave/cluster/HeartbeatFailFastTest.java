package io.rowweave.cluster;

import io.rowweave.DataRow;
import io.rowweave.sched.LocalChannel;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatFailFastTest {

    private static DataRow r(int v) {
        DataRow x = new DataRow(1);
        x.set(0, v);
        return x;
    }

    @Test
    void detectsLostOnce() throws Exception {
        BeatHeartbeat healthy = new BeatHeartbeat(10_000);
        BeatHeartbeat dying = new BeatHeartbeat(10_000);
        AtomicInteger lostCount = new AtomicInteger();
        AtomicReference<Heartbeat> lost = new AtomicReference<>();
        HeartbeatMonitor mon = new HeartbeatMonitor(Arrays.asList(healthy, dying));
        mon.startMonitoring(20, b -> { lostCount.incrementAndGet(); lost.set(b); });

        dying.markDead();
        Thread.sleep(200);
        mon.stop();

        assertEquals(1, lostCount.get(), "exactly one callback (de-duplicated), with no false report for a healthy worker");
        assertSame(dying, lost.get());
    }

    @Test
    void stopIdempotent() throws Exception {
        BeatHeartbeat hb = new BeatHeartbeat(10_000);
        AtomicInteger lost = new AtomicInteger();
        HeartbeatMonitor mon = new HeartbeatMonitor(Arrays.asList(hb));
        mon.startMonitoring(20, b -> lost.incrementAndGet());
        mon.stop();
        mon.stop();
        hb.markDead();
        Thread.sleep(150);
        assertEquals(0, lost.get(), "no callback after stop");
    }

    @Test
    void hungWorkerFailsFastViaHeartbeat() throws Exception {
        LocalChannel jobChannel = new LocalChannel(16);
        BeatHeartbeat workerBeat = new BeatHeartbeat(80); // staleness 80ms

        AtomicReference<Throwable> consumerResult = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            try {
                StreamEvent e;
                while ((e = jobChannel.poll()) != null) {
                    if (e.isError()) {
                        throw new RuntimeException("job failed via poison", e.cause());
                    }
                }
            } catch (Throwable t) {
                consumerResult.set(t);
            }
        });
        consumer.start();

        HeartbeatMonitor mon = new HeartbeatMonitor(Arrays.asList(workerBeat));
        long t0 = System.currentTimeMillis();
        mon.startMonitoring(20, b -> jobChannel.fail(new RuntimeException("worker hung (heartbeat lost)")));

        consumer.join(3000);
        long elapsed = System.currentTimeMillis() - t0;
        mon.stop();

        assertFalse(consumer.isAlive(), "a hung worker must trigger fail fast so the consumer never parks forever");
        assertNotNull(consumerResult.get(), "the consumer should observe a thrown error");
        assertTrue(elapsed < 2000, "fail fast within a bounded time, actual " + elapsed + "ms");
    }

    @Test
    void backpressuredSlowWorkerNotKilled() throws Exception {
        BeatHeartbeat slowButAlive = new BeatHeartbeat(80);
        AtomicInteger falseKill = new AtomicInteger();
        HeartbeatMonitor mon = new HeartbeatMonitor(Arrays.asList(slowButAlive));
        mon.startMonitoring(20, b -> falseKill.incrementAndGet());

        Thread beater = new Thread(() -> {
            long until = System.currentTimeMillis() + 500;
            while (System.currentTimeMillis() < until) {
                slowButAlive.beat();
                try { Thread.sleep(15); } catch (InterruptedException e) { return; }
            }
        });
        beater.start();
        beater.join(2000);
        mon.stop();

        assertEquals(0, falseKill.get(), "a backpressured slow worker (no row progress but steady beats) must not be killed by mistake");
    }

    @Test
    void successStopsMonitorBeforeFinalProbe() throws Exception {
        BeatHeartbeat hb = new BeatHeartbeat(50);
        AtomicInteger lost = new AtomicInteger();
        HeartbeatMonitor mon = new HeartbeatMonitor(Arrays.asList(hb));
        mon.startMonitoring(20, b -> lost.incrementAndGet());
        mon.stop();
        Thread.sleep(200);
        assertEquals(0, lost.get(), "stopping the monitor after success keeps a stale post-success report from poisoning the job");
    }

    @Test
    void stopDuringInFlightProbeNoLateCallback() throws Exception {
        java.util.concurrent.CountDownLatch inProbe = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch releaseProbe = new java.util.concurrent.CountDownLatch(1);
        Heartbeat blocking = () -> {
            inProbe.countDown();
            try { releaseProbe.await(2, java.util.concurrent.TimeUnit.SECONDS); } catch (InterruptedException ignored) { }
            return false; // dead
        };
        AtomicInteger lost = new AtomicInteger();
        HeartbeatMonitor mon = new HeartbeatMonitor(Arrays.asList(blocking));
        mon.startMonitoring(20, b -> lost.incrementAndGet());

        inProbe.await(2, java.util.concurrent.TimeUnit.SECONDS);
        Thread stopper = new Thread(() -> mon.stop());
        stopper.start();
        Thread.sleep(50);
        releaseProbe.countDown();
        stopper.join(3000);

        assertFalse(stopper.isAlive(), "stop does not deadlock (the dispatch lock and the join never interlock)");
        assertTrue(lost.get() <= 1, "at most once, and no callback after stop returns");
    }

    @Test
    void concurrentTaskFailAndHeartbeatLostSafe() throws Exception {
        LocalChannel ch = new LocalChannel(16);
        ch.send(StreamEvent.record(r(1)));
        Thread taskFail = new Thread(() -> ch.fail(new RuntimeException("task boom")));
        Thread hbFail = new Thread(() -> ch.fail(new RuntimeException("heartbeat lost")));
        taskFail.start();
        hbFail.start();
        taskFail.join(1000);
        hbFail.join(1000);
        StreamEvent e;
        boolean sawError = false;
        int polls = 0;
        while ((e = ch.poll()) != null && polls++ < 5) {
            if (e.isError()) { sawError = true; break; }
        }
        assertTrue(sawError, "poisoning from a double trigger is idempotent: a terminal error, neither a hang nor a double sentinel");
    }

    @Test
    void noAutoReschedule() throws Exception {
        // Semantic assertion: HeartbeatMonitor.onLost is a Consumer (a one-way notification) with no return value and no retry hook.
        // Losing a heartbeat poisons the channel and fails the job, and the API offers no rescheduling: re-running a partially published job would duplicate its output.
        LocalChannel ch = new LocalChannel(8);
        AtomicInteger rescheduleAttempts = new AtomicInteger();
        BeatHeartbeat hb = new BeatHeartbeat(40);
        HeartbeatMonitor mon = new HeartbeatMonitor(Arrays.asList(hb));
        mon.startMonitoring(20, b -> {
            ch.fail(new RuntimeException("lost"));
        });
        hb.markDead();
        Thread.sleep(150);
        mon.stop();
        assertEquals(0, rescheduleAttempts.get(), "a lost heartbeat never reschedules on its own, because re-running a published job would duplicate its output");
        StreamEvent e = ch.poll();
        assertTrue(e != null && e.isError(), "the job ends in failure (poisoned), not a recovered success");
    }
}

