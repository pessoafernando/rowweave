package io.rowweave.sched.net;

import io.netty.channel.embedded.EmbeddedChannel;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.sched.MultiProducerChannel;
import io.rowweave.sched.stream.StreamEvent;
import java.util.UUID;
import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DataDemuxHandler is the frame dispatcher on the single data port of a worker. It looks up
 * the frame's targetId in a {@link DataInboundRegistry} to find the downstream stage's
 * {@link MultiProducerChannel} and delivers with a non-blocking offer, so the EventLoop never
 * blocks. An EOS becomes producerDone rather than closing the shared connection, and an
 * unregistered target waits. Driven through EmbeddedChannel, with no real socket.
 */
class DataDemuxHandlerTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));
    private static final RowSerializer SER = new RowSerializer(META);

    private static DataRow row(int v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    private static Frame recordFrame(UUID targetId, int v) {
        return new Frame(FrameCodec.FRAME_RECORD, targetId, SER.serialize(row(v)));
    }

    /** Frames route to the channel registered under their targetId. */
    @Test
    void routesFramesByTargetId() throws Exception {
        DataInboundRegistry reg = new DataInboundRegistry();
        MultiProducerChannel c1 = new MultiProducerChannel(64, 1, 0);
        MultiProducerChannel c2 = new MultiProducerChannel(64, 1, 0);
        reg.register(new UUID(0L,100L), "job1", c1, META);
        reg.register(new UUID(0L,200L), "job1", c2, META);

        EmbeddedChannel ch = new EmbeddedChannel(new DataDemuxHandler(reg, 1000));
        ch.writeInbound(recordFrame(new UUID(0L,100L), 11));
        ch.writeInbound(recordFrame(new UUID(0L,200L), 22));
        ch.writeInbound(recordFrame(new UUID(0L,100L), 33));

        assertEquals(11, c1.poll().row().get(0), "targetId 100 → c1");
        assertEquals(33, c1.poll().row().get(0), "the second frame for targetId 100 arrives on c1");
        assertEquals(22, c2.poll().row().get(0), "targetId 200 → c2");
        ch.finishAndReleaseAll();
    }

    /** An EOS frame calls producerDone on that target's channel, which emits an EOS for a single producer, and leaves other targets alone. */
    @Test
    void eosTriggersProducerDonePerTarget() throws Exception {
        DataInboundRegistry reg = new DataInboundRegistry();
        MultiProducerChannel c1 = new MultiProducerChannel(64, 1, 0);
        MultiProducerChannel c2 = new MultiProducerChannel(64, 1, 0);
        reg.register(new UUID(0L,100L), "job1", c1, META);
        reg.register(new UUID(0L,200L), "job1", c2, META);

        EmbeddedChannel ch = new EmbeddedChannel(new DataDemuxHandler(reg, 1000));
        ch.writeInbound(recordFrame(new UUID(0L,100L), 11));
        ch.writeInbound(new Frame(FrameCodec.FRAME_EOS, new UUID(0L,100L), new byte[0]));

        assertEquals(11, c1.poll().row().get(0));
        assertNull(c1.poll(), "EOS for target 100 puts EOS on c1 and poll returns null");
        // c2 has not seen EOS and stays live, unaffected by the EOS for target 100
        ch.writeInbound(recordFrame(new UUID(0L,200L), 22));
        assertEquals(22, c2.poll().row().get(0), "target 200 is unaffected by the EOS of target 100");
        ch.finishAndReleaseAll();
    }

    /**
     * One target fed by N=2 upstream instances over two connections, so two DemuxHandler
     * instances, each sends its own EOS. The shared channel's producerCount latch must count
     * correctly: the first EOS emits no terminal state, and only the second emits EOS. This
     * shows EOS is independent of connections, since the per-target latch counts upstream
     * instances rather than connections.
     */
    @Test
    void perTargetEosLatchAcrossConnections() throws Exception {
        DataInboundRegistry reg = new DataInboundRegistry();
        MultiProducerChannel c = new MultiProducerChannel(64, 2, 0);  // this target has two upstream instances
        reg.register(new UUID(0L,100L), "job1", c, META);

        // Two connections, so two handlers, for the same targetId 100.
        EmbeddedChannel conn1 = new EmbeddedChannel(new DataDemuxHandler(reg, 1000));
        EmbeddedChannel conn2 = new EmbeddedChannel(new DataDemuxHandler(reg, 1000));
        conn1.writeInbound(recordFrame(new UUID(0L,100L), 11));
        conn2.writeInbound(recordFrame(new UUID(0L,100L), 22));
        conn1.writeInbound(new Frame(FrameCodec.FRAME_EOS, new UUID(0L,100L), new byte[0]));  // the first EOS

        // Both rows have arrived but only one EOS has, so no terminal state is emitted yet: after draining the data, poll must not see an EOS.
        assertEquals(11, c.poll().row().get(0));
        assertEquals(22, c.poll().row().get(0));
        // The queue is empty now and not everything is done: the terminal state arrives only with the second EOS.
        conn2.writeInbound(new Frame(FrameCodec.FRAME_EOS, new UUID(0L,100L), new byte[0]));  // the second EOS
        assertNull(c.poll(), "the final EOS is emitted only after both upstream instances each sent EOS (the per-target latch counted correctly)");
        conn1.finishAndReleaseAll();
        conn2.finishAndReleaseAll();
    }

    /** An ERROR frame fails, that is poisons, the channel of that target. */
    @Test
    void errorFrameFailsTargetChannel() {
        DataInboundRegistry reg = new DataInboundRegistry();
        MultiProducerChannel c1 = new MultiProducerChannel(64, 1, 0);
        reg.register(new UUID(0L,100L), "job1", c1, META);

        EmbeddedChannel ch = new EmbeddedChannel(new DataDemuxHandler(reg, 1000));
        ch.writeInbound(new Frame(FrameCodec.FRAME_ERROR, new UUID(0L,100L), "boom".getBytes(java.nio.charset.StandardCharsets.UTF_8)));

        StreamEvent e = assertDoesNotThrow(() -> c1.poll());
        assertTrue(e.isError(), "an ERROR frame poisons the target channel and poll sees the error sentinel");
        ch.finishAndReleaseAll();
    }

    /** A shared physical connection can outlive several stages and jobs, so a finished target must not linger forever in the demux table. */
    @Test
    void completedMuxTargetIsReleasedFromConnectionMap() {
        UUID target = new UUID(90L, 700L);
        DataInboundRegistry reg = new DataInboundRegistry();
        MultiProducerChannel inbound = new MultiProducerChannel(64, 1, 256);
        reg.register(target, "job", inbound, META, 1, 256);
        DataDemuxHandler handler = new DataDemuxHandler(reg, 1000);
        EmbeddedChannel ch = new EmbeddedChannel(handler);

        ch.writeInbound(new Frame(FrameCodec.FRAME_OPEN, target, new byte[0]));
        ch.writeInbound(new Frame(FrameCodec.FRAME_EOS, target, new byte[0]));
        ch.runPendingTasks();

        assertEquals(0, handler.activeTargetCount(),
                "once the last logical producer sends EOS the connection-target reference is dropped");
        ch.finishAndReleaseAll();
    }

    @Test
    void sequentialProducerCanReopenTargetBeforeAsynchronousRemovalRuns() {
        UUID target = new UUID(90L, 701L);
        DataInboundRegistry reg = new DataInboundRegistry();
        MultiProducerChannel inbound = new MultiProducerChannel(64, 2, 256);
        reg.register(target, "job", inbound, META, 2, 256);
        Runnable[] scheduledDrain = new Runnable[1];
        DataDemuxHandler handler = new DataDemuxHandler(reg, 1000, task -> {
            assertNull(scheduledDrain[0], "frames for the same target are coalesced into one ordered drain");
            scheduledDrain[0] = task;
        });
        EmbeddedChannel ch = new EmbeddedChannel(handler);

        // Frames of two producer generations enter the same target ingress and are then drained by hand, which reliably covers the window
        // where the first generation has finished, its event-loop removal is not yet executed, and the OPEN of the second generation is already pending.
        ch.writeInbound(new Frame(FrameCodec.FRAME_OPEN, target, new byte[0]));
        ch.writeInbound(recordFrame(target, 11));
        ch.writeInbound(new Frame(FrameCodec.FRAME_EOS, target, new byte[0]));
        // finishTarget has queued the removal on the event loop but runPendingTasks has not been called explicitly, so the next producer on the same connection can arrive first.
        ch.writeInbound(new Frame(FrameCodec.FRAME_OPEN, target, new byte[0]));
        ch.writeInbound(recordFrame(target, 22));
        ch.writeInbound(new Frame(FrameCodec.FRAME_EOS, target, new byte[0]));
        assertNotNull(scheduledDrain[0]);
        scheduledDrain[0].run();

        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            assertEquals(11, inbound.poll().row().get(0));
            assertEquals(22, inbound.poll().row().get(0),
                    "while the terminal ingress has not been removed it must not swallow the next generation of OPEN, RECORD or EOS");
            assertNull(inbound.poll(), "both producers EOS count toward the close latch");
        });
        ch.runPendingTasks();
        assertEquals(0, handler.activeTargetCount(), "once the shared channel is globally terminal the long-lived target references are cleaned up");
        ch.finishAndReleaseAll();
    }
}
