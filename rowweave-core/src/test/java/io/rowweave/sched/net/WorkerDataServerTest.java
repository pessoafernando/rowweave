package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.sched.MultiProducerChannel;
import io.rowweave.sched.stream.StreamEvent;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WorkerDataServer is the Netty server on the single data port of a worker, whose pipeline is
 * FrameDecoder then DataDemuxHandler. A real socket sends frames in the new format of type,
 * an 8-byte targetId, length and payload, which shows the demux by targetId into the
 * registered MultiProducerChannel. One server serves many targets, which is the core of
 * single-port multiplexing.
 */
class WorkerDataServerTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));
    private static final RowSerializer SER = new RowSerializer(META);

    private static DataRow row(int v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    /** Write one frame in the new format over a bare socket. */
    private static void writeFrame(DataOutputStream o, byte type, UUID targetId, byte[] payload) throws Exception {
        o.writeByte(type);
        o.writeLong(targetId.getMostSignificantBits());
        o.writeLong(targetId.getLeastSignificantBits());
        o.writeInt(payload.length);
        o.write(payload);
        o.flush();
    }

    /** One port receives frames for several targets, demuxes each to its own channel by targetId, and turns an EOS into the per-target EOS. */
    @Test
    void singlePortDemuxesMultipleTargets() throws Exception {
        DataInboundRegistry reg = new DataInboundRegistry();
        MultiProducerChannel c1 = new MultiProducerChannel(64, 1, 0);
        MultiProducerChannel c2 = new MultiProducerChannel(64, 1, 0);
        reg.register(new UUID(0L,100L), "job1", c1, META);
        reg.register(new UUID(0L,200L), "job1", c2, META);

        try (WorkerDataServer server = new WorkerDataServer(reg, 5000)) {
            int port = server.port();
            assertTrue(port > 0, "the server binds a port");
            // One connection carries frames for two targets, which is multiplexing.
            try (Socket sock = new Socket("127.0.0.1", port)) {
                DataOutputStream o = new DataOutputStream(sock.getOutputStream());
                writeFrame(o, FrameCodec.FRAME_RECORD, new UUID(0L,100L), SER.serialize(row(11)));
                writeFrame(o, FrameCodec.FRAME_RECORD, new UUID(0L,200L), SER.serialize(row(22)));
                writeFrame(o, FrameCodec.FRAME_RECORD, new UUID(0L,100L), SER.serialize(row(33)));
                writeFrame(o, FrameCodec.FRAME_EOS, new UUID(0L,100L), new byte[0]);
                writeFrame(o, FrameCodec.FRAME_EOS, new UUID(0L,200L), new byte[0]);

                // c1 receives 11 and 33, then the EOS
                assertEquals(11, c1.poll().row().get(0));
                assertEquals(33, c1.poll().row().get(0));
                assertNull(c1.poll(), "target 100 EOS");
                // c2 receives 22, then the EOS
                assertEquals(22, c2.poll().row().get(0));
                assertNull(c2.poll(), "target 200 EOS");
            }
        }
    }

    /** A handler blocked in registry.await for an unknown target must not hold up server teardown. */
    @Test
    void closeCancelsHandlerWaitingForUnknownTarget() throws Exception {
        DataInboundRegistry reg = new DataInboundRegistry();
        WorkerDataServer server = new WorkerDataServer(reg, 60_000);
        try (Socket sock = new Socket("127.0.0.1", server.port())) {
            DataOutputStream out = new DataOutputStream(sock.getOutputStream());
            writeFrame(out, FrameCodec.FRAME_RECORD, new UUID(0L, 999L), SER.serialize(row(1)));
            Thread.sleep(50L);  // let DataDemuxHandler enter registry.await.
            assertTimeoutPreemptively(Duration.ofSeconds(3), server::close,
                    "closing cancels registry.await instead of waiting out the 60s timeout");
        } finally {
            server.close();
        }
    }

    /** Trailing corruption on a real socket must poison the target, rather than publishing a prefix that looks valid. */
    @Test
    void trailingRowPayloadBecomesTargetErrorInsteadOfRecord() throws Exception {
        DataInboundRegistry registry = new DataInboundRegistry();
        MultiProducerChannel channel = new MultiProducerChannel(8, 1, 0);
        UUID target = new UUID(0L, 301L);
        registry.register(target, "corrupt-row-job", channel, META);

        byte[] valid = SER.serialize(row(29));
        byte[] corrupted = Arrays.copyOf(valid, valid.length + 1);
        corrupted[valid.length] = 0x5A;

        try (WorkerDataServer server = new WorkerDataServer(registry, 5000);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            writeFrame(new DataOutputStream(socket.getOutputStream()),
                    FrameCodec.FRAME_RECORD, target, corrupted);

            StreamEvent event = assertTimeoutPreemptively(Duration.ofSeconds(3), channel::poll);
            assertTrue(event.isError(), "a corrupt RECORD is never published as a normal DataRow first: " + event);
            assertTrue(event.cause().getMessage().contains("trailing serialized row bytes"),
                    String.valueOf(event.cause()));
        }
    }
}
