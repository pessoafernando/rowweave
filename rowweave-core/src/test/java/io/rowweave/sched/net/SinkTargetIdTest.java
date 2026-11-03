package io.rowweave.sched.net;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.MultiProducerChannel;
import io.rowweave.sched.stream.StreamEvent;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SocketChannelSink.withTargetId sets the multiplexing routing key on outgoing frames: the
 * upstream connects to the worker data port and sends frames carrying the downstream
 * targetId, which the server demuxes to the right stage. End to end: a sink with a targetId,
 * then WorkerDataServer, then the registered channel.
 */
class SinkTargetIdTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));

    private static DataRow row(int v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

/** The sink sets the targetId then sends, and the single-port demux on WorkerDataServer routes to the channel registered for that targetId. */
    @Test
    void sinkWithTargetIdRoutesThroughDataServer() throws Exception {
        DataInboundRegistry reg = new DataInboundRegistry();
        MultiProducerChannel c = new MultiProducerChannel(64, 1, 0);
        reg.register(new UUID(0L,777L), "job1", c, META);

        try (WorkerDataServer server = new WorkerDataServer(reg, 5000)) {
            SocketChannelSink sink = new SocketChannelSink(META, "127.0.0.1", server.port(), 64)
                    .withTargetId(new UUID(0L,777L));
            try {
                sink.send(StreamEvent.record(row(42)));
                sink.close();  // sends EOS
                StreamEvent first = c.poll();
                assertTrue(first.isRecord(), String.valueOf(first));
                assertEquals(42, first.row().get(0), "the frame for sink targetId 777 demultiplexes onto the channel of target 777");
                assertNull(c.poll(), "EOS arrives from the single producer");
            } finally {
                sink.close();
            }
        }
    }
}
