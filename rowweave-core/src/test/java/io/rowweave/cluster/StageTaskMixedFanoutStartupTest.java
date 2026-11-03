package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.sched.LocalShuffleRegistry;
import io.rowweave.sched.MultiProducerChannel;
import io.rowweave.sched.net.FrameCodec;
import io.rowweave.sched.net.ShuffleEndpoint;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageTaskMixedFanoutStartupTest {
    private static final DataSchema META = new DataSchema(new Field("k", DataType.IntegerType));

    private static final class EmptyInput implements Source {
        private static final long serialVersionUID = 1L;
        @Override public DataSchema getSchema() { return META; }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
    }

    @Test
    void socketTargetConnectsBeforeEarlierLocalTargetBecomesReady() throws Exception {
        String localKey = "late-local";
        LocalShuffleRegistry registry = new LocalShuffleRegistry();
        PlanGraph identity = new PlanGraph(Collections.singletonList(
                PlanNode.source(META, Collections.emptyList())));

        ExecutorService threads = Executors.newFixedThreadPool(2);
        try (ServerSocket remote = new ServerSocket(0)) {
            StageCommand command = StageCommand.sourceStageFanout(
                            META, META, 1 << 20, new EmptyInput(), identity,
                            Collections.emptyList(), new String[]{"k"}, false)
                    .withDownTargets(Arrays.asList(
                            ShuffleTarget.local(localKey),
                            ShuffleTarget.socket(new ShuffleEndpoint("127.0.0.1", remote.getLocalPort()))))
                    .withObjectReuse(true);
            StageTask task = new StageTask(command, StageTask.PortListener.NONE);
            task.setLocalShuffle(registry);

            Future<StageTask.StageResult> running = threads.submit(task::run);
            Future<Socket> accepted = threads.submit(remote::accept);
            boolean socketConnectedBeforeLocal;
            Socket peer = null;
            try {
                peer = accepted.get(1, TimeUnit.SECONDS);
                socketConnectedBeforeLocal = true;
            } catch (TimeoutException expectedBeforeFix) {
                socketConnectedBeforeLocal = false;
            } finally {
                registry.register(localKey, new MultiProducerChannel(16, 1));
                if (peer == null) {
                    peer = accepted.get(2, TimeUnit.SECONDS);
                }
                byte[] eos = new byte[FrameCodec.HEADER_BYTES];
                new DataInputStream(peer.getInputStream()).readFully(eos);
                assertEquals(FrameCodec.FRAME_EOS, eos[0]);
                peer.close();
                running.get(5, TimeUnit.SECONDS);
            }
            assertTrue(socketConnectedBeforeLocal,
                    "a mixed fanout must connect every socket first and only then wait for the local target to register");
        } finally {
            threads.shutdownNow();
        }
    }
}

