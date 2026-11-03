package io.rowweave.upgrade;

import io.rowweave.cluster.rpc.ControlMessageId;
import io.rowweave.cluster.rpc.ProtocolHelloV1;
import io.rowweave.cluster.rpc.ProtocolRevisionRange;
import io.rowweave.cluster.rpc.RpcMessage;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WriterRevisionGateTest {

    @Test
    void mixedClusterKeepsTheActiveLowestCommonWriterRevision() {
        StoreFormatHeader active = header(0);
        WriterRevisionDecision decision = WriterRevisionGate.evaluate(active,
                Arrays.asList(hello(0, 1), hello(0, 0), hello(0, 1)));

        assertEquals(0, decision.activeRevision());
        assertEquals(0, decision.minimumCommonRevision());
        assertEquals(0, decision.maximumCommonRevision());
        assertEquals(0, decision.requireCurrent());
        assertThrows(WriterRevisionRejectedException.class,
                () -> decision.requirePromotion(1));
    }

    @Test
    void promotionIsEligibleOnlyAfterEveryWriterSupportsTheTarget() {
        WriterRevisionDecision decision = WriterRevisionGate.evaluate(header(0),
                Arrays.asList(hello(0, 1), hello(0, 2), hello(0, 1)));

        assertEquals(1, decision.requirePromotion(1));
        assertThrows(WriterRevisionRejectedException.class,
                () -> decision.requirePromotion(2));
    }

    @Test
    void activeRevisionManifestAndEpochDriftFailClosed() {
        assertThrows(WriterRevisionRejectedException.class,
                () -> WriterRevisionGate.evaluate(header(1),
                        Collections.singletonList(hello(0, 0))));
        assertThrows(WriterRevisionRejectedException.class,
                () -> WriterRevisionGate.evaluate(header(0),
                        Collections.singletonList(hello('b', 1, 0, 1))));
        assertThrows(WriterRevisionRejectedException.class,
                () -> WriterRevisionGate.evaluate(header(0),
                        Collections.singletonList(hello('a', 2, 0, 1))));
    }

    private static StoreFormatHeader header(int revision) {
        return new StoreFormatHeader(1, "store-1", 1, revision, digest('a'));
    }

    private static ProtocolHelloV1 hello(int minimum, int maximum) {
        return hello('a', 1, minimum, maximum);
    }

    private static ProtocolHelloV1 hello(
            char digest, int epoch, int minimum, int maximum) {
        return new ProtocolHelloV1(digest(digest), epoch,
                new ProtocolRevisionRange(0, maximum),
                new ProtocolRevisionRange(minimum, maximum),
                Collections.singleton(ControlMessageId.wireId(
                        RpcMessage.Type.SUBMIT_RUN)),
                Collections.singleton(3), Collections.singleton(2),
                Collections.singleton(1), Collections.singleton(1),
                Collections.singleton("runtime.batch"),
                Collections.<Integer>emptySet(),
                Collections.<Integer, byte[]>emptyMap(), 1024, 4096L);
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
