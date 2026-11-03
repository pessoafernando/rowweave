package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LeaderSubscriptionSlotTest {

    @Test
    void installTransfersOwnershipAndCloseIsIdempotent() {
        AtomicInteger closes = new AtomicInteger();
        LeaderSubscriptionSlot slot = new LeaderSubscriptionSlot();
        slot.install(closes::incrementAndGet);

        slot.close();
        slot.close();

        assertEquals(1, closes.get());
    }

    @Test
    void subscriptionArrivingAfterCloseIsClosedImmediately() {
        AtomicInteger closes = new AtomicInteger();
        LeaderSubscriptionSlot slot = new LeaderSubscriptionSlot();
        slot.close();

        slot.install(closes::incrementAndGet);

        assertEquals(1, closes.get());
    }

    @Test
    void duplicateInstallIsRejectedWithoutLeakingRejectedSubscription() {
        AtomicInteger firstCloses = new AtomicInteger();
        AtomicInteger rejectedCloses = new AtomicInteger();
        LeaderSubscriptionSlot slot = new LeaderSubscriptionSlot();
        slot.install(firstCloses::incrementAndGet);

        assertThrows(IllegalStateException.class,
                () -> slot.install(rejectedCloses::incrementAndGet));

        assertEquals(1, rejectedCloses.get());
        slot.close();
        assertEquals(1, firstCloses.get());
    }
}
