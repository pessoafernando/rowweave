package io.rowweave.stream.cluster;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** run placement and attempt fencing are shared HA state. */
class SharedStreamClusterRunAuthorityTest {

    @Test
    void replacementMasterAdvancesSharedTermAndFencesPreviousCoordinator() {
        StreamClusterRunStateStore store =
                new InMemoryStreamClusterRunStateStore();
        FileStreamClusterRunAuthority first =
                new FileStreamClusterRunAuthority(store);
        FileStreamClusterRunAuthority replacement =
                new FileStreamClusterRunAuthority(store);

        long firstTerm = store.nextCoordinatorTerm();
        long replacementTerm = store.nextCoordinatorTerm();
        assertTrue(replacementTerm > firstTerm);
        first.create(StreamClusterRunState.initial(
                firstTerm, "master-first", "run-shared", "attempt-1",
                1L, 1L, 1L, placement()));

        StreamClusterRunState takenOver = replacement.replaceCoordinator(
                "run-shared", firstTerm, "master-first",
                replacementTerm, "master-replacement");
        assertEquals(takenOver, first.current("run-shared"));

        StreamClusterProtocolException stale = assertThrows(
                StreamClusterProtocolException.class,
                () -> first.restartAfterWorkerLoss(
                        "run-shared", firstTerm, "master-first",
                        "worker-a", "inc-a", "attempt-2",
                        2L, 2L, 2L, placement()));
        assertEquals(StreamClusterProtocolException.Code.STALE_COORDINATOR,
                stale.code());
    }

    private static StreamWorkerPlacement placement() {
        return new StreamWorkerPlacement(Arrays.asList(
                new StreamWorkerPlacement.Assignment(
                        0, "source", 0, "worker-a", "inc-a"),
                new StreamWorkerPlacement.Assignment(
                        1, "sink", 0, "worker-b", "inc-b")));
    }
}
