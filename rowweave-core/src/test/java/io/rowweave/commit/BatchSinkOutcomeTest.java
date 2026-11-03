package io.rowweave.commit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchSinkOutcomeTest {

    @Test
    void freezesClosedStatesAndBoundedRedactedIdentity() {
        byte[] identity = new byte[32];
        identity[0] = 19;
        BatchSinkOutcome decided = BatchSinkOutcome.of(
                BatchSinkOutcome.State.COMMIT_DECIDED,
                7L, identity, "MANIFEST_INTENT_DURABLE");

        identity[0] = 0;
        assertEquals(BatchSinkOutcome.State.COMMIT_DECIDED, decided.state());
        assertEquals(7L, decided.generation());
        byte[] expected = new byte[32];
        expected[0] = 19;
        assertArrayEquals(expected, decided.identityDigest());
        assertEquals("MANIFEST_INTENT_DURABLE", decided.detailCode());

        BatchSinkOutcome missing = BatchSinkOutcome.notFound();
        assertEquals(BatchSinkOutcome.State.NOT_FOUND, missing.state());
        assertNull(missing.generation());
        assertNull(missing.identityDigest());
        assertNull(missing.detailCode());

        assertThrows(IllegalArgumentException.class, () -> BatchSinkOutcome.of(
                BatchSinkOutcome.State.COMMITTED, 1L, new byte[31], "COMMITTED"));
        assertThrows(IllegalArgumentException.class, () -> BatchSinkOutcome.of(
                BatchSinkOutcome.State.CORRUPT_OR_CONFLICT, null, null,
                "/private/domain/member.data"));
    }
}
