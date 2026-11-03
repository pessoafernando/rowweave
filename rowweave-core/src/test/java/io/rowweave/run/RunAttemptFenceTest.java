package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.text.Normalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunAttemptFenceTest {

    @Test
    void validatesIdentityAndHasValueSemantics() {
        RunAttemptFence fence = new RunAttemptFence("run", "attempt-1", 7L);
        assertEquals("run", fence.runId());
        assertEquals("attempt-1", fence.attemptId());
        assertEquals(7L, fence.fenceToken());
        assertEquals(fence, new RunAttemptFence("run", "attempt-1", 7L));
        assertEquals(fence.hashCode(), new RunAttemptFence("run", "attempt-1", 7L).hashCode());
        assertNotEquals(fence, new RunAttemptFence("run", "attempt-1", 8L));

        assertThrows(IllegalArgumentException.class,
                () -> new RunAttemptFence("", "attempt", 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new RunAttemptFence("run", "bad\0attempt", 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new RunAttemptFence(Normalizer.normalize("é", Normalizer.Form.NFD), "attempt", 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new RunAttemptFence("run", "attempt", 0L));
        assertThrows(IllegalArgumentException.class,
                () -> new RunAttemptFence(repeat('x', 257), "attempt", 1L));
    }

    private static String repeat(char value, int count) {
        StringBuilder out = new StringBuilder(count);
        for (int i = 0; i < count; i++) out.append(value);
        return out.toString();
    }
}
