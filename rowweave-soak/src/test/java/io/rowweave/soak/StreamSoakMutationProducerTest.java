package io.rowweave.soak;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamSoakMutationProducerTest {
    @Test void reusesOneDatabaseSessionAcrossMutationBursts(
            @TempDir Path root) throws Exception {
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger mutated = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        StreamSoakMain.MutationSessionFactory sessions = () -> {
            opened.incrementAndGet();
            return new StreamSoakMain.MutationSession() {
                @Override public java.util.List<String> mutate() {
                    int value = mutated.incrementAndGet();
                    return Collections.singletonList("A|" + value);
                }

                @Override public void close() {
                    closed.incrementAndGet();
                }
            };
        };
        Path expected = root.resolve("expected.log");
        Files.createFile(expected);

        try (StreamSoakMain.MutationProducer producer =
                     new StreamSoakMain.MutationProducer(
                             sessions, expected, 2, 10L)) {
            producer.start();
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(5L);
            while (mutated.get() < 4 && System.nanoTime() < deadline) {
                Thread.sleep(5L);
            }
            assertTrue(mutated.get() >= 4,
                    "producer did not cross multiple bursts");
        }

        assertEquals(1, opened.get());
        assertEquals(1, closed.get());
        assertEquals(mutated.get(), Files.readAllLines(expected,
                StandardCharsets.UTF_8).size());
    }
}
