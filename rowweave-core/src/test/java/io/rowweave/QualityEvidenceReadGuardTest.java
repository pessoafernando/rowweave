package io.rowweave;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** finite read guards interrupt blocked channels. */
class QualityEvidenceReadGuardTest {

    @Test
    void asynchronousCancellationClosesTheRegisteredBlockingChannel()
            throws Exception {
        QualityEvidenceReadGuard guard = QualityEvidenceReadGuard.until(
                System.nanoTime() + TimeUnit.SECONDS.toNanos(30L), () -> false);
        Pipe pipe = Pipe.open();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        try {
            guard.register(pipe.source());
            Future<?> blocked = executor.submit(() -> {
                started.countDown();
                try {
                    pipe.source().read(ByteBuffer.allocate(1));
                } catch (Exception closed) {
                    return;
                }
                throw new AssertionError("blocking read unexpectedly completed");
            });
            assertTrue(started.await(5L, TimeUnit.SECONDS));

            guard.cancel();

            blocked.get(5L, TimeUnit.SECONDS);
            QualityEvidenceReadGuard.Terminated termination = assertThrows(
                    QualityEvidenceReadGuard.Terminated.class, guard::check);
            assertEquals(QualityEvidenceReadGuard.Termination.CANCELLED,
                    termination.termination());
        } finally {
            guard.unregister(pipe.source());
            pipe.source().close();
            pipe.sink().close();
            executor.shutdownNow();
        }
    }

    @Test
    void readerExposesNoPublicUnguardedOverload() {
        java.util.List<java.lang.reflect.Method> publicReads = Arrays.stream(
                        QualityPreparedEvidenceReader.class
                                .getDeclaredMethods())
                .filter(method -> method.getName().equals("read"))
                .filter(method -> java.lang.reflect.Modifier.isPublic(
                        method.getModifiers()))
                .collect(java.util.stream.Collectors.toList());
        assertEquals(2, publicReads.size());
        for (java.lang.reflect.Method read : publicReads) {
            assertEquals(3, read.getParameterCount());
            assertEquals(io.rowweave.plan.PlanGraph.class,
                    read.getParameterTypes()[0]);
            assertTrue(read.getParameterTypes()[1]
                            == io.rowweave.run.ManifestCommitJournalSnapshot.class
                    || read.getParameterTypes()[1]
                            == io.rowweave.run.QualityGateCommitJournalSnapshot.class);
            assertEquals(QualityEvidenceReadGuard.class,
                    read.getParameterTypes()[2]);
        }
    }
}
