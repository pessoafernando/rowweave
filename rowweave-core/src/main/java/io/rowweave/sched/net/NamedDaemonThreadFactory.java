package io.rowweave.sched.net;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/** Named thread factory producing daemon threads, so a socket thread cannot keep the JVM alive. */
final class NamedDaemonThreadFactory implements ThreadFactory {

    private final String prefix;
    private final AtomicInteger seq = new AtomicInteger();

    NamedDaemonThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, prefix + "-" + seq.incrementAndGet());
        t.setDaemon(true);
        return t;
    }
}
