package io.rowweave.sched.net;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.util.Queue;
import java.util.concurrent.TimeUnit;

/** Ordered, time-bounded teardown of the channels, pipelines and executors of the data plane. */
final class NettyTransportShutdown {

    private static final long TIMEOUT_MS = Long.getLong("rowweave.netty.shutdownTimeoutMs", 30_000L);
    private static final Runnable FENCE = () -> { };

    private NettyTransportShutdown() {
    }

    static long timeoutMs() {
        return TIMEOUT_MS;
    }

    /**
     * Installed as the first user handler of a child pipeline, without an external executor.
     * When destroyDown removes it last, the teardown continuations of the handlers installed
     * after it have already returned to the I/O EventLoop and completed.
     */
    static Future<Void> installTeardownBarrier(Channel channel) {
        Promise<Void> removed = channel.eventLoop().newPromise();
        channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void handlerRemoved(ChannelHandlerContext ctx) {
                removed.trySuccess(null);
            }
        });
        return removed;
    }

    /** The server and its accepted children; null means every resource closed inside the limit. */
    static RuntimeException shutdownServer(String owner,
                                           Channel server,
                                           ChannelGroup children,
                                           Queue<Future<Void>> childBarriers,
                                           EventLoopGroup bossGroup,
                                           EventLoopGroup workerGroup,
                                           EventExecutorGroup handlerGroup) {
        Failures failures = new Failures(owner);
        if (!calledOutside(bossGroup, workerGroup, handlerGroup)) {
            return new IllegalStateException(owner + " shutdown invoked from its own Netty executor");
        }

        // A stayClosed ChannelGroup closes any child that joins it after this point.
        close(server, "server channel", failures);
        close(children, "child channels", failures);

        // Terminate the boss once the server is closed, then fence each worker EventExecutor so that a
        // child already accepted but still registering or initializing cannot slip through.
        shutdown(bossGroup, "boss group", failures);
        fenceEvery(workerGroup, "worker registration fence", failures);
        close(children, "late child channels", failures);

        // After boss termination and the worker fences the set stops growing, so what remains is to
        // wait for the destroyDown barrier at the end of each pipeline.
        for (Future<Void> barrier : childBarriers) {
            await(barrier, "child pipeline teardown barrier", failures);
        }

        shutdown(workerGroup, "worker group", failures);
        shutdown(handlerGroup, "handler group", failures);
        return failures.result();
    }

    /** A client channel whose handlers run without an external executor. */
    static RuntimeException shutdownClient(String owner, Channel channel, EventLoopGroup group) {
        Failures failures = new Failures(owner);
        if (!calledOutside(group)) {
            return new IllegalStateException(owner + " shutdown invoked from its own Netty executor");
        }
        close(channel, "client channel", failures);
        shutdown(group, "client group", failures);
        return failures.result();
    }

    /**
     * bind or connect failed during construction, so no child was ever registered: only reclaim
     * the executors that were created.
     */
    static RuntimeException shutdownGroups(String owner, EventExecutorGroup... groups) {
        Failures failures = new Failures(owner);
        if (!calledOutside(groups)) {
            return new IllegalStateException(owner + " shutdown invoked from its own Netty executor");
        }
        for (EventExecutorGroup group : groups) {
            shutdown(group, "constructor group", failures);
        }
        return failures.result();
    }

    private static boolean calledOutside(EventExecutorGroup... groups) {
        for (EventExecutorGroup group : groups) {
            if (group == null) {
                continue;
            }
            for (EventExecutor executor : group) {
                if (executor.inEventLoop()) {
                    return false;
                }
            }
        }
        return true;
    }

    private static void fenceEvery(EventExecutorGroup group, String label, Failures failures) {
        if (group == null) {
            return;
        }
        for (EventExecutor executor : group) {
            try {
                await(executor.submit(FENCE), label, failures);
            } catch (RuntimeException e) {
                failures.add(label, e);
            }
        }
    }

    private static void close(Channel channel, String label, Failures failures) {
        if (channel == null) {
            return;
        }
        try {
            // The peer's channelInactive may already have started the client group's asynchronous
            // self-reclaim, and calling close on a closed channel through a terminated pipeline raises
            // RejectedExecutionException. Here the close future is the idempotent way to wait.
            Future<?> closed = channel.closeFuture().isDone() ? channel.closeFuture() : channel.close();
            await(closed, label, failures);
        } catch (RuntimeException e) {
            failures.add(label, e);
        }
    }

    private static void close(ChannelGroup channels, String label, Failures failures) {
        if (channels == null) {
            return;
        }
        try {
            await(channels.close(), label, failures);
        } catch (RuntimeException e) {
            failures.add(label, e);
        }
    }

    private static void shutdown(EventExecutorGroup group, String label, Failures failures) {
        if (group == null) {
            return;
        }
        try {
            await(group.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS), label, failures);
        } catch (RuntimeException e) {
            failures.add(label, e);
        }
    }

    private static void await(Future<?> future, String label, Failures failures) {
        if (future == null) {
            return;
        }
        if (!future.awaitUninterruptibly(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            failures.add(label, new IllegalStateException("timed out after " + TIMEOUT_MS + " ms"));
            return;
        }
        if (!future.isSuccess()) {
            Throwable cause = future.cause();
            failures.add(label, cause == null ? new IllegalStateException("future failed without cause") : cause);
        }
    }

    private static final class Failures {
        private final String owner;
        private RuntimeException aggregate;

        private Failures(String owner) {
            this.owner = owner;
        }

        private void add(String resource, Throwable cause) {
            IllegalStateException wrapped = new IllegalStateException(
                    owner + " failed to close " + resource + ": " + cause.getMessage(), cause);
            if (aggregate == null) {
                aggregate = new IllegalStateException(owner + " Netty shutdown failed");
            }
            aggregate.addSuppressed(wrapped);
        }

        private RuntimeException result() {
            return aggregate;
        }
    }
}
