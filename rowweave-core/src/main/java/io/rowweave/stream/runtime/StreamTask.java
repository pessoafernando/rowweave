package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamEnvelope;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Single-thread mailbox consumer used by local Stream operator and Sink stages. */
public final class StreamTask {
    private final StreamChannelInput mailbox;
    private final Consumer<StreamEnvelope> handler;
    private final Thread thread;
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private volatile boolean completed;
    private volatile Throwable failure;

    public StreamTask(
            String taskName, StreamChannelInput mailbox,
            Consumer<StreamEnvelope> handler) {
        if (taskName == null || taskName.isEmpty()
                || mailbox == null || handler == null) {
            throw new IllegalArgumentException(
                    "task name, mailbox and handler are required");
        }
        this.mailbox = mailbox;
        this.handler = handler;
        this.thread = new Thread(this::runMailbox,
                "rowweave-stream-task-" + taskName);
    }

    public void start() { thread.start(); }

    public void cancel() {
        if (cancellationRequested.compareAndSet(false, true)) {
            mailbox.cancel();
            thread.interrupt();
        }
    }

    public void awaitCompletion(long timeoutMillis) throws InterruptedException {
        thread.join(timeoutMillis);
    }

    public boolean isCompleted() { return completed; }
    public Throwable failure() { return failure; }

    private void runMailbox() {
        try {
            while (true) {
                StreamEnvelope envelope = mailbox.receive();
                if (envelope == null) return;
                handler.accept(envelope);
            }
        } catch (StreamChannelCancelledException cancelled) {
            // Attempt cancellation is a normal task lifecycle terminal.
        } catch (StreamChannelFailedException channelFailure) {
            failure = channelFailure.getCause();
        } catch (InterruptedException interrupted) {
            if (!cancellationRequested.get()) {
                failure = interrupted;
                mailbox.fail(interrupted);
                Thread.currentThread().interrupt();
            }
        } catch (Throwable thrown) {
            failure = thrown;
            mailbox.fail(thrown);
        } finally {
            completed = true;
        }
    }
}
