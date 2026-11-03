package io.rowweave.ops.join;

import io.rowweave.sched.stream.RowSink;
import io.rowweave.sched.stream.StreamEvent;

import java.util.concurrent.CancellationException;

/** A terminal event is emitted once; if the sink fails, the original terminal cause is kept. */
final class BinaryTerminalEmitter {

    private BinaryTerminalEmitter() {
    }

    static void emit(RowSink out, StreamEvent terminal) {
        try {
            out.emit(terminal);
        } catch (RuntimeException | Error sinkFailure) {
            Throwable original = terminal.isError() ? terminal.cause()
                    : new CancellationException("upstream cancelled binary operator");
            if (original != null && original != sinkFailure && !contains(sinkFailure.getSuppressed(), original)
                    && sinkFailure.getCause() != original) {
                sinkFailure.addSuppressed(original);
            }
            throw sinkFailure;
        }
    }

    private static boolean contains(Throwable[] values, Throwable target) {
        for (Throwable value : values) {
            if (value == target) {
                return true;
            }
        }
        return false;
    }
}
