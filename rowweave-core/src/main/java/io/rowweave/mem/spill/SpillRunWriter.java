package io.rowweave.mem.spill;

import java.util.function.Consumer;

/** Exception-safe template for create, append and finish: a run left unfinished is always aborted. */
public final class SpillRunWriter {

    private SpillRunWriter() {
    }

    public static SpillRun write(SpillStore store, Consumer<SpillRun> body) {
        SpillRun run = store.createRun();
        boolean finished = false;
        Throwable primary = null;
        try {
            body.accept(run);
            run.finishWriting();
            finished = true;
            return run;
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            if (!finished) {
                try {
                    run.abort();
                } catch (RuntimeException | Error cleanup) {
                    if (primary != null) {
                        primary.addSuppressed(cleanup);
                    } else {
                        throw cleanup;
                    }
                }
            }
        }
    }
}
