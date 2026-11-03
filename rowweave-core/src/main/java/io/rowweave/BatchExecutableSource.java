package io.rowweave;

import java.util.Iterator;

/**
 * Versioned bounded-source execution extension that does not depend on the process-global
 * {@link SourceExecutorFactory} registry.
 */
public interface BatchExecutableSource extends BatchSource {

    /** Construct an unopened executor bound to this exact Source instance. */
    SourceExecutor<?> createSourceExecutor();

    /** Direct iteration is deliberately unavailable; execution must use the managed executor. */
    @Override
    default Iterator<DataRow> iterator() {
        throw new UnsupportedOperationException(
                "BatchExecutableSource must be opened through SourceExecutorFactory");
    }
}
