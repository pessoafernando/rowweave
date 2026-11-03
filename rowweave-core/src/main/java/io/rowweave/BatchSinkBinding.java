package io.rowweave;

import java.io.Serializable;

/** Immutable association between one Sink and the Sink descriptor frozen into its Plan node. */
public final class BatchSinkBinding implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Sink output;
    private final BatchSinkDescriptor descriptor;

    public BatchSinkBinding(Sink output, BatchSinkDescriptor descriptor) {
        if (output == null) throw new IllegalArgumentException("batch sink output is required");
        if (descriptor == null) {
            throw new IllegalArgumentException("batch sink descriptor is required");
        }
        this.output = output;
        this.descriptor = descriptor;
    }

    public Sink output() { return output; }
    public BatchSinkDescriptor descriptor() { return descriptor; }
}
