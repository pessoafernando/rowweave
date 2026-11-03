package io.rowweave;

import java.io.Serializable;

/**
 * Immutable association between one application-provided {@link Source} and the split descriptor
 * frozen into a bounded-batch plan.
 */
public final class BatchSourceSplitBinding implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Source input;
    private final BatchSourceSplitDescriptor descriptor;

    public BatchSourceSplitBinding(Source input, BatchSourceSplitDescriptor descriptor) {
        if (input == null) {
            throw new IllegalArgumentException("batch source binding input is required");
        }
        if (descriptor == null) {
            throw new IllegalArgumentException("batch source binding descriptor is required");
        }
        this.input = input;
        this.descriptor = descriptor;
    }

    public Source input() {
        return input;
    }

    public BatchSourceSplitDescriptor descriptor() {
        return descriptor;
    }
}
