package io.rowweave;

/**
 * optional capability for an {@link Source} that exposes a stable bounded-batch split descriptor.
 * Legacy Source implementations remain source compatible and are classified separately by the plan resolver.
 */
public interface BatchSource extends Source {

    /**
     * Return the frozen capability descriptor. Engine built-in compatibility constructors may
     * return null to request an explicit legacy downgrade; custom implementations must not.
     */
    BatchSourceSplitDescriptor batchSourceSplit();
}
