package io.rowweave;

/**
 * The outlet an operator uses to emit rows downstream (used by 1-to-N operators).
 */
public interface Collector {

    void collect(DataRow row);
}
