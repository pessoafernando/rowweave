package io.rowweave;

/** Callback-scoped, quota-enforced main output for a window firing. */
public interface StreamWindowCollector {

    void collect(DataRow row);
}
