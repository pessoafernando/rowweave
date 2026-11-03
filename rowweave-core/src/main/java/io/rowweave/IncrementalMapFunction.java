package io.rowweave;

/**
 * One-to-one changelog map whose output must preserve the operation, primary
 * key, and record-position fields. The incremental runtime verifies that
 * promise for every row.
 */
@FunctionalInterface
public interface IncrementalMapFunction extends MapFunction {
}
