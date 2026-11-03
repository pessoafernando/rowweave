package io.rowweave;

import java.io.Serializable;

/**
 * A 1-to-N row transform (N may be 0), emitted through a Collector.
 */
@FunctionalInterface
public interface FlatMapFunction extends Serializable {

    void flatMap(DataRow row, Collector collector);
}
