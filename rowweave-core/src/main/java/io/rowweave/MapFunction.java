package io.rowweave;

import java.io.Serializable;

/**
 * A 1-to-1 row transform.
 */
@FunctionalInterface
public interface MapFunction extends Serializable {

    DataRow map(DataRow row);
}
