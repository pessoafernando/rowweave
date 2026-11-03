package io.rowweave;

import java.io.Serializable;

/**
 * A filter predicate: a row is kept when this returns true.
 */
@FunctionalInterface
public interface FilterFunction extends Serializable {

    boolean filter(DataRow row);
}
