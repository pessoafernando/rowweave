package io.rowweave;

import java.io.Serializable;

/**
 * RowWeave data source contract: a row sequence carrying a DataSchema.
 *
 * <p>**Serializable is a hard contract** — the distributed engine serializes the source description on
 * the driver and ships it to a worker to rebuild execution there, as Spark RDD and Flink SourceFunction
 * do. A production Source (OrmInput/DbInput) holds query descriptors and connection information, not data.
 *
 * <p>**Splitting is the caller's responsibility**, not the engine's, because the engine cannot know DB key
 * distribution or file offsets. The unit of parallelism is an element of the `Source[]` handed in by the
 * caller, each Source already being one split, and the engine parallelizes over that array (N threads on
 * one machine, N workers in a cluster). A Source is no longer asked to `createSplits` itself (removed).
 * One Source means one split, so parallelism 1.
 */
public interface Source extends Iterable<DataRow>, Serializable {

    DataSchema getSchema();
}
