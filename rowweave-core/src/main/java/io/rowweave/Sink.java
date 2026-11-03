package io.rowweave;

/**
 * The RowWeave result sink: a row-by-row collection endpoint carrying a DataSchema, and the <b>downstream exit</b> of a DataSet (data leaves only through it, and the ExecutionEnvironment runtime reads no return value).
 *
 * <p>{@code extends AutoCloseable} and {@link #close} defaults to a no-op, so a Sink that holds resources
 * (FileSink's BufferedWriter, DbOutput's connection) <b>must override</b> it to flush and release. The engine's {@code commit()}
 * <b>guarantees close is called</b> once every row has been collected - without that, a FileSink buffer would never flush and a commit would leave an empty file.
 * An in-memory Sink (collecting into a List) needs no override; the default empty close suffices.
 *
 * <p>{@code extends Serializable} because a <b>Sink executes in the tail stage of a Worker</b> (data leaves only through the Sink and is never
 * sent back to the client), so it must serialize and travel to the Worker with the PlanGraph. An implementation holds a <b>description</b> (FileSink holds a path, DbOutput a connection string),
 * and the real resource (BufferedWriter/Connection) is marked {@code transient} and opened lazily after deserialization on the Worker. An in-memory Sink (CollectingSink) is local only.
 */
public interface Sink extends AutoCloseable, java.io.Serializable {

    DataSchema getSchema();

    void collect(DataRow row);

    /** Release resources + flush (empty by default: an in-memory Sink needs no override, while FileSink and DbOutput must provide one). */
    @Override
    default void close() {
        // no-op by default
    }

    /**
     * Discard on failure: called when the pipeline fails midway — release resources and **discard the
     * irreversible effects already written**, so no dirty data survives a failed attempt. The default is
     * {@link #close()} (an in-memory Sink has no irreversible effect; its getRows is simply not trusted).
     * A Sink holding external storage (FileSink cleaning this attempt's staging, DbOutput rolling back its
     * transaction) **must override** this, and a cleanup or rollback failure must be thrown explicitly
     * rather than swallowed, so residue is never left behind silently.
     */
    default void abort() {
        close();
    }

    /**
     * Whether this Sink explicitly supports coordinated commit across multiple Sinks. Default false, so that an irreversible close of an older Sink is never mistaken for a prepare.
     */
    default boolean supportsCoordinatedCommit() {
        return false;
    }

    /** Stop writing and solidify the private attempt, but never publish the final externally visible result. */
    default void prepareCommit() {
        throw new UnsupportedOperationException(
                "Sink does not support coordinated commit: " + getClass().getName());
    }

    /** Publish the prepared version, keeping the state that {@link #rollbackCommit()} needs. */
    default void commitPrepared() {
        throw new UnsupportedOperationException(
                "Sink does not support coordinated commit: " + getClass().getName());
    }

    /** Undo this prepare or publish; repeated calls must be safe. */
    default void rollbackCommit() {
        throw new UnsupportedOperationException(
                "Sink does not support coordinated commit: " + getClass().getName());
    }

    /** After the whole group publishes successfully, release backup and other compensating resources, and enter an irreversible terminal state. */
    default void finalizeCommit() {
        throw new UnsupportedOperationException(
                "Sink does not support coordinated commit: " + getClass().getName());
    }

    /**
     * Whether this Sink can write at the <b>Worker tail</b> (FileSink/DbOutput=true; they hold external storage and
     * each Worker shard writes independently). <b>An in-memory collecting Sink (CollectingSink)= false</b> - it collects into client
     * memory, which does not hold on a cluster (the data is on the Worker and the client receives nothing), so it is local only. A real cluster submission checks this capability before the RPC, and the Master
     * checks the original submission once more; false fails fast, and there is no client-collect fallback path.
     */
    default boolean isWorkerWritable() {
        return false;
    }

    /**
     * The external destination identity this Sink owns exclusively; within one PlanGraph a non-null identity may appear only once.
     * The identity must stay stable across a serialization round trip, and computing it must produce no external side effect. With no exclusive destination (memory, stdout, discard) it returns null.
     */
    default String exclusiveDestinationId() {
        return null;
    }
}
