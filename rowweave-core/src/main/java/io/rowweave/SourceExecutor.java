package io.rowweave;

/**
 * Separation of Source (description) from SourceExecutor (execution): SourceExecutor is the data source's
 * **executing reader**, instantiated at execution time inside a worker or stage to do the real
 * open/read/close, while {@link Source} is **pure description** (metadata, connection information, split
 * descriptor) that can be serialized and dispatched.
 *
 * <p>Lifecycle: {@link #open()} → repeated {@link #hasNext()}/{@link #next()} → {@link #close()}
 * (idempotent, the backstop for resource release).
 *
 * <p><b>Contract</b>: every logical new row produced by {@link #next()} **must create a new DataRow**, and
 * an already emitted DataRow object is never reused or overwritten. This does not conflict with passing
 * references directly, since what is passed is a finished row that never changes again; what is forbidden
 * is reusing one object and overwriting it as you emit, because under pure Push with direct references a
 * reference held downstream would be silently rewritten by the next {@code next()} call. Hence there is no
 * {@code next(DataRow reuse)} signature.
 *
 * @param <I> the associated Source description type
 */
public abstract class SourceExecutor<I extends Source> implements AutoCloseable {

    protected final I input;
    protected final DataSchema dataSchema;

    protected SourceExecutor(I input, DataSchema dataSchema) {
        this.input = input;
        this.dataSchema = dataSchema;
    }

    /** Open the data source (make a connection, open a file, position a shard). Lazy: the actual read happens in next. */
    public abstract void open();

    /** Whether another row remains. */
    public abstract boolean hasNext();

    /** Read the next row, <b>producing a new DataRow every time</b> (an already emitted object is never reused). Behaviour is undefined once no rows remain (call hasNext first). */
    public abstract DataRow next();

    /** Release resources (connections, file handles); <b>idempotent</b>, and must also be called when a read throws or is cancelled (no resource leak). */
    @Override
    public abstract void close();
}
