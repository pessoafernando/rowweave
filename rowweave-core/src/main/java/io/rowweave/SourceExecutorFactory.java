package io.rowweave;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * The dispatch factory from a {@link Source} description to a {@link SourceExecutor} reader.
 * At execution time (inside a Worker stage) it builds the executor matching the Source type.
 *
 * <p>Dispatch rule: a built-in Source (currently {@link FileSource}) uses its dedicated executor directly; any other type is looked up in the registry first, and when
 * unregistered it falls back to the {@link IterableInputExecutor} bridge (an unregistered type still implements {@code Iterable<DataRow>}, so it runs with no change at all).
 * A custom production DB or file Source registers its own executor constructor through {@link #register}.
 */
public final class SourceExecutorFactory {

    private static final Map<Class<? extends Source>, Function<Source, SourceExecutor<?>>> REGISTRY =
            new ConcurrentHashMap<>();

    private SourceExecutorFactory() {
    }

    /** Register the dedicated executor constructor for a Source type (for production DB and file Sources). */
    public static <I extends Source> void register(Class<I> inputType, Function<I, SourceExecutor<I>> ctor) {
        @SuppressWarnings("unchecked")
        Function<Source, SourceExecutor<?>> erased = (Function<Source, SourceExecutor<?>>) (Function<?, ?>) ctor;
        REGISTRY.put(inputType, erased);
    }

    /** Build a Source's executor: built-in dedicated, then registered dedicated, then the IterableInputExecutor compatibility bridge. */
    public static SourceExecutor<?> create(Source input) {
        if (input == null) {
            throw new IllegalArgumentException("input must not be null");
        }
        if (input instanceof AssignedSourceBatch) {
            return new AssignedSourceInputBatchExecutor((AssignedSourceBatch) input);
        }
        if (input instanceof ConcatSource) {
            return new ConcatInputExecutor((ConcatSource) input);
        }
        if (input instanceof FileSource) {
            return new FileInputExecutor((FileSource) input);
        }
        if (input instanceof ManifestFileSource) {
            return new ManifestFileInputExecutor((ManifestFileSource) input);
        }
        if (input instanceof BatchExecutableSource) {
            SourceExecutor<?> executor =
                    ((BatchExecutableSource) input).createSourceExecutor();
            if (executor == null) {
                throw new IllegalArgumentException(
                        "BatchExecutableSource returned a null executor");
            }
            if (executor.input != input) {
                throw new IllegalArgumentException(
                        "BatchExecutableSource executor must bind the same Source instance");
            }
            return executor;
        }
        Function<Source, SourceExecutor<?>> ctor = REGISTRY.get(input.getClass());
        if (ctor != null) {
            return ctor.apply(input);
        }
        return new IterableInputExecutor(input); // the fallback bridge (no dedicated executor registered)
    }
}
