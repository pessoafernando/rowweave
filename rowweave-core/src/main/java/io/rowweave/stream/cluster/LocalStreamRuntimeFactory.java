package io.rowweave.stream.cluster;

import io.rowweave.StreamJobClient;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamCommandHandle;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamOfflineRescaleRequest;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.PublicStreamSinkAdapter;
import io.rowweave.stream.spi.StreamFunctionProvider;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.runtime.PublicTransactionalStreamSinkAdapter;
import io.rowweave.stream.runtime.PublicStreamSourceAdapter;

import java.io.Serializable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Internal adapter behind the root public Local Stream factory. */
public final class LocalStreamRuntimeFactory {
    private LocalStreamRuntimeFactory() { }

    public static StreamJobClient create(Path root,
            Collection<? extends StreamSourceProvider> sourceProviders,
            Collection<? extends StreamSinkProvider> sinkProviders,
            Collection<? extends StreamFunctionProvider> functionProviders) {
        return create(root, sourceProviders, sinkProviders,
                java.util.Collections.emptyList(), functionProviders);
    }

    public static StreamJobClient create(Path root,
            Collection<? extends StreamSourceProvider> sourceProviders,
            Collection<? extends StreamSinkProvider> sinkProviders,
            Collection<? extends StreamTransactionalSinkProvider>
                    transactionalSinkProviders,
            Collection<? extends StreamFunctionProvider> functionProviders) {
        if (root == null || sourceProviders == null || sinkProviders == null
                || transactionalSinkProviders == null
                || functionProviders == null) {
            throw new IllegalArgumentException(
                    "Local Stream root and providers are required");
        }
        Ownership ownership = Ownership.acquire(root);
        boolean transferred = false;
        try {
        Map<String, DurableStreamSourceProvider> sources = new LinkedHashMap<>();
        for (StreamSourceProvider provider : sourceProviders) {
            DurableStreamSourceProvider adapter =
                    PublicStreamSourceAdapter.adapt(provider);
            if (sources.put(adapter.providerKey(), adapter) != null) {
                throw new IllegalArgumentException(
                        "Local Stream source provider key is duplicated");
            }
        }
        Map<String, EagerStreamSinkProvider> sinks = new LinkedHashMap<>();
        for (StreamSinkProvider provider : sinkProviders) {
            EagerStreamSinkProvider adapter =
                    PublicStreamSinkAdapter.adapt(provider);
            if (sinks.put(adapter.providerKey(), adapter) != null) {
                throw new IllegalArgumentException(
                        "Local Stream Sink provider key is duplicated");
            }
        }
        Map<String, io.rowweave.stream.runtime.TransactionalStreamSinkProvider>
                transactionalSinks =
                new LinkedHashMap<>();
        for (StreamTransactionalSinkProvider provider
                : transactionalSinkProviders) {
            io.rowweave.stream.runtime.TransactionalStreamSinkProvider adapter =
                    PublicTransactionalStreamSinkAdapter.adapt(provider);
            if (sinks.containsKey(adapter.providerKey())
                    || transactionalSinks.put(
                    adapter.providerKey(), adapter) != null) {
                throw new IllegalArgumentException(
                        "Local Stream Sink provider key is duplicated");
            }
        }
        Map<String, StreamFunctionProvider> functions = new LinkedHashMap<>();
        for (StreamFunctionProvider provider : functionProviders) {
            if (provider == null || provider.deploymentSetId() == null
                    || provider.deploymentSetId().isEmpty()
                    || functions.put(provider.deploymentSetId(), provider)
                    != null) {
                throw new IllegalArgumentException(
                        "Local Stream function deployment is invalid or duplicated");
            }
        }
        List<String> deployments = new ArrayList<>(functions.keySet());
        StreamFunctionResolver resolver = new StreamFunctionResolver() {
            @Override public Serializable resolve(
                    FunctionDeploymentReference reference) {
                return LocalStreamRuntimeFactory.resolve(functions, reference);
            }

            @Override public boolean supportsAsyncReplay(
                    FunctionDeploymentReference reference,
                    io.rowweave.stream.api.StreamAsyncReplaySpec replay) {
                StreamFunctionProvider provider = functions.get(
                        reference.deploymentSetId());
                return provider != null
                        && provider.supportsAsyncReplay(reference, replay);
            }
        };
        InProcessStreamCluster cluster = new InProcessStreamCluster(
                ownership.root, sources, sinks, transactionalSinks,
                resolver, deployments);
        transferred = true;
        return new OwnedClient(cluster, ownership);
        } finally {
            if (!transferred) ownership.close();
        }
    }

    private static Serializable resolve(
            Map<String, StreamFunctionProvider> providers,
            FunctionDeploymentReference reference) {
        StreamFunctionProvider provider = providers.get(
                reference.deploymentSetId());
        if (provider == null) {
            throw new IllegalArgumentException(
                    "Local Stream function deployment is unavailable");
        }
        Serializable function = provider.resolve(reference);
        if (function == null) {
            throw new IllegalArgumentException(
                    "Local Stream function provider returned null");
        }
        return function;
    }

    private static final class Ownership implements AutoCloseable {
        private final Path root;
        private final FileChannel channel;
        private final FileLock lock;
        private Ownership(Path root, FileChannel channel, FileLock lock) {
            this.root = root; this.channel = channel; this.lock = lock;
        }
        static Ownership acquire(Path requested) {
            FileChannel channel = null;
            try {
                Files.createDirectories(requested);
                Path root = requested.toRealPath();
                channel = FileChannel.open(root.resolve("local-runtime.lock"),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock lock;
                try {
                    lock = channel.tryLock();
                } catch (OverlappingFileLockException overlapping) {
                    lock = null;
                }
                if (lock == null) {
                    channel.close();
                    throw new IllegalStateException(
                            "Local Stream state root is already owned");
                }
                return new Ownership(root, channel, lock);
            } catch (IllegalStateException failure) {
                throw failure;
            } catch (IOException failure) {
                if (channel != null) try { channel.close(); }
                catch (IOException close) { failure.addSuppressed(close); }
                throw new IllegalStateException(
                        "Local Stream state root is unavailable", failure);
            }
        }
        @Override public void close() {
            IOException failure = null;
            try { lock.release(); } catch (IOException value) { failure = value; }
            try { channel.close(); } catch (IOException value) {
                if (failure == null) failure = value; else failure.addSuppressed(value);
            }
            if (failure != null) throw new IllegalStateException(
                    "Local Stream state root ownership release failed", failure);
        }
    }

    private static final class OwnedClient implements StreamJobClient {
        private final InProcessStreamCluster delegate;
        private final Ownership ownership;
        private final AtomicBoolean closed = new AtomicBoolean();
        private OwnedClient(InProcessStreamCluster delegate,
                Ownership ownership) {
            this.delegate = delegate; this.ownership = ownership;
        }
        @Override public StreamJobHandle submit(
                StreamSubmissionId id, PlanArtifact artifact) {
            return delegate.submit(id, artifact);
        }
        @Override public StreamJobHandle attachSubmission(StreamSubmissionId id) {
            return delegate.attachSubmission(id);
        }
        @Override public StreamJobHandle attach(StreamJobId id) {
            return delegate.attach(id);
        }
        @Override public StreamCommandHandle attachCancel(
                StreamJobId job, StreamCommandId command) {
            return delegate.attachCancel(job, command);
        }
        @Override public StreamCheckpointHandle attachCheckpoint(
                StreamJobId job, StreamCommandId command) {
            return delegate.attachCheckpoint(job, command);
        }
        @Override public StreamSavepointHandle attachSavepoint(
                StreamJobId job, StreamCommandId command) {
            return delegate.attachSavepoint(job, command);
        }
        @Override public StreamJobHandle restore(StreamSubmissionId id,
                PlanArtifact artifact, StreamRestoreRequest request) {
            return delegate.restore(id, artifact, request);
        }
        @Override public StreamJobHandle offlineRescale(StreamSubmissionId id,
                PlanArtifact artifact, StreamOfflineRescaleRequest request) {
            return delegate.offlineRescale(id, artifact, request);
        }
        @Override public void close() {
            if (!closed.compareAndSet(false, true)) return;
            RuntimeException failure = null;
            try { delegate.close(); } catch (RuntimeException value) {
                failure = value;
            }
            try { ownership.close(); } catch (RuntimeException value) {
                if (failure == null) failure = value; else failure.addSuppressed(value);
            }
            if (failure != null) throw failure;
        }
    }
}
