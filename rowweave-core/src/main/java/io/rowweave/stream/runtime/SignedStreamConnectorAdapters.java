package io.rowweave.stream.runtime;

import io.rowweave.deployment.StreamConnectorProviderRegistry;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import io.rowweave.stream.contract.StreamConnectorProviderContractCodec;
import io.rowweave.stream.spi.StreamCdcSourceContext;
import io.rowweave.stream.spi.StreamCdcCheckpointCompletion;
import io.rowweave.stream.spi.StreamCdcSourceReader;
import io.rowweave.stream.spi.StreamCdcSourceEvent;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamSourceEnumerator;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSecretResolver;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import io.rowweave.stream.contract.CheckpointDecision;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/** Static signed metadata backed by lazy, supervised Worker provider loading. */
public final class SignedStreamConnectorAdapters {
    private final Map<String, DurableStreamSourceProvider> sources;
    private final Map<String, EagerStreamSinkProvider> eagerSinks;
    private final Map<String, TransactionalStreamSinkProvider> transactionalSinks;

    public SignedStreamConnectorAdapters(
            StreamConnectorProviderRegistry registry,
            ProviderActionSupervisor supervisor) {
        this(registry, supervisor, null);
    }

    public SignedStreamConnectorAdapters(
            StreamConnectorProviderRegistry registry,
            ProviderActionSupervisor supervisor,
            StreamSecretResolver resolver) {
        if (registry == null || supervisor == null) {
            throw new IllegalArgumentException(
                    "signed Stream connector registry and supervisor are required");
        }
        registry.requireWorkerRole();
        Map<String, DurableStreamSourceProvider> sourceMap =
                new LinkedHashMap<>();
        Map<String, EagerStreamSinkProvider> eagerMap =
                new LinkedHashMap<>();
        Map<String, TransactionalStreamSinkProvider> transactionalMap =
                new LinkedHashMap<>();
        for (StreamConnectorProviderContract contract : registry.contracts()) {
            if (contract.kind() == StreamConnectorProviderContract.Kind.SOURCE) {
                sourceMap.put(contract.providerKey(),
                        contract.dynamicDiscovery()
                                ? new DynamicSource(registry, supervisor,
                                contract, resolver)
                                : new Source(registry, supervisor,
                                contract, resolver));
            } else if (contract.sinkVisibility()
                    == StreamConnectorProviderContract.SinkVisibility
                    .CHECKPOINT_TRANSACTIONAL) {
                transactionalMap.put(contract.providerKey(),
                        new TransactionalSink(registry, supervisor,
                                contract, resolver));
            } else {
                eagerMap.put(contract.providerKey(),
                        new EagerSink(registry, supervisor,
                                contract, resolver));
            }
        }
        this.sources = Collections.unmodifiableMap(sourceMap);
        this.eagerSinks = Collections.unmodifiableMap(eagerMap);
        this.transactionalSinks = Collections.unmodifiableMap(transactionalMap);
    }

    public Map<String, DurableStreamSourceProvider> sources() {
        return sources;
    }

    public Map<String, EagerStreamSinkProvider> eagerSinks() {
        return eagerSinks;
    }

    public Map<String, TransactionalStreamSinkProvider> transactionalSinks() {
        return transactionalSinks;
    }

    private static long deadline(StreamConnectorProviderContract contract) {
        return Math.addExact(System.nanoTime(),
                TimeUnit.MILLISECONDS.toNanos(contract.openTimeoutMillis()));
    }

    private static final class OpenLimit {
        private final Semaphore permits;

        private OpenLimit(int maximum) {
            permits = new Semaphore(maximum, true);
        }

        private Lease acquire() {
            if (!permits.tryAcquire()) {
                throw new IllegalStateException(
                        "STREAM_CONNECTOR_OPEN_LIMIT");
            }
            return new Lease(permits);
        }
    }

    private static final class Lease implements AutoCloseable {
        private final Semaphore permits;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(Semaphore permits) { this.permits = permits; }

        @Override public void close() {
            if (closed.compareAndSet(false, true)) permits.release();
        }
    }

    private static class Source implements ResidentCdcStreamSourceProvider {
        protected final StreamConnectorProviderRegistry registry;
        protected final ProviderActionSupervisor supervisor;
        protected final StreamConnectorProviderContract contract;
        protected final OpenLimit openLimit;
        protected final StreamSecretResolver resolver;
        private final String digest;

        private Source(StreamConnectorProviderRegistry registry,
                ProviderActionSupervisor supervisor,
                StreamConnectorProviderContract contract,
                StreamSecretResolver resolver) {
            this.registry = registry;
            this.supervisor = supervisor;
            this.contract = contract;
            this.openLimit = new OpenLimit(contract.maximumConcurrentOpens());
            this.resolver = resolver;
            this.digest = StreamConnectorProviderContractCodec.digestHex(contract);
        }

        @Override public String providerKey() { return contract.providerKey(); }
        @Override public String contractDigest() { return digest; }

        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            Lease lease = openLimit.acquire();
            try {
                long limit = deadline(contract);
                final StreamSourceReader reader;
                try (StreamConnectorSecretScope secrets =
                             StreamConnectorSecretScope.resolve(
                                     request.binding().configurationFrame(),
                                     resolver, supervisor, limit)) {
                    StreamSourceProvider provider = registry.loadSource(
                            contract.providerKey(), supervisor, limit);
                    reader = supervisor.call("signed-stream-source-open",
                            limit, () -> PublicStreamSourceAdapter
                                    .adapt(provider, secrets).open(request));
                }
                return new StreamSourceReader() {
                    @Override public StreamSourceRecord next()
                            throws Exception { return reader.next(); }
                    @Override public StreamSourceEvent nextEvent()
                            throws Exception { return reader.nextEvent(); }
                    @Override public byte[] checkpointPositionFrame() {
                        return reader.checkpointPositionFrame();
                    }
                    @Override public void wakeUp() { reader.wakeUp(); }
                    @Override public void cancel() { reader.cancel(); }
                    @Override public void close() throws Exception {
                        try { reader.close(); } finally { lease.close(); }
                    }
                };
            } catch (RuntimeException | Error failure) {
                lease.close();
                throw failure;
            }
        }

        @Override public StreamCdcSourceReader openCdc(
                StreamCdcSourceContext context) throws Exception {
            if (!contract.residentCdc()) {
                throw new IllegalArgumentException(
                        "signed source has no resident CDC capability");
            }
            Lease lease = openLimit.acquire();
            try {
                long limit = deadline(contract);
                final StreamCdcSourceReader reader;
                try (StreamConnectorSecretScope secrets =
                             StreamConnectorSecretScope.resolve(
                                     context.configurationFrame(), resolver,
                                     supervisor, limit)) {
                    StreamSourceProvider provider = registry.loadSource(
                            contract.providerKey(), supervisor, limit);
                    reader = supervisor.call("signed-stream-cdc-open",
                            limit, () -> ((ResidentCdcStreamSourceProvider)
                                    PublicStreamSourceAdapter.adapt(
                                            provider, secrets))
                                    .openCdc(context));
                }
                return new StreamCdcSourceReader() {
                    @Override public StreamCdcSourceEvent next()
                            throws Exception { return reader.next(); }
                    @Override public void notifyCheckpointComplete(
                            StreamCdcCheckpointCompletion completion)
                            throws Exception {
                        reader.notifyCheckpointComplete(completion);
                    }
                    @Override public void wakeUp() { reader.wakeUp(); }
                    @Override public void cancel() { reader.cancel(); }
                    @Override public void close() throws Exception {
                        try { reader.close(); } finally { lease.close(); }
                    }
                };
            } catch (RuntimeException | Error failure) {
                lease.close();
                throw failure;
            }
        }
    }

    private static final class DynamicSource extends Source
            implements DynamicDurableStreamSourceProvider {
        private DynamicSource(StreamConnectorProviderRegistry registry,
                ProviderActionSupervisor supervisor,
                StreamConnectorProviderContract contract,
                StreamSecretResolver resolver) {
            super(registry, supervisor, contract, resolver);
        }

        @Override public StreamSourceEnumerator openEnumerator(
                StreamSourceEnumeratorOpenRequest request) throws Exception {
            Lease lease = openLimit.acquire();
            try {
                long limit = deadline(contract);
                final StreamSourceEnumerator enumerator;
                try (StreamConnectorSecretScope secrets =
                             StreamConnectorSecretScope.resolve(
                                     request.binding().configurationFrame(),
                                     resolver, supervisor, limit)) {
                    StreamSourceProvider provider = registry.loadSource(
                            contract.providerKey(), supervisor, limit);
                    enumerator = supervisor.call(
                            "signed-stream-enumerator-open", limit,
                            () -> ((DynamicDurableStreamSourceProvider)
                                    PublicStreamSourceAdapter.adapt(
                                            provider, secrets))
                                    .openEnumerator(request));
                }
                return new StreamSourceEnumerator() {
                    @Override public StreamSplitDiscovery discover(
                            StreamDiscoveryCursor cursor, int maximum)
                            throws Exception {
                        return enumerator.discover(cursor, maximum);
                    }
                    @Override public void close() throws Exception {
                        try { enumerator.close(); } finally { lease.close(); }
                    }
                };
            } catch (RuntimeException | Error failure) {
                lease.close();
                throw failure;
            }
        }
    }

    private static final class EagerSink implements EagerStreamSinkProvider {
        private final StreamConnectorProviderRegistry registry;
        private final ProviderActionSupervisor supervisor;
        private final StreamConnectorProviderContract contract;
        private final String digest;
        private final OpenLimit openLimit;
        private final StreamSecretResolver resolver;

        private EagerSink(StreamConnectorProviderRegistry registry,
                ProviderActionSupervisor supervisor,
                StreamConnectorProviderContract contract,
                StreamSecretResolver resolver) {
            this.registry = registry;
            this.supervisor = supervisor;
            this.contract = contract;
            this.digest = StreamConnectorProviderContractCodec.digestHex(contract);
            this.openLimit = new OpenLimit(contract.maximumConcurrentOpens());
            this.resolver = resolver;
        }

        @Override public String providerKey() { return contract.providerKey(); }
        @Override public String contractDigest() { return digest; }

        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            Lease lease = openLimit.acquire();
            try {
                long limit = deadline(contract);
                final EagerStreamSinkWriter writer;
                try (StreamConnectorSecretScope secrets =
                             StreamConnectorSecretScope.resolve(
                                     request.binding().configurationFrame(),
                                     resolver, supervisor, limit)) {
                    io.rowweave.stream.spi.StreamSinkProvider provider =
                            registry.loadSink(contract.providerKey(),
                                    supervisor, limit);
                    writer = supervisor.call("signed-stream-sink-open",
                            limit, () -> PublicStreamSinkAdapter
                                    .adapt(provider, secrets).open(request));
                }
                return new EagerStreamSinkWriter() {
                    @Override public void write(byte[] frame) throws Exception {
                        writer.write(frame);
                    }
                    @Override public void cancel() { writer.cancel(); }
                    @Override public void close() throws Exception {
                        try { writer.close(); } finally { lease.close(); }
                    }
                };
            } catch (RuntimeException | Error failure) {
                lease.close();
                throw failure;
            }
        }
    }

    private static final class TransactionalSink
            implements TransactionalStreamSinkProvider {
        private final StreamConnectorProviderRegistry registry;
        private final ProviderActionSupervisor supervisor;
        private final StreamConnectorProviderContract contract;
        private final String digest;
        private final OpenLimit openLimit;
        private final StreamSecretResolver resolver;

        private TransactionalSink(StreamConnectorProviderRegistry registry,
                ProviderActionSupervisor supervisor,
                StreamConnectorProviderContract contract,
                StreamSecretResolver resolver) {
            this.registry = registry;
            this.supervisor = supervisor;
            this.contract = contract;
            this.digest = StreamConnectorProviderContractCodec.digestHex(contract);
            this.openLimit = new OpenLimit(contract.maximumConcurrentOpens());
            this.resolver = resolver;
        }

        @Override public String providerKey() { return contract.providerKey(); }
        @Override public String contractDigest() { return digest; }

        @Override public TransactionalStreamSinkWriter open(
                StreamSinkTransactionOpenRequest request) {
            Lease lease = openLimit.acquire();
            try {
                long limit = deadline(contract);
                final TransactionalStreamSinkWriter writer;
                try (StreamConnectorSecretScope secrets =
                             StreamConnectorSecretScope.resolve(
                                     request.binding().configurationFrame(),
                                     resolver, supervisor, limit)) {
                    io.rowweave.stream.spi.StreamTransactionalSinkProvider
                            provider = registry.loadTransactionalSink(
                            contract.providerKey(), supervisor, limit);
                    writer = supervisor.call(
                            "signed-stream-transactional-sink-open", limit,
                            () -> PublicTransactionalStreamSinkAdapter
                                    .adapt(provider, secrets).open(request));
                }
                return new TransactionalStreamSinkWriter() {
                    @Override public void write(byte[] frame) {
                        writer.write(frame);
                    }
                    @Override public byte[] prepare(String identity) {
                        return writer.prepare(identity);
                    }
                    @Override public TransactionStatus query(String identity) {
                        return writer.query(identity);
                    }
                    @Override public void commit(String identity,
                            CheckpointDecision decision) {
                        writer.commit(identity, decision);
                    }
                    @Override public void finalizeTransaction(String identity) {
                        writer.finalizeTransaction(identity);
                    }
                    @Override public void abort(String identity) {
                        writer.abort(identity);
                    }
                    @Override public void close() {
                        try { writer.close(); } finally { lease.close(); }
                    }
                };
            } catch (RuntimeException | Error failure) {
                lease.close();
                throw failure;
            }
        }
    }
}
