package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;

import java.io.ObjectStreamException;
/**
 * Inert durable-plan carrier for an admitted provider configuration.
 * Only the durable Worker provider executor may turn this value into an action.
 */
public final class ProviderBatchSink implements BatchSink {

    private static final long serialVersionUID = 1L;
    public static final String RUNTIME_REQUIRED = "PROVIDER_RUNTIME_REQUIRED";

    private final DataSchema dataSchema;
    private final BatchSinkDescriptor descriptor;
    private final DurableSinkCodecDescriptor configurationCodec;
    private final byte[] configurationBytes;

    public ProviderBatchSink(
            DataSchema dataSchema, BatchSinkDescriptor descriptor,
            DurableSinkCodecDescriptor configurationCodec,
            byte[] configurationBytes) {
        if (dataSchema == null || descriptor == null || configurationCodec == null
                || configurationBytes == null) {
            throw new IllegalArgumentException(
                    "provider Sink configuration fields are required");
        }
        if (!descriptor.schemaFingerprint().equals(SchemaFingerprint.of(dataSchema))) {
            throw new IllegalArgumentException(
                    "provider Sink schema fingerprint mismatch");
        }
        if (descriptor.capabilityProfile()
                != BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1) {
            throw new IllegalArgumentException(
                    "provider Sink requires atomic-group capability");
        }
        if (configurationBytes.length > configurationCodec.maxPayloadBytes()
                || configurationBytes.length
                > DurableBatchSinkProviderContract.MAX_CONFIGURATION_BYTES) {
            throw new IllegalArgumentException(
                    "provider Sink configuration exceeds codec bound");
        }
        this.dataSchema = dataSchema;
        this.descriptor = descriptor;
        this.configurationCodec = configurationCodec;
        this.configurationBytes = configurationBytes.clone();
    }

    @Override
    public DataSchema getSchema() { return dataSchema; }

    public BatchSinkDescriptor batchSinkDescriptor() { return descriptor; }

    public DurableSinkCodecDescriptor configurationCodec() {
        return configurationCodec;
    }

    public byte[] configurationBytes() { return configurationBytes.clone(); }

    public int configurationSize() { return configurationBytes.length; }

    @Override
    public void collect(DataRow row) { throw runtimeRequired(); }

    @Override
    public void close() { throw runtimeRequired(); }

    @Override
    public void abort() { throw runtimeRequired(); }

    @Override
    public boolean supportsCoordinatedCommit() { return false; }

    @Override
    public void prepareCommit() { throw runtimeRequired(); }

    @Override
    public void commitPrepared() { throw runtimeRequired(); }

    @Override
    public void rollbackCommit() { throw runtimeRequired(); }

    @Override
    public void finalizeCommit() { throw runtimeRequired(); }

    @Override
    public boolean isWorkerWritable() { return false; }

    @Override
    public String exclusiveDestinationId() { return descriptor.destinationId(); }

    private static IllegalStateException runtimeRequired() {
        return new IllegalStateException(RUNTIME_REQUIRED);
    }

    private Object readResolve() throws ObjectStreamException {
        return new ProviderBatchSink(
                dataSchema, descriptor, configurationCodec, configurationBytes);
    }

    @Override
    public String toString() {
        return "ProviderBatchSink{" + descriptor.connectorType()
                + ", codec=" + configurationCodec + '}';
    }
}
