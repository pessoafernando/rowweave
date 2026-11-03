package app.mapfixture;

import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.portable.PortableBatchSinkProvider;
import io.rowweave.portable.PortableConnectorProviderContract;
import io.rowweave.portable.PortableOperatorCode;

/** Signed deployment fixture for a Worker-writable recording sink. */
public final class MapPartitionSinkProvider
        implements PortableBatchSinkProvider {
    public MapPartitionSinkProvider() { }

    @Override
    public PortableConnectorProviderContract contract() {
        return new PortableConnectorProviderContract(
                1, PortableOperatorCode.BindingKind.SINK,
                "map-partition-sink-v1", "rowweave.opaque-bytes.v1", 4096,
                BatchSinkDescriptorCodec.MAX_FRAME_BYTES, 1);
    }

    @Override
    public Sink create(
            byte[] descriptorFrame, byte[] configuration,
            DataSchema inputSchema) {
        return new RecordingMapPartitionOutput(inputSchema,
                BatchSinkDescriptorCodec.decode(descriptorFrame),
                configuration);
    }
}
