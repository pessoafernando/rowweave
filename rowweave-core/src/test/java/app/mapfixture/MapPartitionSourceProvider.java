package app.mapfixture;

import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.Source;
import io.rowweave.portable.PortableBatchSourceProvider;
import io.rowweave.portable.PortableConnectorProviderContract;
import io.rowweave.portable.PortableOperatorCode;

/** Signed deployment fixture for a two-split portable Batch source. */
public final class MapPartitionSourceProvider
        implements PortableBatchSourceProvider {
    public MapPartitionSourceProvider() { }

    @Override
    public PortableConnectorProviderContract contract() {
        return new PortableConnectorProviderContract(
                1, PortableOperatorCode.BindingKind.SOURCE,
                "map-partition-source-v1", "rowweave.opaque-bytes.v1", 16,
                BatchSourceSplitDescriptorCodec.MAX_FRAME_BYTES, 4);
    }

    @Override
    public Source create(byte[] configuration, byte[] splitFrame) {
        return new MapPartitionTestInput(
                BatchSourceSplitDescriptorCodec.decode(splitFrame));
    }
}
