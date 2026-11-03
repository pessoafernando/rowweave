package app.portablefixture;

import io.rowweave.Source;
import io.rowweave.BatchSourceSplitDescriptorCodec;
import io.rowweave.portable.PortableBatchSourceProvider;
import io.rowweave.portable.PortableConnectorProviderContract;
import io.rowweave.portable.PortableOperatorCode;

public final class SourceProvider implements PortableBatchSourceProvider {
    static {
        System.setProperty(
                "rowweave.test.portable.connector.provider.loaded", "yes");
    }

    public SourceProvider() { }

    @Override
    public PortableConnectorProviderContract contract() {
        System.setProperty(
                "rowweave.test.portable.source.contract.thread",
                Thread.currentThread().getName());
        return new PortableConnectorProviderContract(
                1, PortableOperatorCode.BindingKind.SOURCE,
                "test-source-v1", "rowweave.opaque-bytes.v1", 16,
                BatchSourceSplitDescriptorCodec.MAX_FRAME_BYTES, 4);
    }

    @Override
    public Source create(byte[] configuration, byte[] splitFrame) {
        return new TestInput();
    }
}
