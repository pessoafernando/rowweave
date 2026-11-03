package app.portablefixture;

import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.portable.PortableBatchSinkProvider;
import io.rowweave.portable.PortableConnectorProviderContract;
import io.rowweave.portable.PortableOperatorCode;

public final class SinkProvider implements PortableBatchSinkProvider {
    public SinkProvider() { }

    @Override
    public PortableConnectorProviderContract contract() {
        System.setProperty(
                "rowweave.test.portable.sink.contract.thread",
                Thread.currentThread().getName());
        return new PortableConnectorProviderContract(
                1, PortableOperatorCode.BindingKind.SINK,
                "test-sink-v1", "rowweave.opaque-bytes.v1", 16,
                BatchSinkDescriptorCodec.MAX_FRAME_BYTES, 1);
    }

    @Override
    public Sink create(
            byte[] descriptorFrame, byte[] configuration,
            DataSchema inputSchema) {
        return new TestOutput(inputSchema);
    }
}
