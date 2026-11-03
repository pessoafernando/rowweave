package app.mapfixture;

import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.portable.PortableFunctionProvider;
import io.rowweave.portable.PortableFunctionProviderContract;

import java.io.Serializable;

/** Signed provider for the public MAP_PARTITION_V1 ABI. */
public final class MapPartitionFunctionProvider
        implements PortableFunctionProvider {
    public MapPartitionFunctionProvider() { }

    @Override
    public PortableFunctionProviderContract contract() {
        return new PortableFunctionProviderContract(
                1, PrefixingMapPartition.class.getName(),
                FunctionAbi.MAP_PARTITION_V1, "rowweave.opaque-bytes.v1", 4096,
                new FunctionResourceProfile(4096L, 0L, 1));
    }

    @Override
    public Serializable create(byte[] configuration) {
        return new PrefixingMapPartition(configuration);
    }
}
