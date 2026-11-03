package compat.v1_2;

/** Frozen independent v1.2 source/sink-provider contract runner. */
public final class ConnectorProviderContractV12Runner {
    private ConnectorProviderContractV12Runner() { }

    public static void main(String[] args) throws Exception {
        PortableContractsV12Wire.run(PortableContractsV12Wire.CONNECTOR, args);
    }
}
