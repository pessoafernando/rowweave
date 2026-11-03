package compat.v1_2;

/** Frozen independent v1.2 function-provider contract runner. */
public final class FunctionProviderContractV12Runner {
    private FunctionProviderContractV12Runner() { }

    public static void main(String[] args) throws Exception {
        PortableContractsV12Wire.run(PortableContractsV12Wire.FUNCTION, args);
    }
}
