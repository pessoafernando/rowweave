package compat.v1_3;

public final class StoreFormatHeaderV13Runner {
    private StoreFormatHeaderV13Runner() { }
    public static void main(String[] args) throws Exception {
        StoreContractsV13Wire.run(StoreContractsV13Wire.HEADER, args);
    }
}
