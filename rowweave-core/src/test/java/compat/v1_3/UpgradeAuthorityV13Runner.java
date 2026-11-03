package compat.v1_3;

public final class UpgradeAuthorityV13Runner {
    private UpgradeAuthorityV13Runner() { }
    public static void main(String[] args) throws Exception {
        StoreContractsV13Wire.run(StoreContractsV13Wire.AUTHORITY, args);
    }
}
