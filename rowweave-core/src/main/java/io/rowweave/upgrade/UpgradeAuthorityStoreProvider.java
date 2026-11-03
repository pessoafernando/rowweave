package io.rowweave.upgrade;

/** Optional registry capability exposing the deployment store authority. */
public interface UpgradeAuthorityStoreProvider {
    UpgradeAuthorityStore upgradeAuthorityStore();
}
