package io.rowweave.upgrade;

/** Durable authority used to linearize store upgrades with runtime drain state. */
public interface UpgradeAuthorityStore {
    UpgradeAuthorityRecord read();

    UpgradeTransition apply(
            UpgradeCommand command,
            long activeRunCount,
            long activeIntervalCount,
            long authorityTimeMillis);
}
