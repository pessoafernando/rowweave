package io.rowweave.upgrade;

/** Re-discovers durable work so leader replacement cannot forget a live Run. */
@FunctionalInterface
public interface UpgradeActiveWorkCensus {
    UpgradeActiveWorkCensus EMPTY =
            () -> new UpgradeActiveWorkSnapshot(0L, 0L);

    UpgradeActiveWorkSnapshot snapshot();
}
