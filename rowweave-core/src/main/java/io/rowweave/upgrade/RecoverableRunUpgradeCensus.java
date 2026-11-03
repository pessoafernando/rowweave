package io.rowweave.upgrade;

import io.rowweave.run.RecoverableRunCatalog;
import io.rowweave.run.RecoverableRunPage;
import io.rowweave.run.RunRecoveryCatalogKernel;

/** Bounded full-cycle census of durable non-terminal Runs. */
public final class RecoverableRunUpgradeCensus
        implements UpgradeActiveWorkCensus {
    private static final int PAGE_SIZE = RunRecoveryCatalogKernel.MAX_PAGE_SIZE;
    private static final int MAX_PAGES = RunRecoveryCatalogKernel.SHARD_COUNT * 2;

    private final RecoverableRunCatalog catalog;

    public RecoverableRunUpgradeCensus(RecoverableRunCatalog catalog) {
        if (catalog == null) {
            throw new IllegalArgumentException("recoverable Run catalog is required");
        }
        this.catalog = catalog;
    }

    @Override
    public UpgradeActiveWorkSnapshot snapshot() {
        String cursor = null;
        long activeRuns = 0L;
        for (int pageCount = 0; pageCount < MAX_PAGES; pageCount++) {
            RecoverableRunPage page = catalog.scan(cursor, PAGE_SIZE);
            if (page == null || page.nextCursor() == null
                    || page.nextCursor().equals(cursor)) {
                throw new IllegalStateException(
                        "recoverable Run census cursor did not advance");
            }
            activeRuns = Math.addExact(activeRuns, page.runs().size());
            if (page.cycleComplete()) {
                return new UpgradeActiveWorkSnapshot(activeRuns, 0L);
            }
            cursor = page.nextCursor();
        }
        throw new IllegalStateException(
                "recoverable Run census exceeded its page bound");
    }
}
