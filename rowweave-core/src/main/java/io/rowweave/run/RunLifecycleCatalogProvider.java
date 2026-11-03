package io.rowweave.run;

/** Optional infrastructure capability: hands the Master a lifecycle catalog under the same authority as the RunStateStore. */
public interface RunLifecycleCatalogProvider {
    RunLifecycleCatalog runLifecycleCatalog();
}
