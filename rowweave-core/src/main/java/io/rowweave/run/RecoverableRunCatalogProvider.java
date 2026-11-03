package io.rowweave.run;

/** Implemented only by a backend that can commit a Run mutation and its recovery catalog entry atomically. */
public interface RecoverableRunCatalogProvider {
    RecoverableRunCatalog recoverableRunCatalog();
}
