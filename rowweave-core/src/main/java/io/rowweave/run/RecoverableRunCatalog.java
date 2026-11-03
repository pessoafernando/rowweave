package io.rowweave.run;

/** Discovery of non-terminal Runs with a hard bound on the read work. */
public interface RecoverableRunCatalog {
    RecoverableRunPage scan(String cursor, int limit);
}
