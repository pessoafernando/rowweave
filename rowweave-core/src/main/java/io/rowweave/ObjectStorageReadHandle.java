package io.rowweave;

import java.io.InputStream;

/** Exclusive response handle for one exact object read. */
public interface ObjectStorageReadHandle extends AutoCloseable {

    String providerType();

    int providerVersion();

    String versionId();

    long contentLength();

    InputStream stream();

    @Override
    void close();
}
