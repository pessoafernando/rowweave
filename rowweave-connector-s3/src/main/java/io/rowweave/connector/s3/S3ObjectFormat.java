package io.rowweave.connector.s3;

/** Deterministic object formats supported by the bounded S3 source. */
public enum S3ObjectFormat {
    CSV_RFC4180,
    NDJSON_UTF8
}
