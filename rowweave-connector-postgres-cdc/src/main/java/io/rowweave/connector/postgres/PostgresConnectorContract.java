package io.rowweave.connector.postgres;

import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import io.rowweave.stream.contract.StreamConnectorProviderContractCodec;

/** Static signed contract shipped with the PostgreSQL logical CDC source. */
public final class PostgresConnectorContract {
    public static final String DEPLOYMENT_ID =
            "rowweave-connector-postgres-cdc";
    public static final String PROVIDER_KEY =
            "rowweave.postgres.logical-cdc";

    private static final StreamConnectorProviderContract CONTRACT =
            StreamConnectorProviderContract.source(
                    DEPLOYMENT_ID, PROVIDER_KEY,
                    StreamConnectorConfiguration.CODEC_ID,
                    PostgresConnectorConfiguration.MAX_CONFIGURATION_BYTES,
                    16, 120_000L,
                    false, false, true, true);
    private static final String DIGEST =
            StreamConnectorProviderContractCodec.digestHex(CONTRACT);

    private PostgresConnectorContract() { }

    public static StreamConnectorProviderContract contract() {
        return CONTRACT;
    }

    public static String contractDigest() { return DIGEST; }

    public static byte[] encodedContract() {
        return StreamConnectorProviderContractCodec.encode(CONTRACT);
    }
}
