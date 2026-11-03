package io.rowweave.connector.s3;

import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import io.rowweave.stream.contract.StreamConnectorProviderContractCodec;

/** Static signed contract shipped with the production S3 source provider. */
public final class S3ConnectorContract {
    public static final String DEPLOYMENT_ID = "rowweave-connector-s3";
    public static final String PROVIDER_KEY = "rowweave.s3.snapshot";

    private static final StreamConnectorProviderContract CONTRACT =
            StreamConnectorProviderContract.source(
                    DEPLOYMENT_ID, PROVIDER_KEY,
                    StreamConnectorConfiguration.CODEC_ID,
                    64 * 1024, 64, 120_000L,
                    true, false, false, false);
    private static final String DIGEST =
            StreamConnectorProviderContractCodec.digestHex(CONTRACT);

    private S3ConnectorContract() { }

    public static StreamConnectorProviderContract contract() {
        return CONTRACT;
    }

    public static String contractDigest() { return DIGEST; }

    public static byte[] encodedContract() {
        return StreamConnectorProviderContractCodec.encode(CONTRACT);
    }
}
