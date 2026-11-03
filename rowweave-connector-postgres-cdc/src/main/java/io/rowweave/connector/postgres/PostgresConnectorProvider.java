package io.rowweave.connector.postgres;

import io.rowweave.stream.spi.StreamCdcSourceContext;
import io.rowweave.stream.spi.StreamCdcSourceProvider;
import io.rowweave.stream.spi.StreamCdcSourceReader;

import java.util.Collections;

/** Production signed PostgreSQL snapshot-to-pgoutput provider. */
public final class PostgresConnectorProvider
        implements StreamCdcSourceProvider {
    private final PostgresCdcClientFactory clientFactory;

    public PostgresConnectorProvider() {
        this(new JdbcPostgresCdcClientFactory());
    }

    PostgresConnectorProvider(PostgresCdcClientFactory clientFactory) {
        if (clientFactory == null) {
            throw new IllegalArgumentException(
                    "POSTGRES_CDC_PROVIDER_INVALID");
        }
        this.clientFactory = clientFactory;
    }

    @Override public String providerKey() {
        return PostgresConnectorContract.PROVIDER_KEY;
    }

    @Override public String contractDigest() {
        return PostgresConnectorContract.contractDigest();
    }

    @Override public StreamCdcSourceReader openCdc(
            StreamCdcSourceContext context) throws Exception {
        if (context == null || context.snapshotCutSequence() != 0L
                || !Collections.singletonList("snapshot-all").equals(
                context.snapshotSplitIds())
                || (context.phase() == StreamCdcSourceContext.Phase.SNAPSHOT
                && (context.hasExternalPosition()
                || !context.completedSnapshotSplitIds().isEmpty()))
                || (context.phase() == StreamCdcSourceContext.Phase.LOG
                && (!context.hasExternalPosition()
                || !Collections.singletonList("snapshot-all").equals(
                context.completedSnapshotSplitIds())))) {
            throw new IllegalArgumentException(
                    "POSTGRES_CDC_SOURCE_CONTEXT_INVALID");
        }
        PostgresConnectorConfiguration configuration =
                PostgresConnectorConfiguration.decode(
                        context.configurationFrame());
        if (!configuration.snapshotIdentity().equals(
                context.snapshotId())) {
            throw new IllegalArgumentException(
                    "POSTGRES_CDC_SOURCE_CONTEXT_INVALID");
        }
        PostgresCdcClient client = openClient(context, configuration);
        try {
            return new PostgresCdcReader(configuration, context, client);
        } catch (Throwable failure) {
            try { client.close(); } catch (Throwable ignored) { }
            throw failure;
        }
    }

    private PostgresCdcClient openClient(StreamCdcSourceContext context,
            PostgresConnectorConfiguration configuration) throws Exception {
        try {
            return context.secrets().withSecret(
                    PostgresConnectorConfiguration.CREDENTIALS_ALIAS,
                    credentialFrame -> {
                        PostgresCredentials credentials =
                                PostgresCredentialCodec.decode(
                                        credentialFrame);
                        try {
                            if (!configuration.hasTlsTrustReference()) {
                                return clientFactory.create(
                                        configuration, credentials, null);
                            }
                            return context.secrets().withSecret(
                                    PostgresConnectorConfiguration
                                            .TLS_TRUST_ALIAS,
                                    trust -> clientFactory.create(
                                            configuration, credentials,
                                            trust.clone()));
                        } finally {
                            credentials.destroy();
                        }
                    });
        } catch (IllegalArgumentException | IllegalStateException stable) {
            throw stable;
        } catch (Exception failure) {
            // Do not retain driver messages: SQL, values, endpoints and
            // authentication details are outside the public error contract.
            throw new IllegalStateException("POSTGRES_CDC_OPEN_FAILED");
        }
    }
}
