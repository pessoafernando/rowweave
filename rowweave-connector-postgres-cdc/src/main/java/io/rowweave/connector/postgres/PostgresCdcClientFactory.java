package io.rowweave.connector.postgres;

interface PostgresCdcClientFactory {
    PostgresCdcClient create(PostgresConnectorConfiguration configuration,
            PostgresCredentials credentials, byte[] tlsTrust) throws Exception;
}
