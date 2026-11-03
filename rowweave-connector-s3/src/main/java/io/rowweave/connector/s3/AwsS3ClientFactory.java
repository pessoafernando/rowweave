package io.rowweave.connector.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;

final class AwsS3ClientFactory {
    private AwsS3ClientFactory() { }

    static S3Client create(S3ConnectorConfiguration configuration,
            S3Credentials credentials) {
        return create(configuration, credentials,
                configuration.maximumAttempts());
    }

    static S3Client create(S3ConnectorConfiguration configuration,
            S3Credentials credentials, int maximumAttempts) {
        if (maximumAttempts < 1
                || maximumAttempts > configuration.maximumAttempts()) {
            throw new IllegalArgumentException("S3_CONFIGURATION_INVALID");
        }
        byte[] access = credentials.accessKeyId();
        byte[] secret = credentials.secretAccessKey();
        byte[] token = credentials.hasSessionToken()
                ? credentials.sessionToken() : null;
        try {
            String accessText = new String(access, StandardCharsets.UTF_8);
            String secretText = new String(secret, StandardCharsets.UTF_8);
            AwsCredentials awsCredentials = token == null
                    ? AwsBasicCredentials.create(accessText, secretText)
                    : AwsSessionCredentials.create(accessText, secretText,
                    new String(token, StandardCharsets.UTF_8));
            ClientOverrideConfiguration override =
                    ClientOverrideConfiguration.builder()
                            .apiCallTimeout(Duration.ofMillis(
                                    configuration.readTimeoutMillis()))
                            .apiCallAttemptTimeout(Duration.ofMillis(
                                    configuration.readTimeoutMillis()))
                            .retryPolicy(RetryPolicy.builder()
                                    .numRetries(maximumAttempts - 1)
                                    .build())
                            .build();
            return S3Client.builder()
                    .endpointOverride(configuration.endpoint())
                    .region(Region.of(configuration.region()))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            awsCredentials))
                    .httpClientBuilder(UrlConnectionHttpClient.builder()
                            .connectionTimeout(Duration.ofMillis(
                                    configuration.connectTimeoutMillis()))
                            .socketTimeout(Duration.ofMillis(
                                    configuration.readTimeoutMillis())))
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(
                                    configuration.pathStyleAccess())
                            .checksumValidationEnabled(true).build())
                    .overrideConfiguration(override).build();
        } finally {
            Arrays.fill(access, (byte) 0);
            Arrays.fill(secret, (byte) 0);
            if (token != null) Arrays.fill(token, (byte) 0);
        }
    }
}
