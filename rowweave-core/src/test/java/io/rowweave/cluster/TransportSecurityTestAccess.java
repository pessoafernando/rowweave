package io.rowweave.cluster;

/** Test-classpath bridge; production callers cannot invoke the package-private bootstrap. */
public final class TransportSecurityTestAccess {

    private TransportSecurityTestAccess() {
    }

    public static ProcessTlsEnvelope bootstrap() {
        return TransportSecurityBootstrap.bootstrap();
    }
}
