package io.rowweave.cluster.rpc;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;

/** mTLS key, trust and authorization snapshot loaded and validated once at start-up. */
final class ControlTlsContext {
    static final String KEY_STORE_PROPERTY = "rowweave.control.tls.keyStore";
    static final String KEY_STORE_PASSWORD_PROPERTY = "rowweave.control.tls.keyStorePassword";
    static final String KEY_STORE_TYPE_PROPERTY = "rowweave.control.tls.keyStoreType";
    static final String TRUST_STORE_PROPERTY = "rowweave.control.tls.trustStore";
    static final String TRUST_STORE_PASSWORD_PROPERTY = "rowweave.control.tls.trustStorePassword";
    static final String TRUST_STORE_TYPE_PROPERTY = "rowweave.control.tls.trustStoreType";
    static final String AUTHZ_FILE_PROPERTY = "rowweave.control.tls.authzFile";
    private static final int MAX_AUTHZ_BYTES = 1024 * 1024;

    private final SSLContext sslContext;
    private final ControlAuthzPolicy authzPolicy;
    private final String localFingerprint;
    private final EnumSet<ControlRole> localRoles;

    private ControlTlsContext(SSLContext sslContext, ControlAuthzPolicy authzPolicy,
                              String localFingerprint, EnumSet<ControlRole> localRoles) {
        this.sslContext = sslContext;
        this.authzPolicy = authzPolicy;
        this.localFingerprint = localFingerprint;
        this.localRoles = EnumSet.copyOf(localRoles);
    }

    static ControlTlsContext load(Properties properties) {
        String keyPath = required(properties, KEY_STORE_PROPERTY);
        char[] keyPassword = required(properties, KEY_STORE_PASSWORD_PROPERTY).toCharArray();
        String trustPath = required(properties, TRUST_STORE_PROPERTY);
        char[] trustPassword = required(properties, TRUST_STORE_PASSWORD_PROPERTY).toCharArray();
        String authzPath = required(properties, AUTHZ_FILE_PROPERTY);
        String keyType = optional(properties, KEY_STORE_TYPE_PROPERTY, "PKCS12");
        String trustType = optional(properties, TRUST_STORE_TYPE_PROPERTY, "PKCS12");
        try {
            KeyStore keys = loadStore(Paths.get(keyPath), keyType, keyPassword);
            LocalIdentity local = requireLocalIdentity(keys, keyPassword);
            KeyStore trust = loadStore(Paths.get(trustPath), trustType, trustPassword);
            requireTrustCertificates(trust);
            ControlAuthzPolicy authz = ControlAuthzPolicy.parse(readAuthz(Paths.get(authzPath)));
            EnumSet<ControlRole> roles = authz.roles(local.fingerprint);
            if (roles.isEmpty()) {
                throw new IllegalArgumentException(
                        "local certificate fingerprint is absent from control authz policy");
            }
            requireExtendedKeyUsage(local.certificate, roles);

            Provider jsse = Security.getProvider("SunJSSE");
            if (jsse == null) {
                throw new IllegalStateException("SunJSSE provider is unavailable");
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                    "SunX509", jsse);
            kmf.init(keys, keyPassword);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    "PKIX", jsse);
            tmf.init(trust);
            SSLContext context = SSLContext.getInstance("TLS", jsse);
            context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            return new ControlTlsContext(context, authz, local.fingerprint, roles);
        } catch (IllegalArgumentException invalid) {
            throw invalid;
        } catch (Exception failure) {
            throw new IllegalArgumentException("failed to load MTLS material", failure);
        } finally {
            java.util.Arrays.fill(keyPassword, '\0');
            java.util.Arrays.fill(trustPassword, '\0');
        }
    }

    SSLContext sslContext() { return sslContext; }
    ControlAuthzPolicy authzPolicy() { return authzPolicy; }
    String localFingerprint() { return localFingerprint; }
    EnumSet<ControlRole> localRoles() { return EnumSet.copyOf(localRoles); }

    SSLServerSocket newServerSocket() throws IOException {
        SSLServerSocket socket = (SSLServerSocket) sslContext.getServerSocketFactory()
                .createServerSocket();
        socket.setEnabledProtocols(enabledProtocols(socket.getSupportedProtocols()));
        socket.setEnabledCipherSuites(enabledCiphers(socket.getSupportedCipherSuites()));
        socket.setNeedClientAuth(true);
        return socket;
    }

    SSLSocket newClientSocket() throws IOException {
        SSLSocket socket = (SSLSocket) sslContext.getSocketFactory().createSocket();
        socket.setEnabledProtocols(enabledProtocols(socket.getSupportedProtocols()));
        socket.setEnabledCipherSuites(enabledCiphers(socket.getSupportedCipherSuites()));
        SSLParameters parameters = socket.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        socket.setSSLParameters(parameters);
        return socket;
    }

    EnumSet<ControlRole> peerRoles(SSLSocket socket) throws IOException {
        return authenticatedPeer(socket).roles();
    }

    AuthenticatedRpcPeer authenticatedPeer(SSLSocket socket) throws IOException {
        try {
            Certificate[] peer = socket.getSession().getPeerCertificates();
            if (peer.length == 0 || !(peer[0] instanceof X509Certificate)) {
                throw new IOException("mTLS peer has no X509 leaf certificate");
            }
            X509Certificate leaf = (X509Certificate) peer[0];
            leaf.checkValidity();
            String peerFingerprint = fingerprint(leaf);
            EnumSet<ControlRole> roles = authzPolicy.roles(peerFingerprint);
            if (roles.isEmpty()) throw new IOException("mTLS peer certificate is not authorized");
            return new AuthenticatedRpcPeer(decodeFingerprint(peerFingerprint), roles);
        } catch (IOException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IOException("mTLS peer certificate validation failed", failure);
        }
    }

    private static byte[] decodeFingerprint(String hex) {
        byte[] decoded = new byte[DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES];
        for (int i = 0; i < decoded.length; i++) {
            decoded[i] = (byte) ((Character.digit(hex.charAt(i * 2), 16) << 4)
                    | Character.digit(hex.charAt(i * 2 + 1), 16));
        }
        return decoded;
    }

    static String[] enabledProtocols(String[] supported) {
        return intersection(supported, new String[]{"TLSv1.3", "TLSv1.2"}, "TLS protocol");
    }

    /**
     * The data plane allows TLS 1.3 only, which excludes a TLS 1.2 renegotiation from bypassing
     * handshake admission at the protocol level. A TLS 1.3 KeyUpdate neither re-authenticates
     * nor exchanges certificate chains, so it is bounded record processing on an already
     * authenticated connection.
     */
    static String[] enabledDataPlaneProtocols(String[] supported) {
        return intersection(supported, new String[]{"TLSv1.3"}, "data-plane TLS protocol");
    }

    static String[] enabledCiphers(String[] supported) {
        return intersection(supported, new String[]{
                "TLS_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384",
                "TLS_CHACHA20_POLY1305_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
                "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"
        }, "AEAD cipher suite");
    }

    private static String[] intersection(String[] supported, String[] allowed, String name) {
        java.util.Set<String> available = new java.util.HashSet<>(java.util.Arrays.asList(supported));
        java.util.List<String> enabled = new java.util.ArrayList<>();
        for (String value : allowed) if (available.contains(value)) enabled.add(value);
        if (enabled.isEmpty()) throw new IllegalArgumentException("no supported control " + name);
        return enabled.toArray(new String[0]);
    }

    private static KeyStore loadStore(Path path, String type, char[] password) throws Exception {
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException("failed to load MTLS material: unreadable store");
        }
        KeyStore store = KeyStore.getInstance(type);
        try (InputStream in = Files.newInputStream(path)) {
            store.load(in, password);
        }
        return store;
    }

    private static LocalIdentity requireLocalIdentity(KeyStore keys, char[] password)
            throws Exception {
        List<LocalIdentity> identities = new ArrayList<>();
        Enumeration<String> aliases = keys.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (!keys.isKeyEntry(alias)) continue;
            Key key = keys.getKey(alias, password);
            Certificate certificate = keys.getCertificate(alias);
            if (key != null && certificate instanceof X509Certificate) {
                X509Certificate leaf = (X509Certificate) certificate;
                leaf.checkValidity();
                requirePrivateKeyMatches(key, leaf);
                identities.add(new LocalIdentity(leaf, fingerprint(leaf)));
            }
        }
        if (identities.size() != 1) {
            throw new IllegalArgumentException(
                    "MTLS keyStore must contain exactly one X509 private-key identity");
        }
        return identities.get(0);
    }

    private static void requirePrivateKeyMatches(Key key, X509Certificate certificate)
            throws Exception {
        if (!(key instanceof PrivateKey)) {
            throw new IllegalArgumentException("MTLS key entry does not contain a private key");
        }
        String algorithm = key.getAlgorithm();
        String signatureAlgorithm;
        if ("RSA".equalsIgnoreCase(algorithm)) signatureAlgorithm = "SHA256withRSA";
        else if ("EC".equalsIgnoreCase(algorithm)) signatureAlgorithm = "SHA256withECDSA";
        else if ("DSA".equalsIgnoreCase(algorithm)) signatureAlgorithm = "SHA256withDSA";
        else throw new IllegalArgumentException("unsupported MTLS private key algorithm");
        byte[] challenge = "rowweave-control-key-match-v1".getBytes(StandardCharsets.US_ASCII);
        Signature signer = Signature.getInstance(signatureAlgorithm);
        signer.initSign((PrivateKey) key);
        signer.update(challenge);
        byte[] signed = signer.sign();
        Signature verifier = Signature.getInstance(signatureAlgorithm);
        verifier.initVerify(certificate.getPublicKey());
        verifier.update(challenge);
        if (!verifier.verify(signed)) {
            throw new IllegalArgumentException(
                    "MTLS private key does not match leaf certificate public key");
        }
    }

    private static void requireTrustCertificates(KeyStore trust) throws Exception {
        int certificates = 0;
        Enumeration<String> aliases = trust.aliases();
        while (aliases.hasMoreElements()) {
            Certificate certificate = trust.getCertificate(aliases.nextElement());
            if (certificate instanceof X509Certificate) {
                ((X509Certificate) certificate).checkValidity();
                certificates++;
            }
        }
        if (certificates == 0) {
            throw new IllegalArgumentException("MTLS trustStore must not be empty");
        }
    }

    private static String readAuthz(Path path) throws IOException {
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException("failed to load MTLS material: unreadable authz file");
        }
        long size = Files.size(path);
        if (size <= 0L || size > MAX_AUTHZ_BYTES) {
            throw new IllegalArgumentException("MTLS authz file length out of bounds");
        }
        return new String(Files.readAllBytes(path), StandardCharsets.US_ASCII);
    }

    private static void requireExtendedKeyUsage(X509Certificate certificate,
                                                EnumSet<ControlRole> roles) throws Exception {
        List<String> usage = certificate.getExtendedKeyUsage();
        if (usage == null) return; // absent EKU means unrestricted per X.509/JSSE semantics.
        boolean client = usage.contains("1.3.6.1.5.5.7.3.2");
        boolean server = usage.contains("1.3.6.1.5.5.7.3.1");
        if (!client || ((roles.contains(ControlRole.MASTER)
                || roles.contains(ControlRole.WORKER)) && !server)) {
            throw new IllegalArgumentException("local certificate EKU does not allow configured control roles");
        }
    }

    static String fingerprint(X509Certificate certificate) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded());
        StringBuilder hex = new StringBuilder(64);
        for (byte value : digest) hex.append(String.format("%02x", value & 0xff));
        return hex.toString();
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("missing required MTLS property: " + key);
        }
        return value.trim();
    }

    private static String optional(Properties properties, String key, String fallback) {
        String value = properties.getProperty(key);
        return value == null || value.trim().isEmpty() ? fallback : value.trim();
    }

    private static final class LocalIdentity {
        private final X509Certificate certificate;
        private final String fingerprint;

        private LocalIdentity(X509Certificate certificate, String fingerprint) {
            this.certificate = certificate;
            this.fingerprint = fingerprint;
        }
    }
}
