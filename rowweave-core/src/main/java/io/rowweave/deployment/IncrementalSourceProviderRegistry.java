package io.rowweave.deployment;

import io.rowweave.IncrementalSourceProvider;
import io.rowweave.incremental.IncrementalSourceProviderContract;
import io.rowweave.incremental.IncrementalSourceProviderContractCodec;
import io.rowweave.incremental.IncrementalSourceProviderKey;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Immutable signed-resource registry; only a Worker role may load source code. */
public final class IncrementalSourceProviderRegistry {
    public static final String SERVICE_RESOURCE =
            "META-INF/services/io.rowweave.IncrementalSourceProvider";
    public static final int MAX_SERVICE_BYTES = 32 * 1024;
    public static final int MAX_PROVIDERS_PER_DEPLOYMENT = 32;
    public static final int MAX_CANDIDATES = 256;
    public static final int MAX_NAME_BYTES = 256;

    public enum Role { STATIC_ONLY, WORKER }

    private static final Pattern BINARY_CLASS = Pattern.compile(
            "[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+");

    private final VerifiedDeploymentSet deployments;
    private final Role role;
    private final Map<IncrementalSourceProviderKey, Candidate> candidates;

    private IncrementalSourceProviderRegistry(
            VerifiedDeploymentSet deployments, Role role,
            Map<IncrementalSourceProviderKey, Candidate> candidates) {
        this.deployments = deployments;
        this.role = role;
        this.candidates = Collections.unmodifiableMap(candidates);
    }

    public static IncrementalSourceProviderRegistry scan(
            VerifiedDeploymentSet deployments, Role role) {
        return scan(deployments,
                IncrementalSourceProviderAdmissionPolicy.production(), role);
    }

    static IncrementalSourceProviderRegistry scanForTesting(
            VerifiedDeploymentSet deployments,
            IncrementalSourceProviderAdmissionPolicy policy, Role role) {
        return scan(deployments, policy, role);
    }

    private static IncrementalSourceProviderRegistry scan(
            VerifiedDeploymentSet deployments,
            IncrementalSourceProviderAdmissionPolicy policy, Role role) {
        if (deployments == null || policy == null || role == null) {
            throw new IllegalArgumentException(
                    "deployments, source provider policy and role are required");
        }
        deployments.requireAdmissionOpen();
        List<Candidate> discovered = new ArrayList<>();
        for (VerifiedDeploymentImage image : deployments.imagesInCanonicalOrder()) {
            discoverImage(image, discovered);
        }
        Map<IncrementalSourceProviderKey, Candidate> byKey =
                new LinkedHashMap<>();
        for (IncrementalSourceProviderContract builtin
                : IncrementalSourceBuiltinContracts.all()) {
            String providerClass = builtin.providerKey().equals(
                    io.rowweave.incremental.JdbcIncrementalSourceProfile
                            .sequenceContract().providerKey())
                    ? IncrementalSourceProviderAdmissionPolicy
                    .JDBC_SEQUENCE_PROVIDER_CLASS
                    : builtin.providerKey().equals(
                    io.rowweave.incremental.JdbcIncrementalSourceProfile
                            .timestampKeyContract().providerKey())
                    ? IncrementalSourceProviderAdmissionPolicy
                    .JDBC_TIMESTAMP_PROVIDER_CLASS
                    : IncrementalSourceProviderAdmissionPolicy
                    .OBJECT_MANIFEST_PROVIDER_CLASS;
            IncrementalSourceProviderAdmissionPolicy.Entry admitted =
                    policy.match(
                            IncrementalSourceProviderAdmissionPolicy
                                    .CORE_DEPLOYMENT_ID,
                            providerClass, builtin.providerKey());
            if (admitted == null) {
                throw failure("SOURCE_PROVIDER_NOT_ADMITTED");
            }
            if (!admitted.digestMatches(
                    IncrementalSourceProviderContractCodec.digest(builtin))) {
                throw failure("SOURCE_PROVIDER_CONTRACT_DRIFT");
            }
            byKey.put(builtin.providerKey(), Candidate.builtin(builtin));
        }
        for (Candidate candidate : discovered) {
            if (byKey.put(candidate.contract.providerKey(), candidate) != null) {
                throw failure("SOURCE_PROVIDER_DUPLICATE");
            }
            IncrementalSourceProviderAdmissionPolicy.Entry admitted = policy.match(
                    candidate.deploymentId, candidate.providerClassName,
                    candidate.contract.providerKey());
            if (admitted == null) {
                throw failure("SOURCE_PROVIDER_NOT_ADMITTED");
            }
            if (!admitted.digestMatches(candidate.contractDigest)) {
                throw failure("SOURCE_PROVIDER_CONTRACT_DRIFT");
            }
        }
        return new IncrementalSourceProviderRegistry(deployments, role, byKey);
    }

    public IncrementalSourceProviderContract requireContract(
            IncrementalSourceProviderKey providerKey) {
        Candidate candidate = candidates.get(providerKey);
        if (candidate == null) throw failure("SOURCE_PROVIDER_UNKNOWN");
        return candidate.contract;
    }

    public Set<IncrementalSourceProviderKey> providerKeys() {
        return Collections.unmodifiableSet(candidates.keySet());
    }

    public IncrementalSourceProvider loadProvider(
            IncrementalSourceProviderKey providerKey,
            io.rowweave.internal.ProviderActionSupervisor supervisor,
            long deadlineNanos) {
        if (role != Role.WORKER) {
            throw new IllegalStateException("SOURCE_PROVIDER_RUNTIME_REQUIRED");
        }
        if (supervisor == null) {
            throw new IllegalArgumentException(
                    "provider action supervisor is required");
        }
        Candidate candidate = candidates.get(providerKey);
        if (candidate == null) throw failure("SOURCE_PROVIDER_UNKNOWN");
        return supervisor.call("incremental-provider-load", deadlineNanos,
                () -> candidate.load(deployments));
    }

    private static void discoverImage(
            VerifiedDeploymentImage image, List<Candidate> discovered) {
        int serviceSize = image.entryExpandedSize(SERVICE_RESOURCE);
        if (serviceSize < 0) return;
        if (serviceSize == 0 || serviceSize > MAX_SERVICE_BYTES) {
            throw failure("SOURCE_PROVIDER_SERVICE_INVALID");
        }
        byte[] service = readBounded(image, SERVICE_RESOURCE,
                MAX_SERVICE_BYTES, "SOURCE_PROVIDER_SERVICE_INVALID");
        List<ServiceEntry> entries = parseService(service);
        if (entries.size() > MAX_PROVIDERS_PER_DEPLOYMENT
                || discovered.size() + entries.size() > MAX_CANDIDATES) {
            throw failure("SOURCE_PROVIDER_SERVICE_INVALID");
        }
        Set<String> classes = new HashSet<>();
        Set<String> resources = new HashSet<>();
        for (ServiceEntry entry : entries) {
            if (!classes.add(entry.providerClassName)
                    || !resources.add(entry.contractResource)
                    || !image.hasClass(entry.providerClassName)) {
                throw failure("SOURCE_PROVIDER_SERVICE_INVALID");
            }
            byte[] contractBytes = readBounded(image, entry.contractResource,
                    IncrementalSourceProviderContractCodec.MAX_FRAME_BYTES,
                    "SOURCE_PROVIDER_CONTRACT_DRIFT");
            final IncrementalSourceProviderContract contract;
            try {
                contract = IncrementalSourceProviderContractCodec.decode(
                        contractBytes);
            } catch (RuntimeException invalid) {
                throw failure(contractFailureReason(invalid), invalid);
            }
            if (!MessageDigest.isEqual(contractBytes,
                    IncrementalSourceProviderContractCodec.encode(contract))) {
                throw failure("SOURCE_PROVIDER_CONTRACT_DRIFT");
            }
            if (contract.deploymentCapability()
                    != IncrementalSourceProviderContract.DeploymentCapability
                    .SIGNED_TEST_CONFORMANCE_V1) {
                throw failure("SOURCE_PROVIDER_NOT_ADMITTED");
            }
            discovered.add(new Candidate(image.manifest().deploymentId(),
                    entry.providerClassName, contractBytes, contract));
        }
    }

    private static byte[] readBounded(
            VerifiedDeploymentImage image, String resource,
            int maximumBytes, String reason) {
        int size = image.entryExpandedSize(resource);
        if (size < 1 || size > maximumBytes) throw failure(reason);
        try (VerifiedDeploymentImage.BorrowedEntry entry =
                     image.borrowResource(resource)) {
            if (entry == null || entry.bytes().length != size) {
                throw failure(reason);
            }
            return entry.bytes().clone();
        }
    }

    private static List<ServiceEntry> parseService(byte[] bytes) {
        final String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException invalid) {
            throw failure("SOURCE_PROVIDER_SERVICE_INVALID", invalid);
        }
        if (text.isEmpty() || text.charAt(text.length() - 1) != '\n'
                || text.indexOf('\r') >= 0 || text.indexOf('\0') >= 0) {
            throw failure("SOURCE_PROVIDER_SERVICE_INVALID");
        }
        String[] lines = text.substring(0, text.length() - 1)
                .split("\\n", -1);
        if (lines.length < 1 || lines.length > MAX_PROVIDERS_PER_DEPLOYMENT) {
            throw failure("SOURCE_PROVIDER_SERVICE_INVALID");
        }
        List<ServiceEntry> entries = new ArrayList<>(lines.length);
        for (String line : lines) {
            int delimiter = line.indexOf('=');
            if (delimiter <= 0 || delimiter != line.lastIndexOf('=')
                    || delimiter == line.length() - 1) {
                throw failure("SOURCE_PROVIDER_SERVICE_INVALID");
            }
            String className = boundedName(line.substring(0, delimiter));
            String resource = boundedName(line.substring(delimiter + 1));
            if (!BINARY_CLASS.matcher(className).matches()
                    || resource.startsWith("/") || resource.indexOf('\\') >= 0
                    || resource.contains("//") || resource.contains("/../")
                    || resource.startsWith("../") || resource.endsWith("/..")
                    || resource.contains("/./") || resource.startsWith("./")
                    || resource.endsWith("/.")) {
                throw failure("SOURCE_PROVIDER_SERVICE_INVALID");
            }
            entries.add(new ServiceEntry(className, resource));
        }
        return entries;
    }

    private static String boundedName(String value) {
        if (value.isEmpty() || value.getBytes(StandardCharsets.UTF_8).length
                > MAX_NAME_BYTES) {
            throw failure("SOURCE_PROVIDER_SERVICE_INVALID");
        }
        return value;
    }

    private static DeploymentValidationException failure(String reason) {
        return new DeploymentValidationException(
                reason, "incremental source provider admission failed");
    }

    private static DeploymentValidationException failure(
            String reason, Throwable cause) {
        return new DeploymentValidationException(reason,
                "incremental source provider admission failed", cause);
    }

    private static String contractFailureReason(Throwable failure) {
        for (Throwable current = failure; current != null;
             current = current.getCause()) {
            String message = current.getMessage();
            if (message == null) continue;
            if (message.contains("budget") || message.contains("bound")
                    || message.contains("timeout")
                    || message.contains("provider handle")) {
                return "SOURCE_PROVIDER_BUDGET_INVALID";
            }
        }
        return "SOURCE_PROVIDER_CONTRACT_DRIFT";
    }

    private static final class ServiceEntry {
        private final String providerClassName;
        private final String contractResource;

        private ServiceEntry(String providerClassName, String contractResource) {
            this.providerClassName = providerClassName;
            this.contractResource = contractResource;
        }
    }

    private static final class Candidate {
        private final String deploymentId;
        private final String providerClassName;
        private final byte[] contractBytes;
        private final byte[] contractDigest;
        private final IncrementalSourceProviderContract contract;
        private final boolean builtin;
        private volatile IncrementalSourceProvider loaded;

        private Candidate(String deploymentId, String providerClassName,
                          byte[] contractBytes,
                          IncrementalSourceProviderContract contract) {
            this.deploymentId = deploymentId;
            this.providerClassName = providerClassName;
            this.contractBytes = contractBytes.clone();
            this.contractDigest =
                    IncrementalSourceProviderContractCodec.digest(contract);
            this.contract = contract;
            this.builtin = false;
        }

        private Candidate(IncrementalSourceProviderContract contract) {
            this.deploymentId = null;
            this.providerClassName = null;
            this.contractBytes =
                    IncrementalSourceProviderContractCodec.encode(contract);
            this.contractDigest =
                    IncrementalSourceProviderContractCodec.digest(contract);
            this.contract = contract;
            this.builtin = true;
        }

        private static Candidate builtin(
                IncrementalSourceProviderContract contract) {
            return new Candidate(contract);
        }

        private IncrementalSourceProvider load(
                VerifiedDeploymentSet deployments) {
            if (builtin) {
                IncrementalSourceProviderKey key = contract.providerKey();
                if (key.equals(io.rowweave.incremental
                        .JdbcIncrementalSourceProfile.sequenceContract()
                        .providerKey())) {
                    return new io.rowweave.incremental
                            .JdbcSequenceIncrementalSourceProvider();
                }
                if (key.equals(io.rowweave.incremental
                        .JdbcIncrementalSourceProfile.timestampKeyContract()
                        .providerKey())) {
                    return new io.rowweave.incremental
                            .JdbcTimestampKeyIncrementalSourceProvider();
                }
                if (key.equals(io.rowweave.incremental
                        .ObjectManifestIncrementalSourceProfile
                        .builtInContract().providerKey())) {
                    return new io.rowweave.incremental
                            .ObjectManifestIncrementalSourceProvider();
                }
                throw failure("SOURCE_PROVIDER_UNKNOWN");
            }
            IncrementalSourceProvider result = loaded;
            if (result != null) return result;
            synchronized (this) {
                result = loaded;
                if (result != null) return result;
                try {
                    DeploymentClassLoader loader =
                            deployments.classLoader(deploymentId);
                    Class<?> type = Class.forName(
                            providerClassName, false, loader);
                    if (type.getClassLoader() != loader
                            || !deployments.trustsClass(type)
                            || !IncrementalSourceProvider.class
                            .isAssignableFrom(type)
                            || !Modifier.isPublic(type.getModifiers())) {
                        throw failure("SOURCE_PROVIDER_NOT_ADMITTED");
                    }
                    Constructor<?> constructor = type.getConstructor();
                    if (!Modifier.isPublic(constructor.getModifiers())) {
                        throw failure("SOURCE_PROVIDER_NOT_ADMITTED");
                    }
                    result = (IncrementalSourceProvider)
                            constructor.newInstance();
                    byte[] first = IncrementalSourceProviderContractCodec.encode(
                            result.contract());
                    byte[] second = IncrementalSourceProviderContractCodec.encode(
                            result.contract());
                    if (!MessageDigest.isEqual(first, second)
                            || !MessageDigest.isEqual(first, contractBytes)) {
                        throw failure("SOURCE_PROVIDER_CONTRACT_DRIFT");
                    }
                    loaded = result;
                    return result;
                } catch (DeploymentValidationException failure) {
                    throw failure;
                } catch (ReflectiveOperationException | LinkageError
                         | RuntimeException failure) {
                    throw failure(contractFailureReason(failure), failure);
                }
            }
        }
    }
}
