package io.rowweave.deployment;

import io.rowweave.stream.contract.StreamStateMigrationBinding;
import io.rowweave.stream.runtime.StreamStateMigrationAdapter;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Resolves state migration code only from an exact verified signed image. */
public final class SignedStreamStateMigrationResolver {
    public enum Role { STATIC_ONLY, WORKER }

    private final VerifiedDeploymentSet deployments;
    private final Role role;

    public SignedStreamStateMigrationResolver(
            VerifiedDeploymentSet deployments, Role role) {
        if (deployments == null || role == null) {
            throw new IllegalArgumentException(
                    "verified deployment set and resolver role are required");
        }
        this.deployments = deployments;
        this.role = role;
    }

    public StreamStateMigrationAdapter resolve(
            StreamStateMigrationBinding binding) {
        if (role != Role.WORKER) {
            throw new IllegalStateException(
                    "STATE_MIGRATION_RUNTIME_REQUIRED");
        }
        deployments.requireAdmissionOpen();
        if (binding == null) {
            throw new IllegalArgumentException(
                    "state migration binding is required");
        }
        if (!digestEquals(deployments.deploymentSetId(),
                binding.deploymentSetId())) {
            throw failure("STATE_MIGRATION_DEPLOYMENT_MISMATCH");
        }
        final VerifiedDeploymentImage image;
        try {
            image = deployments.image(binding.deploymentId());
        } catch (IllegalArgumentException missing) {
            throw failure("STATE_MIGRATION_DEPLOYMENT_MISMATCH", missing);
        }
        if (!digestEquals(hex(image.manifest().sha256()),
                binding.deploymentJarDigest())) {
            throw failure("STATE_MIGRATION_DEPLOYMENT_MISMATCH");
        }
        if (!image.hasClass(binding.adapterClassName())) {
            throw failure("STATE_MIGRATION_CLASS_INVALID");
        }
        try {
            Class<?> type = deployments.classLoader(binding.deploymentId())
                    .loadClass(binding.adapterClassName());
            requireAdapterShape(type);
            Object instance = type.getConstructor().newInstance();
            if (!(instance instanceof StreamStateMigrationAdapter)
                    || !image.ownsClass(type)) {
                throw failure("STATE_MIGRATION_CLASS_INVALID");
            }
            return (StreamStateMigrationAdapter) instance;
        } catch (DeploymentValidationException failure) {
            throw failure;
        } catch (ReflectiveOperationException | LinkageError
                 | SecurityException invalid) {
            throw failure("STATE_MIGRATION_CLASS_INVALID", invalid);
        }
    }

    private static void requireAdapterShape(Class<?> type) {
        int modifiers = type.getModifiers();
        if (!Modifier.isPublic(modifiers) || !Modifier.isFinal(modifiers)
                || Modifier.isAbstract(modifiers) || type.isInterface()) {
            throw failure("STATE_MIGRATION_CLASS_INVALID");
        }
        for (Field field : type.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                throw failure("STATE_MIGRATION_CLASS_INVALID");
            }
        }
        Constructor<?>[] constructors = type.getDeclaredConstructors();
        if (constructors.length != 1
                || constructors[0].getParameterTypes().length != 0
                || !Modifier.isPublic(constructors[0].getModifiers())) {
            throw failure("STATE_MIGRATION_CLASS_INVALID");
        }
    }

    private static boolean digestEquals(String left, String right) {
        return MessageDigest.isEqual(
                left.getBytes(StandardCharsets.US_ASCII),
                right.getBytes(StandardCharsets.US_ASCII));
    }

    private static String hex(byte[] bytes) {
        char[] result = new char[bytes.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int index = 0; index < bytes.length; index++) {
            result[index * 2] = digits[(bytes[index] >>> 4) & 0xf];
            result[index * 2 + 1] = digits[bytes[index] & 0xf];
        }
        return new String(result);
    }

    private static DeploymentValidationException failure(String code) {
        return new DeploymentValidationException(
                code, "signed state migration adapter admission failed");
    }

    private static DeploymentValidationException failure(
            String code, Throwable cause) {
        return new DeploymentValidationException(code,
                "signed state migration adapter admission failed", cause);
    }
}
