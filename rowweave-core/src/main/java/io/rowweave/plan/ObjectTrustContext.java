package io.rowweave.plan;

import io.rowweave.CollectionSource;
import io.rowweave.CompatibilityDataLimits;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.BatchExecutableSource;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkBinding;
import io.rowweave.AssignedSourceBatch;
import io.rowweave.ConcatSource;
import io.rowweave.CollectingSink;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.IncrementalSourceSplit;
import io.rowweave.IterableSource;
import io.rowweave.JdbcSnapshotSource;
import io.rowweave.JdbcSnapshotReadRequest;
import io.rowweave.JdbcSnapshotSourceProvider;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageQualitySource;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;
import io.rowweave.PrintSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.incremental.IncrementalSourceConfig;
import io.rowweave.incremental.IncrementalSourceProviderKey;

import java.io.InvalidClassException;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.security.CodeSource;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Trust cannot be inferred from a package name prefix. */
public final class ObjectTrustContext {
    private static final Set<Class<?>> BUILT_INS = Collections.unmodifiableSet(
            new HashSet<Class<?>>(Arrays.<Class<?>>asList(
                    FileSource.class, ManifestFileSource.class, CollectionSource.class,
                    CompatibilityDataLimits.class, BatchSourceSplitDescriptor.class,
                    BatchSourceSplitDescriptor.ReplayGuarantee.class,
                    BatchSourceSplitBinding.class, BatchExecutableSource.class,
                    IncrementalSourceSplit.class,
                    IncrementalSourceConfig.class, IncrementalSourceProviderKey.class,
                    AssignedSourceBatch.class,
                    ObjectStorageReadRequest.class, ObjectStorageSnapshotSource.class,
                    ObjectStorageQualitySource.class,
                    ObjectStorageSourceProvider.class,
                    JdbcSnapshotReadRequest.class, JdbcSnapshotSource.class,
                    JdbcSnapshotSourceProvider.class,
                    BatchSinkDescriptor.class,
                    BatchSinkDescriptor.CapabilityProfile.class,
                    BatchSinkBinding.class,
                    ConcatSource.class, IterableSource.class,
                    FileSink.class, ManifestFileSink.class, DiscardSink.class,
                    PrintSink.class, CollectingSink.class,
                    DataSchema.class, Field.class, DataRow.class, DataType.class)));

    private final boolean localTrusted;
    private final ClassLoader applicationLoader;
    private final boolean quantitativeFilterRequired;
    private final io.rowweave.deployment.VerifiedDeploymentSet deployments;

    private ObjectTrustContext(boolean localTrusted, ClassLoader applicationLoader,
                               boolean quantitativeFilterRequired,
                               io.rowweave.deployment.VerifiedDeploymentSet deployments) {
        this.localTrusted = localTrusted;
        this.applicationLoader = applicationLoader;
        this.quantitativeFilterRequired = quantitativeFilterRequired;
        this.deployments = deployments;
    }

    /** Local trust profile: the application class must be resolved by the runtime classloader given explicitly. */
    public static ObjectTrustContext localTrusted(ClassLoader applicationLoader) {
        if (applicationLoader == null) {
            throw new IllegalArgumentException("application classloader is required");
        }
        return new ObjectTrustContext(true, applicationLoader, false, null);
    }

    public static ObjectTrustContext localTrusted() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) loader = ObjectTrustContext.class.getClassLoader();
        return localTrusted(loader);
    }

    /** Before a deployment is installed, MTLS may load only the exact built-in class closure of core. */
    public static ObjectTrustContext mtlsBuiltIns() {
        io.rowweave.memory.ResidentObjectInputStream.requireFilterRuntime();
        return new ObjectTrustContext(false, ObjectTrustContext.class.getClassLoader(), true, null);
    }

    public static ObjectTrustContext mtlsDeployments(
            io.rowweave.deployment.VerifiedDeploymentSet deployments) {
        if (deployments == null) throw new IllegalArgumentException("deployment set is required");
        io.rowweave.memory.ResidentObjectInputStream.requireFilterRuntime();
        return new ObjectTrustContext(false, ObjectTrustContext.class.getClassLoader(),
                true, deployments);
    }

    /** Every socket and InProcess cluster profile must carry a filter with quantified bounds. */
    public static ObjectTrustContext loopbackCluster() {
        io.rowweave.memory.ResidentObjectInputStream.requireFilterRuntime();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) loader = ObjectTrustContext.class.getClassLoader();
        return new ObjectTrustContext(true, loader, true, null);
    }

    boolean quantitativeFilterRequired() { return quantitativeFilterRequired; }

    Class<?> resolveApplicationClass(String name) throws ClassNotFoundException {
        if (!localTrusted && deployments != null) return deployments.resolveTrustedClass(name);
        return Class.forName(name, false, applicationLoader);
    }

    void verifyNonJdkClass(Class<?> type) throws InvalidClassException {
        if (localTrusted) {
            try {
                if (Class.forName(type.getName(), false, applicationLoader) != type) {
                    throw rejected(type.getName(), "class identity differs from local trusted runtime");
                }
            } catch (ClassNotFoundException missing) {
                throw rejected(type.getName(), "class is not visible from local trusted runtime");
            }
            return;
        }
        boolean builtIn = BUILT_INS.contains(type)
                && type.getClassLoader() == Source.class.getClassLoader()
                && sameCodeSource(type, Source.class);
        if (!builtIn && (deployments == null || !deployments.trustsClass(type))) {
            throw rejected(type.getName(),
                    "class is not in the exact core built-in closure or a trusted deployment");
        }
    }

    void verifyRootForWrite(Object value, Class<?> expectedRoot) throws InvalidClassException {
        if (value == null || expectedRoot == null || !expectedRoot.isInstance(value)) {
            throw rejected(value == null ? "null" : value.getClass().getName(),
                    "root object must implement "
                            + (expectedRoot == null ? "<missing>" : expectedRoot.getName()));
        }
        if (Proxy.isProxyClass(value.getClass())) {
            throw rejected(value.getClass().getName(), "dynamic proxy is forbidden");
        }
        // Serializable lambdas are synthetic runtime classes whose writeReplace yields a
        // SerializedLambda; their capturing class is checked in verifyObjectForWrite.
        if (!value.getClass().isSynthetic()) {
            TrustedObjectInputStream.verifyClassPolicy(value.getClass(), this);
        }
    }

    void verifyObjectForWrite(Object value) throws InvalidClassException {
        if (value == null) return;
        Class<?> type = value.getClass();
        if (Proxy.isProxyClass(type)) {
            throw rejected(type.getName(), "dynamic proxy is forbidden");
        }
        if (value instanceof SerializedLambda) {
            String capturingName = ((SerializedLambda) value).getCapturingClass().replace('/', '.');
            try {
                verifyNonJdkClass(resolveApplicationClass(capturingName));
            } catch (ClassNotFoundException missing) {
                throw rejected(capturingName, "lambda capturing class is not trusted");
            }
        }
        TrustedObjectInputStream.verifyClassPolicy(type, this);
    }

    private static boolean sameCodeSource(Class<?> left, Class<?> right) {
        CodeSource leftSource = left.getProtectionDomain().getCodeSource();
        CodeSource rightSource = right.getProtectionDomain().getCodeSource();
        URL leftLocation = leftSource == null ? null : leftSource.getLocation();
        URL rightLocation = rightSource == null ? null : rightSource.getLocation();
        return leftLocation == null ? rightLocation == null : leftLocation.equals(rightLocation);
    }

    static InvalidClassException rejected(String name, String reason) {
        return new InvalidClassException(name, reason);
    }
}
