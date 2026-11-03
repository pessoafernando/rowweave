package io.rowweave.deployment;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.security.CodeSource;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;

/** One closeable, isolated loader over exactly one immutable signed image. */
public final class DeploymentClassLoader extends ClassLoader implements AutoCloseable {
    static { registerAsParallelCapable(); }

    private final VerifiedDeploymentImage image;
    private final ProtectionDomain protectionDomain;
    private final AtomicBoolean closed = new AtomicBoolean();

    DeploymentClassLoader(VerifiedDeploymentImage image, ClassLoader parent) {
        super(parent);
        this.image = image;
        try {
            URL location = new URL(null, "rowweave-deployment:/" + image.manifest().deploymentId() + '/',
                    new ResourceHandler(this, null));
            Permissions permissions = new Permissions();
            permissions.add(new java.security.AllPermission());
            permissions.setReadOnly();
            this.protectionDomain = new ProtectionDomain(
                    new CodeSource(location, (java.security.cert.Certificate[]) null),
                    permissions, this, null);
        } catch (java.net.MalformedURLException impossible) {
            throw new IllegalStateException("cannot create deployment code source", impossible);
        }
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            if (closed.get()) throw new ClassNotFoundException("deployment loader is closed: " + name);
            Class<?> loaded = findLoadedClass(name);
            if (loaded == null) {
                if (image.hasClass(name)) {
                    loaded = findClass(name);
                } else {
                    loaded = super.loadClass(name, false);
                }
            }
            if (resolve) resolveClass(loaded);
            return loaded;
        }
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        if (closed.get()) throw new ClassNotFoundException("deployment loader is closed: " + name);
        VerifiedDeploymentImage.BorrowedEntry entry = image.borrowClass(name);
        if (entry == null) throw new ClassNotFoundException(name);
        try {
            byte[] bytes = entry.bytes();
            return defineClass(name, bytes, 0, bytes.length, protectionDomain);
        } finally {
            entry.close();
        }
    }

    @Override
    public URL getResource(String name) {
        if (closed.get()) return null;
        if (image.hasEntry(name)) return findResource(name);
        return super.getResource(name);
    }

    @Override
    protected URL findResource(String name) {
        if (closed.get() || !image.hasEntry(name)) return null;
        try {
            String encoded = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                    name.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return new URL(null, "rowweave-deployment:/" + image.manifest().deploymentId()
                    + "/resource/" + encoded,
                    new ResourceHandler(this, name));
        } catch (java.net.MalformedURLException impossible) {
            throw new IllegalStateException("cannot create deployment resource URL", impossible);
        }
    }

    @Override
    protected Enumeration<URL> findResources(String name) {
        URL resource = findResource(name);
        return resource == null ? Collections.<URL>emptyEnumeration()
                : Collections.enumeration(Collections.singleton(resource));
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        if (closed.get()) return null;
        if (!image.hasEntry(name)) return super.getResourceAsStream(name);
        VerifiedDeploymentImage.BorrowedEntry entry = image.borrowResource(name);
        return entry == null ? null : new LeasedInputStream(entry);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) image.closeFromLoader(this);
    }

    void markClosed() { closed.set(true); }

    boolean owns(Class<?> type) {
        if (type == null || type.getClassLoader() != this) return false;
        CodeSource actual = type.getProtectionDomain().getCodeSource();
        return actual != null && protectionDomain.getCodeSource().getLocation()
                .equals(actual.getLocation());
    }

    private static final class LeasedInputStream extends FilterInputStream {
        private final VerifiedDeploymentImage.BorrowedEntry entry;
        private final AtomicBoolean closed = new AtomicBoolean();

        LeasedInputStream(VerifiedDeploymentImage.BorrowedEntry entry) {
            super(new ByteArrayInputStream(entry.bytes()));
            this.entry = entry;
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                try {
                    super.close();
                } finally {
                    entry.close();
                }
            }
        }
    }

    private static final class ResourceHandler extends URLStreamHandler {
        private final DeploymentClassLoader loader;
        private final String resourceName;
        ResourceHandler(DeploymentClassLoader loader, String resourceName) {
            this.loader = loader;
            this.resourceName = resourceName;
        }

        @Override
        protected URLConnection openConnection(URL url) {
            return new URLConnection(url) {
                @Override public void connect() { connected = true; }
                @Override public InputStream getInputStream() throws IOException {
                    if (resourceName == null) throw new IOException("deployment code source is not a resource");
                    InputStream input = loader.getResourceAsStream(resourceName);
                    if (input == null) throw new IOException(
                            "deployment resource unavailable: " + resourceName);
                    return input;
                }
            };
        }
    }
}
