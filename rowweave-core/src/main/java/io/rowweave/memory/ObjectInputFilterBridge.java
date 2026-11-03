package io.rowweave.memory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/** Bridge that loads under JDK 8 and installs a JEP 290 per-stream filter by reflection under JDK 17. */
final class ObjectInputFilterBridge {
    private static final RuntimeApi API = RuntimeApi.load();

    private ObjectInputFilterBridge() {
    }

    static boolean available() {
        return API != null;
    }

    static void install(ObjectInputStream stream,
                        ResidentObjectInputStream.StreamFilter filter,
                        boolean required) throws IOException {
        if (API == null) {
            if (required) {
                throw new IOException("JEP 290 ObjectInputFilter runtime is required for cluster object loading");
            }
            return;
        }
        API.install(stream, filter);
    }

    private static final class RuntimeApi {
        private final Class<?> filterClass;
        private final Method setFilter;
        private final Method depth;
        private final Method references;
        private final Method streamBytes;
        private final Method arrayLength;
        private final Method serialClass;
        private final Object undecided;
        private final Object allowed;
        private final Object rejected;

        private RuntimeApi(Class<?> filterClass, Method setFilter, Method depth,
                           Method references, Method streamBytes, Method arrayLength,
                           Method serialClass, Object undecided, Object allowed, Object rejected) {
            this.filterClass = filterClass;
            this.setFilter = setFilter;
            this.depth = depth;
            this.references = references;
            this.streamBytes = streamBytes;
            this.arrayLength = arrayLength;
            this.serialClass = serialClass;
            this.undecided = undecided;
            this.allowed = allowed;
            this.rejected = rejected;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static RuntimeApi load() {
            try {
                Class<?> filter = Class.forName("java.io.ObjectInputFilter");
                Class<?> info = Class.forName("java.io.ObjectInputFilter$FilterInfo");
                Class<? extends Enum> status = (Class<? extends Enum>) Class.forName(
                        "java.io.ObjectInputFilter$Status");
                return new RuntimeApi(filter,
                        ObjectInputStream.class.getMethod("setObjectInputFilter", filter),
                        info.getMethod("depth"), info.getMethod("references"),
                        info.getMethod("streamBytes"), info.getMethod("arrayLength"),
                        info.getMethod("serialClass"),
                        Enum.valueOf(status, "UNDECIDED"), Enum.valueOf(status, "ALLOWED"),
                        Enum.valueOf(status, "REJECTED"));
            } catch (ClassNotFoundException | NoSuchMethodException unavailable) {
                return null;
            }
        }

        private void install(ObjectInputStream stream,
                             ResidentObjectInputStream.StreamFilter filter) throws IOException {
            InvocationHandler handler = (proxy, method, args) -> {
                if (method.getDeclaringClass() == Object.class) {
                    if ("toString".equals(method.getName())) return "RowWeaveObjectInputFilterBridge";
                    if ("hashCode".equals(method.getName())) return System.identityHashCode(proxy);
                    if ("equals".equals(method.getName())) return proxy == args[0];
                }
                Object info = args[0];
                ResidentObjectInputStream.StreamDecision decision = filter.check(
                        new ResidentObjectInputStream.StreamInfo(
                                number(depth.invoke(info)), number(references.invoke(info)),
                                number(streamBytes.invoke(info)), number(arrayLength.invoke(info)),
                                (Class<?>) serialClass.invoke(info)));
                if (decision == ResidentObjectInputStream.StreamDecision.REJECTED) return rejected;
                if (decision == ResidentObjectInputStream.StreamDecision.ALLOWED) return allowed;
                return undecided;
            };
            Object proxy = Proxy.newProxyInstance(
                    ObjectInputFilterBridge.class.getClassLoader(),
                    new Class<?>[]{filterClass}, handler);
            try {
                setFilter.invoke(stream, proxy);
            } catch (IllegalAccessException | InvocationTargetException failure) {
                Throwable cause = failure instanceof InvocationTargetException
                        ? ((InvocationTargetException) failure).getCause() : failure;
                throw new IOException("failed to install JEP 290 object filter", cause);
            }
        }

        private static long number(Object value) {
            return ((Number) value).longValue();
        }
    }
}
