package io.rowweave.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/** Forked-JVM probe so an intentional poison never contaminates the test JVM. */
public final class ProviderActionSupervisorProbe {
    private ProviderActionSupervisorProbe() { }

    public static void main(String[] args) throws Exception {
        try {
            Class<?> type = Class.forName(
                    "io.rowweave.internal.ProviderActionSupervisor");
            Object supervisor = type.getMethod("processWide", int.class)
                    .invoke(null, 2);
            Object same = type.getMethod("processWide", int.class)
                    .invoke(null, 64);
            require(supervisor == same, "supervisor was replaceable");
            require(((Integer) type.getMethod("capacity").invoke(supervisor)) == 2,
                    "first capacity was not frozen");

            Method call = type.getMethod(
                    "call", String.class, long.class, Callable.class);
            Object value = call.invoke(supervisor, "normal",
                    System.nanoTime() + 1_000_000_000L,
                    (Callable<String>) () -> "ok");
            require("ok".equals(value), "normal call failed");

            Callable<Void> blocking = () -> {
                for (;;) {
                    try { Thread.sleep(1000L); }
                    catch (InterruptedException ignored) { }
                }
            };
            String action = args.length == 0 ? "control" : args[0];
            try {
                if ("control".equals(action)) {
                    call.invoke(supervisor, action,
                            System.nanoTime() + 50_000_000L, blocking);
                } else {
                    Object attempt = type.getMethod(
                            "openAttempt", String.class, long.class)
                            .invoke(supervisor, "attempt",
                                    System.nanoTime() + 50_000_000L);
                    Method attemptCall = attempt.getClass().getMethod(
                            "call", String.class, Callable.class);
                    attemptCall.invoke(attempt, action, blocking);
                }
                throw new AssertionError("blocking action unexpectedly returned");
            } catch (InvocationTargetException expected) {
                require(expected.getCause().getMessage().contains(
                        "PROVIDER_ACTION_TIMEOUT"), "wrong timeout result");
            }

            require((Boolean) type.getMethod("isPoisoned").invoke(supervisor),
                    "supervisor was not permanently poisoned");
            long before = System.nanoTime();
            try {
                call.invoke(type.getMethod("processWide", int.class)
                                .invoke(null, 1), "after-poison",
                        System.nanoTime() + 1_000_000_000L,
                        (Callable<String>) () -> "forbidden");
                throw new AssertionError("poison was bypassed");
            } catch (InvocationTargetException expected) {
                require(expected.getCause().getMessage().contains(
                        "PROVIDER_ACTION_SUPERVISOR_POISONED"),
                        "wrong poison result");
            }
            require(System.nanoTime() - before < 250_000_000L,
                    "poisoned request did not fail fast");
            int threads = 0;
            for (Thread thread : Thread.getAllStackTraces().keySet()) {
                if (thread.getName().startsWith("rowweave-provider-action-")) {
                    threads++;
                }
            }
            require(threads <= 2, "provider action thread cap was exceeded");
            System.out.println("POISONED capacity=2 threads=" + threads);
            System.exit(0);
        } catch (Throwable failure) {
            failure.printStackTrace(System.out);
            System.exit(1);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
