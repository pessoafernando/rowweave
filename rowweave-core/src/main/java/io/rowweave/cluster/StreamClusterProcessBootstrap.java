package io.rowweave.cluster;

/** Earliest certified process boundary used only by StreamClusterMain.main. */
public final class StreamClusterProcessBootstrap {
    private StreamClusterProcessBootstrap() { }

    public static void run(String[] arguments) throws Exception {
        ProcessBootstrapEnvelope processEnvelope =
                TransportSecurityBootstrap.bootstrapProcess();
        io.rowweave.StreamClusterMain.runCertified(
                arguments, processEnvelope);
    }
}
