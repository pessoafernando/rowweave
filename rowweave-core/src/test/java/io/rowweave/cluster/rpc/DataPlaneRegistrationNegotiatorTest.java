package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataPlaneRegistrationNegotiatorTest {

    @Test
    void mtlsMasterRejectsMissingOrPlainWorkerCapability() {
        assertThrows(IllegalArgumentException.class,
                () -> DataPlaneRegistrationNegotiator.requireMasterCompatible(
                        ControlSecurityProfile.MTLS, null));
        assertThrows(IllegalArgumentException.class,
                () -> DataPlaneRegistrationNegotiator.requireMasterCompatible(
                        ControlSecurityProfile.MTLS,
                        DataPlaneRegistration.loopbackPlainV0()));
    }

    @Test
    void loopbackMasterAllowsLegacyButRejectsMtlsCapability() {
        assertNull(DataPlaneRegistrationNegotiator.requireMasterCompatible(
                ControlSecurityProfile.LOOPBACK, null));
        assertDoesNotThrow(() -> DataPlaneRegistrationNegotiator.requireMasterCompatible(
                ControlSecurityProfile.LOOPBACK,
                DataPlaneRegistration.loopbackPlainV0()));
        assertThrows(IllegalArgumentException.class,
                () -> DataPlaneRegistrationNegotiator.requireMasterCompatible(
                        ControlSecurityProfile.LOOPBACK,
                        DataPlaneRegistration.shuffleMtlsV1(fingerprint(1))));
    }

    @Test
    void mtlsWorkerRejectsOldAckMissingFieldsOrAnyMismatch() {
        DataPlaneRegistration expected = DataPlaneRegistration.shuffleMtlsV1(fingerprint(3));

        assertThrows(IllegalStateException.class,
                () -> DataPlaneRegistrationNegotiator.requireWorkerAccepted(
                        ControlSecurityProfile.MTLS, expected, "inc", RpcMessage.ack()));
        assertThrows(IllegalStateException.class,
                () -> DataPlaneRegistrationNegotiator.requireWorkerAccepted(
                        ControlSecurityProfile.MTLS, expected, "inc",
                        RpcMessage.workerRegistered("other-inc", expected)));
        assertThrows(IllegalStateException.class,
                () -> DataPlaneRegistrationNegotiator.requireWorkerAccepted(
                        ControlSecurityProfile.MTLS, expected, "inc",
                        RpcMessage.workerRegistered("inc",
                                DataPlaneRegistration.shuffleMtlsV1(fingerprint(4)))));
        assertThrows(IllegalStateException.class,
                () -> DataPlaneRegistrationNegotiator.requireWorkerAccepted(
                        ControlSecurityProfile.MTLS, expected, "inc",
                        RpcMessage.workerRegistered("inc",
                                DataPlaneRegistration.loopbackPlainV0())));
    }

    @Test
    void exactMtlsEchoAndOrdinaryLoopbackAckAreAccepted() {
        DataPlaneRegistration expected = DataPlaneRegistration.shuffleMtlsV1(fingerprint(9));
        assertDoesNotThrow(() -> DataPlaneRegistrationNegotiator.requireWorkerAccepted(
                ControlSecurityProfile.MTLS, expected, "inc",
                RpcMessage.workerRegistered("inc", expected)));
        assertDoesNotThrow(() -> DataPlaneRegistrationNegotiator.requireWorkerAccepted(
                ControlSecurityProfile.LOOPBACK,
                DataPlaneRegistration.loopbackPlainV0(), "local-inc", RpcMessage.ack()));
        assertThrows(IllegalStateException.class,
                () -> DataPlaneRegistrationNegotiator.requireWorkerAccepted(
                        ControlSecurityProfile.LOOPBACK,
                        DataPlaneRegistration.loopbackPlainV0(), "local-inc",
                        RpcMessage.workerRegistered("local-inc",
                                DataPlaneRegistration.loopbackPlainV0())));
    }

    private static byte[] fingerprint(int seed) {
        byte[] value = new byte[32];
        for (int i = 0; i < value.length; i++) value[i] = (byte) (seed + i);
        return value;
    }
}

