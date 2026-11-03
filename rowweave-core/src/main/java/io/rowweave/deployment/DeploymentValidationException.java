package io.rowweave.deployment;

/** Stable fail-closed diagnostic for signed deployment admission. */
public final class DeploymentValidationException extends IllegalArgumentException {
    private final String code;

    DeploymentValidationException(String code, String message) {
        super(code + ": " + message);
        this.code = code;
    }

    DeploymentValidationException(String code, String message, Throwable cause) {
        super(code + ": " + message, cause);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
