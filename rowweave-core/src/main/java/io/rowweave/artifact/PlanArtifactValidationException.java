package io.rowweave.artifact;

/** A stable code and path form the program contract; the message is for diagnosis only. */
public final class PlanArtifactValidationException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private final String code;
    private final String path;

    PlanArtifactValidationException(String code, String path, String message) {
        super(message);
        this.code = code;
        this.path = path;
    }

    PlanArtifactValidationException(String code, String path, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.path = path;
    }

    public String code() {
        return code;
    }

    public String path() {
        return path;
    }
}
