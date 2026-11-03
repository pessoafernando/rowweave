package example.extension;

import java.io.Serializable;

public final class ExternalConnectorDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String locator;

    public ExternalConnectorDescriptor(String locator) {
        this.locator = locator;
    }

    public String locator() {
        return locator;
    }
}

