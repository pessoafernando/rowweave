package io.rowweave;

import java.io.Serializable;

/**
 * The column definition of a DataSchema: name, alias and DataType.
 * When alias is absent or null, it falls back to name.
 */
public final class Field implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final String alias;
    private final DataType dataType;

    public Field(String name, DataType dataType) {
        this(name, name, dataType);
    }

    public Field(String name, String alias, DataType dataType) {
        this.name = name;
        this.alias = (alias != null) ? alias : name;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }

    public DataType getDataType() {
        return dataType;
    }
}
