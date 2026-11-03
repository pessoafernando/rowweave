package io.rowweave;

import java.io.Serializable;
import java.util.Arrays;

/**
 * The header metadata of a DataSet, made up of ordered Field entries.
 */
public final class DataSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Field[] fields;

    public DataSchema(Field... fields) {
        this.fields = (fields != null) ? fields.clone() : new Field[0];
    }

    public int getFieldCount() {
        return fields.length;
    }

    public Field getField(int index) {
        return fields[index];
    }

    public Field[] getFields() {
        return fields.clone();
    }

    /** Looks up a field index by name; returns -1 when absent. Matches name first, then alias. */
    public int getFieldIndex(String name) {
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].getName().equals(name)) {
                return i;
            }
        }
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].getAlias().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        return "DataSchema" + Arrays.toString(fields);
    }
}
