package io.rowweave.connector.postgres;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Exact PostgreSQL relation/catalog binding used for drift detection. */
public final class PostgresTableBinding {
    public static final int MAX_COLUMNS = 1024;
    private static final String CODE = "POSTGRES_CDC_BINDING_INVALID";

    private final String schemaName;
    private final String tableName;
    private final long tableOid;
    private final PostgresReplicaIdentity replicaIdentity;
    private final long replicaIdentityIndexOid;
    private final List<PostgresColumnBinding> columns;
    private final List<Integer> primaryKeyAttributeNumbers;

    public PostgresTableBinding(String schemaName, String tableName,
            long tableOid, PostgresReplicaIdentity replicaIdentity,
            long replicaIdentityIndexOid,
            List<PostgresColumnBinding> columns,
            List<Integer> primaryKeyAttributeNumbers) {
        PostgresCodecSupport.utf8(schemaName, 63, false, CODE);
        PostgresCodecSupport.utf8(tableName, 63, false, CODE);
        PostgresCodecSupport.requireUint32(tableOid, false, CODE);
        if (replicaIdentity == null || columns == null || columns.isEmpty()
                || columns.size() > MAX_COLUMNS
                || primaryKeyAttributeNumbers == null
                || primaryKeyAttributeNumbers.isEmpty()
                || primaryKeyAttributeNumbers.size() > columns.size()
                || (replicaIdentity == PostgresReplicaIdentity.USING_INDEX)
                != (replicaIdentityIndexOid != 0L)) {
            throw invalid();
        }
        PostgresCodecSupport.requireUint32(replicaIdentityIndexOid,
                true, CODE);

        List<PostgresColumnBinding> columnCopy =
                new ArrayList<>(columns.size());
        Set<String> fieldIds = new HashSet<>();
        Set<String> names = new HashSet<>();
        Set<Integer> attributes = new HashSet<>();
        for (PostgresColumnBinding column : columns) {
            if (column == null || !fieldIds.add(column.fieldId())
                    || !names.add(column.columnName())
                    || !attributes.add(column.attributeNumber())) {
                throw invalid();
            }
            columnCopy.add(column);
        }
        List<Integer> keyCopy = new ArrayList<>(
                primaryKeyAttributeNumbers.size());
        Set<Integer> keys = new HashSet<>();
        for (Integer attribute : primaryKeyAttributeNumbers) {
            PostgresColumnBinding column = find(columnCopy, attribute);
            if (attribute == null || column == null || column.nullable()
                    || !keys.add(attribute)) {
                throw invalid();
            }
            keyCopy.add(attribute);
        }
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableOid = tableOid;
        this.replicaIdentity = replicaIdentity;
        this.replicaIdentityIndexOid = replicaIdentityIndexOid;
        this.columns = Collections.unmodifiableList(columnCopy);
        this.primaryKeyAttributeNumbers =
                Collections.unmodifiableList(keyCopy);
    }

    private static PostgresColumnBinding find(
            List<PostgresColumnBinding> columns, Integer attribute) {
        if (attribute == null) return null;
        for (PostgresColumnBinding column : columns) {
            if (column.attributeNumber() == attribute) return column;
        }
        return null;
    }

    public String schemaName() { return schemaName; }
    public String tableName() { return tableName; }
    public long tableOid() { return tableOid; }
    public PostgresReplicaIdentity replicaIdentity() {
        return replicaIdentity;
    }
    public long replicaIdentityIndexOid() {
        return replicaIdentityIndexOid;
    }
    public List<PostgresColumnBinding> columns() { return columns; }
    public List<Integer> primaryKeyAttributeNumbers() {
        return primaryKeyAttributeNumbers;
    }

    @Override public String toString() {
        return "PostgresTableBinding{oid=" + tableOid
                + ",columns=" + columns.size() + '}';
    }

    private static IllegalArgumentException invalid() {
        return PostgresCodecSupport.invalid(CODE);
    }
}
