package io.patchfox.data_service.jdbc;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Metadata for mapping entity fields to SQL table columns.
 * Used by JdbcQueryService to translate QueryDSL-style params to raw SQL.
 */
@Slf4j
@Getter
public class TableMetadata {

    private final String tableName;
    private final Class<?> entityClass;
    private final Map<String, String> fieldToColumn;
    private final Map<String, Class<?>> fieldTypes;

    public TableMetadata(String tableName, Class<?> entityClass, Map<String, String> fieldToColumn) {
        this.tableName = tableName;
        this.entityClass = entityClass;
        this.fieldToColumn = fieldToColumn;
        this.fieldTypes = buildFieldTypes(entityClass, fieldToColumn);
    }

    private Map<String, Class<?>> buildFieldTypes(Class<?> clazz, Map<String, String> fields) {
        Map<String, Class<?>> types = new HashMap<>();
        for (String fieldName : fields.keySet()) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                types.put(fieldName, field.getType());
            } catch (NoSuchFieldException e) {
                log.warn("Field {} not found in class {}", fieldName, clazz.getSimpleName());
            }
        }
        return types;
    }

    /**
     * Get the SQL column name for an entity field.
     * Supports nested fields like "dataset.name" by returning null (handled separately).
     */
    public String getColumn(String fieldName) {
        if (fieldName.contains(".")) {
            return null; // Nested fields handled separately
        }
        return fieldToColumn.get(fieldName);
    }

    /**
     * Get the Java type for a field.
     */
    public Class<?> getFieldType(String fieldName) {
        return fieldTypes.get(fieldName);
    }

    /**
     * Get comma-separated column names for SELECT clause.
     */
    public String getSelectColumns() {
        return fieldToColumn.values().stream()
            .collect(Collectors.joining(", "));
    }

    /**
     * Check if a field exists in this table's metadata.
     */
    public boolean hasField(String fieldName) {
        return fieldToColumn.containsKey(fieldName);
    }
}
