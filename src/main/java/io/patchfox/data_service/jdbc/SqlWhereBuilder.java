package io.patchfox.data_service.jdbc;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

/**
 * Translates QueryDSL-style query parameters to SQL WHERE clauses.
 *
 * Supports:
 * - Operators: gt., gte., lt., lte., eq. (default is equals or ILIKE for strings)
 * - Multiple values: comma-separated values become IN clauses
 * - Types: String, Long, Integer, Double, Boolean, ZonedDateTime, UUID, Enums
 */
@Slf4j
public class SqlWhereBuilder {

    private static final Set<String> SKIP_PARAMS = Set.of("sort", "size", "page", "select");

    private final TableMetadata meta;
    private final List<String> conditions = new ArrayList<>();
    private final List<Object> parameters = new ArrayList<>();

    public SqlWhereBuilder(TableMetadata meta) {
        this.meta = meta;
    }

    /**
     * Build WHERE clause from query parameters.
     * Returns empty string if no conditions.
     */
    public String build(Map<String, String> params) {
        conditions.clear();
        parameters.clear();

        for (Map.Entry<String, String> entry : params.entrySet()) {
            String fieldName = entry.getKey();
            String value = entry.getValue();

            if (value == null || value.trim().isEmpty()) {
                continue;
            }

            if (SKIP_PARAMS.contains(fieldName)) {
                continue;
            }

            // Skip nested fields for now - they need JOIN support
            if (fieldName.contains(".")) {
                log.debug("Skipping nested field {} - not supported in JDBC mode", fieldName);
                continue;
            }

            String column = meta.getColumn(fieldName);
            if (column == null) {
                log.warn("Unknown field {} for table {}, skipping", fieldName, meta.getTableName());
                continue;
            }

            Class<?> fieldType = meta.getFieldType(fieldName);
            if (fieldType == null) {
                log.warn("Unknown type for field {}, skipping", fieldName);
                continue;
            }

            try {
                if (value.contains(",")) {
                    conditions.add(buildInClause(column, value, fieldType));
                } else {
                    conditions.add(buildCondition(column, value, fieldType));
                }
            } catch (Exception e) {
                log.error("Error building condition for field {} with value {}: {}",
                    fieldName, value, e.getMessage());
                throw new IllegalArgumentException(
                    "Invalid value '" + value + "' for field '" + fieldName + "'", e);
            }
        }

        if (conditions.isEmpty()) {
            return "";
        }

        return String.join(" AND ", conditions);
    }

    /**
     * Build a single condition with operator support.
     */
    private String buildCondition(String column, String value, Class<?> type) {
        String op = "=";
        String val = value;

        // Parse operator prefix
        if (value.startsWith("gt.")) {
            op = ">";
            val = value.substring(3);
        } else if (value.startsWith("gte.")) {
            op = ">=";
            val = value.substring(4);
        } else if (value.startsWith("lt.")) {
            op = "<";
            val = value.substring(3);
        } else if (value.startsWith("lte.")) {
            op = "<=";
            val = value.substring(4);
        } else if (value.startsWith("eq.")) {
            op = "=";
            val = value.substring(3);
        }

        // Format based on type
        if (type == String.class) {
            if (op.equals("=") && !value.startsWith("eq.")) {
                // Default string behavior: case-insensitive contains (matches QueryDslHelpers)
                return String.format("%s ILIKE '%%%s%%'", column, escapeSql(val));
            }
            return String.format("%s %s '%s'", column, op, escapeSql(val));
        } else if (type == ZonedDateTime.class) {
            return String.format("%s %s '%s'::timestamptz", column, op, val);
        } else if (type == UUID.class) {
            return String.format("%s %s '%s'::uuid", column, op, val);
        } else if (type.isEnum()) {
            return String.format("%s %s '%s'", column, op, escapeSql(val));
        } else if (type == Boolean.class || type == boolean.class) {
            return String.format("%s %s %s", column, op, Boolean.parseBoolean(val));
        } else if (type == Long.class || type == long.class) {
            return String.format("%s %s %d", column, op, Long.parseLong(val));
        } else if (type == Integer.class || type == int.class) {
            return String.format("%s %s %d", column, op, Integer.parseInt(val));
        } else if (type == Double.class || type == double.class) {
            return String.format("%s %s %s", column, op, Double.parseDouble(val));
        } else {
            // Fallback for other numeric types
            return String.format("%s %s %s", column, op, val);
        }
    }

    /**
     * Build IN clause for comma-separated values.
     */
    private String buildInClause(String column, String value, Class<?> type) {
        String[] values = value.split(",");

        List<String> formatted = Arrays.stream(values)
            .map(String::trim)
            .filter(v -> !v.isEmpty())
            .map(v -> v.startsWith("eq.") ? v.substring(3) : v)
            .map(v -> formatValueForIn(v, type))
            .collect(Collectors.toList());

        if (formatted.isEmpty()) {
            // Return a condition that's always false if no values
            return "1 = 0";
        }

        return String.format("%s IN (%s)", column, String.join(", ", formatted));
    }

    /**
     * Format a single value for use in IN clause.
     */
    private String formatValueForIn(String val, Class<?> type) {
        if (type == String.class || type.isEnum()) {
            return "'" + escapeSql(val) + "'";
        } else if (type == ZonedDateTime.class) {
            return "'" + val + "'::timestamptz";
        } else if (type == UUID.class) {
            return "'" + val + "'::uuid";
        } else if (type == Boolean.class || type == boolean.class) {
            return String.valueOf(Boolean.parseBoolean(val));
        } else if (type == Long.class || type == long.class) {
            return String.valueOf(Long.parseLong(val));
        } else if (type == Integer.class || type == int.class) {
            return String.valueOf(Integer.parseInt(val));
        } else if (type == Double.class || type == double.class) {
            return String.valueOf(Double.parseDouble(val));
        }
        return val;
    }

    /**
     * Escape single quotes in SQL strings to prevent SQL injection.
     */
    private String escapeSql(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("'", "''");
    }
}
