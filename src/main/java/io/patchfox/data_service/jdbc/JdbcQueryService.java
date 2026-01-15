package io.patchfox.data_service.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import io.patchfox.data_service.dto.DatasetDTO;
import io.patchfox.data_service.dto.DatasetMetricsDTO;
import io.patchfox.data_service.dto.DatasourceDTO;
import io.patchfox.data_service.dto.DatasourceEventDTO;
import io.patchfox.data_service.dto.DatasourceMetricsDTO;
import io.patchfox.data_service.dto.DatasourceMetricsCurrentDTO;
import io.patchfox.data_service.dto.EditDTO;
import io.patchfox.data_service.dto.FindingDTO;
import io.patchfox.data_service.dto.FindingDataDTO;
import io.patchfox.data_service.dto.FindingReporterDTO;
import io.patchfox.data_service.dto.PackageDTO;
import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.db_entities.entities.DatasetMetrics;
import io.patchfox.db_entities.entities.Datasource;
import io.patchfox.db_entities.entities.DatasourceEvent;
import io.patchfox.db_entities.entities.DatasourceMetrics;
import io.patchfox.db_entities.entities.DatasourceMetricsCurrent;
import io.patchfox.db_entities.entities.Edit;
import io.patchfox.db_entities.entities.FindingData;
import lombok.extern.slf4j.Slf4j;

/**
 * JDBC-based query service that bypasses Hibernate entirely.
 *
 * This service translates QueryDSL-style parameters to raw SQL and executes
 * via JdbcTemplate, returning DTOs instead of JPA entities.
 *
 * Benefits:
 * - No @Fetch(FetchMode.JOIN) relationship explosions
 * - No lazy loading cascades
 * - No Hibernate session management overhead
 * - Predictable, flat SQL queries
 * - Controlled relationship loading (e.g., datasources without packageIndexes)
 */
@Slf4j
@Service
public class JdbcQueryService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // Table metadata registry
    private static final Map<String, TableMetadata> TABLES = new HashMap<>();

    static {
        // Dataset table metadata
        TABLES.put("dataset", new TableMetadata(
            "dataset",
            Dataset.class,
            Map.of(
                "id", "id",
                "latestTxid", "latest_txid",
                "latestJobId", "latest_job_id",
                "name", "name",
                "updatedAt", "updated_at",
                "status", "status"
            )
        ));

        // Datasource table metadata (without edits and packageIndexes)
        TABLES.put("datasource", new TableMetadata(
            "datasource",
            Datasource.class,
            Map.ofEntries(
                Map.entry("id", "id"),
                Map.entry("latestTxid", "latest_txid"),
                Map.entry("latestJobId", "latest_job_id"),
                Map.entry("purl", "purl"),
                Map.entry("domain", "domain"),
                Map.entry("name", "name"),
                Map.entry("commitBranch", "commit_branch"),
                Map.entry("type", "type"),
                Map.entry("numberEventsReceived", "number_events_received"),
                Map.entry("numberEventProcessingErrors", "number_event_processing_errors"),
                Map.entry("firstEventReceivedAt", "first_event_received_at"),
                Map.entry("lastEventReceivedAt", "last_event_received_at"),
                Map.entry("lastEventReceivedStatus", "last_event_received_status"),
                Map.entry("status", "status")
                // NOTE: edits and packageIndexes deliberately excluded
            )
        ));

        // Package table metadata (without findings and datasourceEvents)
        TABLES.put("package", new TableMetadata(
            "package",
            io.patchfox.db_entities.entities.Package.class,
            Map.ofEntries(
                Map.entry("id", "id"),
                Map.entry("purl", "purl"),
                Map.entry("type", "type"),
                Map.entry("namespace", "namespace"),
                Map.entry("name", "name"),
                Map.entry("version", "version"),
                Map.entry("numberVersionsBehindHead", "number_versions_behind_head"),
                Map.entry("numberMajorVersionsBehindHead", "number_major_versions_behind_head"),
                Map.entry("numberMinorVersionsBehindHead", "number_minor_versions_behind_head"),
                Map.entry("numberPatchVersionsBehindHead", "number_patch_versions_behind_head"),
                Map.entry("mostRecentVersion", "most_recent_version"),
                Map.entry("mostRecentVersionPublishedAt", "most_recent_version_published_at"),
                Map.entry("thisVersionPublishedAt", "this_version_published_at"),
                Map.entry("updatedAt", "updated_at")
                // NOTE: findings, criticalFindings, highFindings, mediumFindings, lowFindings,
                // and datasourceEvents deliberately excluded - these cause the cascade explosion
            )
        ));

        // Finding table metadata (without packages and reporters)
        // Note: Finding queries JOIN with finding_data for full info
        TABLES.put("finding", new TableMetadata(
            "finding",
            io.patchfox.db_entities.entities.Finding.class,
            Map.of(
                "id", "f.id",
                "identifier", "f.identifier"
                // NOTE: packages and reporters deliberately excluded - these cause the cascade explosion
                // Finding queries use a JOIN with finding_data to get severity, description, etc.
            )
        ));

        // FindingReporter table metadata (without findings)
        TABLES.put("findingreporter", new TableMetadata(
            "finding_reporter",
            io.patchfox.db_entities.entities.FindingReporter.class,
            Map.of(
                "id", "id",
                "name", "name"
                // NOTE: findings deliberately excluded - causes cascade back to Package
            )
        ));

        // DatasourceEvent table metadata (without packages)
        TABLES.put("datasourceevent", new TableMetadata(
            "datasource_event",
            DatasourceEvent.class,
            Map.ofEntries(
                Map.entry("id", "id"),
                Map.entry("purl", "purl"),
                Map.entry("txid", "txid"),
                Map.entry("jobId", "job_id"),
                Map.entry("commitHash", "commit_hash"),
                Map.entry("commitBranch", "commit_branch"),
                Map.entry("commitDateTime", "commit_date_time"),
                Map.entry("eventDateTime", "event_date_time"),
                Map.entry("status", "status"),
                Map.entry("processingError", "processing_error"),
                Map.entry("ossEnriched", "oss_enriched"),
                Map.entry("packageIndexEnriched", "package_index_enriched"),
                Map.entry("analyzed", "analyzed"),
                Map.entry("forecasted", "forecasted"),
                Map.entry("recommended", "recommended"),
                Map.entry("datasourceId", "datasource_id")
                // NOTE: packages deliberately excluded - causes cascade to Package -> Finding death spiral
                // NOTE: payload is included in SELECT but not in field mappings (not used for filtering)
            )
        ));

        // DatasetMetrics table metadata (without edits)
        TABLES.put("datasetmetrics", new TableMetadata(
            "dataset_metrics",
            DatasetMetrics.class,
            Map.ofEntries(
                Map.entry("id", "id"),
                Map.entry("datasetId", "dataset_id"),
                Map.entry("dataset.id", "dataset_id"),
                Map.entry("dataset.name", "dataset_id"), // Note: requires JOIN for name filtering
                Map.entry("txid", "txid"),
                Map.entry("jobId", "job_id"),
                Map.entry("commitDateTime", "commit_date_time"),
                Map.entry("eventDateTime", "event_date_time"),
                Map.entry("forecastMaturityDate", "forecast_maturity_date"),
                Map.entry("datasourceCount", "datasource_count"),
                Map.entry("datasourceEventCount", "datasource_event_count"),
                Map.entry("isCurrent", "is_current"),
                Map.entry("isForecastSameCourse", "is_forecast_same_course"),
                Map.entry("isForecastRecommendationsTaken", "is_forecast_recommendations_taken"),
                Map.entry("recommendationType", "recommendation_type"),
                Map.entry("recommendationHeadline", "recommendation_headline"),
                Map.entry("rpsScore", "rps_score")
                // NOTE: edits deliberately excluded - causes cascade explosion
                // NOTE: many more scalar fields exist but not needed for filtering
            )
        ));

        // Edit table metadata (without datasetMetrics and datasource relationships)
        TABLES.put("edit", new TableMetadata(
            "edit",
            Edit.class,
            Map.ofEntries(
                Map.entry("id", "id"),
                Map.entry("datasetMetricsId", "dataset_metrics_id"),
                Map.entry("datasourceId", "datasource_id"),
                Map.entry("commitDateTime", "commit_date_time"),
                Map.entry("eventDateTime", "event_date_time"),
                Map.entry("editType", "edit_type"),
                Map.entry("before", "before"),
                Map.entry("after", "after"),
                Map.entry("isSameEdit", "is_same_edit"),
                Map.entry("sameEditCount", "same_edit_count"),
                Map.entry("isPfRecommendedEdit", "is_pf_recommended_edit"),
                Map.entry("isUserEdit", "is_user_edit")
                // NOTE: datasetMetrics and datasource relationships excluded
            )
        ));

        // FindingData table metadata (without finding relationship)
        TABLES.put("findingdata", new TableMetadata(
            "finding_data",
            FindingData.class,
            Map.ofEntries(
                Map.entry("id", "id"),
                Map.entry("findingId", "finding_id"),
                Map.entry("identifier", "identifier"),
                Map.entry("severity", "severity"),
                Map.entry("description", "description"),
                Map.entry("reportedAt", "reported_at"),
                Map.entry("publishedAt", "published_at")
                // NOTE: finding relationship excluded - causes cascade to packages
            )
        ));

        // DatasourceMetrics table metadata
        TABLES.put("datasourcemetrics", new TableMetadata(
            "datasource_metrics",
            DatasourceMetrics.class,
            Map.ofEntries(
                Map.entry("id", "id"),
                Map.entry("datasourceEventCount", "datasource_event_count"),
                Map.entry("commitDateTime", "commit_date_time"),
                Map.entry("eventDateTime", "event_date_time"),
                Map.entry("txid", "txid"),
                Map.entry("jobId", "job_id"),
                Map.entry("purl", "purl"),
                Map.entry("totalFindings", "total_findings"),
                Map.entry("criticalFindings", "critical_findings"),
                Map.entry("highFindings", "high_findings"),
                Map.entry("mediumFindings", "medium_findings"),
                Map.entry("lowFindings", "low_findings"),
                Map.entry("packages", "packages"),
                Map.entry("packagesWithFindings", "packages_with_findings"),
                Map.entry("downlevelPackages", "downlevel_packages"),
                Map.entry("stalePackages", "stale_packages"),
                Map.entry("patches", "patches"),
                Map.entry("patchEfficacyScore", "patch_efficacy_score")
                // NOTE: scalar fields only - no relationships
            )
        ));

        // DatasourceMetricsCurrent table metadata
        TABLES.put("datasourcemetricscurrent", new TableMetadata(
            "datasource_metrics_current",
            DatasourceMetricsCurrent.class,
            Map.ofEntries(
                Map.entry("id", "id"),
                Map.entry("datasourceEventCount", "datasource_event_count"),
                Map.entry("commitDateTime", "commit_date_time"),
                Map.entry("eventDateTime", "event_date_time"),
                Map.entry("txid", "txid"),
                Map.entry("jobId", "job_id"),
                Map.entry("purl", "purl"),
                Map.entry("totalFindings", "total_findings"),
                Map.entry("criticalFindings", "critical_findings"),
                Map.entry("highFindings", "high_findings"),
                Map.entry("mediumFindings", "medium_findings"),
                Map.entry("lowFindings", "low_findings"),
                Map.entry("packages", "packages"),
                Map.entry("packagesWithFindings", "packages_with_findings"),
                Map.entry("downlevelPackages", "downlevel_packages"),
                Map.entry("stalePackages", "stale_packages"),
                Map.entry("patches", "patches")
                // NOTE: scalar fields only - no relationships
            )
        ));
    }

    /**
     * Execute a query for Dataset, including related Datasources (without heavy fields).
     */
    public Page<DatasetDTO> queryDataset(Map<String, String> params, Pageable pageable) {
        TableMetadata meta = TABLES.get("dataset");

        // Build WHERE clause
        SqlWhereBuilder whereBuilder = new SqlWhereBuilder(meta);
        String whereClause = whereBuilder.build(params);

        // Build ORDER BY
        String orderBy = buildOrderBy(pageable, meta);

        // Count query
        String countSql = buildCountSql("dataset", whereClause);
        log.debug("Dataset count SQL: {}", countSql);

        Long total = jdbcTemplate.queryForObject(countSql, Long.class);
        if (total == null || total == 0) {
            return new PageImpl<>(new ArrayList<>(), pageable, 0);
        }

        // Data query for datasets
        String dataSql = buildDataSql(
            DatasetDTO.SELECT_COLUMNS,
            "dataset",
            whereClause,
            orderBy,
            pageable
        );
        log.info("Dataset data SQL: {}", dataSql);

        List<DatasetDTO> datasets = jdbcTemplate.query(dataSql, DatasetDTO.ROW_MAPPER);

        if (datasets.isEmpty()) {
            return new PageImpl<>(datasets, pageable, total);
        }

        // Load datasources for all datasets in one query
        loadDatasourcesForDatasets(datasets);

        return new PageImpl<>(datasets, pageable, total);
    }

    /**
     * Load datasources for a list of datasets using a single efficient query.
     * Excludes heavy fields (edits, packageIndexes).
     */
    private void loadDatasourcesForDatasets(List<DatasetDTO> datasets) {
        if (datasets.isEmpty()) {
            return;
        }

        // Get all dataset IDs
        List<Long> datasetIds = datasets.stream()
            .map(DatasetDTO::getId)
            .collect(Collectors.toList());

        String idList = datasetIds.stream()
            .map(String::valueOf)
            .collect(Collectors.joining(", "));

        // Query to get datasources with their dataset associations
        // Join through datasource_dataset to get the mapping
        String sql = String.format(
            "SELECT ds.%s, dd.dataset_id " +
            "FROM datasource ds " +
            "JOIN datasource_dataset dd ON ds.id = dd.datasource_id " +
            "WHERE dd.dataset_id IN (%s) " +
            "ORDER BY ds.name",
            DatasourceDTO.SELECT_COLUMNS,
            idList
        );

        log.info("Datasources SQL: {}", sql);

        // Map to group datasources by dataset_id
        Map<Long, List<DatasourceDTO>> datasourcesByDatasetId = new HashMap<>();
        for (Long id : datasetIds) {
            datasourcesByDatasetId.put(id, new ArrayList<>());
        }

        // Execute query and populate the map
        jdbcTemplate.query(sql, (rs, rowNum) -> {
            DatasourceDTO dto = DatasourceDTO.ROW_MAPPER.mapRow(rs, rowNum);
            Long datasetId = rs.getLong("dataset_id");
            datasourcesByDatasetId.get(datasetId).add(dto);
            return dto;
        });

        // Attach datasources to each dataset
        for (DatasetDTO dataset : datasets) {
            List<DatasourceDTO> dsForDataset = datasourcesByDatasetId.get(dataset.getId());
            if (dsForDataset != null) {
                dataset.setDatasources(dsForDataset);
            }
        }

        log.info("Loaded datasources for {} datasets", datasets.size());
    }

    /**
     * Execute a query for Finding, JOINed with FindingData for full info.
     */
    public Page<FindingDTO> queryFinding(Map<String, String> params, Pageable pageable) {
        TableMetadata meta = TABLES.get("finding");

        // Build WHERE clause
        SqlWhereBuilder whereBuilder = new SqlWhereBuilder(meta);
        String whereClause = whereBuilder.build(params);

        // Build ORDER BY
        String orderBy = buildOrderByWithAlias(pageable, meta, "f");

        // Count query (just the finding table)
        String countSql = "SELECT COUNT(*) FROM finding f" +
            (whereClause.isEmpty() ? "" : " WHERE " + whereClause);
        log.debug("Finding count SQL: {}", countSql);

        Long total = jdbcTemplate.queryForObject(countSql, Long.class);
        if (total == null || total == 0) {
            return new PageImpl<>(new ArrayList<>(), pageable, 0);
        }

        // Data query with JOIN to finding_data
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(FindingDTO.SELECT_COLUMNS);
        sb.append(" FROM ").append(FindingDTO.FROM_CLAUSE);
        if (!whereClause.isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        if (!orderBy.isEmpty()) {
            sb.append(" ").append(orderBy);
        }
        if (pageable != null && !pageable.isUnpaged()) {
            sb.append(" LIMIT ").append(pageable.getPageSize());
            sb.append(" OFFSET ").append(pageable.getOffset());
        }

        String dataSql = sb.toString();
        log.info("Finding data SQL: {}", dataSql);

        List<FindingDTO> findings = jdbcTemplate.query(dataSql, FindingDTO.ROW_MAPPER);
        return new PageImpl<>(findings, pageable, total);
    }

    /**
     * Build ORDER BY with table alias prefix.
     */
    private String buildOrderByWithAlias(Pageable pageable, TableMetadata meta, String alias) {
        if (pageable == null || pageable.getSort().isUnsorted()) {
            return "";
        }

        List<String> orderClauses = new ArrayList<>();
        for (Sort.Order order : pageable.getSort()) {
            String property = order.getProperty();
            // The column already has alias from metadata
            String column = meta.getColumn(property);

            if (column == null) {
                log.warn("Unknown sort field {} for table {}, skipping", property, meta.getTableName());
                continue;
            }

            String direction = order.isAscending() ? "ASC" : "DESC";
            orderClauses.add(column + " " + direction);
        }

        if (orderClauses.isEmpty()) {
            return "";
        }

        return "ORDER BY " + String.join(", ", orderClauses);
    }

    /**
     * Generic query method - delegates to specific implementations.
     */
    @SuppressWarnings("unchecked")
    public <T> Page<T> query(String tableName, Map<String, String> params, Pageable pageable) {
        String table = tableName.toLowerCase();

        if ("dataset".equals(table)) {
            return (Page<T>) queryDataset(params, pageable);
        }

        if ("datasetmetrics".equals(table)) {
            return (Page<T>) queryDatasetMetrics(params, pageable);
        }

        if ("finding".equals(table)) {
            return (Page<T>) queryFinding(params, pageable);
        }

        // For other tables, use simple query without relationship loading
        return querySimple(tableName, params, pageable);
    }

    /**
     * Simple query for tables without relationship loading.
     */
    @SuppressWarnings("unchecked")
    private <T> Page<T> querySimple(String tableName, Map<String, String> params, Pageable pageable) {
        TableMetadata meta = TABLES.get(tableName.toLowerCase());
        if (meta == null) {
            throw new IllegalArgumentException("Unknown table: " + tableName);
        }

        RowMapper<T> rowMapper = getRowMapper(tableName);

        SqlWhereBuilder whereBuilder = new SqlWhereBuilder(meta);
        String whereClause = whereBuilder.build(params);
        String orderBy = buildOrderBy(pageable, meta);

        String countSql = buildCountSql(meta.getTableName(), whereClause);
        Long total = jdbcTemplate.queryForObject(countSql, Long.class);
        if (total == null) {
            total = 0L;
        }

        if (total == 0) {
            return new PageImpl<>(new ArrayList<>(), pageable, 0);
        }

        String dataSql = buildDataSql(
            getSelectColumns(tableName),
            meta.getTableName(),
            whereClause,
            orderBy,
            pageable
        );
        log.info("Data SQL: {}", dataSql);

        List<T> content = jdbcTemplate.query(dataSql, rowMapper);
        return new PageImpl<>(content, pageable, total);
    }

    @SuppressWarnings("unchecked")
    private <T> RowMapper<T> getRowMapper(String tableName) {
        return switch (tableName.toLowerCase()) {
            case "dataset" -> (RowMapper<T>) DatasetDTO.ROW_MAPPER;
            case "datasetmetrics" -> (RowMapper<T>) DatasetMetricsDTO.ROW_MAPPER;
            case "datasource" -> (RowMapper<T>) DatasourceDTO.ROW_MAPPER;
            case "datasourceevent" -> (RowMapper<T>) DatasourceEventDTO.ROW_MAPPER;
            case "datasourcemetrics" -> (RowMapper<T>) DatasourceMetricsDTO.ROW_MAPPER;
            case "datasourcemetricscurrent" -> (RowMapper<T>) DatasourceMetricsCurrentDTO.ROW_MAPPER;
            case "edit" -> (RowMapper<T>) EditDTO.ROW_MAPPER;
            case "package" -> (RowMapper<T>) PackageDTO.ROW_MAPPER;
            case "finding" -> (RowMapper<T>) FindingDTO.ROW_MAPPER;
            case "findingdata" -> (RowMapper<T>) FindingDataDTO.ROW_MAPPER;
            case "findingreporter" -> (RowMapper<T>) FindingReporterDTO.ROW_MAPPER;
            default -> throw new IllegalArgumentException("No RowMapper for table: " + tableName);
        };
    }

    private String getSelectColumns(String tableName) {
        return switch (tableName.toLowerCase()) {
            case "dataset" -> DatasetDTO.SELECT_COLUMNS;
            case "datasetmetrics" -> DatasetMetricsDTO.SELECT_COLUMNS;
            case "datasource" -> DatasourceDTO.SELECT_COLUMNS;
            case "datasourceevent" -> DatasourceEventDTO.SELECT_COLUMNS;
            case "datasourcemetrics" -> DatasourceMetricsDTO.SELECT_COLUMNS;
            case "datasourcemetricscurrent" -> DatasourceMetricsCurrentDTO.SELECT_COLUMNS;
            case "edit" -> EditDTO.SELECT_COLUMNS;
            case "package" -> PackageDTO.SELECT_COLUMNS;
            case "finding" -> FindingDTO.SELECT_COLUMNS;
            case "findingdata" -> FindingDataDTO.SELECT_COLUMNS;
            case "findingreporter" -> FindingReporterDTO.SELECT_COLUMNS;
            default -> throw new IllegalArgumentException("No SELECT columns for table: " + tableName);
        };
    }

    /**
     * Check if this service supports a given table.
     */
    public boolean supportsTable(String tableName) {
        return TABLES.containsKey(tableName.toLowerCase());
    }

    /**
     * Load package IDs for a list of datasource event IDs.
     * Uses the datasource_event_package join table directly.
     */
    public Map<Long, List<Long>> getPackageIdsForDatasourceEvents(List<Long> datasourceEventIds) {
        if (datasourceEventIds == null || datasourceEventIds.isEmpty()) {
            return new HashMap<>();
        }

        String idList = datasourceEventIds.stream()
            .map(String::valueOf)
            .collect(Collectors.joining(", "));

        String sql = String.format(
            "SELECT datasource_event_id, package_id FROM datasource_event_package " +
            "WHERE datasource_event_id IN (%s)",
            idList
        );

        Map<Long, List<Long>> result = new HashMap<>();
        for (Long id : datasourceEventIds) {
            result.put(id, new ArrayList<>());
        }

        jdbcTemplate.query(sql, (rs, rowNum) -> {
            Long eventId = rs.getLong("datasource_event_id");
            Long packageId = rs.getLong("package_id");
            result.get(eventId).add(packageId);
            return null;
        });

        return result;
    }

    /**
     * Execute a query for DatasetMetrics with JOIN to dataset for name filtering.
     */
    public Page<DatasetMetricsDTO> queryDatasetMetrics(Map<String, String> params, Pageable pageable) {
        TableMetadata meta = TABLES.get("datasetmetrics");

        // Check if we need dataset.name filtering
        String datasetName = params.remove("dataset.name");

        // Build WHERE clause
        SqlWhereBuilder whereBuilder = new SqlWhereBuilder(meta);
        String whereClause = whereBuilder.build(params);

        // If dataset.name is specified, we need to JOIN and filter
        String fromClause = "dataset_metrics dm";
        String additionalWhere = "";
        if (datasetName != null && !datasetName.isEmpty()) {
            fromClause = "dataset_metrics dm JOIN dataset d ON dm.dataset_id = d.id";
            // Handle multiple dataset names (comma-separated)
            String[] names = datasetName.split(",");
            if (names.length == 1) {
                additionalWhere = "d.name = '" + names[0].trim().replace("'", "''") + "'";
            } else {
                String inClause = Arrays.stream(names)
                    .map(n -> "'" + n.trim().replace("'", "''") + "'")
                    .collect(Collectors.joining(", "));
                additionalWhere = "d.name IN (" + inClause + ")";
            }
        }

        // Combine where clauses
        String fullWhere = "";
        if (!whereClause.isEmpty() && !additionalWhere.isEmpty()) {
            fullWhere = whereClause + " AND " + additionalWhere;
        } else if (!whereClause.isEmpty()) {
            fullWhere = whereClause;
        } else if (!additionalWhere.isEmpty()) {
            fullWhere = additionalWhere;
        }

        // Build ORDER BY
        String orderBy = buildOrderBy(pageable, meta);

        // Count query
        String countSql = "SELECT COUNT(*) FROM " + fromClause +
            (fullWhere.isEmpty() ? "" : " WHERE " + fullWhere);
        log.debug("DatasetMetrics count SQL: {}", countSql);

        Long total = jdbcTemplate.queryForObject(countSql, Long.class);
        if (total == null || total == 0) {
            return new PageImpl<>(new ArrayList<>(), pageable, 0);
        }

        // Data query - need to prefix columns with dm.
        String selectColumns = "dm." + DatasetMetricsDTO.SELECT_COLUMNS.replace(", ", ", dm.");
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(selectColumns);
        sb.append(" FROM ").append(fromClause);
        if (!fullWhere.isEmpty()) {
            sb.append(" WHERE ").append(fullWhere);
        }
        if (!orderBy.isEmpty()) {
            sb.append(" ").append(orderBy.replace("commit_date_time", "dm.commit_date_time"));
        }
        if (pageable != null && !pageable.isUnpaged()) {
            sb.append(" LIMIT ").append(pageable.getPageSize());
            sb.append(" OFFSET ").append(pageable.getOffset());
        }

        String dataSql = sb.toString();
        log.info("DatasetMetrics data SQL: {}", dataSql);

        List<DatasetMetricsDTO> results = jdbcTemplate.query(dataSql, DatasetMetricsDTO.ROW_MAPPER);
        return new PageImpl<>(results, pageable, total);
    }

    /**
     * Load datasource purls for a dataset ID.
     * Uses datasource_dataset join table.
     */
    public List<String> getDatasourcePurlsForDataset(Long datasetId) {
        if (datasetId == null) {
            return new ArrayList<>();
        }

        String sql = "SELECT ds.purl FROM datasource ds " +
            "JOIN datasource_dataset dd ON ds.id = dd.datasource_id " +
            "WHERE dd.dataset_id = " + datasetId;

        return jdbcTemplate.queryForList(sql, String.class);
    }

    /**
     * Load edit info (ID and datasource purl) for dataset metrics IDs.
     * Used by internal methods that need to filter by datasource purl.
     */
    public List<EditWithDatasourcePurl> getEditsWithDatasourcePurl(List<Long> datasetMetricsIds) {
        if (datasetMetricsIds == null || datasetMetricsIds.isEmpty()) {
            return new ArrayList<>();
        }

        String idList = datasetMetricsIds.stream()
            .map(String::valueOf)
            .collect(Collectors.joining(", "));

        String sql = String.format(
            "SELECT e.id, e.dataset_metrics_id, e.commit_date_time, ds.purl as datasource_purl " +
            "FROM edit e " +
            "JOIN datasource ds ON e.datasource_id = ds.id " +
            "WHERE e.dataset_metrics_id IN (%s)",
            idList
        );

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            EditWithDatasourcePurl ewp = new EditWithDatasourcePurl();
            ewp.editId = rs.getLong("id");
            ewp.datasetMetricsId = rs.getLong("dataset_metrics_id");
            ewp.datasourcePurl = rs.getString("datasource_purl");
            java.time.OffsetDateTime cdt = rs.getObject("commit_date_time", java.time.OffsetDateTime.class);
            if (cdt != null) {
                ewp.commitDateTime = cdt.atZoneSameInstant(java.time.ZoneOffset.UTC);
            }
            return ewp;
        });
    }

    /**
     * Simple holder for edit info with datasource purl.
     */
    public static class EditWithDatasourcePurl {
        public Long editId;
        public Long datasetMetricsId;
        public String datasourcePurl;
        public java.time.ZonedDateTime commitDateTime;
    }

    /**
     * Load edit IDs for a list of dataset metrics IDs.
     */
    public Map<Long, List<Long>> getEditIdsForDatasetMetrics(List<Long> datasetMetricsIds) {
        if (datasetMetricsIds == null || datasetMetricsIds.isEmpty()) {
            return new HashMap<>();
        }

        String idList = datasetMetricsIds.stream()
            .map(String::valueOf)
            .collect(Collectors.joining(", "));

        String sql = String.format(
            "SELECT dataset_metrics_id, id FROM edit WHERE dataset_metrics_id IN (%s)",
            idList
        );

        Map<Long, List<Long>> result = new HashMap<>();
        for (Long id : datasetMetricsIds) {
            result.put(id, new ArrayList<>());
        }

        jdbcTemplate.query(sql, (rs, rowNum) -> {
            Long dsmId = rs.getLong("dataset_metrics_id");
            Long editId = rs.getLong("id");
            result.get(dsmId).add(editId);
            return null;
        });

        return result;
    }

    /**
     * Load package purls for a list of datasource event IDs.
     * JOINs datasource_event_package with package table.
     */
    public Map<Long, List<String>> getPackagePurlsForDatasourceEvents(List<Long> datasourceEventIds) {
        if (datasourceEventIds == null || datasourceEventIds.isEmpty()) {
            return new HashMap<>();
        }

        String idList = datasourceEventIds.stream()
            .map(String::valueOf)
            .collect(Collectors.joining(", "));

        String sql = String.format(
            "SELECT dep.datasource_event_id, p.purl FROM datasource_event_package dep " +
            "JOIN package p ON dep.package_id = p.id " +
            "WHERE dep.datasource_event_id IN (%s)",
            idList
        );

        Map<Long, List<String>> result = new HashMap<>();
        for (Long id : datasourceEventIds) {
            result.put(id, new ArrayList<>());
        }

        jdbcTemplate.query(sql, (rs, rowNum) -> {
            Long eventId = rs.getLong("datasource_event_id");
            String purl = rs.getString("purl");
            result.get(eventId).add(purl);
            return null;
        });

        return result;
    }

    private String buildCountSql(String tableName, String whereClause) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*) FROM ").append(tableName);
        if (whereClause != null && !whereClause.isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
    }

    private String buildDataSql(
        String selectColumns,
        String tableName,
        String whereClause,
        String orderBy,
        Pageable pageable
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(selectColumns);
        sb.append(" FROM ").append(tableName);

        if (whereClause != null && !whereClause.isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }

        if (orderBy != null && !orderBy.isEmpty()) {
            sb.append(" ").append(orderBy);
        }

        if (pageable != null && !pageable.isUnpaged()) {
            sb.append(" LIMIT ").append(pageable.getPageSize());
            sb.append(" OFFSET ").append(pageable.getOffset());
        }

        return sb.toString();
    }

    private String buildOrderBy(Pageable pageable, TableMetadata meta) {
        if (pageable == null || pageable.getSort().isUnsorted()) {
            return "";
        }

        List<String> orderClauses = new ArrayList<>();
        for (Sort.Order order : pageable.getSort()) {
            String property = order.getProperty();
            String column = meta.getColumn(property);

            if (column == null) {
                log.warn("Unknown sort field {} for table {}, skipping", property, meta.getTableName());
                continue;
            }

            String direction = order.isAscending() ? "ASC" : "DESC";
            orderClauses.add(column + " " + direction);
        }

        if (orderClauses.isEmpty()) {
            return "";
        }

        return "ORDER BY " + String.join(", ", orderClauses);
    }
}
