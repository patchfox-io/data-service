package io.patchfox.data_service.dto;

import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.springframework.jdbc.core.RowMapper;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for Edit entity - scalar fields only.
 * Excludes datasetMetrics and datasource relationships.
 * Includes FK references instead.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EditDTO {

    private Long id;
    private Long datasetMetricsId;
    private Long datasourceId;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime commitDateTime;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime eventDateTime;

    private String editType;
    private String before;
    private String after;

    private boolean isSameEdit;
    private int sameEditCount;
    private boolean isPfRecommendedEdit;
    private boolean isUserEdit;

    private Integer criticalFindings;
    private Integer highFindings;
    private Integer mediumFindings;
    private Integer lowFindings;

    // Recommendation indexes
    private Integer reduceCvesIndex;
    private Integer reduceCveGrowthIndex;
    private Integer reduceCveBacklogIndex;
    private Integer reduceCveBacklogGrowthIndex;
    private Integer reduceStalePackagesIndex;
    private Integer reduceStalePackagesGrowthIndex;
    private Integer reduceDownlevelPackagesIndex;
    private Integer reduceDownlevelPackagesGrowthIndex;
    private Integer growPatchEfficacyIndex;
    private Integer removeRedundantPackagesIndex;

    // Ranks
    private Integer decreaseBacklogRank;
    private Integer decreaseVulnerabilityCountRank;
    private Integer avoidsVulnerabilitiesRank;
    private Integer increaseImpactRank;

    // NO datasetMetrics - would cause cascade
    // NO datasource - would cause cascade

    /**
     * RowMapper for converting JDBC ResultSet to EditDTO.
     */
    public static final RowMapper<EditDTO> ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        EditDTO dto = new EditDTO();
        dto.setId(rs.getLong("id"));

        // Handle nullable FK
        long dsmId = rs.getLong("dataset_metrics_id");
        dto.setDatasetMetricsId(rs.wasNull() ? null : dsmId);

        dto.setDatasourceId(rs.getLong("datasource_id"));

        // Timestamps
        OffsetDateTime commitDateTime = rs.getObject("commit_date_time", OffsetDateTime.class);
        if (commitDateTime != null) {
            dto.setCommitDateTime(commitDateTime.atZoneSameInstant(ZoneOffset.UTC));
        }

        OffsetDateTime eventDateTime = rs.getObject("event_date_time", OffsetDateTime.class);
        if (eventDateTime != null) {
            dto.setEventDateTime(eventDateTime.atZoneSameInstant(ZoneOffset.UTC));
        }

        dto.setEditType(rs.getString("edit_type"));
        dto.setBefore(rs.getString("before"));
        dto.setAfter(rs.getString("after"));

        dto.setSameEdit(rs.getBoolean("is_same_edit"));
        dto.setSameEditCount(rs.getInt("same_edit_count"));
        dto.setPfRecommendedEdit(rs.getBoolean("is_pf_recommended_edit"));
        dto.setUserEdit(rs.getBoolean("is_user_edit"));

        // Nullable Integer fields
        int cf = rs.getInt("critical_findings");
        dto.setCriticalFindings(rs.wasNull() ? null : cf);

        int hf = rs.getInt("high_findings");
        dto.setHighFindings(rs.wasNull() ? null : hf);

        int mf = rs.getInt("medium_findings");
        dto.setMediumFindings(rs.wasNull() ? null : mf);

        int lf = rs.getInt("low_findings");
        dto.setLowFindings(rs.wasNull() ? null : lf);

        // Recommendation indexes
        int idx = rs.getInt("reduce_cves_index");
        dto.setReduceCvesIndex(rs.wasNull() ? null : idx);

        idx = rs.getInt("reduce_cve_growth_index");
        dto.setReduceCveGrowthIndex(rs.wasNull() ? null : idx);

        idx = rs.getInt("reduce_cve_backlog_index");
        dto.setReduceCveBacklogIndex(rs.wasNull() ? null : idx);

        idx = rs.getInt("reduce_cve_backlog_growth_index");
        dto.setReduceCveBacklogGrowthIndex(rs.wasNull() ? null : idx);

        idx = rs.getInt("reduce_stale_packages_index");
        dto.setReduceStalePackagesIndex(rs.wasNull() ? null : idx);

        idx = rs.getInt("reduce_stale_packages_growth_index");
        dto.setReduceStalePackagesGrowthIndex(rs.wasNull() ? null : idx);

        idx = rs.getInt("reduce_downlevel_packages_index");
        dto.setReduceDownlevelPackagesIndex(rs.wasNull() ? null : idx);

        idx = rs.getInt("reduce_downlevel_packages_growth_index");
        dto.setReduceDownlevelPackagesGrowthIndex(rs.wasNull() ? null : idx);

        idx = rs.getInt("grow_patch_efficacy_index");
        dto.setGrowPatchEfficacyIndex(rs.wasNull() ? null : idx);

        idx = rs.getInt("remove_redundant_packages_index");
        dto.setRemoveRedundantPackagesIndex(rs.wasNull() ? null : idx);

        // Ranks
        idx = rs.getInt("decrease_backlog_rank");
        dto.setDecreaseBacklogRank(rs.wasNull() ? null : idx);

        idx = rs.getInt("decrease_vulnerability_count_rank");
        dto.setDecreaseVulnerabilityCountRank(rs.wasNull() ? null : idx);

        idx = rs.getInt("avoids_vulnerabilities_rank");
        dto.setAvoidsVulnerabilitiesRank(rs.wasNull() ? null : idx);

        idx = rs.getInt("increase_impact_rank");
        dto.setIncreaseImpactRank(rs.wasNull() ? null : idx);

        return dto;
    };

    /**
     * Column list for SELECT.
     */
    public static final String SELECT_COLUMNS =
        "id, dataset_metrics_id, datasource_id, commit_date_time, event_date_time, " +
        "edit_type, before, after, " +
        "is_same_edit, same_edit_count, is_pf_recommended_edit, is_user_edit, " +
        "critical_findings, high_findings, medium_findings, low_findings, " +
        "reduce_cves_index, reduce_cve_growth_index, reduce_cve_backlog_index, reduce_cve_backlog_growth_index, " +
        "reduce_stale_packages_index, reduce_stale_packages_growth_index, " +
        "reduce_downlevel_packages_index, reduce_downlevel_packages_growth_index, " +
        "grow_patch_efficacy_index, remove_redundant_packages_index, " +
        "decrease_backlog_rank, decrease_vulnerability_count_rank, avoids_vulnerabilities_rank, increase_impact_rank";
}
