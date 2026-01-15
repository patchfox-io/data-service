package io.patchfox.data_service.dto;

import java.sql.Array;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.RowMapper;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for DatasetMetrics entity - scalar fields only.
 * Excludes edits relationship to prevent cascade explosion.
 * Includes dataset_id as FK reference.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasetMetricsDTO {

    private Long id;
    private Long datasetId;
    private UUID txid;
    private UUID jobId;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime commitDateTime;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime eventDateTime;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime forecastMaturityDate;

    private long datasourceCount;
    private long datasourceEventCount;

    private boolean isCurrent;
    private boolean isForecastSameCourse;
    private boolean isForecastRecommendationsTaken;

    private String recommendationType;
    private String recommendationHeadline;

    private double rpsScore;

    // Finding counts
    private long totalFindings;
    private long criticalFindings;
    private long highFindings;
    private long mediumFindings;
    private long lowFindings;

    // Findings avoided
    private long findingsAvoidedByPatchingPastYear;
    private long criticalFindingsAvoidedByPatchingPastYear;
    private long highFindingsAvoidedByPatchingPastYear;
    private long mediumFindingsAvoidedByPatchingPastYear;
    private long lowFindingsAvoidedByPatchingPastYear;

    // Backlog 30-60 days
    private double findingsInBacklogBetweenThirtyAndSixtyDays;
    private double criticalFindingsInBacklogBetweenThirtyAndSixtyDays;
    private double highFindingsInBacklogBetweenThirtyAndSixtyDays;
    private double mediumFindingsInBacklogBetweenThirtyAndSixtyDays;
    private double lowFindingsInBacklogBetweenThirtyAndSixtyDays;

    // Backlog 60-90 days
    private double findingsInBacklogBetweenSixtyAndNinetyDays;
    private double criticalFindingsInBacklogBetweenSixtyAndNinetyDays;
    private double highFindingsInBacklogBetweenSixtyAndNinetyDays;
    private double mediumFindingsInBacklogBetweenSixtyAndNinetyDays;
    private double lowFindingsInBacklogBetweenSixtyAndNinetyDays;

    // Backlog 90+ days
    private double findingsInBacklogOverNinetyDays;
    private double criticalFindingsInBacklogOverNinetyDays;
    private double highFindingsInBacklogOverNinetyDays;
    private double mediumFindingsInBacklogOverNinetyDays;
    private double lowFindingsInBacklogOverNinetyDays;

    // Package counts
    private long packages;
    private long packagesWithFindings;
    private long packagesWithCriticalFindings;
    private long packagesWithHighFindings;
    private long packagesWithMediumFindings;
    private long packagesWithLowFindings;

    // Downlevel packages
    private long downlevelPackages;
    private long downlevelPackagesMajor;
    private long downlevelPackagesMinor;
    private long downlevelPackagesPatch;

    // Stale packages
    private long stalePackages;
    private long stalePackagesSixMonths;
    private long stalePackagesOneYear;
    private long stalePackagesOneYearSixMonths;
    private long stalePackagesTwoYears;

    // Patches
    private long patches;
    private long samePatches;
    private long differentPatches;
    private long patchFoxPatches;

    // Efficacy
    private double patchEfficacyScore;
    private double patchImpact;
    private double patchEffort;

    // Package indexes (element collection)
    @Builder.Default
    private List<Long> packageIndexes = new ArrayList<>();

    // NO edits - that's the cascade explosion
    // NO packageFamilies - element collection, can be loaded separately if needed

    /**
     * RowMapper for converting JDBC ResultSet to DatasetMetricsDTO.
     */
    public static final RowMapper<DatasetMetricsDTO> ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        DatasetMetricsDTO dto = new DatasetMetricsDTO();
        dto.setId(rs.getLong("id"));
        dto.setDatasetId(rs.getLong("dataset_id"));

        String txidStr = rs.getString("txid");
        if (txidStr != null) {
            dto.setTxid(UUID.fromString(txidStr));
        }

        String jobIdStr = rs.getString("job_id");
        if (jobIdStr != null) {
            dto.setJobId(UUID.fromString(jobIdStr));
        }

        // Timestamps
        OffsetDateTime commitDateTime = rs.getObject("commit_date_time", OffsetDateTime.class);
        if (commitDateTime != null) {
            dto.setCommitDateTime(commitDateTime.atZoneSameInstant(ZoneOffset.UTC));
        }

        OffsetDateTime eventDateTime = rs.getObject("event_date_time", OffsetDateTime.class);
        if (eventDateTime != null) {
            dto.setEventDateTime(eventDateTime.atZoneSameInstant(ZoneOffset.UTC));
        }

        OffsetDateTime forecastMaturityDate = rs.getObject("forecast_maturity_date", OffsetDateTime.class);
        if (forecastMaturityDate != null) {
            dto.setForecastMaturityDate(forecastMaturityDate.atZoneSameInstant(ZoneOffset.UTC));
        }

        dto.setDatasourceCount(rs.getLong("datasource_count"));
        dto.setDatasourceEventCount(rs.getLong("datasource_event_count"));

        dto.setCurrent(rs.getBoolean("is_current"));
        dto.setForecastSameCourse(rs.getBoolean("is_forecast_same_course"));
        dto.setForecastRecommendationsTaken(rs.getBoolean("is_forecast_recommendations_taken"));

        dto.setRecommendationType(rs.getString("recommendation_type"));
        dto.setRecommendationHeadline(rs.getString("recommendation_headline"));

        dto.setRpsScore(rs.getDouble("rps_score"));

        // Finding counts
        dto.setTotalFindings(rs.getLong("total_findings"));
        dto.setCriticalFindings(rs.getLong("critical_findings"));
        dto.setHighFindings(rs.getLong("high_findings"));
        dto.setMediumFindings(rs.getLong("medium_findings"));
        dto.setLowFindings(rs.getLong("low_findings"));

        // Findings avoided
        dto.setFindingsAvoidedByPatchingPastYear(rs.getLong("findings_avoided_by_patching_past_year"));
        dto.setCriticalFindingsAvoidedByPatchingPastYear(rs.getLong("critical_findings_avoided_by_patching_past_year"));
        dto.setHighFindingsAvoidedByPatchingPastYear(rs.getLong("high_findings_avoided_by_patching_past_year"));
        dto.setMediumFindingsAvoidedByPatchingPastYear(rs.getLong("medium_findings_avoided_by_patching_past_year"));
        dto.setLowFindingsAvoidedByPatchingPastYear(rs.getLong("low_findings_avoided_by_patching_past_year"));

        // Backlog 30-60
        dto.setFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("findings_in_backlog_between_thirty_and_sixty_days"));
        dto.setCriticalFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("critical_findings_in_backlog_between_thirty_and_sixty_days"));
        dto.setHighFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("high_findings_in_backlog_between_thirty_and_sixty_days"));
        dto.setMediumFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("medium_findings_in_backlog_between_thirty_and_sixty_days"));
        dto.setLowFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("low_findings_in_backlog_between_thirty_and_sixty_days"));

        // Backlog 60-90
        dto.setFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("findings_in_backlog_between_sixty_and_ninety_days"));
        dto.setCriticalFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("critical_findings_in_backlog_between_sixty_and_ninety_days"));
        dto.setHighFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("high_findings_in_backlog_between_sixty_and_ninety_days"));
        dto.setMediumFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("medium_findings_in_backlog_between_sixty_and_ninety_days"));
        dto.setLowFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("low_findings_in_backlog_between_sixty_and_ninety_days"));

        // Backlog 90+
        dto.setFindingsInBacklogOverNinetyDays(rs.getDouble("findings_in_backlog_over_ninety_days"));
        dto.setCriticalFindingsInBacklogOverNinetyDays(rs.getDouble("critical_findings_in_backlog_over_ninety_days"));
        dto.setHighFindingsInBacklogOverNinetyDays(rs.getDouble("high_findings_in_backlog_over_ninety_days"));
        dto.setMediumFindingsInBacklogOverNinetyDays(rs.getDouble("medium_findings_in_backlog_over_ninety_days"));
        dto.setLowFindingsInBacklogOverNinetyDays(rs.getDouble("low_findings_in_backlog_over_ninety_days"));

        // Package counts
        dto.setPackages(rs.getLong("packages"));
        dto.setPackagesWithFindings(rs.getLong("packages_with_findings"));
        dto.setPackagesWithCriticalFindings(rs.getLong("packages_with_critical_findings"));
        dto.setPackagesWithHighFindings(rs.getLong("packages_with_high_findings"));
        dto.setPackagesWithMediumFindings(rs.getLong("packages_with_medium_findings"));
        dto.setPackagesWithLowFindings(rs.getLong("packages_with_low_findings"));

        // Downlevel
        dto.setDownlevelPackages(rs.getLong("downlevel_packages"));
        dto.setDownlevelPackagesMajor(rs.getLong("downlevel_packages_major"));
        dto.setDownlevelPackagesMinor(rs.getLong("downlevel_packages_minor"));
        dto.setDownlevelPackagesPatch(rs.getLong("downlevel_packages_patch"));

        // Stale
        dto.setStalePackages(rs.getLong("stale_packages"));
        dto.setStalePackagesSixMonths(rs.getLong("stale_packages_six_months"));
        dto.setStalePackagesOneYear(rs.getLong("stale_packages_one_year"));
        dto.setStalePackagesOneYearSixMonths(rs.getLong("stale_packages_one_year_six_months"));
        dto.setStalePackagesTwoYears(rs.getLong("stale_packages_two_years"));

        // Patches
        dto.setPatches(rs.getLong("patches"));
        dto.setSamePatches(rs.getLong("same_patches"));
        dto.setDifferentPatches(rs.getLong("different_patches"));
        dto.setPatchFoxPatches(rs.getLong("patch_fox_patches"));

        // Efficacy
        dto.setPatchEfficacyScore(rs.getDouble("patch_efficacy_score"));
        dto.setPatchImpact(rs.getDouble("patch_impact"));
        dto.setPatchEffort(rs.getDouble("patch_effort"));

        // Package indexes array
        Array packageIndexesArray = rs.getArray("package_indexes");
        if (packageIndexesArray != null) {
            Long[] indexes = (Long[]) packageIndexesArray.getArray();
            dto.setPackageIndexes(Arrays.asList(indexes));
        } else {
            dto.setPackageIndexes(new ArrayList<>());
        }

        return dto;
    };

    /**
     * Column list for SELECT.
     */
    public static final String SELECT_COLUMNS =
        "id, dataset_id, txid, job_id, commit_date_time, event_date_time, forecast_maturity_date, " +
        "datasource_count, datasource_event_count, " +
        "is_current, is_forecast_same_course, is_forecast_recommendations_taken, " +
        "recommendation_type, recommendation_headline, rps_score, " +
        "total_findings, critical_findings, high_findings, medium_findings, low_findings, " +
        "findings_avoided_by_patching_past_year, critical_findings_avoided_by_patching_past_year, " +
        "high_findings_avoided_by_patching_past_year, medium_findings_avoided_by_patching_past_year, " +
        "low_findings_avoided_by_patching_past_year, " +
        "findings_in_backlog_between_thirty_and_sixty_days, critical_findings_in_backlog_between_thirty_and_sixty_days, " +
        "high_findings_in_backlog_between_thirty_and_sixty_days, medium_findings_in_backlog_between_thirty_and_sixty_days, " +
        "low_findings_in_backlog_between_thirty_and_sixty_days, " +
        "findings_in_backlog_between_sixty_and_ninety_days, critical_findings_in_backlog_between_sixty_and_ninety_days, " +
        "high_findings_in_backlog_between_sixty_and_ninety_days, medium_findings_in_backlog_between_sixty_and_ninety_days, " +
        "low_findings_in_backlog_between_sixty_and_ninety_days, " +
        "findings_in_backlog_over_ninety_days, critical_findings_in_backlog_over_ninety_days, " +
        "high_findings_in_backlog_over_ninety_days, medium_findings_in_backlog_over_ninety_days, " +
        "low_findings_in_backlog_over_ninety_days, " +
        "packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings, " +
        "packages_with_medium_findings, packages_with_low_findings, " +
        "downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch, " +
        "stale_packages, stale_packages_six_months, stale_packages_one_year, " +
        "stale_packages_one_year_six_months, stale_packages_two_years, " +
        "patches, same_patches, different_patches, patch_fox_patches, " +
        "patch_efficacy_score, patch_impact, patch_effort, package_indexes";
}
