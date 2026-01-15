package io.patchfox.data_service.dto;

import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;

import org.springframework.jdbc.core.RowMapper;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for DatasourceMetrics entity - scalar fields only.
 * No relationships.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasourceMetricsDTO {

    private Long id;
    private long datasourceEventCount;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime commitDateTime;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime eventDateTime;

    private UUID txid;
    private UUID jobId;
    private String purl;

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

    // Scores
    private double patchEfficacyScore;
    private double patchImpact;
    private double patchEffort;

    public static final String SELECT_COLUMNS = """
        id, datasource_event_count, commit_date_time, event_date_time, txid, job_id, purl,
        total_findings, critical_findings, high_findings, medium_findings, low_findings,
        findings_avoided_by_patching_past_year, critical_findings_avoided_by_patching_past_year,
        high_findings_avoided_by_patching_past_year, medium_findings_avoided_by_patching_past_year,
        low_findings_avoided_by_patching_past_year,
        findings_in_backlog_between_thirty_and_sixty_days, critical_findings_in_backlog_between_thirty_and_sixty_days,
        high_findings_in_backlog_between_thirty_and_sixty_days, medium_findings_in_backlog_between_thirty_and_sixty_days,
        low_findings_in_backlog_between_thirty_and_sixty_days,
        findings_in_backlog_between_sixty_and_ninety_days, critical_findings_in_backlog_between_sixty_and_ninety_days,
        high_findings_in_backlog_between_sixty_and_ninety_days, medium_findings_in_backlog_between_sixty_and_ninety_days,
        low_findings_in_backlog_between_sixty_and_ninety_days,
        findings_in_backlog_over_ninety_days, critical_findings_in_backlog_over_ninety_days,
        high_findings_in_backlog_over_ninety_days, medium_findings_in_backlog_over_ninety_days,
        low_findings_in_backlog_over_ninety_days,
        packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings,
        packages_with_medium_findings, packages_with_low_findings,
        downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch,
        stale_packages, stale_packages_six_months, stale_packages_one_year,
        stale_packages_one_year_six_months, stale_packages_two_years,
        patches, same_patches, different_patches, patch_fox_patches,
        patch_efficacy_score, patch_impact, patch_effort
        """;

    public static final RowMapper<DatasourceMetricsDTO> ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        DatasourceMetricsDTO dto = new DatasourceMetricsDTO();
        dto.setId(rs.getLong("id"));
        dto.setDatasourceEventCount(rs.getLong("datasource_event_count"));

        OffsetDateTime commitOdt = rs.getObject("commit_date_time", OffsetDateTime.class);
        dto.setCommitDateTime(commitOdt != null ? commitOdt.atZoneSameInstant(ZoneOffset.UTC) : null);

        OffsetDateTime eventOdt = rs.getObject("event_date_time", OffsetDateTime.class);
        dto.setEventDateTime(eventOdt != null ? eventOdt.atZoneSameInstant(ZoneOffset.UTC) : null);

        dto.setTxid((UUID) rs.getObject("txid"));
        dto.setJobId((UUID) rs.getObject("job_id"));
        dto.setPurl(rs.getString("purl"));

        dto.setTotalFindings(rs.getLong("total_findings"));
        dto.setCriticalFindings(rs.getLong("critical_findings"));
        dto.setHighFindings(rs.getLong("high_findings"));
        dto.setMediumFindings(rs.getLong("medium_findings"));
        dto.setLowFindings(rs.getLong("low_findings"));

        dto.setFindingsAvoidedByPatchingPastYear(rs.getLong("findings_avoided_by_patching_past_year"));
        dto.setCriticalFindingsAvoidedByPatchingPastYear(rs.getLong("critical_findings_avoided_by_patching_past_year"));
        dto.setHighFindingsAvoidedByPatchingPastYear(rs.getLong("high_findings_avoided_by_patching_past_year"));
        dto.setMediumFindingsAvoidedByPatchingPastYear(rs.getLong("medium_findings_avoided_by_patching_past_year"));
        dto.setLowFindingsAvoidedByPatchingPastYear(rs.getLong("low_findings_avoided_by_patching_past_year"));

        dto.setFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("findings_in_backlog_between_thirty_and_sixty_days"));
        dto.setCriticalFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("critical_findings_in_backlog_between_thirty_and_sixty_days"));
        dto.setHighFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("high_findings_in_backlog_between_thirty_and_sixty_days"));
        dto.setMediumFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("medium_findings_in_backlog_between_thirty_and_sixty_days"));
        dto.setLowFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("low_findings_in_backlog_between_thirty_and_sixty_days"));

        dto.setFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("findings_in_backlog_between_sixty_and_ninety_days"));
        dto.setCriticalFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("critical_findings_in_backlog_between_sixty_and_ninety_days"));
        dto.setHighFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("high_findings_in_backlog_between_sixty_and_ninety_days"));
        dto.setMediumFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("medium_findings_in_backlog_between_sixty_and_ninety_days"));
        dto.setLowFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("low_findings_in_backlog_between_sixty_and_ninety_days"));

        dto.setFindingsInBacklogOverNinetyDays(rs.getDouble("findings_in_backlog_over_ninety_days"));
        dto.setCriticalFindingsInBacklogOverNinetyDays(rs.getDouble("critical_findings_in_backlog_over_ninety_days"));
        dto.setHighFindingsInBacklogOverNinetyDays(rs.getDouble("high_findings_in_backlog_over_ninety_days"));
        dto.setMediumFindingsInBacklogOverNinetyDays(rs.getDouble("medium_findings_in_backlog_over_ninety_days"));
        dto.setLowFindingsInBacklogOverNinetyDays(rs.getDouble("low_findings_in_backlog_over_ninety_days"));

        dto.setPackages(rs.getLong("packages"));
        dto.setPackagesWithFindings(rs.getLong("packages_with_findings"));
        dto.setPackagesWithCriticalFindings(rs.getLong("packages_with_critical_findings"));
        dto.setPackagesWithHighFindings(rs.getLong("packages_with_high_findings"));
        dto.setPackagesWithMediumFindings(rs.getLong("packages_with_medium_findings"));
        dto.setPackagesWithLowFindings(rs.getLong("packages_with_low_findings"));

        dto.setDownlevelPackages(rs.getLong("downlevel_packages"));
        dto.setDownlevelPackagesMajor(rs.getLong("downlevel_packages_major"));
        dto.setDownlevelPackagesMinor(rs.getLong("downlevel_packages_minor"));
        dto.setDownlevelPackagesPatch(rs.getLong("downlevel_packages_patch"));

        dto.setStalePackages(rs.getLong("stale_packages"));
        dto.setStalePackagesSixMonths(rs.getLong("stale_packages_six_months"));
        dto.setStalePackagesOneYear(rs.getLong("stale_packages_one_year"));
        dto.setStalePackagesOneYearSixMonths(rs.getLong("stale_packages_one_year_six_months"));
        dto.setStalePackagesTwoYears(rs.getLong("stale_packages_two_years"));

        dto.setPatches(rs.getLong("patches"));
        dto.setSamePatches(rs.getLong("same_patches"));
        dto.setDifferentPatches(rs.getLong("different_patches"));
        dto.setPatchFoxPatches(rs.getLong("patch_fox_patches"));

        dto.setPatchEfficacyScore(rs.getDouble("patch_efficacy_score"));
        dto.setPatchImpact(rs.getDouble("patch_impact"));
        dto.setPatchEffort(rs.getDouble("patch_effort"));

        return dto;
    };
}
