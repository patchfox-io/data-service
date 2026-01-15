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
 * DTO for DatasourceMetricsCurrent entity - scalar fields only.
 * No relationships.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasourceMetricsCurrentDTO {

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

    // Patches
    private long patches;
    private long samePatches;
    private long differentPatches;
    private long patchFoxPatches;

    public static final String SELECT_COLUMNS = """
        id, datasource_event_count, commit_date_time, event_date_time, txid, job_id, purl,
        total_findings, critical_findings, high_findings, medium_findings, low_findings,
        packages, packages_with_findings, packages_with_critical_findings, packages_with_high_findings,
        packages_with_medium_findings, packages_with_low_findings,
        downlevel_packages, downlevel_packages_major, downlevel_packages_minor, downlevel_packages_patch,
        stale_packages, patches, same_patches, different_patches, patch_fox_patches
        """;

    public static final RowMapper<DatasourceMetricsCurrentDTO> ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        DatasourceMetricsCurrentDTO dto = new DatasourceMetricsCurrentDTO();
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

        dto.setPatches(rs.getLong("patches"));
        dto.setSamePatches(rs.getLong("same_patches"));
        dto.setDifferentPatches(rs.getLong("different_patches"));
        dto.setPatchFoxPatches(rs.getLong("patch_fox_patches"));

        return dto;
    };
}
