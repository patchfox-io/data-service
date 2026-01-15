package io.patchfox.data_service.dto;

import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;

import org.springframework.jdbc.core.RowMapper;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for DatasourceEvent entity - scalar fields only.
 * Excludes packages relationship to prevent cascade explosion.
 * Excludes payload (compressed blob) to keep responses lightweight.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasourceEventDTO {

    private Long id;
    private String purl;
    private UUID txid;
    private UUID jobId;
    private String commitHash;
    private String commitBranch;
    private ZonedDateTime commitDateTime;
    private ZonedDateTime eventDateTime;
    private String status;
    private String processingError;
    private boolean ossEnriched;
    private boolean packageIndexEnriched;
    private boolean analyzed;
    private boolean forecasted;
    private boolean recommended;

    // Foreign key reference instead of full entity
    private Long datasourceId;

    // NO packages - that's the death spiral to Package -> Finding -> FindingReporter
    // NO payload - it's a large compressed blob

    /**
     * RowMapper for converting JDBC ResultSet to DatasourceEventDTO.
     */
    public static final RowMapper<DatasourceEventDTO> ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        DatasourceEventDTO dto = new DatasourceEventDTO();
        dto.setId(rs.getLong("id"));
        dto.setPurl(rs.getString("purl"));

        // Handle UUID fields
        String txidStr = rs.getString("txid");
        if (txidStr != null) {
            dto.setTxid(UUID.fromString(txidStr));
        }

        String jobIdStr = rs.getString("job_id");
        if (jobIdStr != null) {
            dto.setJobId(UUID.fromString(jobIdStr));
        }

        dto.setCommitHash(rs.getString("commit_hash"));
        dto.setCommitBranch(rs.getString("commit_branch"));

        // Handle timestamps
        OffsetDateTime commitDateTime = rs.getObject("commit_date_time", OffsetDateTime.class);
        if (commitDateTime != null) {
            dto.setCommitDateTime(commitDateTime.atZoneSameInstant(ZoneOffset.UTC));
        }

        OffsetDateTime eventDateTime = rs.getObject("event_date_time", OffsetDateTime.class);
        if (eventDateTime != null) {
            dto.setEventDateTime(eventDateTime.atZoneSameInstant(ZoneOffset.UTC));
        }

        dto.setStatus(rs.getString("status"));
        dto.setProcessingError(rs.getString("processing_error"));
        dto.setOssEnriched(rs.getBoolean("oss_enriched"));
        dto.setPackageIndexEnriched(rs.getBoolean("package_index_enriched"));
        dto.setAnalyzed(rs.getBoolean("analyzed"));
        dto.setForecasted(rs.getBoolean("forecasted"));
        dto.setRecommended(rs.getBoolean("recommended"));
        dto.setDatasourceId(rs.getLong("datasource_id"));

        return dto;
    };

    /**
     * Column list for SELECT.
     * Excludes payload (large compressed blob) and packages relationship.
     */
    public static final String SELECT_COLUMNS =
        "id, purl, txid, job_id, commit_hash, commit_branch, " +
        "commit_date_time, event_date_time, status, processing_error, " +
        "oss_enriched, package_index_enriched, analyzed, forecasted, recommended, " +
        "datasource_id";
}
