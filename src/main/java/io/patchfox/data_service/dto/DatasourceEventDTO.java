package io.patchfox.data_service.dto;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.springframework.jdbc.core.RowMapper;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.patchfox.package_utils.data.pkg.PackageWrapper;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * DTO for DatasourceEvent entity - scalar fields only.
 * Excludes packages relationship to prevent cascade explosion.
 * Includes payload (decompressed from database storage and deserialized to PackageWrapper).
 */
@Slf4j
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

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime commitDateTime;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
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

    // Payload - decompressed from database and deserialized
    private PackageWrapper packageWrapper;

    // NO packages - that's the death spiral to Package -> Finding -> FindingReporter

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

        // Decompress payload from database and deserialize to PackageWrapper
        byte[] compressedPayload = rs.getBytes("payload");
        if (compressedPayload != null && compressedPayload.length > 0) {
            try {
                Inflater inflater = new Inflater();
                inflater.setInput(compressedPayload);

                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[1024];

                    while (!inflater.finished()) {
                        int decompressedSize = inflater.inflate(buffer);
                        outputStream.write(buffer, 0, decompressedSize);
                    }

                    inflater.end();
                    byte[] decompressedPayload = outputStream.toByteArray();

                    // Deserialize to PackageWrapper
                    ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
                    PackageWrapper packageWrapper = mapper.readValue(decompressedPayload, PackageWrapper.class);
                    dto.setPackageWrapper(packageWrapper);
                }
            } catch (IOException | DataFormatException e) {
                log.error("Failed to decompress/deserialize payload for datasource event id={}", rs.getLong("id"), e);
                dto.setPackageWrapper(null);
            }
        } else {
            dto.setPackageWrapper(null);
        }

        return dto;
    };

    /**
     * Column list for SELECT.
     * Includes payload (will be decompressed and deserialized to PackageWrapper in RowMapper).
     * Excludes packages relationship.
     */
    public static final String SELECT_COLUMNS =
        "id, purl, txid, job_id, commit_hash, commit_branch, " +
        "commit_date_time, event_date_time, status, processing_error, " +
        "oss_enriched, package_index_enriched, analyzed, forecasted, recommended, " +
        "datasource_id, payload";
}
