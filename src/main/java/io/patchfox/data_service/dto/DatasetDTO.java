package io.patchfox.data_service.dto;

import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.RowMapper;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for Dataset entity - includes datasources but without their heavy fields.
 * This prevents the Hibernate relationship loading explosion.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasetDTO {

    private Long id;
    private UUID latestTxid;
    private UUID latestJobId;
    private String name;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime updatedAt;
    private String status;

    // Datasources WITHOUT edits/packageIndexes - populated via separate query
    @Builder.Default
    private List<DatasourceDTO> datasources = new ArrayList<>();

    /**
     * RowMapper for converting JDBC ResultSet to DatasetDTO.
     * Note: This only maps the Dataset fields. Datasources are loaded separately.
     */
    public static final RowMapper<DatasetDTO> ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        DatasetDTO dto = new DatasetDTO();
        dto.setId(rs.getLong("id"));

        // Handle UUID columns - may be null
        Object txidObj = rs.getObject("latest_txid");
        if (txidObj != null) {
            dto.setLatestTxid(UUID.fromString(txidObj.toString()));
        }

        Object jobIdObj = rs.getObject("latest_job_id");
        if (jobIdObj != null) {
            dto.setLatestJobId(UUID.fromString(jobIdObj.toString()));
        }

        dto.setName(rs.getString("name"));

        // Handle timestamp - convert to ZonedDateTime
        OffsetDateTime odt = rs.getObject("updated_at", OffsetDateTime.class);
        if (odt != null) {
            dto.setUpdatedAt(odt.atZoneSameInstant(ZoneOffset.UTC));
        }

        dto.setStatus(rs.getString("status"));

        // Initialize empty list - will be populated by JdbcQueryService
        dto.setDatasources(new ArrayList<>());

        return dto;
    };

    /**
     * Column list for SELECT.
     */
    public static final String SELECT_COLUMNS =
        "id, latest_txid, latest_job_id, name, updated_at, status";
}
