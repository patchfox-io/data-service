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
 * DTO for Datasource entity - excludes heavy fields (edits, packageIndexes).
 * Also excludes the bidirectional datasets relationship to prevent cycles.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasourceDTO {

    private Long id;
    private UUID latestTxid;
    private UUID latestJobId;
    private String purl;
    private String domain;
    private String name;
    private String commitBranch;
    private String type;
    private double numberEventsReceived;
    private double numberEventProcessingErrors;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime firstEventReceivedAt;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime lastEventReceivedAt;
    private String lastEventReceivedStatus;
    private String status;

    // NO edits - that's a heavy relationship
    // NO packageIndexes - that's a huge list
    // NO datasets - bidirectional, causes cycles

    /**
     * RowMapper for converting JDBC ResultSet to DatasourceDTO.
     */
    public static final RowMapper<DatasourceDTO> ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        DatasourceDTO dto = new DatasourceDTO();
        dto.setId(rs.getLong("id"));

        Object txidObj = rs.getObject("latest_txid");
        if (txidObj != null) {
            dto.setLatestTxid(UUID.fromString(txidObj.toString()));
        }

        Object jobIdObj = rs.getObject("latest_job_id");
        if (jobIdObj != null) {
            dto.setLatestJobId(UUID.fromString(jobIdObj.toString()));
        }

        dto.setPurl(rs.getString("purl"));
        dto.setDomain(rs.getString("domain"));
        dto.setName(rs.getString("name"));
        dto.setCommitBranch(rs.getString("commit_branch"));
        dto.setType(rs.getString("type"));
        dto.setNumberEventsReceived(rs.getDouble("number_events_received"));
        dto.setNumberEventProcessingErrors(rs.getDouble("number_event_processing_errors"));

        OffsetDateTime firstEvent = rs.getObject("first_event_received_at", OffsetDateTime.class);
        if (firstEvent != null) {
            dto.setFirstEventReceivedAt(firstEvent.atZoneSameInstant(ZoneOffset.UTC));
        }

        OffsetDateTime lastEvent = rs.getObject("last_event_received_at", OffsetDateTime.class);
        if (lastEvent != null) {
            dto.setLastEventReceivedAt(lastEvent.atZoneSameInstant(ZoneOffset.UTC));
        }

        dto.setLastEventReceivedStatus(rs.getString("last_event_received_status"));
        dto.setStatus(rs.getString("status"));

        return dto;
    };

    /**
     * Column list for SELECT (excludes package_indexes).
     */
    public static final String SELECT_COLUMNS =
        "id, latest_txid, latest_job_id, purl, domain, name, commit_branch, type, " +
        "number_events_received, number_event_processing_errors, " +
        "first_event_received_at, last_event_received_at, last_event_received_status, status";
}
