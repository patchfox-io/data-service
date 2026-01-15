package io.patchfox.data_service.dto;

import java.sql.Array;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.springframework.jdbc.core.RowMapper;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for FindingData entity - scalar fields only.
 * Excludes finding relationship to prevent cascade.
 * Includes finding_id as FK reference.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FindingDataDTO {

    private Long id;
    private Long findingId;

    private String identifier;
    private String severity;
    private String description;

    @Builder.Default
    private Set<String> cpes = new HashSet<>();

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime reportedAt;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime publishedAt;

    @Builder.Default
    private Set<String> patchedIn = new HashSet<>();

    // NO finding - that causes cascade back to packages

    /**
     * RowMapper for converting JDBC ResultSet to FindingDataDTO.
     */
    public static final RowMapper<FindingDataDTO> ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        FindingDataDTO dto = new FindingDataDTO();
        dto.setId(rs.getLong("id"));
        dto.setFindingId(rs.getLong("finding_id"));

        dto.setIdentifier(rs.getString("identifier"));
        dto.setSeverity(rs.getString("severity"));
        dto.setDescription(rs.getString("description"));

        // Handle cpes array
        Array cpesArray = rs.getArray("cpes");
        if (cpesArray != null) {
            String[] cpes = (String[]) cpesArray.getArray();
            dto.setCpes(new HashSet<>(Arrays.asList(cpes)));
        } else {
            dto.setCpes(new HashSet<>());
        }

        // Timestamps
        OffsetDateTime reportedAt = rs.getObject("reported_at", OffsetDateTime.class);
        if (reportedAt != null) {
            dto.setReportedAt(reportedAt.atZoneSameInstant(ZoneOffset.UTC));
        }

        OffsetDateTime publishedAt = rs.getObject("published_at", OffsetDateTime.class);
        if (publishedAt != null) {
            dto.setPublishedAt(publishedAt.atZoneSameInstant(ZoneOffset.UTC));
        }

        // Handle patched_in array
        Array patchedInArray = rs.getArray("patched_in");
        if (patchedInArray != null) {
            String[] patchedIn = (String[]) patchedInArray.getArray();
            dto.setPatchedIn(new HashSet<>(Arrays.asList(patchedIn)));
        } else {
            dto.setPatchedIn(new HashSet<>());
        }

        return dto;
    };

    /**
     * Column list for SELECT.
     */
    public static final String SELECT_COLUMNS =
        "id, finding_id, identifier, severity, description, cpes, reported_at, published_at, patched_in";
}
