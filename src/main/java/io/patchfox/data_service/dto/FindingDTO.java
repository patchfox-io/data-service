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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for Finding entity - combines Finding + FindingData via JOIN.
 * Excludes all relationships (packages, reporters) to prevent cascade explosion.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FindingDTO {

    // From finding table
    private Long id;
    private String identifier;

    // From finding_data table (joined)
    private Long dataId;
    private String severity;
    private String description;
    @Builder.Default
    private Set<String> cpes = new HashSet<>();
    private ZonedDateTime reportedAt;
    private ZonedDateTime publishedAt;
    @Builder.Default
    private Set<String> patchedIn = new HashSet<>();

    // NO packages - that's the death spiral
    // NO reporters - that's also the death spiral

    /**
     * RowMapper for converting JDBC ResultSet to FindingDTO.
     * Expects a JOIN between finding and finding_data tables.
     */
    public static final RowMapper<FindingDTO> ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        FindingDTO dto = new FindingDTO();

        // finding fields
        dto.setId(rs.getLong("id"));
        dto.setIdentifier(rs.getString("identifier"));

        // finding_data fields (may be null if no matching data)
        dto.setDataId(rs.getObject("data_id") != null ? rs.getLong("data_id") : null);
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

        // Handle timestamps
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
     * Column list for SELECT - joins finding and finding_data.
     */
    public static final String SELECT_COLUMNS =
        "f.id, f.identifier, " +
        "fd.id as data_id, fd.severity, fd.description, fd.cpes, " +
        "fd.reported_at, fd.published_at, fd.patched_in";

    /**
     * FROM clause with JOIN.
     */
    public static final String FROM_CLAUSE =
        "finding f LEFT JOIN finding_data fd ON f.id = fd.finding_id";
}
