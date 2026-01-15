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
 * DTO for Package entity - scalar fields only.
 * Excludes all relationships (findings, datasourceEvents) to prevent cascade explosion.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PackageDTO {

    private Long id;
    private String purl;
    private String type;
    private String namespace;
    private String name;
    private String version;

    // Version tracking
    private int numberVersionsBehindHead;
    private int numberMajorVersionsBehindHead;
    private int numberMinorVersionsBehindHead;
    private int numberPatchVersionsBehindHead;
    private String mostRecentVersion;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime mostRecentVersionPublishedAt;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime thisVersionPublishedAt;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private ZonedDateTime updatedAt;

    // NO findings - that's the death spiral
    // NO criticalFindings, highFindings, mediumFindings, lowFindings
    // NO datasourceEvents - bidirectional cascade

    /**
     * RowMapper for converting JDBC ResultSet to PackageDTO.
     */
    public static final RowMapper<PackageDTO> ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        PackageDTO dto = new PackageDTO();
        dto.setId(rs.getLong("id"));
        dto.setPurl(rs.getString("purl"));
        dto.setType(rs.getString("type"));
        dto.setNamespace(rs.getString("namespace"));
        dto.setName(rs.getString("name"));
        dto.setVersion(rs.getString("version"));

        dto.setNumberVersionsBehindHead(rs.getInt("number_versions_behind_head"));
        dto.setNumberMajorVersionsBehindHead(rs.getInt("number_major_versions_behind_head"));
        dto.setNumberMinorVersionsBehindHead(rs.getInt("number_minor_versions_behind_head"));
        dto.setNumberPatchVersionsBehindHead(rs.getInt("number_patch_versions_behind_head"));
        dto.setMostRecentVersion(rs.getString("most_recent_version"));

        OffsetDateTime mostRecentPub = rs.getObject("most_recent_version_published_at", OffsetDateTime.class);
        if (mostRecentPub != null) {
            dto.setMostRecentVersionPublishedAt(mostRecentPub.atZoneSameInstant(ZoneOffset.UTC));
        }

        OffsetDateTime thisPub = rs.getObject("this_version_published_at", OffsetDateTime.class);
        if (thisPub != null) {
            dto.setThisVersionPublishedAt(thisPub.atZoneSameInstant(ZoneOffset.UTC));
        }

        OffsetDateTime updated = rs.getObject("updated_at", OffsetDateTime.class);
        if (updated != null) {
            dto.setUpdatedAt(updated.atZoneSameInstant(ZoneOffset.UTC));
        }

        return dto;
    };

    /**
     * Column list for SELECT.
     */
    public static final String SELECT_COLUMNS =
        "id, purl, type, namespace, name, version, " +
        "number_versions_behind_head, number_major_versions_behind_head, " +
        "number_minor_versions_behind_head, number_patch_versions_behind_head, " +
        "most_recent_version, most_recent_version_published_at, " +
        "this_version_published_at, updated_at";
}
