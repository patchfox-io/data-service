package io.patchfox.data_service.dto;

import java.sql.ResultSet;

import org.springframework.jdbc.core.RowMapper;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for FindingReporter entity - scalar fields only.
 * Excludes findings relationship to prevent cascade explosion.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FindingReporterDTO {

    private Long id;
    private String name;

    // NO findings - that's the death spiral back to Package

    /**
     * RowMapper for converting JDBC ResultSet to FindingReporterDTO.
     */
    public static final RowMapper<FindingReporterDTO> ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        FindingReporterDTO dto = new FindingReporterDTO();
        dto.setId(rs.getLong("id"));
        dto.setName(rs.getString("name"));
        return dto;
    };

    /**
     * Column list for SELECT.
     */
    public static final String SELECT_COLUMNS = "id, name";
}
