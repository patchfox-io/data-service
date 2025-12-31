package io.patchfox.data_service.services;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import io.patchfox.data_service.json.DatasourceView;
import io.patchfox.data_service.repositories.DatasourceRepository;
import io.patchfox.db_entities.entities.Datasource;
import io.patchfox.package_utils.json.ApiResponse;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@Service
public class DatasourceService {
    @Autowired
    DatasourceRepository datasourceRepository;
    
    @Autowired
    JdbcTemplate jdbcTemplate;
    

    /**
     * Get all datasources, optionally filtered by dataset name
     * 
     * @param txid
     * @param requestReceivedAt
     * @param dataset optional dataset name to filter by
     * @return
     */
    public ApiResponse getDatasources(
            UUID txid, ZonedDateTime 
            requestReceivedAt, 
            Optional<String> dataset
    ) {
        
        List<Datasource> datasources;
        
        if (dataset.isEmpty()) {
            datasources = findAllDatasources(); //datasourceRepository.findAll();
        } else {
            datasources = findDatasourcesByDataset(dataset.get());
        }
        
        var data = datasources.stream().map(ds ->
            new DatasourceView(
                ds.getDomain(),
                ds.getName(),
                ds.getType(),
                ds.getNumberEventsReceived(),
                ds.getNumberEventProcessingErrors(),
                ds.getFirstEventReceivedAt().toString(),
                ds.getLastEventReceivedAt().toString(),
                ds.getLastEventReceivedStatus(),
                ds.getDatasets().stream().map(x -> x.getName()).toList()
            )
        ).toList();
        
        return ApiResponse.builder()
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt)
                          .code(HttpStatus.OK.value())
                          .data(Map.of("datasources", data))
                          .build();
    }
    

    /**
     * Find all datasources using JDBC
     * 
     * @return list of all datasources
     */
    private List<Datasource> findAllDatasources() {
        String sql = """
            SELECT ds.*
            FROM datasource ds
            """;
        
        return jdbcTemplate.query(sql, new DatasourceRowMapper());
    }


    /**
     * Find datasources that are associated with a specific dataset using JDBC
     * 
     * @param datasetName the name of the dataset to filter by
     * @return list of datasources associated with the given dataset
     */
    private List<Datasource> findDatasourcesByDataset(String datasetName) {
        String sql = """
            SELECT DISTINCT ds.*
            FROM datasource ds
            INNER JOIN datasource_dataset dd ON ds.id = dd.datasource_id
            INNER JOIN dataset d ON dd.dataset_id = d.id
            WHERE d.name = ?
            """;
        
        return jdbcTemplate.query(sql, new DatasourceRowMapper(), datasetName);
    }
    

    /**
     * RowMapper to convert ResultSet to Datasource entity
     */
    private static class DatasourceRowMapper implements RowMapper<Datasource> {
        @Override
        public Datasource mapRow(ResultSet rs, int rowNum) throws SQLException {
            Datasource datasource = new Datasource();
            datasource.setId(rs.getLong("id"));
            datasource.setDomain(rs.getString("domain"));
            datasource.setName(rs.getString("name"));
            datasource.setPurl(rs.getString("purl"));
            datasource.setType(rs.getString("type"));
            datasource.setCommitBranch(rs.getString("commit_branch"));
            datasource.setNumberEventsReceived(rs.getDouble("number_events_received"));
            datasource.setNumberEventProcessingErrors(rs.getDouble("number_event_processing_errors"));
            
            // Convert Timestamp to ZonedDateTime
            var firstEventTimestamp = rs.getTimestamp("first_event_received_at");
            if (firstEventTimestamp != null) {
                datasource.setFirstEventReceivedAt(ZonedDateTime.ofInstant(
                    firstEventTimestamp.toInstant(), 
                    java.time.ZoneOffset.UTC
                ));
            }
            
            var lastEventTimestamp = rs.getTimestamp("last_event_received_at");
            if (lastEventTimestamp != null) {
                datasource.setLastEventReceivedAt(ZonedDateTime.ofInstant(
                    lastEventTimestamp.toInstant(), 
                    java.time.ZoneOffset.UTC
                ));
            }
            
            datasource.setLastEventReceivedStatus(rs.getString("last_event_received_status"));
            datasource.setStatus(Datasource.Status.valueOf(rs.getString("status")));
            
            // Handle optional fields
            String latestTxidStr = rs.getString("latest_txid");
            if (latestTxidStr != null) {
                datasource.setLatestTxid(UUID.fromString(latestTxidStr));
            }
            
            String latestJobIdStr = rs.getString("latest_job_id");
            if (latestJobIdStr != null) {
                datasource.setLatestJobId(UUID.fromString(latestJobIdStr));
            }
            
            return datasource;
        }
    }
}
