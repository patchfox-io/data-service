package io.patchfox.data_service.services;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.catalina.connector.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import io.patchfox.data_service.json.RecommendDetailView;
import io.patchfox.data_service.json.RecommendTopView;
import io.patchfox.data_service.json.RecommendDetailView.RecommendationCard;
import io.patchfox.data_service.repositories.DatasetMetricsRepository;
import io.patchfox.db_entities.entities.DatasetMetrics;
import io.patchfox.db_entities.entities.Datasource;
import io.patchfox.db_entities.entities.Edit;
import io.patchfox.package_utils.json.ApiResponse;
import io.patchfox.package_utils.util.Pair;
import lombok.extern.slf4j.Slf4j;


@Component
@Slf4j
public class RecommendViewService {

    @Autowired
    DatasetMetricsRepository datasetMetricsRepository;
    
    @Autowired
    JdbcTemplate jdbcTemplate;

    public ApiResponse getTopRecommendView(UUID jobId, ZonedDateTime requestReceivedAt, Optional<String> dataset) {

        // we are counting on the controller to validate arguments
        var txidOptional = datasetMetricsRepository.getLatestDatasetMetricsRecordTxidForJobId(jobId);

        List<DatasetMetrics> currentRecords;
        if (dataset.isPresent()) {
            currentRecords = findDatasetMetricsByTxidAndDataset(txidOptional.get(), dataset.get());
        } else {
            currentRecords = findDatasetMetricsByTxid(txidOptional.get());
        }

        var rv = new RecommendTopView();

        for (var currentRecord : currentRecords) {
            rv.addCard(currentRecord.getRecommendationType().toString(), currentRecord.getRecommendationHeadline());
        }
        
        return ApiResponse.builder()
                          .code(Response.SC_OK)
                          .data(Map.of("cards", rv))
                          .build();
    }


    public ApiResponse getDetailRecommendView(UUID jobId, ZonedDateTime requestReceivedAt, String type, Optional<String> dataset) {

        // we are counting on the controller to validate arguments
        var txidOptional = datasetMetricsRepository.getLatestDatasetMetricsRecordTxidForJobId(jobId);

        List<DatasetMetrics> currentRecords;
        if (dataset.isPresent()) {
            currentRecords = findDatasetMetricsByTxidAndDataset(txidOptional.get(), dataset.get());
        } else {
            currentRecords = findDatasetMetricsByTxid(txidOptional.get());
        }

        log.info("type is: {}", type);
        DatasetMetrics currentRecord = null;
        for (var currentRecordTemp : currentRecords) {
            if (currentRecordTemp.getRecommendationType().toString().equals(type)) {
                currentRecord = currentRecordTemp;
            }
        }                                                  

        log.info("currentRecord is: {}", currentRecord);
        if (currentRecord == null) {
            return ApiResponse.builder()
                              .code(Response.SC_OK)
                              .txid(jobId)
                              .data(Map.of("payload", new RecommendDetailView()))
                              .build();           
        }

        // Fetch edits for this dataset_metrics record using JDBC
        var edits = findEditsByDatasetMetricsId(currentRecord.getId());
        currentRecord.setEdits(new HashSet<>(edits));

        var rv = new RecommendDetailView();
        rv.title = type.replace("_", " ");

        var editMap = new HashMap<String, List<Edit>>();
        
        for (var edit : currentRecord.getEdits()) {
            var datasourcePurl = edit.getDatasource().getPurl();
            if ( !editMap.containsKey(datasourcePurl) ) {
                editMap.put(datasourcePurl, new ArrayList<Edit>());
            }

            editMap.get(datasourcePurl).add(edit);
        }

        var recCards = new ArrayList<RecommendationCard>();
        for (var entry : editMap.entrySet()) {
            var patch_ds = entry.getKey();

            var patchBuffer = new ArrayList<Pair<String, String>>();
            var count = 0;
            var avoidsVulnerabilitiesTotal = 0;
            var decreaseVulnerabilitiesTotal = 0;
            var decreaseBacklogTotal = 0;
            var increaseImpacttotal = 0;
            for (var edit : entry.getValue()) {
                patchBuffer.add(new Pair<>(edit.getBefore(), edit.getAfter()));
                count += 1;
                avoidsVulnerabilitiesTotal += edit.getAvoidsVulnerabilitiesRank();
                decreaseVulnerabilitiesTotal += edit.getDecreaseVulnerabilityCountRank();
                decreaseBacklogTotal += edit.getDecreaseBacklogRank();
                increaseImpacttotal += edit.getIncreaseImpactRank();

                if (count > 2) {
                    var recCard = rv.new RecommendationCard();
                    var ticketText = String.format(
                        "in git repository: %s\n" + 
                        "\npatch the following: \n" +
                        "* %s --> %s \n" +
                        "* %s --> %s \n" + 
                        "* %s --> %s \n",
                        entry.getKey(),
                        patchBuffer.get(0).getLeft(),
                        patchBuffer.get(0).getRight(),
                        patchBuffer.get(1).getLeft(),
                        patchBuffer.get(1).getRight(),
                        patchBuffer.get(2).getLeft(),
                        patchBuffer.get(2).getRight()
                    );
                    recCard.avoidsVulnerabilitiesScore = avoidsVulnerabilitiesTotal / count;
                    recCard.decreaseVulnerabilitiesScore = decreaseVulnerabilitiesTotal / count;
                    recCard.decreaseBacklogScore = decreaseBacklogTotal / count;
                    recCard.increaseImpactScore = increaseImpacttotal / count;
                    recCard.ticketText = ticketText;
                    recCard.patches = Map.of(patch_ds, new ArrayList<>(patchBuffer));
                    log.info("patches now: {}", recCard.patches);
                    recCards.add(recCard);

                    patchBuffer.clear();
                    count = 0;
                    avoidsVulnerabilitiesTotal = 0;
                    decreaseVulnerabilitiesTotal = 0;
                    decreaseBacklogTotal = 0;
                    increaseImpacttotal = 0;
                }
            }

            if ( !patchBuffer.isEmpty() ) {
                var ticketText = String.format(
                    "in git repository: %s\n" + 
                    "patch the following: \n", 
                    entry.getKey()
                );

                for (var e : patchBuffer) {
                    ticketText = ticketText + "\n * " + e.getLeft() + " " + e.getRight();
                }

                var recCard = rv.new RecommendationCard();
                recCard.avoidsVulnerabilitiesScore = avoidsVulnerabilitiesTotal / count;
                recCard.decreaseVulnerabilitiesScore = decreaseVulnerabilitiesTotal / count;
                recCard.decreaseBacklogScore = decreaseBacklogTotal / count;
                recCard.increaseImpactScore = increaseImpacttotal / count;
                recCard.ticketText = ticketText;
                recCard.patches = Map.of(patch_ds, new ArrayList<>(patchBuffer));
                log.info("adding patches: {}", recCard.patches);
                recCards.add(recCard);
            }
        }     
        
        
        rv.recommendations = recCards;
        var apiResponse = ApiResponse.builder()
                                    .code(Response.SC_OK)
                                    .data(Map.of("payload", rv))
                                    .build();

        return apiResponse;
    }

    /**
     * Find edits for a given dataset_metrics_id using JDBC
     * 
     * @param datasetMetricsId the dataset metrics id
     * @return list of edits
     */
    private List<Edit> findEditsByDatasetMetricsId(Long datasetMetricsId) {
        String sql = """
            SELECT e.*, d.purl as datasource_purl
            FROM edit e
            INNER JOIN datasource d ON e.datasource_id = d.id
            WHERE e.dataset_metrics_id = ?
            """;
        
        return jdbcTemplate.query(sql, new EditRowMapper(), datasetMetricsId);
    }

    /**
     * RowMapper to convert ResultSet to Edit entity
     */
    private static class EditRowMapper implements RowMapper<Edit> {
        @Override
        public Edit mapRow(ResultSet rs, int rowNum) throws SQLException {
            Edit edit = new Edit();
            edit.setId(rs.getLong("id"));
            edit.setBefore(rs.getString("before"));
            edit.setAfter(rs.getString("after"));
            edit.setAvoidsVulnerabilitiesRank(rs.getInt("avoids_vulnerabilities_rank"));
            edit.setDecreaseVulnerabilityCountRank(rs.getInt("decrease_vulnerability_count_rank"));
            edit.setDecreaseBacklogRank(rs.getInt("decrease_backlog_rank"));
            edit.setIncreaseImpactRank(rs.getInt("increase_impact_rank"));
            
            // Create a minimal Datasource object with just the purl
            Datasource datasource = new Datasource();
            datasource.setPurl(rs.getString("datasource_purl"));
            edit.setDatasource(datasource);
            
            return edit;
        }
    }

    /**
     * Find dataset metrics by txid using JDBC (no dataset filter)
     * 
     * @param txid the transaction id
     * @return list of dataset metrics that are forecast recommendations taken
     */
    private List<DatasetMetrics> findDatasetMetricsByTxid(UUID txid) {
        String sql = """
            SELECT dm.*
            FROM dataset_metrics dm
            WHERE dm.txid = ?
            AND dm.is_forecast_recommendations_taken = true
            """;
        
        return jdbcTemplate.query(sql, new DatasetMetricsRowMapper(), txid);
    }

    /**
     * Find dataset metrics by txid and dataset name using JDBC
     * 
     * @param txid the transaction id
     * @param datasetName the dataset name to filter by
     * @return list of dataset metrics that are forecast recommendations taken for the specified dataset
     */
    private List<DatasetMetrics> findDatasetMetricsByTxidAndDataset(UUID txid, String datasetName) {
        String sql = """
            SELECT dm.*
            FROM dataset_metrics dm
            INNER JOIN dataset d ON dm.dataset_id = d.id
            WHERE dm.txid = ?
            AND d.name = ?
            AND dm.is_forecast_recommendations_taken = true
            """;
        
        return jdbcTemplate.query(sql, new DatasetMetricsRowMapper(), txid, datasetName);
    }

    /**
     * RowMapper to convert ResultSet to DatasetMetrics entity
     */
    private static class DatasetMetricsRowMapper implements RowMapper<DatasetMetrics> {
        @Override
        public DatasetMetrics mapRow(ResultSet rs, int rowNum) throws SQLException {
            DatasetMetrics dm = new DatasetMetrics();
            dm.setId(rs.getLong("id"));
            dm.setDatasourceCount(rs.getLong("datasource_count"));
            dm.setDatasourceEventCount(rs.getLong("datasource_event_count"));
            
            // Convert Timestamp to ZonedDateTime in UTC
            var commitTimestamp = rs.getTimestamp("commit_date_time");
            if (commitTimestamp != null) {
                dm.setCommitDateTime(ZonedDateTime.ofInstant(
                    commitTimestamp.toInstant(), 
                    java.time.ZoneOffset.UTC
                ));
            }
            
            var eventTimestamp = rs.getTimestamp("event_date_time");
            if (eventTimestamp != null) {
                dm.setEventDateTime(ZonedDateTime.ofInstant(
                    eventTimestamp.toInstant(), 
                    java.time.ZoneOffset.UTC
                ));
            }
            
            var forecastTimestamp = rs.getTimestamp("forecast_maturity_date");
            if (forecastTimestamp != null) {
                dm.setForecastMaturityDate(ZonedDateTime.ofInstant(
                    forecastTimestamp.toInstant(), 
                    java.time.ZoneOffset.UTC
                ));
            }
            
            // Handle UUIDs
            String txidStr = rs.getString("txid");
            if (txidStr != null) {
                dm.setTxid(UUID.fromString(txidStr));
            }
            
            String jobIdStr = rs.getString("job_id");
            if (jobIdStr != null) {
                dm.setJobId(UUID.fromString(jobIdStr));
            }
            
            dm.setCurrent(rs.getBoolean("is_current"));
            dm.setForecastSameCourse(rs.getBoolean("is_forecast_same_course"));
            dm.setForecastRecommendationsTaken(rs.getBoolean("is_forecast_recommendations_taken"));
            
            String recTypeStr = rs.getString("recommendation_type");
            if (recTypeStr != null) {
                dm.setRecommendationType(DatasetMetrics.RecommendationType.valueOf(recTypeStr));
            }
            
            dm.setRecommendationHeadline(rs.getString("recommendation_headline"));
            dm.setRpsScore(rs.getDouble("rps_score"));
            
            // Set all the findings fields
            dm.setTotalFindings(rs.getLong("total_findings"));
            dm.setCriticalFindings(rs.getLong("critical_findings"));
            dm.setHighFindings(rs.getLong("high_findings"));
            dm.setMediumFindings(rs.getLong("medium_findings"));
            dm.setLowFindings(rs.getLong("low_findings"));
            
            dm.setFindingsAvoidedByPatchingPastYear(rs.getLong("findings_avoided_by_patching_past_year"));
            dm.setCriticalFindingsAvoidedByPatchingPastYear(rs.getLong("critical_findings_avoided_by_patching_past_year"));
            dm.setHighFindingsAvoidedByPatchingPastYear(rs.getLong("high_findings_avoided_by_patching_past_year"));
            dm.setMediumFindingsAvoidedByPatchingPastYear(rs.getLong("medium_findings_avoided_by_patching_past_year"));
            dm.setLowFindingsAvoidedByPatchingPastYear(rs.getLong("low_findings_avoided_by_patching_past_year"));
            
            // Backlog fields
            dm.setFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("findings_in_backlog_between_thirty_and_sixty_days"));
            dm.setCriticalFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("critical_findings_in_backlog_between_thirty_and_sixty_days"));
            dm.setHighFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("high_findings_in_backlog_between_thirty_and_sixty_days"));
            dm.setMediumFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("medium_findings_in_backlog_between_thirty_and_sixty_days"));
            dm.setLowFindingsInBacklogBetweenThirtyAndSixtyDays(rs.getDouble("low_findings_in_backlog_between_thirty_and_sixty_days"));
            
            dm.setFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("findings_in_backlog_between_sixty_and_ninety_days"));
            dm.setCriticalFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("critical_findings_in_backlog_between_sixty_and_ninety_days"));
            dm.setHighFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("high_findings_in_backlog_between_sixty_and_ninety_days"));
            dm.setMediumFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("medium_findings_in_backlog_between_sixty_and_ninety_days"));
            dm.setLowFindingsInBacklogBetweenSixtyAndNinetyDays(rs.getDouble("low_findings_in_backlog_between_sixty_and_ninety_days"));
            
            dm.setFindingsInBacklogOverNinetyDays(rs.getDouble("findings_in_backlog_over_ninety_days"));
            dm.setCriticalFindingsInBacklogOverNinetyDays(rs.getDouble("critical_findings_in_backlog_over_ninety_days"));
            dm.setHighFindingsInBacklogOverNinetyDays(rs.getDouble("high_findings_in_backlog_over_ninety_days"));
            dm.setMediumFindingsInBacklogOverNinetyDays(rs.getDouble("medium_findings_in_backlog_over_ninety_days"));
            dm.setLowFindingsInBacklogOverNinetyDays(rs.getDouble("low_findings_in_backlog_over_ninety_days"));
            
            // Package fields
            dm.setPackages(rs.getLong("packages"));
            dm.setPackagesWithFindings(rs.getLong("packages_with_findings"));
            dm.setPackagesWithCriticalFindings(rs.getLong("packages_with_critical_findings"));
            dm.setPackagesWithHighFindings(rs.getLong("packages_with_high_findings"));
            dm.setPackagesWithMediumFindings(rs.getLong("packages_with_medium_findings"));
            dm.setPackagesWithLowFindings(rs.getLong("packages_with_low_findings"));
            
            // Downlevel packages
            dm.setDownlevelPackages(rs.getLong("downlevel_packages"));
            dm.setDownlevelPackagesMajor(rs.getLong("downlevel_packages_major"));
            dm.setDownlevelPackagesMinor(rs.getLong("downlevel_packages_minor"));
            dm.setDownlevelPackagesPatch(rs.getLong("downlevel_packages_patch"));
            
            // Stale packages
            dm.setStalePackages(rs.getLong("stale_packages"));
            dm.setStalePackagesSixMonths(rs.getLong("stale_packages_six_months"));
            dm.setStalePackagesOneYear(rs.getLong("stale_packages_one_year"));
            dm.setStalePackagesOneYearSixMonths(rs.getLong("stale_packages_one_year_six_months"));
            dm.setStalePackagesTwoYears(rs.getLong("stale_packages_two_years"));
            
            // Patches
            dm.setPatches(rs.getLong("patches"));
            dm.setSamePatches(rs.getLong("same_patches"));
            dm.setDifferentPatches(rs.getLong("different_patches"));
            dm.setPatchFoxPatches(rs.getLong("patch_fox_patches"));
            
            dm.setPatchEfficacyScore(rs.getDouble("patch_efficacy_score"));
            dm.setPatchImpact(rs.getDouble("patch_impact"));
            dm.setPatchEffort(rs.getDouble("patch_effort"));
            
            // Note: edits and dataset relationships are lazy-loaded and won't be populated here
            // If you need them, you'll need to fetch them separately
            
            return dm;
        }
    }

}
