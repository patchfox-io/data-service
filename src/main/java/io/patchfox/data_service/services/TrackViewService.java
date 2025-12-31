package io.patchfox.data_service.services;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import io.patchfox.data_service.json.TrackCveBacklogView;
import io.patchfox.data_service.json.TrackCveSeverityView;
import io.patchfox.data_service.json.TrackCvesAvoidedView;
import io.patchfox.data_service.json.TrackDownlevelPackagesView;
import io.patchfox.data_service.json.TrackPesView;
import io.patchfox.data_service.json.TrackDoubleView;
import io.patchfox.data_service.json.TrackStalePackagesView;

import io.patchfox.data_service.repositories.DatasetMetricsRepository;
import io.patchfox.db_entities.entities.DatasetMetrics;
import io.patchfox.package_utils.json.ApiResponse;
import io.patchfox.package_utils.util.CvssSeverity;
import lombok.extern.slf4j.Slf4j;



@Service
@Slf4j
public class TrackViewService {
 
    @Autowired
    DatasetMetricsRepository datasetMetricsRepository;

    public static String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    public ApiResponse getTrackView(
            UUID txid, 
            ZonedDateTime requestReceivedAt, 
            Optional<Boolean> synopsisOptional,
            Optional<String> dataset
    ) {
        var synopsisFlag = synopsisOptional.isPresent() ? synopsisOptional.get() : true;
        var rv = new HashMap<String, Object>();

        var latestDatasetMetricsRecordOptional = 
            datasetMetricsRepository.findFirstByIsCurrentOrderByCommitDateTimeDesc(true);

        if (latestDatasetMetricsRecordOptional.isPresent()) {
            var latestDatasetMetricsRecord = latestDatasetMetricsRecordOptional.get();
            log.info("latestDatasetMetricsRecord id is: {}", latestDatasetMetricsRecord.getId());

            var commitDateTime = latestDatasetMetricsRecord.getCommitDateTime();
            var threeMonthsPriorToCommitDateTime = commitDateTime.minusMonths(3);
            log.info("threeMonthsPriorCommit datetime is: {}", threeMonthsPriorToCommitDateTime);

            var historicalDatasetMetricsRecordsAsc = 
                datasetMetricsRepository.findByIsCurrentAndCommitDateTimeAfterOrderByCommitDateTimeAsc(
                    true,
                    threeMonthsPriorToCommitDateTime
                );

            if (dataset.isPresent()) {
                historicalDatasetMetricsRecordsAsc = filterByDataset(historicalDatasetMetricsRecordsAsc, dataset.get());

                // no records present for requested dataset
                if (historicalDatasetMetricsRecordsAsc.isEmpty()) {
                    return ApiResponse.builder()
                                      .requestReceivedAt(requestReceivedAt)
                                      .txid(txid)
                                      .code(HttpStatus.NOT_FOUND.value())
                                      .serverMessage("unknown dataset")
                                      .build();
                }
            }

            historicalDatasetMetricsRecordsAsc = applySynopsisCondition(synopsisFlag, historicalDatasetMetricsRecordsAsc);
            rv.put("latestCommitDateTime", commitDateTime.format(DateTimeFormatter.ofPattern(ISO_FORMAT)));

            rv.put(
                "historicalCommitDateTime", 
                historicalDatasetMetricsRecordsAsc.get(0)
                                                  .getCommitDateTime()
                                                  .format(DateTimeFormatter.ofPattern(ISO_FORMAT))
            );

            // eject with empty reponse if there's not enough data to do what we need to do. 
            if (historicalDatasetMetricsRecordsAsc.size() < 2) {
                return ApiResponse.builder()
                                  .requestReceivedAt(requestReceivedAt)
                                  .txid(txid)
                                  .code(HttpStatus.OK.value())
                                  .data(rv)
                                  .serverMessage("not enough historical data to render track data")
                                  .build();
            }

            rv.put("cvesAvoided", getTrackCvesAvoidedView(historicalDatasetMetricsRecordsAsc));
    
            rv.put("cveSeverity", getTrackCveSeverityView(historicalDatasetMetricsRecordsAsc));
    
            rv.put("cveBacklog", getTrackCveBacklogView(historicalDatasetMetricsRecordsAsc));
    
            rv.put("stalePackages", getTrackStalePackagesView(historicalDatasetMetricsRecordsAsc));
    
            rv.put("downLevelPackages", getTrackDownlevelPackagesView(historicalDatasetMetricsRecordsAsc));
    
            rv.put("pes", getTrackPesView(historicalDatasetMetricsRecordsAsc));
    
            rv.put("rps", getTrackRpsView(historicalDatasetMetricsRecordsAsc));
        }

        return ApiResponse.builder()
                          .requestReceivedAt(requestReceivedAt)
                          .txid(txid)
                          .code(HttpStatus.OK.value())
                          .data(rv)
                          .build();
    }


    /**
     * Filter dataset metrics by dataset name, preserving order
     * 
     * @param metrics the list of dataset metrics to filter
     * @param datasetName the name of the dataset to filter by
     * @return filtered list maintaining original order
     */
    private List<DatasetMetrics> filterByDataset(List<DatasetMetrics> metrics, String datasetName) {
        return metrics.stream()
                      .filter(dm -> dm.getDataset() != null && datasetName.equals(dm.getDataset().getName()))
                      .collect(Collectors.toList());
    }


    /**
     * if synopsisFlag, method returns a view of argument series with only one datapoint per calendar day 
     */
    public List<DatasetMetrics> applySynopsisCondition(boolean synopsisFlag, List<DatasetMetrics> series) {

        if (!synopsisFlag) { return series; }

        Map<String, List<DatasetMetrics>> bucketedByDate = 
            series.stream()
                .collect(
                    Collectors.groupingBy(
                        x -> x.getCommitDateTime()
                              .format(DateTimeFormatter.ofPattern("MM_dd_yyyy")
                    )
                )
            );

        log.debug("bucketedByDate is: {}", bucketedByDate);
        var rv = new ArrayList<DatasetMetrics>();
        for (var entrySet : bucketedByDate.entrySet()) {
            var numberOfDatapoints = entrySet.getValue().size();
            if (numberOfDatapoints < 2) { 
                rv.add(entrySet.getValue().getFirst());
                continue; 
            }
            log.info("filtering: {} of {} datapoints for date: {}", numberOfDatapoints - 1, numberOfDatapoints, entrySet.getKey());
            rv.add(entrySet.getValue().getLast());
        }

        rv.sort((x1, x2) -> x1.getCommitDateTime().compareTo(x2.getCommitDateTime()));
        return rv;
    }


    /**
     * 
     * @return
     */
    //
    // TODO update card to be in alignment with current production ones 
    //
    //
    //    
    public TrackCvesAvoidedView getTrackCvesAvoidedView(List<DatasetMetrics> historicalDatasetMetricsRecordsAsc) {
        var historicalRecord = historicalDatasetMetricsRecordsAsc.get(0);
        var currentRecord = historicalDatasetMetricsRecordsAsc.getLast();
        var rv = new TrackCvesAvoidedView();
        rv.setName("CVEs avoided");
        return rv;
    }


    /**
     * 
     * @return
     */
    public TrackCveSeverityView getTrackCveSeverityView(List<DatasetMetrics> historicalDatasetMetricsRecordsAsc) {
        var rv = new TrackCveSeverityView();
        rv.setName("CVE Severity");

        if (historicalDatasetMetricsRecordsAsc.isEmpty()) { 
            log.warn("no historical data found - returning empty payload");
            return rv; 
        }

        var series = historicalDatasetMetricsRecordsAsc.stream().map(hsdsm -> cveSeverityViewHelper(hsdsm)).toList();
        rv.setSeries(series);
        rv.setCurrent(series.getLast());

        var prior = series.size() < 2 ? series.getLast() : series.get(series.size() - 2);
        rv.setPrior(prior);

        rv.setHistorical(series.getFirst());

        var currentRecord = historicalDatasetMetricsRecordsAsc.getLast();
        var priorRecord = historicalDatasetMetricsRecordsAsc.get(historicalDatasetMetricsRecordsAsc.size() - 2);
        var historicalRecord = historicalDatasetMetricsRecordsAsc.getFirst();



        //
        var currentTotal = 
            currentRecord.getCriticalFindings()
            + currentRecord.getHighFindings()
            + currentRecord.getMediumFindings()
            + currentRecord.getLowFindings();

        var priorTotal = 
            priorRecord.getCriticalFindings()
            + priorRecord.getHighFindings()
            + priorRecord.getMediumFindings()
            + priorRecord.getLowFindings();
        
        var historicalTotal = 
            historicalRecord.getCriticalFindings()
            + historicalRecord.getHighFindings()
            + historicalRecord.getMediumFindings()
            + historicalRecord.getLowFindings();       

        var percentChangePrior = calculatePercentageDifference(currentTotal, priorTotal);
        //(double)(currentTotal - priorTotal) / currentTotal;
        percentChangePrior = percentChangePrior == 0 ? percentChangePrior *= -1 : percentChangePrior;
        rv.setPercentChangePrior(doubleHelper(percentChangePrior));

        var positiveImpactPrior = percentChangePrior > 0 ? false : true;
        rv.setPositiveImpactPrior(positiveImpactPrior);

        var trendingUpPrior = !positiveImpactPrior;
        rv.setTrendingUpPrior(trendingUpPrior);

        var percentChangeHistorical = calculatePercentageDifference(currentTotal, historicalTotal);
        //(double)(currentTotal - historicalTotal) / currentTotal;
        percentChangeHistorical = percentChangeHistorical == 0 ? percentChangeHistorical *= -1 : percentChangeHistorical;
        rv.setPercentChangeHistorical(doubleHelper(percentChangeHistorical));

        var positiveImpactHistorical = percentChangeHistorical > 0 ? false : true;
        rv.setPositiveImpactHistorical(positiveImpactHistorical);

        var trendingUpHistorical = !positiveImpactHistorical;
        rv.setTrendingUpHistorical(trendingUpHistorical);

        return rv;
    }


    /**
     * 
     * @param record
     * @return
     */
    public TrackCveSeverityView.Value cveSeverityViewHelper(DatasetMetrics record) {
        var name = record.getCommitDateTime().toString();

        var value = List.of(
            new TrackDoubleView.Value(
                CvssSeverity.CRITICAL.toString(), 
                record.getCriticalFindings(),
                ""
            ),        
            new TrackDoubleView.Value(
                CvssSeverity.HIGH.toString(), 
                record.getHighFindings(),
                ""
            ), 
            new TrackDoubleView.Value(
                CvssSeverity.MEDIUM.toString(), 
                record.getMediumFindings(),
                ""
            ), 
            new TrackDoubleView.Value(
                CvssSeverity.LOW.toString(), 
                record.getLowFindings(),
                ""
            )
        );

        return new TrackCveSeverityView.Value(name, value, "");

    }



    /**
     * 
     * @return
     */
    public TrackCveBacklogView getTrackCveBacklogView(List<DatasetMetrics> historicalDatasetMetricsRecordsAsc) {
        var rv = new TrackCveBacklogView();
        rv.setName("CVE Backlog");

        if (historicalDatasetMetricsRecordsAsc.isEmpty()) { 
            log.warn("no historical data found - returning empty payload");
            return rv; 
        }

        var series = historicalDatasetMetricsRecordsAsc.stream().map(hsdsr -> cveBacklogViewHelper(hsdsr)).toList();
        rv.setSeries(series);
        rv.setCurrent(series.getLast());
        var prior = series.size() == 1 ? series.getLast() : series.get(series.size() - 2);
        rv.setPrior(prior);
        rv.setHistorical(series.getFirst());


        //
        var currentRecord = historicalDatasetMetricsRecordsAsc.getLast();
        var currentBacklogTotal = 
            currentRecord.getFindingsInBacklogBetweenThirtyAndSixtyDays()
            + currentRecord.getFindingsInBacklogBetweenSixtyAndNinetyDays()
            + currentRecord.getFindingsInBacklogOverNinetyDays();

            
        var priorRecord = historicalDatasetMetricsRecordsAsc.size() == 1 
                                ? historicalDatasetMetricsRecordsAsc.getLast()
                                : historicalDatasetMetricsRecordsAsc.get(historicalDatasetMetricsRecordsAsc.size() - 2);

        var priorBacklogTotal = 
            priorRecord.getFindingsInBacklogBetweenThirtyAndSixtyDays()
            + priorRecord.getFindingsInBacklogBetweenSixtyAndNinetyDays()
            + priorRecord.getFindingsInBacklogOverNinetyDays();
        

        var historicalRecord = historicalDatasetMetricsRecordsAsc.getFirst();
        var historicalBacklogTotal = 
            historicalRecord.getFindingsInBacklogBetweenThirtyAndSixtyDays()
            + historicalRecord.getFindingsInBacklogBetweenSixtyAndNinetyDays()
            + historicalRecord.getFindingsInBacklogOverNinetyDays();        

        var percentChangePrior = calculatePercentageDifference(currentBacklogTotal, priorBacklogTotal);
        //(double)(currentBacklogTotal - priorBacklogTotal) / Math.abs(currentBacklogTotal);
        //percentChangePrior = priorBacklogTotal == 0 ? percentChangePrior *= -1 : percentChangePrior;
        rv.setPercentChangePrior(doubleHelper(percentChangePrior));

        var positiveImpactPrior = percentChangePrior > 0 ? false : true;
        rv.setPositiveImpactPrior(positiveImpactPrior);

        var trendingUpPrior = !positiveImpactPrior;
        rv.setTrendingUpPrior(trendingUpPrior);

        var percentChangeHistorical = calculatePercentageDifference(currentBacklogTotal, historicalBacklogTotal);
        //(double)(currentBacklogTotal - historicalBacklogTotal) / Math.abs(currentBacklogTotal);
        //percentChangeHistorical = historicalBacklogTotal == 0 ? percentChangeHistorical *= -1 : percentChangeHistorical;
        rv.setPercentChangeHistorical(doubleHelper(percentChangeHistorical));

        var positiveImpactHistorical = percentChangeHistorical > 0 ? false : true;
        rv.setPositiveImpactHistorical(positiveImpactHistorical);

        var trendingUpHistorical = !positiveImpactHistorical;
        rv.setTrendingUpHistorical(trendingUpHistorical);

        return rv;
    }


    /**
     * 
     * @param record
     * @return
     */
    public TrackCveBacklogView.Value cveBacklogViewHelper(DatasetMetrics record) {
        var currentThirty = List.of(
            new TrackDoubleView.Value(
                CvssSeverity.CRITICAL.toString(), 
                record.getCriticalFindingsInBacklogBetweenThirtyAndSixtyDays(),
                ""
            ),        
            new TrackDoubleView.Value(
                CvssSeverity.HIGH.toString(), 
                record.getHighFindingsInBacklogBetweenThirtyAndSixtyDays(),
                ""
            ), 
            new TrackDoubleView.Value(
                CvssSeverity.MEDIUM.toString(), 
                record.getMediumFindingsInBacklogBetweenThirtyAndSixtyDays(),
                ""
            ), 
            new TrackDoubleView.Value(
                CvssSeverity.LOW.toString(), 
                record.getLowFindingsInBacklogBetweenThirtyAndSixtyDays(),
                ""
            )
        );

        var currentSixty = List.of(
            new TrackDoubleView.Value(
                CvssSeverity.CRITICAL.toString(), 
                record.getCriticalFindingsInBacklogBetweenSixtyAndNinetyDays(),
                ""
            ),        
            new TrackDoubleView.Value(
                CvssSeverity.HIGH.toString(), 
                record.getHighFindingsInBacklogBetweenSixtyAndNinetyDays(),
                ""
            ), 
            new TrackDoubleView.Value(
                CvssSeverity.MEDIUM.toString(), 
                record.getMediumFindingsInBacklogBetweenSixtyAndNinetyDays(),
                ""
            ), 
            new TrackDoubleView.Value(
                CvssSeverity.LOW.toString(), 
                record.getLowFindingsInBacklogBetweenSixtyAndNinetyDays(),
                ""
            )
        );

        var currentNinety = List.of(
            new TrackDoubleView.Value(
                CvssSeverity.CRITICAL.toString(), 
                record.getCriticalFindingsInBacklogOverNinetyDays(),
                ""
            ),        
            new TrackDoubleView.Value(
                CvssSeverity.HIGH.toString(), 
                record.getHighFindingsInBacklogOverNinetyDays(),
                ""
            ), 
            new TrackDoubleView.Value(
                CvssSeverity.MEDIUM.toString(), 
                record.getMediumFindingsInBacklogOverNinetyDays(),
                ""
            ), 
            new TrackDoubleView.Value(
                CvssSeverity.LOW.toString(), 
                record.getLowFindingsInBacklogOverNinetyDays(),
                ""
            )
        ); 

        var name = record.getCommitDateTime().toString();

        var value = Map.of(
            "betweenThirtyAndSixtyDays", currentThirty,
            "betweenSixtyAndNinetyDays", currentSixty,
            "ninetyDaysPlus", currentNinety
        );

        return new TrackCveBacklogView.Value(name, value, "");

    }


    /**
     * 
     * @return
     */  
    public TrackStalePackagesView getTrackStalePackagesView(List<DatasetMetrics> historicalDatasetMetricsRecordsAsc) {
        var historicalRecord = historicalDatasetMetricsRecordsAsc.get(0);
        var currentRecord = historicalDatasetMetricsRecordsAsc.getLast();      
        var priorRecord = historicalDatasetMetricsRecordsAsc.get(historicalDatasetMetricsRecordsAsc.size() - 2); 
        
        var rv = new TrackStalePackagesView();
        rv.setName("Stale Packages");

        List<TrackStalePackagesView.Value> series = new ArrayList<>();
        for (var hdsm : historicalDatasetMetricsRecordsAsc) {
            var seriesValue = new TrackStalePackagesView.Value(
                hdsm.getCommitDateTime().toString(), 
                Map.of(
                    "totalStalePackages", new TrackDoubleView.Value("Total Stale Packages", (double)hdsm.getStalePackages(), ""),
                    "sixMonthStalePackages", new TrackDoubleView.Value("Stale Packages Six Months", (double)hdsm.getStalePackagesSixMonths(), ""),
                    "oneYearStalePackages", new TrackDoubleView.Value("Stale Packages One Year", (double)hdsm.getStalePackagesOneYear(), ""),
                    "oneYearSixMonthsStalePackages", new TrackDoubleView.Value("Stale Packages One Year Six Months", (double)hdsm.getStalePackagesOneYearSixMonths(), ""),
                    "twoYearStalePackages", new TrackDoubleView.Value("Stale Packages Two Years", (double)hdsm.getStalePackagesTwoYears(), "")
                ), 
                ""
            );

            series.add(seriesValue);
        }

        rv.setSeries(series);
        rv.setCurrent(series.getLast());
        rv.setPrior(series.get(series.size() - 2));
        rv.setHistorical(series.get(0));

        // var percentChangedPrior = 
        //     (double)currentRecord.getStalePackages() / 
        //         (double)(currentRecord.getStalePackages() - priorRecord.getStalePackages()) * 100;
        
        var percentChangedPrior = calculatePercentageDifference(currentRecord.getStalePackages(), priorRecord.getStalePackages());
        percentChangedPrior = doubleHelper(percentChangedPrior);        

        rv.setPercentChangePrior(percentChangedPrior);
        rv.setPositiveImpactPrior(percentChangedPrior <= 0);
        rv.setTrendingUpPrior(percentChangedPrior > 0);

        // var percentChangedHistorical = 
        //     (double)currentRecord.getStalePackages() / 
        //         (double)(currentRecord.getStalePackages() - historicalRecord.getStalePackages()) * 100;
        
        var percentChangedHistorical = calculatePercentageDifference(currentRecord.getStalePackages(), historicalRecord.getStalePackages());
        percentChangedHistorical = doubleHelper(percentChangedHistorical);        

        rv.setPercentChangeHistorical(percentChangedHistorical);
        rv.setPositiveImpactHistorical(percentChangedHistorical <= 0);
        rv.setTrendingUpHistorical(percentChangedHistorical > 0);

        // TODO deal with future trends 
        
        return rv;
    }

 
    /**
     * 
     * @return
     */
    public TrackDownlevelPackagesView getTrackDownlevelPackagesView(List<DatasetMetrics> historicalDatasetMetricsRecordsAsc) {
        var historicalRecord = historicalDatasetMetricsRecordsAsc.get(0);
        var currentRecord = historicalDatasetMetricsRecordsAsc.getLast();      
        var priorRecord = historicalDatasetMetricsRecordsAsc.get(historicalDatasetMetricsRecordsAsc.size() - 2); 

        var rv = new TrackDownlevelPackagesView();
        rv.setName("Downlevel Packages");
        
        List<TrackStalePackagesView.Value> series = new ArrayList<>();
        for (var hdsm : historicalDatasetMetricsRecordsAsc) {
            var seriesValue = new TrackStalePackagesView.Value(
                hdsm.getCommitDateTime().toString(), 
                Map.of(
                    "totalDownlevelPackages", new TrackDoubleView.Value("Total Downlevel Packages", (double)hdsm.getDownlevelPackages(), ""),
                    "majorDownlevelPackages", new TrackDoubleView.Value("Major Downlevel Packages", (double)hdsm.getDownlevelPackagesMajor(), ""),
                    "minorDownlevelPackages", new TrackDoubleView.Value("Minor Downlevel Packages", (double)hdsm.getDownlevelPackagesMinor(), ""),
                    "patchDownlevelPackages", new TrackDoubleView.Value("Patch Downlevel Packages", (double)hdsm.getDownlevelPackagesPatch(), "")
                ), 
                ""
            );

            series.add(seriesValue);
        }

        rv.setSeries(series);
        rv.setCurrent(series.getLast());
        rv.setPrior(series.get(series.size() - 2));
        rv.setHistorical(series.get(0));

        // var percentChangedPrior = 
        //     (double)currentRecord.getDownlevelPackages() / 
        //         (double)(currentRecord.getDownlevelPackages() - priorRecord.getDownlevelPackages()) * 100;
            
        var percentChangedPrior = calculatePercentageDifference(currentRecord.getDownlevelPackages(), priorRecord.getDownlevelPackages());
        percentChangedPrior = doubleHelper(percentChangedPrior);

        rv.setPercentChangePrior(percentChangedPrior);
        rv.setPositiveImpactPrior(percentChangedPrior <= 0);
        rv.setTrendingUpPrior(percentChangedPrior > 0);

        // var percentChangedHistorical = 
        //     (double)currentRecord.getDownlevelPackages() / 
        //         (double)(currentRecord.getDownlevelPackages() - historicalRecord.getDownlevelPackages()) * 100;
        
        var percentChangedHistorical = calculatePercentageDifference(currentRecord.getDownlevelPackages(), historicalRecord.getDownlevelPackages());
        percentChangedHistorical = doubleHelper(percentChangedHistorical);

        rv.setPercentChangeHistorical(percentChangedHistorical);
        rv.setPositiveImpactHistorical(percentChangedHistorical <= 0);
        rv.setTrendingUpHistorical(percentChangedHistorical > 0);

        // TODO deal with future trends 
        
        return rv;
    }


    /**
     * 
     * @return
     */
    public TrackPesView getTrackPesView(List<DatasetMetrics> historicalDatasetMetricsRecordsAsc) {
        var rv = new TrackPesView();
        rv.setName("Patch Efficacy Score (PES)");

        var series = historicalDatasetMetricsRecordsAsc.stream()
                                                       .map(
                                                          hdsmr -> 
                                                          new TrackPesView.Value(
                                                              hdsmr.getCommitDateTime().toString(),
                                                              hdsmr.getPatchEfficacyScore(),
                                                              ""
                                                          )
                                                       )
                                                       .toList();

        rv.setSeries(series);

        if (series.isEmpty()) { 
            log.warn("no historical data found - returning empty payload");
            return rv; 
        }

        var impactSeries = historicalDatasetMetricsRecordsAsc.stream()
                                                             .map(
                                                                 hdsmr -> 
                                                                 new TrackPesView.Value(
                                                                     hdsmr.getCommitDateTime().toString(),
                                                                     hdsmr.getPatchImpact(),
                                                                     ""
                                                                 )
                                                             )
                                                             .toList();

        rv.setImpactSeries(impactSeries);

        var effortSeries = historicalDatasetMetricsRecordsAsc.stream()
                                                             .map(
                                                                hdsmr -> 
                                                                    new TrackPesView.Value(
                                                                        hdsmr.getCommitDateTime().toString(),
                                                                        hdsmr.getPatchEffort(),
                                                                        ""
                                                                    )
                                                                )
                                                             .toList();

        rv.setEffortSeries(effortSeries);

        var current = series.getLast();
        rv.setCurrent(current);

        var prior = series.get(series.size() - 2);
        rv.setPrior(prior);

        var historical = series.get(0);
        rv.setHistorical(historical);

        //var percentChangeFromPrior = (double)(current.getData() - prior.getData()) / Math.abs(current.getData());
        var percentChangeFromPrior = calculatePercentageDifference(current.getData(), prior.getData());
        //percentChangeFromPrior = prior == 0 ? percentChangeFromPrior *= -1 : percentChangeFromPrior;
        rv.setPercentChangePrior(doubleHelper(percentChangeFromPrior));

        // positiveImpact means Redundant Package Score did not go up 
        var positiveImpactPrior = current.getData() > prior.getData() ? true : false;
        rv.setPositiveImpactPrior(positiveImpactPrior);

        var trendingUpPrior = positiveImpactPrior;
        rv.setTrendingUpPrior(trendingUpPrior);

        //var percentChangeHistorical = (double)(current.getData() - historical.getData()) / Math.abs(current.getData());
        var percentChangeHistorical = calculatePercentageDifference(current.getData(), historical.getData());
        //percentChangeHistorical = historical == 0 ? percentChangeHistorical *= -1 : percentChangeHistorical;
        rv.setPercentChangeHistorical(doubleHelper(percentChangeHistorical));

        var positiveImpactHistorical = current.getData() > historical.getData() ? true : false;
        rv.setPositiveImpactHistorical(positiveImpactHistorical);

        var trendingUpHistorical = positiveImpactHistorical;
        rv.setTrendingUpHistorical(trendingUpHistorical);

        return rv;
    }
    
  
    /**
     * 
     * @return
     */
    public TrackDoubleView getTrackRpsView(List<DatasetMetrics> historicalDatasetMetricsRecordsAsc) {

        var rv = new TrackDoubleView();
        rv.setName("Redundant Package Score (RPS)");
        
        var series = historicalDatasetMetricsRecordsAsc.stream()
                                                       .map(
                                                            hdsmr -> 
                                                                new TrackDoubleView.Value(
                                                                    hdsmr.getCommitDateTime().toString(),
                                                                    hdsmr.getRpsScore(),
                                                                    ""
                                                                )
                                                       )
                                                       .toList();

        rv.setSeries(series);

        if (series.isEmpty()) { 
            log.warn("no historical data found - returning empty payload");
            return rv; 
        }

        var current = series.getLast();
        rv.setCurrent(current);

        var prior = series.get(series.size() - 2);
        rv.setPrior(prior);

        var historical = series.get(0);
        rv.setHistorical(historical);

        //var percentChangeFromPrior = (double)(current.getData() - prior.getData()) / Math.abs(current.getData());
        var percentChangeFromPrior = calculatePercentageDifference(current.getData(), prior.getData());
        //percentChangeFromPrior = prior == 0 ? percentChangeFromPrior *= -1 : percentChangeFromPrior;
        rv.setPercentChangePrior(doubleHelper(percentChangeFromPrior));

        // positiveImpact means Redundant Package Score did not go up 
        var positiveImpactPrior = current.getData() > prior.getData() ? false : true;
        rv.setPositiveImpactPrior(positiveImpactPrior);

        var trendingUpPrior = !positiveImpactPrior;
        rv.setTrendingUpPrior(trendingUpPrior);

        //var percentChangeHistorical = (double)(current.getData() - historical.getData()) / Math.abs(current.getData());
        var percentChangeHistorical = calculatePercentageDifference(current.getData(), historical.getData());
        //percentChangeHistorical = historical == 0 ? percentChangeHistorical *= -1 : percentChangeHistorical;
        rv.setPercentChangeHistorical(doubleHelper(percentChangeHistorical));

        var positiveImpactHistorical = current.getData() > historical.getData() ? false : true;
        rv.setPositiveImpactHistorical(positiveImpactHistorical);

        var trendingUpHistorical = !positiveImpactHistorical;
        rv.setTrendingUpHistorical(trendingUpHistorical);

        return rv;
    }

    /**
     * 
     * @param d
     */
    public double doubleHelper(double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            return 0.0;
        } else {
            return d;
        }
    }


    /**
     * 
     * @param num1
     * @param num2
     * @return
     */
    public double calculatePercentageDifference(double num1, double num2) {
        if (num2 == 0) { return 0; }
        return ((num1 - num2) / num2) * 100;
    }

}
