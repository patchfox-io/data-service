package io.patchfox.data_service.services;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.hibernate.Hibernate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import io.patchfox.QueryDslHelpers;
import io.patchfox.data_service.controllers.DatabaseQueryController;
import io.patchfox.data_service.jdbc.JdbcQueryService;
import io.patchfox.data_service.dto.DatasetDTO;
import io.patchfox.data_service.dto.DatasetMetricsDTO;
import io.patchfox.data_service.dto.DatasourceEventDTO;
import io.patchfox.data_service.dto.PackageDTO;
import io.patchfox.data_service.jdbc.JdbcQueryService.EditWithDatasourcePurl;
import io.patchfox.data_service.repositories.DatasetMetricsRepository;
import io.patchfox.data_service.repositories.DatasetRepository;
import io.patchfox.data_service.repositories.DatasourceEventRepository;
import io.patchfox.data_service.repositories.DatasourceMetricsCurrentRepository;
import io.patchfox.data_service.repositories.DatasourceMetricsRepository;
import io.patchfox.data_service.repositories.DatasourceRepository;
import io.patchfox.data_service.repositories.EditRepository;
import io.patchfox.data_service.repositories.FindingDataRepository;
import io.patchfox.data_service.repositories.FindingReporterRepository;
import io.patchfox.data_service.repositories.FindingRepository;
import io.patchfox.data_service.repositories.PackageRepository;
import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.db_entities.entities.DatasetMetrics;
import io.patchfox.db_entities.entities.Datasource;
import io.patchfox.db_entities.entities.DatasourceEvent;
import io.patchfox.db_entities.entities.DatasourceMetrics;
import io.patchfox.db_entities.entities.DatasourceMetricsCurrent;
import io.patchfox.db_entities.entities.Edit;
import io.patchfox.db_entities.entities.Finding;
import io.patchfox.db_entities.entities.FindingData;
import io.patchfox.db_entities.entities.FindingReporter;
import io.patchfox.db_entities.entities.QDataset;
import io.patchfox.db_entities.entities.QDatasetMetrics;
import io.patchfox.db_entities.entities.QDatasource;
import io.patchfox.db_entities.entities.QDatasourceEvent;
import io.patchfox.db_entities.entities.QDatasourceMetrics;
import io.patchfox.db_entities.entities.QDatasourceMetricsCurrent;
import io.patchfox.db_entities.entities.QEdit;
import io.patchfox.db_entities.entities.QFinding;
import io.patchfox.db_entities.entities.QFindingData;
import io.patchfox.db_entities.entities.QFindingReporter;
import io.patchfox.db_entities.entities.QPackage;
import io.patchfox.package_utils.json.ApiResponse;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Persistence;
import jakarta.persistence.PersistenceContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@Transactional(readOnly = true)
public class DatabaseQueryService {
    
    public static Integer MIN_SIZE = 1;

    public static Integer MAX_SIZE = 1000;

    public static String TITLE_PAGE_KEY = "titlePage";

    public static String ID_KEY = "id";

    public static String TXID_KEY = "txid";

    public static String PURL_KEY = "purl";

    public static String COMMIT_DATE_TIME_KEY = "commitDateTime";

    public static String SORT_KEY = "sort";

    // this is in db-entities which has NO spring anything in its dependency graph 
    private QueryDslHelpers queryDslHelpers = new QueryDslHelpers();

    //

    @Autowired
    private DatasetMetricsRepository datasetMetricsRepository;

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private DatasourceEventRepository datasourceEventRepository;

    @Autowired
    private DatasourceRepository datasourceRepository;

    @Autowired
    private EditRepository editRepository;

    @Autowired
    private FindingDataRepository findingDataRepository;

    @Autowired
    private FindingReporterRepository findingReporterRepository;

    @Autowired
    private FindingRepository findingRepository;

    @Autowired
    private PackageRepository packageRepository;

    @Autowired
    private DatasourceMetricsRepository datasourceMetricsRepository;

    @Autowired
    private DatasourceMetricsCurrentRepository datasourceMetricsCurrentRepository;

    @Autowired
    private JdbcQueryService jdbcQueryService;

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * 
     * this lets you apply filter criteria to a datasetmetrics query then get the edit objects hanging off those 
     * datasetmetrics records 
     * 
     * @param txid
     * @param requestReceivedAt
     * @param params
     * @param pageable
     * @return
     */
    public ApiResponse handleDatasetMetricsEditSubQuery(
        UUID txid, 
        ZonedDateTime requestReceivedAt,
        Map<String, String> params, 
        Pageable pageable
    ) {

        var dsmRecords = getDatasetMetrics(txid, requestReceivedAt, params, pageable);

        if (dsmRecords.isEmpty()) {
            return ApiResponse.builder()
                              .txid(txid)
                              .requestReceivedAt(requestReceivedAt)
                              .code(HttpStatus.OK.value())
                              .build();
        }

        // Using JDBC - DatasetMetricsDTO has no edits relationship, load via helper
        var dsmIds = dsmRecords.stream().map(DatasetMetricsDTO::getId).toList();
        var editIdsMap = jdbcQueryService.getEditIdsForDatasetMetrics(dsmIds);
        var editIndexes = editIdsMap.values().stream()
                                    .flatMap(List::stream)
                                    .map(String::valueOf)
                                    .collect(Collectors.joining(","));

        params.put(ID_KEY, editIndexes);
        log.info("params is now: {}", params);
        var mappedResult = getMappedResult(txid, requestReceivedAt, "edit", params, pageable);

        return ApiResponse.builder()
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt)
                          .code(HttpStatus.OK.value())
                          .data(mappedResult)
                          .build();
    }


    /**
     *
     * this lets you first apply filter criteria against datasetmetrics records then additionally allows you to scope
     * the query down to a subset of datasources present in those datasetmetrics records. the effect is to take
     * what you would get from handleDataestMetricsEditSubQuery and additionally scope things down to only the edits
     * associated with a subset of datasources in the dataset. 
     * 
     * @param txid
     * @param requestReceivedAt
     * @param params
     * @param pageable
     * @return
     */
    public ApiResponse handleDatasourceEditSubQuery(
        UUID txid,
        ZonedDateTime requestReceivedAt,
        Map<String, String> params,
        Pageable pageable
    ) {

        var dsmRecords = getDatasetMetrics(txid, requestReceivedAt, params, pageable);
        log.info("got {} dsmRecords back from datasource sub query", dsmRecords.size());
        if (dsmRecords.isEmpty()) {
            return ApiResponse.builder()
                              .txid(txid)
                              .requestReceivedAt(requestReceivedAt)
                              .code(HttpStatus.OK.value())
                              .data(Map.of())
                              .build();
        }
        // we assume controller has validated this argument is present
        // this is an Edit obj key, not a DatasetMetrics key so we'll pull it out now
        var datasourcesPurlValue = params.get(DatabaseQueryController.EDIT_DATASOURCES_PURL_KEY);

        List<String> datasourcePurls = Arrays.stream(datasourcesPurlValue.split(","))
                                             .map(String::trim)
                                             .filter(s -> !s.isEmpty())
                                             .toList();

        // Using JDBC - DatasetMetricsDTO has no edits relationship, load via helper
        var dsmIds = dsmRecords.stream().map(DatasetMetricsDTO::getId).toList();
        var editsWithPurl = jdbcQueryService.getEditsWithDatasourcePurl(dsmIds);
        var editIndexes = editsWithPurl.stream()
                                       .filter(ewp -> datasourcePurls.stream()
                                                                     .anyMatch(p -> ewp.datasourcePurl.contains(p)))
                                       .map(ewp -> ewp.editId)
                                       .toList();

        log.info("size of editIndexes is: {}", editIndexes.size());


        var editIndexesAsString = editIndexes.stream()
                                             .map(String::valueOf)
                                             .collect(Collectors.joining(","));

        // this is a hack to ensure when something doesn't match the datasource parameter
        // we don't return ALL edit results
        //
        // also doing this with another trip to the db is ridiculous I know - this is because I want
        // the paged and mapped result and don't want to have to make that by hand rn.
        if (editIndexes.isEmpty()) { editIndexesAsString = "-1"; }
        params.put(ID_KEY, editIndexesAsString);
        var mappedResult = getMappedResult(txid, requestReceivedAt, "edit", params, pageable);

        return ApiResponse.builder()
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt)
                          .code(HttpStatus.OK.value())
                          .data(mappedResult)
                          .build();
    }





    /**
     * this lets you apply filter criteria to a datasetmetrics query then get the package objects hanging off those 
     * datasetmetrics records 
     * 
     * @param txid
     * @param requestReceivedAt
     * @param params
     * @param pageable
     * @return
     */
    public ApiResponse handleDatasetMetricsPackageSubQuery(
        UUID txid, 
        ZonedDateTime requestReceivedAt,
        Map<String, String> params, 
        Pageable pageable
    ) {

        // expectation of contract is that this method will return only the most recent dsm record if commitDateTime
        // parameter is not specified 
        var dsmRecords = getDatasetMetrics(txid, requestReceivedAt, params, pageable);

        if (dsmRecords.isEmpty()) {
            return ApiResponse.builder()
                              .txid(txid)
                              .requestReceivedAt(requestReceivedAt)
                              .code(HttpStatus.OK.value())
                              .build();            
        } 

        // dedpulicate the list to help cull the number of parameters. >65k and we're cooked
        var packageIndexes = dsmRecords.stream()
                                       .flatMap(dsm -> dsm.getPackageIndexes().stream())
                                       .collect(Collectors.toSet())
                                       .stream()
                                       .map(String::valueOf)
                                       .collect(Collectors.joining(","));

        // because this query will deduplicate the list - we need to go in and re-duplicate it 
        var packageCounts = dsmRecords.stream()
                                      .flatMap(dsm -> dsm.getPackageIndexes().stream())
                                      .collect(Collectors.groupingBy(
                                        Function.identity(),
                                        Collectors.counting()
                                      ));
        log.info("packageCounts is: {}", packageCounts);

        // at this point all remaining k/v in params should be intended by the caller for the package table
        // here we add the package ids to create a subset of packages that existed in the in the dataset(s) specified 
        // by the caller
        params.put(ID_KEY, packageIndexes);
        var mappedResult = getMappedResult(txid, requestReceivedAt, "package", params, pageable, packageCounts);


        return ApiResponse.builder()
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt)
                          .code(HttpStatus.OK.value())
                          .data(mappedResult)
                          .build();

                          
        // var dsmIds = dsmRecords.stream().map(dsm -> dsm.getId()).toList();

        // // doing this by way of a repository method because using queryDSL results in a deduplicated list and 
        // // we don't want that. 
        // var packagePage = packageRepository.findPackagesPreservingDuplicates(dsmIds, pageable);

        // // to prevent recursive references that cause things to go boom
        // packagePage.stream()
        //         .forEach(
        //                 p -> {
        //                     p.getDatasourceEvents().clear(); 
        //                     p.getFindings().stream().forEach(
        //                             f -> {
        //                                 f.getPackages().clear();
        //                                 f.getReporters().stream().forEach(fr -> fr.getFindings().clear());
        //                                 f.getData().setFinding(null);
        //                             }
        //                         ); 

                            
        //                     p.getCriticalFindings().clear();
        //                     p.getHighFindings().clear();
        //                     p.getMediumFindings().clear();
        //                     p.getLowFindings().clear();     
        //                     }
        //             );

        // return ApiResponse.builder()
        //                   .txid(txid)
        //                   .requestReceivedAt(requestReceivedAt)
        //                   .code(HttpStatus.OK.value())
        //                   .data(Map.of(TITLE_PAGE_KEY, packagePage))
        //                   .build();

    }



    /**
     * this lets you first apply filter criteria against datasetmetrics records then additionally allows you to scope
     * the query down to a subset of datasources present in those datasetmetrics records. the effect is to take 
     * what you would get from handleDatasetMetricsPackageSubQuery and additionally scope things down to only the packages 
     * associated with a subset of datasources in the dataset. 
     * 
     * @param txid
     * @param requestReceivedAt
     * @param params
     * @param pageable
     * @return
     */
    public ApiResponse handleDatasourcePackageSubQuery(
        UUID txid,
        ZonedDateTime requestReceivedAt,
        Map<String, String> params,
        Pageable pageable
    ) {

        var dsmRecords = getDatasetMetrics(txid, requestReceivedAt, params, pageable);
        log.info("got {} dsmRecords back from datasource sub query", dsmRecords.size());
        if (dsmRecords.isEmpty()) {
            return ApiResponse.builder()
                              .txid(txid)
                              .requestReceivedAt(requestReceivedAt)
                              .code(HttpStatus.OK.value())
                              .data(Map.of())
                              .build();
        }
        // we assume controller has validated this argument is present
        // this is an Edit obj key, not a DatasetMetrics key so we'll pull it out now
        var datasourcesPurlValue = params.get(DatabaseQueryController.EDIT_DATASOURCES_PURL_KEY);

        List<String> datasourcePurls = Arrays.stream(datasourcesPurlValue.split(","))
                                             .map(String::trim)
                                             .filter(s -> !s.isEmpty())
                                             .toList();

        // Using JDBC - DatasetMetricsDTO has no edits relationship, load via helper
        var dsmIds = dsmRecords.stream().map(DatasetMetricsDTO::getId).toList();
        var editsWithPurl = jdbcQueryService.getEditsWithDatasourcePurl(dsmIds);
        var commitDateTimes = editsWithPurl.stream()
                      .filter(ewp -> datasourcePurls.stream()
                                                    .anyMatch(p -> ewp.datasourcePurl.contains(p)))
                      .map(ewp -> ewp.commitDateTime)
                      .filter(cdt -> cdt != null)
                      .toList();

        var commitDateTimesAsString = commitDateTimes.stream()
                                                     .map(String::valueOf)
                                                     .collect(Collectors.joining(","));

        params.put(COMMIT_DATE_TIME_KEY, commitDateTimesAsString);
        var mappedResult = getMappedResult(txid, requestReceivedAt, "datasourceEvent", params, pageable);


        /*
        you are not wrong. all of this is ridiculous. rn this is the most reliable way to ensure the user's
        pagination and sort preferences are respected.
        */

        // Now using JDBC - DatasourceEventDTO has no packages, so we load them separately
        @SuppressWarnings("unchecked")
        var dseContent = ((Page<DatasourceEventDTO>)mappedResult.get(TITLE_PAGE_KEY)).getContent();
        var eventIds = dseContent.stream()
                                 .map(DatasourceEventDTO::getId)
                                 .toList();

        // Load package purls for these events via JDBC join table query
        var packagePurlsMap = jdbcQueryService.getPackagePurlsForDatasourceEvents(eventIds);
        var packagePurls = packagePurlsMap.values().stream()
                                          .flatMap(List::stream)
                                          .toList();

        var purlsAsString = packagePurls.stream()
                                        .map(String::valueOf)
                                        .collect(Collectors.joining(","));

        params.remove(TXID_KEY);
        params.put(PURL_KEY, purlsAsString);
        mappedResult = getMappedResult(txid, requestReceivedAt, "package", params, pageable);

        return ApiResponse.builder()
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt)
                          .code(HttpStatus.OK.value())
                          .data(mappedResult)
                          .build();
    }



    /**
     * this lets you apply filter criteria to a datasetmetrics query then get the package objects (deduplicated) 
     * hanging off those datasetmetrics records 
     * @param txid
     * @param requestReceivedAt
     * @param params
     * @param pageable
     * @return
     */
    public ApiResponse handleDatasetMetricsPackageTypeSubQuery(
        UUID txid, 
        ZonedDateTime requestReceivedAt,
        Map<String, String> params, 
        Pageable pageable
    ) {

        var dsmRecords = getDatasetMetrics(txid, requestReceivedAt, params, pageable);

        if (dsmRecords.isEmpty()) {
            return ApiResponse.builder()
                              .txid(txid)
                              .requestReceivedAt(requestReceivedAt)
                              .code(HttpStatus.OK.value())
                              .build();            
        } 

        // dedpulicate the list to help cull the number of parameters. >65k and we're cooked
        var packageIndexes = dsmRecords.stream()
                                       .flatMap(dsm -> dsm.getPackageIndexes().stream())
                                       .collect(Collectors.toSet())
                                       .stream()
                                       .map(String::valueOf)
                                       .collect(Collectors.joining(","));

        // at this point all remeining k/v in params should be intended by the caller for the package table
        // here we add the package ids to create a subset of packages that existed in the in the dataset(s) specified 
        // by the caller
        params.put(ID_KEY, packageIndexes);
        var mappedResult = getMappedResult(txid, requestReceivedAt, "package", params, pageable);

        return ApiResponse.builder()
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt)
                          .code(HttpStatus.OK.value())
                          .data(mappedResult)
                          .build();
    }


    /**
     * 
     * @param txid
     * @param requestReceivedAt
     * @param params
     * @param pageable
     * @return
     */
    public ApiResponse handleDatasetMetricsPackageSubQueryReturnFindingType(
        UUID txid, 
        ZonedDateTime requestReceivedAt,
        Map<String, String> params, 
        Pageable pageable
    ) {

        var dsmRecords = getDatasetMetrics(txid, requestReceivedAt, params, pageable);

        if (dsmRecords.isEmpty()) {
            return ApiResponse.builder()
                              .txid(txid)
                              .requestReceivedAt(requestReceivedAt)
                              .code(HttpStatus.OK.value())
                              .build();            
        } 

        // dedpulicate the list to help cull the number of parameters. >65k and we're cooked
        var packageIndexes = dsmRecords.stream()
                                       .flatMap(dsm -> dsm.getPackageIndexes().stream())
                                       .map(String::valueOf)
                                       .collect(Collectors.toSet())
                                       .stream()
                                       .map(String::valueOf)
                                       .collect(Collectors.joining(","));


        params.put("packages.id", packageIndexes);
        var mappedResult = getMappedResult(txid, requestReceivedAt, "finding", params, pageable);


        return ApiResponse.builder()
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt)
                          .code(HttpStatus.OK.value())
                          .data(mappedResult)
                          .build();
    }


    public ApiResponse handleDatasetMetricsDatasourcePackageSubQueryReturnFindingType(
        UUID txid,
        ZonedDateTime requestReceivedAt,
        Map<String, String> params,
        Pageable pageable
    ) {
        var dsmRecords = getDatasetMetrics(txid, requestReceivedAt, params, pageable);
        log.info("got {} dsmRecords back from datasource sub query", dsmRecords.size());
        if (dsmRecords.isEmpty()) {
            return ApiResponse.builder()
                              .txid(txid)
                              .requestReceivedAt(requestReceivedAt)
                              .code(HttpStatus.OK.value())
                              .data(Map.of())
                              .build();
        }
        // we assume controller has validated this argument is present
        // this is an Edit obj key, not a DatasetMetrics key so we'll pull it out now
        var datasourcesPurlValue = params.get(DatabaseQueryController.EDIT_DATASOURCES_PURL_KEY);

        List<String> datasourcePurls = Arrays.stream(datasourcesPurlValue.split(","))
                                             .map(String::trim)
                                             .filter(s -> !s.isEmpty())
                                             .toList();

        // Using JDBC - DatasetMetricsDTO has no edits relationship, load via helper
        var dsmIds = dsmRecords.stream().map(DatasetMetricsDTO::getId).toList();
        var editsWithPurl = jdbcQueryService.getEditsWithDatasourcePurl(dsmIds);
        var commitDateTimes = editsWithPurl.stream()
                      .filter(ewp -> datasourcePurls.stream()
                                                    .anyMatch(p -> ewp.datasourcePurl.contains(p)))
                      .map(ewp -> ewp.commitDateTime)
                      .filter(cdt -> cdt != null)
                      .toList();

        var commitDateTimesAsString = commitDateTimes.stream()
                                                     .map(String::valueOf)
                                                     .collect(Collectors.joining(","));

        params.put(COMMIT_DATE_TIME_KEY, commitDateTimesAsString);
        var mappedResult = getMappedResult(txid, requestReceivedAt, "datasourceEvent", params, pageable);


        /*
        you are not wrong. all of this is ridiculous. rn this is the most reliable way to ensure the user's
        pagination and sort preferences are respected.
        */

        // Now using JDBC - DatasourceEventDTO has no packages, so we load them separately
        @SuppressWarnings("unchecked")
        var dseContent = ((Page<DatasourceEventDTO>)mappedResult.get(TITLE_PAGE_KEY)).getContent();
        var eventIds = dseContent.stream()
                                 .map(DatasourceEventDTO::getId)
                                 .toList();

        // Load package IDs for these events via JDBC join table query
        var packageIdsMap = jdbcQueryService.getPackageIdsForDatasourceEvents(eventIds);
        var packageIndexes = packageIdsMap.values().stream()
                                          .flatMap(List::stream)
                                          .toList();

        var packageIndexesAsString = packageIndexes.stream()
                                                   .map(String::valueOf)
                                                   .collect(Collectors.joining(","));

        params.remove(COMMIT_DATE_TIME_KEY);
        params.put("packages.id", packageIndexesAsString);
        mappedResult = getMappedResult(txid, requestReceivedAt, "finding", params, pageable);

        return ApiResponse.builder()
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt)
                          .code(HttpStatus.OK.value())
                          .data(mappedResult)
                          .build();
    }


    /**
     * 
     * @param txid
     * @param requestReceivedAt
     * @param params
     * @param pageable
     * @return
     */
    public List<DatasetMetricsDTO> getDatasetMetrics(
        UUID txid,
        ZonedDateTime requestReceivedAt,
        Map<String, String> params,
        Pageable pageable
    ) {
        if (params.containsKey(DatabaseQueryController.EDIT_DATASOURCES_PURL_KEY)) {
            return processDatasetMetricsWithSubQueryByDatasource(txid, requestReceivedAt, params, pageable);
        } else {
            return processDatasetMetricsWithSubQuery(txid, requestReceivedAt, params, pageable);
        }
    }


    /**
     * 
     * @param txid
     * @param requestReceivedAt
     * @param params
     * @param pageable
     * @return
     */
    public List<DatasetMetricsDTO> processDatasetMetricsWithSubQueryByDatasource(
        UUID txid,
        ZonedDateTime requestReceivedAt,
        Map<String, String> params,
        Pageable pageable
    ) {
        // we assume controller has validated this argument is present
        // this is an Edit obj key, not a DatasetMetrics key so we'll pull it out now
        var datasourcesPurlValue = params.get(DatabaseQueryController.EDIT_DATASOURCES_PURL_KEY);

        List<String> datasourcePurls = Arrays.stream(datasourcesPurlValue.split(","))
                                             .map(String::trim)
                                             .filter(s -> !s.isEmpty())
                                             .toList();

        var dsmRecords = processDatasetMetricsWithSubQuery(txid, requestReceivedAt, params, pageable);

        if (dsmRecords.isEmpty()) {
            return new ArrayList<>();
        }

        // Using JDBC - DatasetMetricsDTO has datasetId but no datasources relationship
        // Load datasource purls for each dataset and filter
        return dsmRecords.stream()
                         .filter(dsm -> {
                             List<String> dsPurls = jdbcQueryService.getDatasourcePurlsForDataset(dsm.getDatasetId());
                             return dsPurls.stream()
                                           .anyMatch(dsPurl -> datasourcePurls.stream().anyMatch(dsPurl::contains));
                         })
                         .toList();
    }


    /**
     * 
     * @param txid
     * @param requestReceivedAt
     * @param params calls to this method expect the params map to be overloaded with queries - some intended for 
     *               the DatasetMetrics table and some intended for the Package table. 
     * @param pageable
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<DatasetMetricsDTO> processDatasetMetricsWithSubQuery(
        UUID txid,
        ZonedDateTime requestReceivedAt,
        Map<String, String> params,
        Pageable pageable
    ) {

        // we're first going to segregate the params intended for the datasetMetrics table and those intended for
        // the package table.
        var hadCommitDateTimeParam = false;
        var dsmParams = new HashMap<String, String>();

        // ensure we sort by commit date time so that we can pull out the most recent record if need be
        dsmParams.put(SORT_KEY, DatabaseQueryController.COMMIT_DATE_TIME_KEY + ".desc");

        //
        // first get the relevant dataset_metrics record
        // if it's missing then return emtpy response (controller should have handled this)
        //
        var datasetNames = params.getOrDefault(DatabaseQueryController.DATASET_NAME_KEY, "");

        if (datasetNames.isEmpty()) {
            return new ArrayList<>();
        } else {
            dsmParams.put("dataset.name", params.remove(DatabaseQueryController.DATASET_NAME_KEY));
        }

        //
        // commitDateTime is optional
        //
        if (params.containsKey(DatabaseQueryController.COMMIT_DATE_TIME_KEY)) {
            dsmParams.put(
                DatabaseQueryController.COMMIT_DATE_TIME_KEY,
                params.remove(DatabaseQueryController.COMMIT_DATE_TIME_KEY)
            );
            hadCommitDateTimeParam = true;
        }


        //
        // add flag(s)
        //
        if (params.containsKey(DatabaseQueryController.IS_CURRENT_KEY)) {
            dsmParams.put(DatabaseQueryController.IS_CURRENT_KEY, params.remove(DatabaseQueryController.IS_CURRENT_KEY));
        }

        if (params.containsKey(DatabaseQueryController.IS_FORECAST_SAME_COURSE_KEY)) {
            dsmParams.put(
                DatabaseQueryController.IS_FORECAST_SAME_COURSE_KEY,
                params.remove(DatabaseQueryController.IS_FORECAST_SAME_COURSE_KEY)
            );

        }

        if (params.containsKey(DatabaseQueryController.IS_FORECAST_RECOMMENDATIONS_TAKEN_KEY)) {
            dsmParams.put(
                DatabaseQueryController.IS_FORECAST_RECOMMENDATIONS_TAKEN_KEY,
                params.remove(DatabaseQueryController.IS_FORECAST_RECOMMENDATIONS_TAKEN_KEY)
            );

        }


        // ultimately it's the spring Pageable object that's spring is going to use to determine sort and page size
        // we added a sort argument to dsmParams expecting method getMappedResult(...) to handle the mapping therin
        try {
            // If no commitDateTime specified, we only need the most recent record - use LIMIT 1
            // This avoids loading 200k+ records just to take the first one
            if (!hadCommitDateTimeParam) {
                Pageable dsmPageable = PageRequest.of(0, 1); // Just get 1 record
                var dsmMappedResult = getMappedResult(txid, requestReceivedAt, "datasetMetrics", dsmParams, dsmPageable);
                @SuppressWarnings("unchecked")
                var dsmPage = (Page<DatasetMetricsDTO>) dsmMappedResult.get(TITLE_PAGE_KEY);
                var content = dsmPage.getContent();
                if (content == null || content.isEmpty()) {
                    return new ArrayList<>();
                }
                log.info("dsmRecordId is: {}", content.get(0).getId());
                return new ArrayList<>(content);
            }

            // When commitDateTime IS specified, we may need multiple records
            // Use a dedicated pageable for the DSM stage.
            // Keep caller's page size if present; otherwise choose something reasonable.
            int pageSize = pageable != null && !pageable.isUnpaged() ? pageable.getPageSize() : 200;
            int pageNum = 0;

            List<DatasetMetricsDTO> dsmRecords = new ArrayList<>();

            while (true) {
                Pageable dsmPageable = PageRequest.of(pageNum, pageSize); // sort handled by dsmParams -> getMappedResult()

                var dsmMappedResult = getMappedResult(txid, requestReceivedAt, "datasetMetrics", dsmParams, dsmPageable);
                @SuppressWarnings("unchecked")
                var dsmPage = (Page<DatasetMetricsDTO>) dsmMappedResult.get(TITLE_PAGE_KEY);

                var content = dsmPage.getContent();
                if (content == null || content.isEmpty()) {
                    break;
                }

                dsmRecords.addAll(content);

                if (!dsmPage.hasNext()) {
                    break;
                }

                pageNum++;
            }

            return dsmRecords;
        } catch (Exception e) {
            log.error("caught unexpected exception during datasetMetrics subquery first stage", e);
            return new ArrayList<>();
        }
    }


    public ApiResponse process(
        UUID txid, 
        ZonedDateTime requestReceivedAt,
        String table, 
        Map<String, String> params, 
        Pageable pageable
    ) {
        var mappedResult = getMappedResult(txid, requestReceivedAt, table, params, pageable);

        return ApiResponse.builder()
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt)
                          .code(HttpStatus.OK.value())
                          .data(mappedResult)
                          .build();
    }


    /**
     * 
     * @param txid
     * @param requestReceivedAt
     * @param table
     * @param params
     * @param pageable
     * @param indexes
     * @return
     */
    Map<String, Object> getMappedResult(
        UUID txid, 
        ZonedDateTime requestReceivedAt,
        String table, 
        Map<String, String> params, 
        Pageable pageable,
        Map<Long, Long>... indexesCollection
    ) {

            // ensure queryDSL sort and size calls are handled by spring-data by way of the PAGE interface 
            if ( !pageable.isUnpaged() && params.containsKey("sort")) {
                var sortParam = params.remove("sort");
                log.info("found sort parameter: {}  adding to Pageable object...", sortParam);
                var sortArg = parseSortParameter(sortParam);
                pageable = PageRequest.of(
                  pageable.getPageNumber(),
                  pageable.getPageSize(),
                  sortArg
                );
            }
            if ( !pageable.isUnpaged() && params.containsKey("size")) {
                var sizeParam = params.remove("size");
                var sizeArg = Integer.parseInt(sizeParam);
                sizeArg = sizeArg < MIN_SIZE ? MIN_SIZE : sizeArg;
                sizeArg = sizeArg > MAX_SIZE ? MAX_SIZE : sizeArg;
                log.info("found size parameter -- adding: {} to Pageable object", sizeArg);
                pageable = PageRequest.of(
                  pageable.getPageNumber(),
                  sizeArg,
                  pageable.getSort()
                );
            }


            var titlePageName = TITLE_PAGE_KEY;
            switch (table.toUpperCase()) {
                case "DATASETMETRICS":
                    // Use JDBC to bypass Hibernate relationship loading explosion
                    // DatasetMetricsDTO has NO edits - just scalar fields and dataset_id FK
                    var dsmPage = jdbcQueryService.query("datasetmetrics", params, pageable);
                    log.info("JDBC query returned {} dataset metrics", dsmPage.getTotalElements());
                    return Map.of(titlePageName, dsmPage);
                    
                case "DATASET":
                    // Use JDBC to bypass Hibernate relationship loading explosion
                    // This returns DatasetDTO (flat, no relationships) instead of Dataset entity
                    var dPage = jdbcQueryService.query("dataset", params, pageable);
                    log.info("JDBC query returned {} results for dataset", dPage.getTotalElements());
                    return Map.of(titlePageName, dPage);
                case "DATASOURCEEVENT":
                    // Use JDBC to bypass Hibernate relationship loading explosion
                    // DatasourceEventDTO has NO packages - just scalar fields (includes payload)
                    var dsePage = jdbcQueryService.query("datasourceevent", params, pageable);
                    log.info("JDBC query returned {} datasource events", dsePage.getTotalElements());
                    return Map.of(titlePageName, dsePage);
                case "DATASOURCE":
                    // Use JDBC to bypass Hibernate relationship loading explosion
                    // DatasourceDTO has NO edits, NO datasets, NO packageIndexes
                    var dsPage = jdbcQueryService.query("datasource", params, pageable);
                    log.info("JDBC query returned {} datasources", dsPage.getTotalElements());
                    return Map.of(titlePageName, dsPage);
                case "EDIT":
                    // Use JDBC to bypass Hibernate relationship loading explosion
                    // EditDTO has NO datasetMetrics, NO datasource - just scalar fields and FK IDs
                    var editPage = jdbcQueryService.query("edit", params, pageable);
                    log.info("JDBC query returned {} edits", editPage.getTotalElements());
                    return Map.of(titlePageName, editPage);
                case "FINDINGDATA":
                    // Use JDBC to bypass Hibernate relationship loading explosion
                    // FindingDataDTO has NO finding - just scalar fields and finding_id FK
                    var findingDataPage = jdbcQueryService.query("findingdata", params, pageable);
                    log.info("JDBC query returned {} finding data records", findingDataPage.getTotalElements());
                    return Map.of(titlePageName, findingDataPage);
                case "FINDINGREPORTER":
                    // Use JDBC to bypass Hibernate relationship loading explosion
                    // FindingReporterDTO has NO findings - just id and name
                    var findingReporterPage = jdbcQueryService.query("findingreporter", params, pageable);
                    log.info("JDBC query returned {} finding reporters", findingReporterPage.getTotalElements());
                    return Map.of(titlePageName, findingReporterPage);            
                case "FINDING":
                    // Use JDBC to bypass Hibernate relationship loading explosion
                    // FindingDTO JOINs with finding_data, has NO packages, NO reporters
                    var findingPage = jdbcQueryService.query("finding", params, pageable);
                    log.info("JDBC query returned {} findings", findingPage.getTotalElements());
                    return Map.of(titlePageName, findingPage);               
                case "PACKAGE":
                    // Use JDBC to bypass Hibernate relationship loading explosion
                    // PackageDTO has NO findings, NO datasourceEvents - just scalar fields

                    // For re-duplication case (when indexesCollection is provided)
                    if (indexesCollection.length > 0) {
                        var indexes = indexesCollection[0];

                        // Query all packages matching the criteria (unpaged for re-duplication)
                        var jdbcPackagePage = jdbcQueryService.query(
                            "package",
                            params,
                            PageRequest.of(0, Integer.MAX_VALUE)
                        );

                        // Re-duplicate packages based on index counts
                        var reDuplicatedPackages = new ArrayList<PackageDTO>();
                        jdbcPackagePage.getContent().forEach(p -> {
                            PackageDTO pkg = (PackageDTO) p;
                            var count = indexes.get(pkg.getId());
                            if (count != null) {
                                LongStream.range(0L, count).forEach(i -> {
                                    reDuplicatedPackages.add(pkg);
                                });
                            }
                        });

                        log.info("reDuplicatedPackages size is: {}", reDuplicatedPackages.size());

                        // Manual pagination on re-duplicated results
                        int totalDuplicatedSize = reDuplicatedPackages.size();
                        int pageSize = pageable.getPageSize();
                        int requestedPage = pageable.getPageNumber();
                        int maxPage = Math.max(0, (totalDuplicatedSize - 1) / pageSize);
                        int actualPage = Math.min(requestedPage, maxPage);
                        int start = actualPage * pageSize;
                        int end = Math.min(start + pageSize, totalDuplicatedSize);

                        List<PackageDTO> pageContent = start < totalDuplicatedSize
                            ? reDuplicatedPackages.subList(start, end)
                            : Collections.emptyList();

                        Pageable actualPageable = PageRequest.of(actualPage, pageSize, pageable.getSort());
                        Page<PackageDTO> packagePage = new PageImpl<>(pageContent, actualPageable, totalDuplicatedSize);

                        return Map.of(titlePageName, packagePage);
                    } else {
                        // Simple case - just query with JDBC and return
                        var packagePage = jdbcQueryService.query("package", params, pageable);
                        log.info("JDBC query returned {} packages", packagePage.getTotalElements());
                        return Map.of(titlePageName, packagePage);
                    }  
                case "DATASOURCEMETRICS":
                    // Use JDBC to bypass Hibernate relationship loading explosion
                    // DatasourceMetricsDTO has NO relationships - just scalar fields
                    var dsMetricsPage = jdbcQueryService.query("datasourcemetrics", params, pageable);
                    log.info("JDBC query returned {} datasource metrics", dsMetricsPage.getTotalElements());
                    return Map.of(titlePageName, dsMetricsPage);
                case "DATASOURCEMETRICSCURRENT":
                    // Use JDBC to bypass Hibernate relationship loading explosion
                    // DatasourceMetricsCurrentDTO has NO relationships - just scalar fields
                    var dsMetricsCurrentPage = jdbcQueryService.query("datasourcemetricscurrent", params, pageable);
                    log.info("JDBC query returned {} datasource metrics current", dsMetricsCurrentPage.getTotalElements());
                    return Map.of(titlePageName, dsMetricsCurrentPage);
                default:
                    log.error("unexpectedly discovered no matching db object set for table: {}", table);
                    throw new IllegalArgumentException("unexpectedly discovered no matching db object set for table: " + table);
        }

    }


    //
    // here and not in db-entities because we don't want any Spring dependencies in db-entities 
    //
    // things that deal with queryDSL in and of themselves are in db-entities
    // things that touch spring stay in the services 
    //
    public Sort parseSortParameter(String sortParam) {
        String[] sortFields = sortParam.split(",");
        List<Sort.Order> orders = new ArrayList<>();
        
        for (String sortField : sortFields) {
            sortField = sortField.trim();
            if (sortField.isEmpty()) continue;
            
            boolean ascending = true;
            String property = sortField;
            
            // Handle direction indicators
            if (sortField.startsWith("-")) {
                ascending = false;
                property = sortField.substring(1);
            } else if (sortField.startsWith("+")) {
                ascending = true;
                property = sortField.substring(1);
            } else if (sortField.toLowerCase().endsWith(".desc")) {
                ascending = false;
                property = sortField.substring(0, sortField.length() - 5);
            } else if (sortField.toLowerCase().endsWith(".asc")) {
                ascending = true;
                property = sortField.substring(0, sortField.length() - 4);
            }
            
            Sort.Order order = ascending ? Sort.Order.asc(property) : Sort.Order.desc(property);
            orders.add(order);
        }
        
        return Sort.by(orders);
    }


}
