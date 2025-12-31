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

        var editIndexes = dsmRecords.stream()
                                    .flatMap(dsm -> dsm.getEdits().stream())
                                    .map(e -> e.getId())
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

        var editIndexes = dsmRecords.stream()
                                    .flatMap(dsm -> dsm.getEdits().stream())
                                    .filter(e -> datasourcePurls.stream()
                                                                .anyMatch(p -> e.getDatasource().getPurl().contains(p))
                                    )
                                    .map(Edit::getId)
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

        var commitDateTimes = 
            dsmRecords.stream()
                      .flatMap(dsm -> dsm.getEdits().stream())
                      .filter(e -> datasourcePurls.stream()
                                                  .anyMatch(p -> e.getDatasource().getPurl().contains(p))
                      )
                      .map(e -> e.getCommitDateTime())
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

        var packagePurls = 
            ((Page<DatasourceEvent>)mappedResult.get(TITLE_PAGE_KEY)).getContent()
                                                                     .stream()
                                                                     .flatMap(dse -> dse.getPackages().stream())
                                                                     .map(p -> p.getPurl())
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

        var commitDateTimes = 
            dsmRecords.stream()
                      .flatMap(dsm -> dsm.getEdits().stream())
                      .filter(e -> datasourcePurls.stream()
                                                  .anyMatch(p -> e.getDatasource().getPurl().contains(p))
                      )
                      .map(e -> e.getCommitDateTime())
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

        var packageIndexes = 
            ((Page<DatasourceEvent>)mappedResult.get(TITLE_PAGE_KEY)).getContent()
                                                                     .stream()
                                                                     .flatMap(dse -> dse.getPackages().stream())
                                                                     .map(p -> p.getId())
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
    public List<DatasetMetrics> getDatasetMetrics(
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
    public List<DatasetMetrics> processDatasetMetricsWithSubQueryByDatasource(
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

        return dsmRecords.stream()
                         .filter(dsm -> dsm.getDataset()
                                           .getDatasources()
                                           .stream()
                                           .anyMatch(
                                                e -> datasourcePurls.stream().anyMatch(e.getPurl()::contains)
                                           )
                         )
                         .toList();

        // return dsmRecords.stream()
        //                  .filter(dsm ->
        //                      dsm.getEdits()
        //                         .stream()
        //                         .anyMatch(
        //                             e -> {
        //                                 var purl = e.getDatasource().getPurl();
        //                                 return datasourcePurls.stream().anyMatch(purl::contains);
        //                             }
        //                         )
        //                  )
        //                  .toList();
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
    public List<DatasetMetrics> processDatasetMetricsWithSubQuery(
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
            // var dsmMappedResult = getMappedResult(txid, requestReceivedAt, "datasetMetrics", dsmParams, pageable);
            // var dsmPage = (Page<DatasetMetrics>) dsmMappedResult.get(TITLE_PAGE_KEY);
            // var dsmRecords = dsmPage.getContent();
 
            // Use a dedicated pageable for the DSM stage.
            // Keep caller's page size if present; otherwise choose something reasonable.
            int pageSize = pageable != null && !pageable.isUnpaged() ? pageable.getPageSize() : 200;
            int pageNum = 0;

            List<DatasetMetrics> dsmRecords = new ArrayList<>();

            while (true) {
                Pageable dsmPageable = PageRequest.of(pageNum, pageSize); // sort handled by dsmParams -> getMappedResult()

                var dsmMappedResult = getMappedResult(txid, requestReceivedAt, "datasetMetrics", dsmParams, dsmPageable);
                var dsmPage = (Page<DatasetMetrics>) dsmMappedResult.get(TITLE_PAGE_KEY);

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

            if ( !hadCommitDateTimeParam && !dsmRecords.isEmpty() ) {
                dsmRecords = List.of(dsmRecords.get(0));
                log.info("dsmRecordId is: {}", dsmRecords.get(0).getId());
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
                    var builder = queryDslHelpers.getBuilder(params, QDatasetMetrics.datasetMetrics, DatasetMetrics.class);
                    var dsmPage = datasetMetricsRepository.findAll(builder, pageable);

                    // to prevent recursive references that cause things to go boom
                    dsmPage.stream()
                        .forEach(
                                dsm -> {
                                    if (dsm.getDataset() == null) {
                                        log.warn("null dataset field for dsm record id is: {}", dsm.getId());
                                        log.warn("dsmId={}, dsmClass={}, datasetLoaded={}, sessionLoadedEntity={}",
                                            dsm.getId(),
                                            dsm.getClass().getName(),
                                            Persistence.getPersistenceUtil().isLoaded(dsm, "dataset")
                                        );
                                    }
                                    dsm.setPackageFamilies(new HashSet<String>());

                                    dsm.getEdits()
                                       .stream()
                                       .forEach( 
                                            e -> {
                                                e.setDatasetMetrics(null); 
                                                e.getDatasource().setDatasets(null);
                                                e.getDatasource().setEdits(null);
                                            }
                                       );
                                    //dsm.getDataset().setDatasources(new HashSet<Datasource>());
                                    dsm.getDataset()
                                       .getDatasources()
                                       .stream()
                                       .forEach(
                                            e -> {
                                                e.setDatasets(null);
                                                e.setEdits(null);
                                                e.setPackageIndexes(null);
                                            }
                                       );
                                    
                                }
                            );

                    return Map.of(titlePageName, dsmPage);
                    
                case "DATASET":
                    builder = queryDslHelpers.getBuilder(params, QDataset.dataset, Dataset.class);
                    var dPage = datasetRepository.findAll(builder, pageable);

                    // to prevent recursive references that cause things to go boom
                    dPage.stream()
                        .forEach(
                            d -> {
                                d.getDatasources()
                                .stream()
                                .forEach(ds -> {ds.getDatasets().clear(); ds.getEdits().clear();});
                
                            }
                        );

                    return Map.of(titlePageName, dPage);
                case "DATASOURCEEVENT":
                    builder = queryDslHelpers.getBuilder(params, QDatasourceEvent.datasourceEvent, DatasourceEvent.class);
                    var dsePage = datasourceEventRepository.findAll(builder, pageable);

                    // to prevent recursive reference that cause things to go boom 
                    dsePage.stream()
                            .forEach(
                                dse -> {
                                    if (dse.getDatasource().getDatasets() != null) {
                                        dse.getDatasource().getDatasets().clear();
                                    }
                                    
                                    if (dse.getDatasource().getEdits() != null) {
                                        dse.getDatasource().getEdits().clear();
                                    }
                                    
                                    try {
                                        dse.setPayload(new byte[1]);
                                    } catch (IOException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                    }
                                    //dse.getPackages().clear();
                                    dse.getPackages().stream().forEach(p -> {
                                        p.getDatasourceEvents().clear();
                                        p.getFindings().stream().forEach(f -> {
                                            f.getPackages().clear();
                                            f.getReporters().stream().forEach(r -> r.getFindings().clear());
                                            f.getData().setFinding(null);
                                        });
                                        p.getCriticalFindings().stream().forEach(f -> {
                                            f.getPackages().clear();
                                            f.getReporters().stream().forEach(r -> r.getFindings().clear());
                                            f.getData().setFinding(null);
                                        });
                                        p.getHighFindings().stream().forEach(f -> {
                                            f.getPackages().clear();
                                            f.getReporters().stream().forEach(r -> r.getFindings().clear());
                                            f.getData().setFinding(null);
                                        });
                                        p.getMediumFindings().stream().forEach(f -> {
                                            f.getPackages().clear();
                                            f.getReporters().stream().forEach(r -> r.getFindings().clear());
                                            f.getData().setFinding(null);
                                        });
                                        p.getLowFindings().stream().forEach(f -> {
                                            f.getPackages().clear();
                                            f.getReporters().stream().forEach(r -> r.getFindings().clear());
                                            f.getData().setFinding(null);
                                        });
                                    });
                                }
                            );

                    return Map.of(titlePageName, dsePage);
                case "DATASOURCE":
                    builder = queryDslHelpers.getBuilder(params, QDatasource.datasource, Datasource.class);
                    var dsPage = datasourceRepository.findAll(builder, pageable);

                    // to prevent recursive reference that cause things to go boom 
                    dsPage.stream()
                        .forEach(
                            ds -> {
                                ds.getDatasets().stream().forEach(d -> d.getDatasources().clear());
                                ds.getEdits().stream().forEach(e -> {
                                        e.setDatasetMetrics(null);
                                        e.setDatasource(null);
                                });
                            }
                        );

                    return Map.of(titlePageName, dsPage);
                case "EDIT":
                    builder = queryDslHelpers.getBuilder(params, QEdit.edit, Edit.class);
                    var editPage = editRepository.findAll(builder, pageable);

                    // to prevent recursive reference that cause things to go boom 
                    editPage.stream()
                            .forEach(
                                e -> {

                                    // these only seem to be null when there is a chain of calls that results in 
                                    // a query with an edit.id constraint 
                                    if ( e.getDatasetMetrics() != null ) {
                                        e.getDatasetMetrics().getDataset().getDatasources().clear();
                                        e.getDatasetMetrics().getEdits().clear();
                                    }

                                    if (e.getDatasource() != null && e.getDatasource().getEdits() != null ) {
                                        e.getDatasource().getEdits().clear();
                                    }
                                    
                                }
                            );

                    return Map.of(titlePageName, editPage);
                case "FINDINGDATA":
                    builder = queryDslHelpers.getBuilder(params, QFindingData.findingData, FindingData.class);
                    var findingDataPage = findingDataRepository.findAll(builder, pageable);

                    // to prevent recursive reference that cause things to go boom 
                    findingDataPage.stream()
                                .forEach(
                                        fd -> {
                                            fd.getFinding().getPackages().clear();
                                            fd.getFinding().getReporters().stream().forEach(fr -> fr.getFindings().clear());
                                            fd.getFinding().setData(null);
                                        }
                                );

                    return Map.of(titlePageName, findingDataPage);
                case "FINDINGREPORTER":
                    builder = queryDslHelpers.getBuilder(params, QFindingReporter.findingReporter, FindingReporter.class);
                    var findingReporterPage = findingReporterRepository.findAll(builder, pageable);

                    // to prevent recursive reference that cause things to go boom 
                    findingReporterPage.stream()
                                    .forEach(
                                            fr -> {
                                                fr.getFindings().stream().forEach(f -> {
                                                    f.getPackages().clear();
                                                    f.getReporters().clear();
                                                    f.setData(null);
                                                });
                                            }
                                    );

                    return Map.of(titlePageName, findingReporterPage);            
                case "FINDING":
                    builder = queryDslHelpers.getBuilder(params, QFinding.finding, Finding.class);
                    var findingPage = findingRepository.findAll(builder, pageable);

                    // to prevent recursive reference that cause things to go boom 
                    findingPage.stream()
                            .forEach(
                                f -> {
                                        f.getReporters().stream().forEach(fr -> fr.getFindings().clear());
                                        f.getData().setFinding(null);
                                        f.getPackages().stream().forEach(p -> {
                                            p.getFindings().clear();
                                            p.getCriticalFindings().clear();
                                            p.getHighFindings().clear();
                                            p.getMediumFindings().clear();
                                            p.getLowFindings().clear();
                                            p.getDatasourceEvents().clear();
                                        });
                                }
                            );

                    return Map.of(titlePageName, findingPage);               
                case "PACKAGE":
                    builder = queryDslHelpers.getBuilder(params, QPackage.package$, io.patchfox.db_entities.entities.Package.class);
                    
                    // unpaged because we need to get all the results in one go - then deduplicate
                    // MAX_VALUE because Pageable.unpaged is sending us down a codepath that causes predicates not to be processed correctly
                    // and I am not into debugging that shit rn
                    var packagePage = packageRepository.findAll(builder, PageRequest.of(0, Integer.MAX_VALUE)); //Pageable.unpaged());

                    var reDuplicatedPackages = new ArrayList<io.patchfox.db_entities.entities.Package>();
                    if (indexesCollection.length > 0) {
                        var indexes = indexesCollection[0];
                        packagePage.getContent().stream().forEach( p -> {
                            var count = indexes.get(p.getId());
                            LongStream.range(0L, count).forEach( i -> {

                                //to prevent recursive references that cause things to go boom
                                p.getDatasourceEvents().clear(); 
                                p.getFindings().stream().forEach(
                                        f -> {
                                            f.getPackages().clear();
                                            f.getReporters().stream().forEach(fr -> fr.getFindings().clear());
                                            f.getData().setFinding(null);
                                        }
                                    ); 
 
                                p.getCriticalFindings().clear();
                                p.getHighFindings().clear();
                                p.getMediumFindings().clear();
                                p.getLowFindings().clear();  

                                reDuplicatedPackages.add(p);
                            });
                        });
                        log.info("reDuplicatedPackages size is: {}", reDuplicatedPackages.size());
                        log.info("reDuplicatedPackages is: {}", reDuplicatedPackages);

                        // Calculate new pagination based on duplicated results
                        int totalDuplicatedSize = reDuplicatedPackages.size();
                        int pageSize = pageable.getPageSize();
                        int requestedPage = pageable.getPageNumber();

                        // If the requested page is beyond what we have, go to the last valid page
                        int maxPage = Math.max(0, (totalDuplicatedSize - 1) / pageSize);
                        int actualPage = Math.min(requestedPage, maxPage);

                        int start = actualPage * pageSize;
                        int end = Math.min(start + pageSize, totalDuplicatedSize);

                        log.info("start is: {}  end is: {}", start, end);

                        List<io.patchfox.db_entities.entities.Package> pageContent = 
                            start < totalDuplicatedSize 
                            ? reDuplicatedPackages.subList(start, end) 
                            : Collections.emptyList();

                        log.info("pageContent is: {}", pageContent);

                        // Create new pageable with the actual page we're returning
                        Pageable actualPageable = PageRequest.of(actualPage, pageSize, pageable.getSort());

                        packagePage = new PageImpl<>(
                            pageContent, 
                            actualPageable, 
                            totalDuplicatedSize  
                        );

                    } else {
                        // to prevent recursive references that cause things to go boom
                        packagePage.stream()
                                   .forEach(
                                        p -> {
                                            p.getDatasourceEvents().clear(); 
                                            p.getFindings().stream().forEach(
                                                    f -> {
                                                        f.getPackages().clear();
                                                        f.getReporters().stream().forEach(fr -> fr.getFindings().clear());
                                                        f.getData().setFinding(null);
                                                    }
                                                ); 

                                            
                                            p.getCriticalFindings().clear();
                                            p.getHighFindings().clear();
                                            p.getMediumFindings().clear();
                                            p.getLowFindings().clear();     
                                            }
                                    );

                        // Calculate new pagination based on new results
                        int totalSize = packagePage.getContent().size();
                        int pageSize = pageable.getPageSize();
                        int requestedPage = pageable.getPageNumber();

                        // If the requested page is beyond what we have, go to the last valid page
                        int maxPage = Math.max(0, (totalSize - 1) / pageSize);
                        int actualPage = Math.min(requestedPage, maxPage);

                        int start = actualPage * pageSize;
                        int end = Math.min(start + pageSize, totalSize);

                        List<io.patchfox.db_entities.entities.Package> pageContent = 
                            start < totalSize 
                            ? packagePage.getContent().subList(start, end) 
                            : Collections.emptyList();

                        // Create new pageable with the actual page we're returning
                        Pageable actualPageable = PageRequest.of(actualPage, pageSize, pageable.getSort());
                        
                        packagePage = new PageImpl<>(
                            pageContent, 
                            actualPageable, 
                            totalSize  
                        );
                    }

                    return Map.of(titlePageName, packagePage);  
                case "DATASOURCEMETRICS":
                    builder = queryDslHelpers.getBuilder(params, QDatasourceMetrics.datasourceMetrics, DatasourceMetrics.class);
                    var datasourceMetricsPage = datasourceMetricsRepository.findAll(builder, pageable);
                    return Map.of(titlePageName, datasourceMetricsPage);
                case "DATASOURCEMETRICSCURRENT":
                    builder = queryDslHelpers.getBuilder(params, QDatasourceMetricsCurrent.datasourceMetricsCurrent, DatasourceMetricsCurrent.class);
                    var datasourceMetricsCurrentPage = datasourceMetricsCurrentRepository.findAll(builder, pageable);
                    return Map.of(titlePageName, datasourceMetricsCurrentPage);                
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
