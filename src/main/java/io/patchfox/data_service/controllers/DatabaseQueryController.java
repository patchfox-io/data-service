package io.patchfox.data_service.controllers;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.patchfox.data_service.components.EnvironmentComponent;
import io.patchfox.data_service.services.DatabaseQueryService;
import io.patchfox.package_utils.json.ApiResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class DatabaseQueryController {


    //

    @Autowired
    private EnvironmentComponent env;

    @Autowired
    private DatabaseQueryService databaseQueryService;

    public static final String API_PATH_PREFIX = "/api/v1/db";    
    
    public static final String TABLE_QUERY_PATH = API_PATH_PREFIX + "/{table}/query";
    public static final String GET_TABLE_QUERY_SIGNATURE = "GET_" + TABLE_QUERY_PATH;

    //

    // here when we filter by datasource we get all datasetMetrics records that contain the datasource as a member of 
    // the dataset.  

    public static final String DATASET_METRICS_EDIT_QUERY_PATH = API_PATH_PREFIX + "/datasetMetrics/edit/query";
    public static final String GET_DATASET_METRICS_EDIT_QUERY_SIGNATURE = "GET_" + DATASET_METRICS_EDIT_QUERY_PATH;

    public static final String DATASET_METRICS_PACKAGE_TYPE_QUERY_PATH = API_PATH_PREFIX + "/datasetMetrics/packageType/query";
    public static final String GET_DATASET_METRICS_PACKAGE_TYPE_QUERY_SIGNATURE = "GET_" + DATASET_METRICS_PACKAGE_TYPE_QUERY_PATH;

    public static final String DATASET_METRICS_PACKAGE_QUERY_PATH = API_PATH_PREFIX + "/datasetMetrics/package/query";
    public static final String GET_DATASET_METRICS_PACKAGE_QUERY_SIGNATURE = "GET_" + DATASET_METRICS_PACKAGE_QUERY_PATH;

    public static final String DATASET_METRICS_PACKAGE_FINDING_QUERY_PATH = API_PATH_PREFIX + "/datasetMetrics/package/finding/query";
    public static final String GET_DATASET_METRICS_PACKAGE_FINDING_QUERY_SIGNATURE = "GET_" + DATASET_METRICS_PACKAGE_FINDING_QUERY_PATH;

    //

    // here when we filter by datasource we first get all datasetMetrics records that contain the datasource as a member
    // of the datasource and then an additional filter to only look at datasetMetrics records where the datasource 
    // was the one that caused the datasetMetrics record to be created. practiclaly speaking that means these endpoints
    // only return edits, packages, or findings that involve at least one of the supplied datasource purls. 
    //
    // these endpoints are how we ask questions about edits, packages, and findings 

    public static final String DATASOURCE_EDIT_QUERY_PATH = API_PATH_PREFIX + "/datasetMetrics/datasource/edit/query";
    public static final String GET_DATASOURCE_EDIT_QUERY_SIGNATURE = "GET_" + DATASOURCE_EDIT_QUERY_PATH;

    public static final String DATASOURCE_PACKAGE_TYPE_QUERY_PATH = API_PATH_PREFIX + "/datasetMetrics/datasource/packageType/query";
    public static final String GET_DATASOURCE_PACKAGE_TYPE_QUERY_SIGNATURE = "GET_" + DATASOURCE_PACKAGE_TYPE_QUERY_PATH;

    public static final String DATASOURCE_PACKAGE_QUERY_PATH = API_PATH_PREFIX + "/datasetMetrics/datasource/package/query";
    public static final String GET_DATASOURCE_PACKAGE_QUERY_SIGNATURE = "GET_" + DATASOURCE_PACKAGE_QUERY_PATH;

    public static final String DATASOURCE_PACKAGE_FINDING_QUERY_PATH = API_PATH_PREFIX + "/datasetMetrics/datasource/package/finding/query";
    public static final String GET_DATASOURCE_PACKAGE_FINDING_QUERY_SIGNATURE = "GET_" + DATASOURCE_PACKAGE_FINDING_QUERY_PATH;    


    //

    public static final String DATASOURCE_NAME = "datasourceName";
    public static final String DATASET_NAME_KEY = "datasetName";
    public static final String COMMIT_DATE_TIME_KEY = "commitDateTime";
    public static final String IS_CURRENT_KEY = "isCurrent";
    public static final String IS_FORECAST_SAME_COURSE_KEY = "isForecastSameCourse";
    public static final String IS_FORECAST_RECOMMENDATIONS_TAKEN_KEY ="isForecastRecommendationsTaken";
    public static final String DATASOURCE_KEY = "datasource";
    public static final String EDIT_DATASOURCES_PURL_KEY = "datasources.purl";


    @GetMapping(TABLE_QUERY_PATH)
    public ResponseEntity<ApiResponse> queryDatabase (
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @PathVariable("table") String table,
        @RequestParam Map<String, String> params,
        Pageable pageable
    ) {

        var validTable = getIsValidTableArgument(table);

        if ( !validTable ) { 
            log.warn("table argument: {} is not valid", table);
            var rv = ApiResponse.builder()
                                .txid(txid)
                                .requestReceivedAt(requestReceivedAt)
                                .code(HttpStatus.BAD_REQUEST.value())
                                .serverMessage("invalid table argument")
                                .build();

            return ResponseEntity.status(rv.getCode()).body(rv);         
        }

        var rv = databaseQueryService.process(txid, requestReceivedAt, table, params, pageable);
        return ResponseEntity.status(rv.getCode()).body(rv);
    }
    

    //

    @GetMapping(DATASET_METRICS_EDIT_QUERY_PATH)
    public ResponseEntity<ApiResponse> queryDatabaseForDatasetMetricsEdit (
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestParam Map<String, String> params,
        Pageable pageable
    ) {

        var optionalRv = validateDsmArguments(txid, requestReceivedAt, params);
        if (optionalRv.isPresent()) { return optionalRv.get(); }
        var rv = databaseQueryService.handleDatasetMetricsEditSubQuery(txid, requestReceivedAt, params, pageable);
        return ResponseEntity.status(rv.getCode()).body(rv);
    }


    @GetMapping(DATASET_METRICS_PACKAGE_TYPE_QUERY_PATH)
    public ResponseEntity<ApiResponse> queryDatabaseForDatasetMetricsPackageType (
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestParam Map<String, String> params,
        Pageable pageable
    ) {

        var optionalRv = validateDsmArguments(txid, requestReceivedAt, params);
        if (optionalRv.isPresent()) { return optionalRv.get(); }
        var rv = databaseQueryService.handleDatasetMetricsPackageTypeSubQuery(txid, requestReceivedAt, params, pageable);
        return ResponseEntity.status(rv.getCode()).body(rv);
    }

    @GetMapping(DATASET_METRICS_PACKAGE_QUERY_PATH)
    public ResponseEntity<ApiResponse> queryDatabaseForDatasetMetricsPackage (
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestParam Map<String, String> params,
        Pageable pageable
    ) {

        var optionalRv = validateDsmArguments(txid, requestReceivedAt, params);
        if (optionalRv.isPresent()) { return optionalRv.get(); }
        var rv = databaseQueryService.handleDatasetMetricsPackageSubQuery(txid, requestReceivedAt, params, pageable);
        return ResponseEntity.status(rv.getCode()).body(rv);
    }


    @GetMapping(DATASET_METRICS_PACKAGE_FINDING_QUERY_PATH)
    public ResponseEntity<ApiResponse> queryDatabaseForDatasetMetricsPackageFindings (
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestParam Map<String, String> params,
        Pageable pageable
    ) {

        var optionalRv = validateDsmArguments(txid, requestReceivedAt, params);
        if (optionalRv.isPresent()) { return optionalRv.get(); }
        var rv = databaseQueryService.handleDatasetMetricsPackageSubQueryReturnFindingType(txid, requestReceivedAt, params, pageable);
        return ResponseEntity.status(rv.getCode()).body(rv);
    }

    //

    @GetMapping(DATASOURCE_EDIT_QUERY_PATH)
    public ResponseEntity<ApiResponse> queryDatabaseForEditsByDatasetMetricsByDatasources (
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestParam Map<String, String> params,
        Pageable pageable
    ) {

        var optionalRv = validateDsmArguments(txid, requestReceivedAt, params);
        if (optionalRv.isPresent()) { return optionalRv.get(); }

        if ( 
            !params.keySet().contains(EDIT_DATASOURCES_PURL_KEY) 
            || params.get(EDIT_DATASOURCES_PURL_KEY).isBlank()
            || params.get(EDIT_DATASOURCES_PURL_KEY).length() > 512
        ) {
            log.warn("missing required query string parameter: %s", EDIT_DATASOURCES_PURL_KEY);
            var rv = ApiResponse.builder()
                                .txid(txid)
                                .requestReceivedAt(requestReceivedAt)
                                .code(HttpStatus.BAD_REQUEST.value())
                                .serverMessage(String.format("missing required query string parameter: {} missing", EDIT_DATASOURCES_PURL_KEY))
                                .build();            
                        
            return ResponseEntity.status(rv.getCode()).body(rv);
        }

        var rv = databaseQueryService.handleDatasourceEditSubQuery(txid, requestReceivedAt, params, pageable);
        return ResponseEntity.status(rv.getCode()).body(rv);
    }


    @GetMapping(DATASOURCE_PACKAGE_QUERY_PATH)
    public ResponseEntity<ApiResponse> queryDatabaseForPackagesByDatasetMetricsByDatasources (
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestParam Map<String, String> params,
        Pageable pageable
    ) {

        var optionalRv = validateDsmArguments(txid, requestReceivedAt, params);
        if (optionalRv.isPresent()) { return optionalRv.get(); }

        if ( 
            !params.keySet().contains(EDIT_DATASOURCES_PURL_KEY) 
            || params.get(EDIT_DATASOURCES_PURL_KEY).isBlank()
            || params.get(EDIT_DATASOURCES_PURL_KEY).length() > 512
        ) {
            log.warn("missing required query string parameter: %s", EDIT_DATASOURCES_PURL_KEY);
            var rv = ApiResponse.builder()
                                .txid(txid)
                                .requestReceivedAt(requestReceivedAt)
                                .code(HttpStatus.BAD_REQUEST.value())
                                .serverMessage(String.format("missing required query string parameter: {} missing", EDIT_DATASOURCES_PURL_KEY))
                                .build();            
                        
            return ResponseEntity.status(rv.getCode()).body(rv);
        }

        var rv = databaseQueryService.handleDatasourcePackageSubQuery(txid, requestReceivedAt, params, pageable);
        return ResponseEntity.status(rv.getCode()).body(rv);
    }


    @GetMapping(DATASOURCE_PACKAGE_FINDING_QUERY_PATH)
    public ResponseEntity<ApiResponse> queryDatabaseForFindingsPackagesByDatasetMetricsByDatasources (
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestParam Map<String, String> params,
        Pageable pageable
    ) {

        var optionalRv = validateDsmArguments(txid, requestReceivedAt, params);
        if (optionalRv.isPresent()) { return optionalRv.get(); }

        if ( 
            !params.keySet().contains(EDIT_DATASOURCES_PURL_KEY) 
            || params.get(EDIT_DATASOURCES_PURL_KEY).isBlank()
            || params.get(EDIT_DATASOURCES_PURL_KEY).length() > 512
        ) {
            log.warn("missing required query string parameter: %s", EDIT_DATASOURCES_PURL_KEY);
            var rv = ApiResponse.builder()
                                .txid(txid)
                                .requestReceivedAt(requestReceivedAt)
                                .code(HttpStatus.BAD_REQUEST.value())
                                .serverMessage(String.format("missing required query string parameter: {} missing", EDIT_DATASOURCES_PURL_KEY))
                                .build();            
                        
            return ResponseEntity.status(rv.getCode()).body(rv);
        }

        var rv = databaseQueryService.handleDatasetMetricsDatasourcePackageSubQueryReturnFindingType(txid, requestReceivedAt, params, pageable);
        return ResponseEntity.status(rv.getCode()).body(rv);
    }

    


    /**
     * 
     * @param table
     * @return
     */
    public boolean getIsValidTableArgument(String table) {
        return env.getValidDbTables()
                  .stream()
                  .map(x -> x.equalsIgnoreCase(table))
                  .filter(x -> x == true)
                  .findFirst()
                  .isPresent();
    }


    public Optional<ResponseEntity<ApiResponse>> validateDsmArguments(
            UUID txid, 
            ZonedDateTime requestReceivedAt, 
            Map<String, String> params
    ) {
        Optional<ResponseEntity<ApiResponse>> rvOptional = Optional.empty();

        if ( !params.keySet().contains(DATASET_NAME_KEY) ) {
            log.warn("missing required query string parameter: %s", DATASET_NAME_KEY);
            var rv = ApiResponse.builder()
                                .txid(txid)
                                .requestReceivedAt(requestReceivedAt)
                                .code(HttpStatus.BAD_REQUEST.value())
                                .serverMessage(String.format("missing required query string parameter: {} missing", DATASET_NAME_KEY))
                                .build();

            rvOptional = Optional.of(ResponseEntity.status(rv.getCode()).body(rv));               
        }


        if (
            !params.keySet().contains(IS_CURRENT_KEY)
                && !params.keySet().contains(IS_FORECAST_SAME_COURSE_KEY)
                && !params.keySet().contains(IS_FORECAST_RECOMMENDATIONS_TAKEN_KEY)
        ) {
            var message = String.format(
                "missing one of required query string parameter: %s, %s, %s", 
                IS_CURRENT_KEY, 
                IS_FORECAST_SAME_COURSE_KEY, 
                IS_FORECAST_RECOMMENDATIONS_TAKEN_KEY
            );
            log.warn(message);
            var rv = ApiResponse.builder()
                                .txid(txid)
                                .requestReceivedAt(requestReceivedAt)
                                .code(HttpStatus.BAD_REQUEST.value())
                                .serverMessage(message)
                                .build();

            rvOptional = Optional.of(ResponseEntity.status(rv.getCode()).body(rv));
        }

        return rvOptional;
    }

}
