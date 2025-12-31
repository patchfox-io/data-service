package io.patchfox.data_service.controllers;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.catalina.connector.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.patchfox.data_service.helpers.Validator;
import io.patchfox.data_service.json.RecommendDetailView;
import io.patchfox.data_service.json.RecommendTopView;
import io.patchfox.data_service.repositories.DatasetMetricsRepository;
import io.patchfox.data_service.repositories.DatasetRepository;
import io.patchfox.data_service.services.RecommendViewService;
import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.db_entities.entities.DatasetMetrics;
import io.patchfox.package_utils.json.ApiResponse;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@RestController
public class RecommendViewController {

    public static final String API_PATH_PREFIX = "/api/v1";
    
    public static final String RECOMMEND_TOP_PATH = API_PATH_PREFIX + "/recommend";
    public static final String RECOMMEND_DETAIL_PATH = RECOMMEND_TOP_PATH + "/{type}";

    public static final String GET_RECOMMEND_TOP_SIGNATURE = "GET_" + RECOMMEND_TOP_PATH;
    public static final String GET_RECOMMEND_DETAIL_SIGNATURE = "GET_" + RECOMMEND_DETAIL_PATH;

    public static final String REDUCE_BACKLOG = "Reduce Backlog";
    public static final String REDUCE_BACKLOG_GROWTH = "Reduce Backlog Growth";
    public static final String REDUCE_STALE_PACKAGES = "Reduce Stale Packages";
    public static final String REDUCE_STALE_PACKAGE_GROWTH = "Reduce Stale Package Growth";
    public static final String REDUCE_DOWNLEVEL_PACKAGES = "Reduce Downlevel Packages";


    @Autowired
    RecommendViewService recommendViewService;

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    DatasetMetricsRepository datasetMetricsRepository;

    @GetMapping(
        value = {
            RECOMMEND_TOP_PATH, 
            RECOMMEND_DETAIL_PATH, 
        },
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    ResponseEntity<ApiResponse> recommendViewHandler(
        @RequestAttribute UUID txid,
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @PathVariable(required = false) Optional<String> type,
        @RequestParam(required = false) Optional<String> dataset
    ) {

        var apiResponse = ApiResponse.builder()
                                     .txid(txid)
                                     .code(Response.SC_OK)
                                     .build();

        if (dataset.isPresent()) {
            var isValid = Validator.validateDatasetNameArg(dataset.get());
            if (!isValid) {
                apiResponse = ApiResponse.builder()
                                         .code(HttpStatus.BAD_REQUEST.value())
                                         .serverMessage("bad dataset argument")
                                         .txid(txid)
                                         .requestReceivedAt(requestReceivedAt)
                                         .build();

                return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
            }
        }

        var jobIdOptional = datasetMetricsRepository.getLatestJobIdWithRecommendations();

        // handle request for detail view
        // 
        if (type.isPresent()) {
            var typeAllowList = 
                List.of(
                    RecommendTopView.REDUCE_CVES_TITLE,
                    RecommendTopView.REDUCE_CVE_GROWTH_TITLE,
                    RecommendTopView.REDUCE_CVE_BACKLOG_TITLE,
                    RecommendTopView.REDUCE_CVE_BACKLOG_GROWTH_TITLE,
                    RecommendTopView.REDUCE_STALE_PACKAGES_TITLE,
                    RecommendTopView.REDUCE_STALE_PACKAGES_GROWTH_TITLE,
                    RecommendTopView.REDUCE_DOWNLEVEL_PACKAGES_TITLE,
                    RecommendTopView.REDUCE_DOWNLEVEL_PACKAGES_GROWTH_TITLE,
                    RecommendTopView.GROW_PATCH_EFFICACY_TITLE,
                    RecommendTopView.REMOVE_REDUNDANT_PACKAGES_TITLE
                );

            var typeValue = type.get();
            log.info("raw typeValue is: {}", typeValue);
            if (typeValue.contains(" ")) {
                typeValue = typeValue.replace(" ", "_");
            } else if (typeValue.contains("%20")) {
                typeValue = typeValue.replace("%20", "_");
            }
            log.info("typeValue now: {}", typeValue);

            if ( !typeAllowList.contains(typeValue) ) {
                apiResponse.setCode(Response.SC_BAD_REQUEST);
                apiResponse.setServerMessage("disallowed value in type argument");
            } else if (jobIdOptional.isEmpty()) {
                apiResponse.setData(Map.of("payload", new RecommendDetailView()));
            } else {
                apiResponse = 
                    recommendViewService.getDetailRecommendView(
                        jobIdOptional.get(), 
                        requestReceivedAt, 
                        typeValue,
                        dataset
                    );
            }

        } 
        // handle request for top level view 
        //
        else {
            if (jobIdOptional.isEmpty()) {
                apiResponse.setData(Map.of("cards", new RecommendTopView()));
            } else {
                apiResponse = recommendViewService.getTopRecommendView(jobIdOptional.get(), requestReceivedAt, dataset);
            }
    
        }


        return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
    }


}
