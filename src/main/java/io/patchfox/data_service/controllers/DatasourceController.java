package io.patchfox.data_service.controllers;

import java.net.http.HttpResponse;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.patchfox.data_service.helpers.Validator;
import io.patchfox.data_service.services.DatasourceService;
import io.patchfox.package_utils.json.ApiResponse;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RestController
public class DatasourceController {

    public static final String API_PATH_PREFIX = "/api/v1";    
    public static final String DATASOURCES_PATH = API_PATH_PREFIX + "/datasources";
    public static final String DATASOURCES_BY_DATASET_PATH = DATASOURCES_PATH + "/{dataset}";
    public static final String GET_DATASOURCES_SIGNATURE = "GET_" + DATASOURCES_PATH;
    public static final String GET_DATASOURCES_BY_DATASET_SIGNATURE = "GET_" + DATASOURCES_BY_DATASET_PATH;


    @Autowired
    DatasourceService datasourceService;


    @GetMapping(
        value = {DATASOURCES_PATH, DATASOURCES_BY_DATASET_PATH},
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    ResponseEntity<ApiResponse> datasourcesHandler(
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @PathVariable(required = false) Optional<String> dataset
    ) throws JsonProcessingException {
        if (dataset.isPresent()) {
            var isValid = Validator.validateDatasetNameArg(dataset.get());
            if (!isValid) {
                var apiResponse = ApiResponse.builder()
                                             .code(HttpStatus.BAD_REQUEST.value())
                                             .serverMessage("bad dataset argument")
                                             .txid(txid)
                                             .requestReceivedAt(requestReceivedAt)
                                             .build();

                return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
            }
        }
        var apiResponse = datasourceService.getDatasources(txid, requestReceivedAt, dataset);
        return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
    }

}
