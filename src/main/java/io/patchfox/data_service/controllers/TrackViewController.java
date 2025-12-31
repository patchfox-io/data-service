package io.patchfox.data_service.controllers;

import java.time.ZonedDateTime;
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

import io.patchfox.data_service.helpers.Validator;
import io.patchfox.data_service.services.TrackViewService;
import io.patchfox.package_utils.json.ApiResponse;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class TrackViewController {

    @Autowired
    TrackViewService trackViewService;

    public static final String API_PATH_PREFIX = "/api/v1";
    public static final String TRACK_PATH = API_PATH_PREFIX + "/track";
    public static final String TRACK_BY_DATASET_PATH = TRACK_PATH + "/{dataset}";
    public static final String GET_TRACK_SIGNATURE = "GET_" + TRACK_PATH;    
    public static final String GET_TRACK_BY_DATASET_SIGNATURE = "GET_" + TRACK_BY_DATASET_PATH;

    @GetMapping(
        value = { TRACK_PATH, TRACK_BY_DATASET_PATH },
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    ResponseEntity<ApiResponse> trackViewHandler(
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestParam Optional<Boolean> synopsis,
        @PathVariable(required = false) Optional<String> dataset
    ) {
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
        var apiResponse = trackViewService.getTrackView(txid, requestReceivedAt, synopsis, dataset);
        return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
    }

}
