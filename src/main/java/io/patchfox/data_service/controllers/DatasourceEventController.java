package io.patchfox.data_service.controllers;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.packageurl.MalformedPackageURLException;
import com.github.packageurl.PackageURLBuilder;

import io.patchfox.data_service.services.DatasourceEventService;
import io.patchfox.package_utils.json.ApiResponse;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RestController
public class DatasourceEventController {

    public static final String API_PATH_PREFIX = "/api/v1";

    public static final String DATASOURCE_EVENTS_PATH = API_PATH_PREFIX + "/datasourceEvents";
    public static final String GET_DATASOURCES_SIGNATURE = "GET_" + DATASOURCE_EVENTS_PATH;

    public static final String DATASOURCE_EVENTS_LATEST_PATH = DATASOURCE_EVENTS_PATH + "/latest";
    public static final String GET_DATASOURCES_LATEST_SIGNATURE = "GET_" + DATASOURCE_EVENTS_LATEST_PATH;

    @Autowired
    DatasourceEventService datasourceEventService;


    @GetMapping(
        value = DATASOURCE_EVENTS_LATEST_PATH, 
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    ResponseEntity<ApiResponse> datasourceEventsHandler(
        @RequestAttribute UUID txid, 
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestParam("domain") String domain,
        @RequestParam("name") String name,
        @RequestParam("branch") String branch,
        @RequestParam("type") String type
    ) throws JsonProcessingException {

        var datasourcePurl = new String();

        try {
            var purl = PackageURLBuilder.aPackageURL()
                                        .withType("generic")
                                        .withNamespace(domain)
                                        .withName(name + "::" + branch)
                                        .withVersion(type)
                                        .build();

            datasourcePurl = purl.toString();
        } catch (MalformedPackageURLException e) {
            log.warn("caller sent parameters that don't create a valid purl");
            log.warn("domain: {}, name: {}, branch: {}, type: {}", domain, name, branch, type);
            var apiResponse =  ApiResponse.builder()
                                          .txid(txid)
                                          .requestReceivedAt(requestReceivedAt)
                                          .code(HttpStatus.BAD_REQUEST.value())
                                          .build();

            return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
        }

        var apiResponse = datasourceEventService.getDatasourceEvent(txid, requestReceivedAt, datasourcePurl);
        return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);

    }
}
