package io.patchfox.data_service.json;

import java.time.ZonedDateTime;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


@Getter 
@Setter
@AllArgsConstructor
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Slf4j
public class DatasourceView {

    @JsonProperty("domain")
    private String domain;

    @JsonProperty("packedName")
    private String packedName;

    @JsonProperty("type")
    private String type;

    @JsonProperty("numberEventsReceived")
    private double numberEventsReceived;

    @JsonProperty("numberEventProcessingErrors")
    private double numberEventProcessingErrors;
    
    @JsonProperty("firstEventReceivedAt")
    private String firstEventReceivedAt;

    @JsonProperty("lastEventReceivedAt")
    private String lastEventReceivedAt;

    @JsonProperty("lastEventReceivedStatus")
    private String lastEventReceivedStatus;

    @JsonProperty("datasets")
    private List<String> datasets;

}
