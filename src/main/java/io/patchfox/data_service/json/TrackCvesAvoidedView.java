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
public class TrackCvesAvoidedView {

    @JsonProperty("current")
    int current;

    @JsonProperty("percentChangeFromHistorical")
    double percentChangeFromHistorical;

    @JsonProperty("name")
    String name;

    @JsonProperty("series")
    List<Historical> historical;
    
    /**
     * 
     */
    public class Historical {
        
        @JsonProperty("name")
        ZonedDateTime name;
        
        @JsonProperty("value")
        int value;

    }

}
