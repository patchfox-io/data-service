package io.patchfox.data_service.json;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.patchfox.data_service.json.TrackDoubleView.Value;
import io.patchfox.package_utils.util.CvssSeverity;
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
public class TrackCveBacklogView {

    @JsonProperty("name")
    String name;

    @JsonProperty("series")
    List<Value> series;

    @JsonProperty("current")
    Value current;

    @JsonProperty("prior")
    Value prior;

    @JsonProperty("historical")
    Value historical;    

    @JsonProperty("future")
    Value future;  

    // TODO move this to a super class so it is shared with doubleview
    @JsonProperty("percentChangePrior")
    double percentChangePrior;

    @JsonProperty("positiveImpactPrior")
    boolean positiveImpactPrior;

    @JsonProperty("trendingUpPrior")
    boolean trendingUpPrior;

    @JsonProperty("percentChangeHistorical")
    double percentChangeHistorical;

    @JsonProperty("positiveImpactHistorical")
    boolean positiveImpactHistorical;

    @JsonProperty("trendingUpHistorical")
    boolean trendingUpHistorical;

    @JsonProperty("percentChangeFuture")
    double percentChangeFuture;

    @JsonProperty("positiveImpactFuture")
    boolean positiveImpactFuture;

    @JsonProperty("trendingUpFuture")
    boolean trendingUpFuture;


    @AllArgsConstructor
    @Getter
    @Setter
    public static class Value {
        @JsonProperty("name")
        String name;

        @JsonProperty("value")
        Map<String, List<TrackDoubleView.Value>> data; 

        @JsonProperty("label")
        String label;
        
    }

}
