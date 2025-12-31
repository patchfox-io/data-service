package io.patchfox.data_service.json;

import java.util.List;
import java.util.Map;

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
public class TrackPesView extends TrackDoubleView {

    @JsonProperty("impactSeries")
    List<Value> impactSeries;

    @JsonProperty("effortSeries")
    List<Value> effortSeries;

}
