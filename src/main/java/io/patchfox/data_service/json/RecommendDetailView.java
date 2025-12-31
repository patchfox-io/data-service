package io.patchfox.data_service.json;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.patchfox.package_utils.util.Pair;
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
public class RecommendDetailView {

    @JsonProperty("title")
    public String title;

    @JsonProperty("recommendations")
    public List<RecommendationCard> recommendations;


    public class RecommendationCard {

        @JsonProperty("patches")
        public Map<String, List<Pair<String, String>>> patches;

        @JsonProperty("decreaseBacklogScore")
        public int decreaseBacklogScore;

        @JsonProperty("decreaseVulnerabilitiesScore")
        public int decreaseVulnerabilitiesScore;

        @JsonProperty("avoidsVulnerabilitiesScore")
        public int avoidsVulnerabilitiesScore;

        @JsonProperty("increaseImpactScore")
        public int increaseImpactScore;

        @JsonProperty("ticketText")
        public String ticketText;
    }

}
