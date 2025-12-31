package io.patchfox.data_service.json;


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

public class DatasourceEventView {

    @JsonProperty("purl")
    private String purl;

    @JsonProperty("txid")
    private String txid;

    @JsonProperty("commitHash")
    private String commitHash;

    @JsonProperty("commitBranch")
    private String commitBranch;

    @JsonProperty("commitDatetime")
    private String commitDatetime;

    @JsonProperty("status")
    private String status;

}
