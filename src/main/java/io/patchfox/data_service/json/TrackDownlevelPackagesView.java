package io.patchfox.data_service.json;


import com.fasterxml.jackson.annotation.JsonInclude;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter 
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
@Slf4j
public class TrackDownlevelPackagesView extends TrackStalePackagesView { }
