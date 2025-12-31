package io.patchfox.data_service.services;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import io.patchfox.data_service.json.DatasourceEventView;
import io.patchfox.data_service.repositories.DatasourceEventRepository;
import io.patchfox.data_service.repositories.DatasourceRepository;
import io.patchfox.data_service.repositories.DatasourceEventRepository;
import io.patchfox.data_service.json.DatasourceEventView;

import io.patchfox.package_utils.json.ApiResponse;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Service
public class DatasourceEventService {

    @Autowired
    DatasourceRepository datasourceRepository;

    @Autowired
    DatasourceEventRepository datasourceEventRepository;


    /**
     * 
     * @param txid
     * @param requestReceivedAt
     * @param datasourcePurl
     * @return
     */
    public ApiResponse getDatasourceEvent(UUID txid, ZonedDateTime requestReceivedAt, String datasourcePurl) {

        var baseResponse = ApiResponse.builder()
                                      .txid(txid)
                                      .requestReceivedAt(requestReceivedAt)
                                      .build();     

        var datasources = datasourceRepository.findAllByPurl(datasourcePurl);
        if (datasources.isEmpty()) { 
            baseResponse.setCode(HttpStatus.NOT_FOUND.value());
            baseResponse.setServerMessage(String.format("datasource %s not found", datasourcePurl));
            return baseResponse;
        }

        // TODO -- move this to a standard helper. you've got these checks all over the place 
        //         and they all do the same thing. 
        if (datasources.size() > 1) {
            log.error(
                "something went wrong. expected only one record but found: {} for datasource purl: {}", 
                datasources.size(),
                datasourcePurl
            );
            
            baseResponse.setCode(HttpStatus.INTERNAL_SERVER_ERROR.value());
            return baseResponse;

        }

        var datasource = datasources.get(0);
        var lastEventReceivedAt = datasource.getLastEventReceivedAt();

        var datasourceEvents = 
                datasourceEventRepository.findFirstByDatasourcePurlOrderByCommitDateTimeDesc(datasourcePurl);

        // TODO - not doing the check here because it's fucking reduntant. MAKE THE HELPER METHODS for this 
        if (datasourceEvents.isEmpty()) {
            baseResponse.setCode(HttpStatus.NOT_FOUND.value());
            baseResponse.setServerMessage(String.format("no events for datasource %s were found", datasourcePurl));
            return baseResponse;
        }

        // we did findFirst - so we know there's only going to be one record if any at all
        var datasourceEvent = datasourceEvents.get(0);

        var datasourceEventView = new DatasourceEventView(
            datasourceEvent.getPurl(),
            datasourceEvent.getTxid().toString(),
            datasourceEvent.getCommitHash(),
            datasourceEvent.getCommitBranch(),
            datasourceEvent.getCommitDateTime().toString(),
            datasourceEvent.getStatus().toString()
        );

        baseResponse.setCode(HttpStatus.OK.value());
        baseResponse.setData(Map.of("datasourceEvent", datasourceEventView));
        return baseResponse;

    }


}
