package io.patchfox.data_service.repositories;

import java.time.ZonedDateTime;
import java.util.List;

import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.binding.QuerydslBinderCustomizer;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

import io.patchfox.db_entities.entities.DatasourceEvent;
import io.patchfox.db_entities.entities.QDatasourceEvent;

@RepositoryRestResource(path = "datasourceEvent", collectionResourceRel = "datasourceEvent")
public interface DatasourceEventRepository extends 
        JpaRepository<DatasourceEvent, Long>,
        QuerydslPredicateExecutor<DatasourceEvent> {
    List<DatasourceEvent> findFirstByDatasourcePurlOrderByEventDateTimeDesc(String datasourcePurl);

    List<DatasourceEvent> findFirstByDatasourcePurlOrderByCommitDateTimeDesc(String datasourcePurl);

}
