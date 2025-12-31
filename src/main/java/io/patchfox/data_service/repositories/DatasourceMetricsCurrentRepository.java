package io.patchfox.data_service.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

import io.patchfox.db_entities.entities.DatasourceMetricsCurrent;

@RepositoryRestResource(path = "datasourceMetricsCurrent", collectionResourceRel = "datasourceMetricsCurrent")
public interface DatasourceMetricsCurrentRepository extends 
        JpaRepository<DatasourceMetricsCurrent, Long>,
        QuerydslPredicateExecutor<DatasourceMetricsCurrent> {

}
