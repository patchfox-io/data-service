package io.patchfox.data_service.repositories;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.binding.QuerydslBinderCustomizer;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

import io.patchfox.db_entities.entities.Datasource;
import io.patchfox.db_entities.entities.QDatasource;

@RepositoryRestResource(path = "datasource", collectionResourceRel = "datasource")
public interface DatasourceRepository extends 
        JpaRepository<Datasource, Long>,
        QuerydslPredicateExecutor<Datasource> {

    List<Datasource> findAllByPurl(String purl);
}

