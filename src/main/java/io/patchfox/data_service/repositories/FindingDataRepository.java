package io.patchfox.data_service.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.binding.QuerydslBinderCustomizer;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

import io.patchfox.db_entities.entities.FindingData;
import io.patchfox.db_entities.entities.QFindingData;

@RepositoryRestResource(path = "findingData", collectionResourceRel = "findingData")
public interface FindingDataRepository extends 
    JpaRepository<FindingData, Long>,
    QuerydslPredicateExecutor<FindingData> {

    }
