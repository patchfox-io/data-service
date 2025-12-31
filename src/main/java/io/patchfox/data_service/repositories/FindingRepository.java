package io.patchfox.data_service.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.binding.QuerydslBinderCustomizer;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

import io.patchfox.db_entities.entities.Finding;
import io.patchfox.db_entities.entities.QFinding;

@RepositoryRestResource(path = "findingRepository", collectionResourceRel = "findingRepository")
public interface FindingRepository extends 
    JpaRepository<Finding, Long>,
    QuerydslPredicateExecutor<Finding> {
        
    }
