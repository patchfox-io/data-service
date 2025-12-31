package io.patchfox.data_service.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.binding.QuerydslBinderCustomizer;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

import io.patchfox.db_entities.entities.FindingReporter;
import io.patchfox.db_entities.entities.QFindingReporter;


@RepositoryRestResource(path = "findingReporter", collectionResourceRel = "findingReporter")
public interface FindingReporterRepository extends 
    JpaRepository<FindingReporter, Long>,
    QuerydslPredicateExecutor<FindingReporter> {

}
