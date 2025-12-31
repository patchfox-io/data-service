package io.patchfox.data_service.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.binding.QuerydslBinderCustomizer;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

import io.patchfox.db_entities.entities.Edit;
import io.patchfox.db_entities.entities.QEdit;

@RepositoryRestResource(path = "edit", collectionResourceRel = "edit")
public interface EditRepository extends 
    JpaRepository<Edit, Long>,
    QuerydslPredicateExecutor<Edit> {

}
