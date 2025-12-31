package io.patchfox.data_service.repositories;


import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.binding.QuerydslBinderCustomizer;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.db_entities.entities.QDataset;

@RepositoryRestResource(path = "dataset", collectionResourceRel = "dataset")
public interface DatasetRepository extends 
        JpaRepository<Dataset, Long>,
        QuerydslPredicateExecutor<Dataset> {

    }
