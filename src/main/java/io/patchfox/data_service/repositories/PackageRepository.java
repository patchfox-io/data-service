package io.patchfox.data_service.repositories;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Page;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.binding.QuerydslBinderCustomizer;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;

import io.patchfox.db_entities.entities.Package;
import io.patchfox.db_entities.entities.QPackage;

@RepositoryRestResource(path = "package", collectionResourceRel = "package")

public interface PackageRepository extends 
    JpaRepository<Package, Long>,     
    QuerydslPredicateExecutor<Package> {
        
    @Query(value = """
        SELECT p.* 
        FROM dataset_metrics dm 
        CROSS JOIN LATERAL unnest(dm.package_indexes) AS package_id 
        JOIN package p ON p.id = package_id 
        WHERE dm.id IN (:datasetMetricsIds)
        """, 
        countQuery = """
        SELECT COUNT(*) 
        FROM dataset_metrics dm 
        CROSS JOIN LATERAL unnest(dm.package_indexes) AS package_id 
        WHERE dm.id IN (:datasetMetricsIds)
        """,
        nativeQuery = true
    )
    Page<Package> findPackagesPreservingDuplicates(@Param("datasetMetricsIds") List<Long> datasetMetricsIds, Pageable pageable);

}
