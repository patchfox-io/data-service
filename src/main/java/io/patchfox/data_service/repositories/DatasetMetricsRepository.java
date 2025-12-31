package io.patchfox.data_service.repositories;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.time.ZonedDateTime;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.querydsl.binding.QuerydslBinderCustomizer;
import org.springframework.data.querydsl.binding.QuerydslBindings;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import org.springframework.data.rest.core.annotation.RestResource;
import org.springframework.web.bind.annotation.RequestParam;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.dsl.DateTimePath;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.StringPath;

import io.patchfox.db_entities.entities.DatasetMetrics;
import io.patchfox.db_entities.entities.QDatasetMetrics;


@RepositoryRestResource(path = "datasetMetrics", collectionResourceRel = "datasetMetrics")
public interface DatasetMetricsRepository extends 
        JpaRepository<DatasetMetrics, Long>,
        QuerydslPredicateExecutor<DatasetMetrics> {
    
    
    List<DatasetMetrics> findByIsCurrentAndCommitDateTimeAfterOrderByCommitDateTimeAsc(
        boolean isCurrent,
        ZonedDateTime commitDateTime
    );


    Optional<DatasetMetrics> findFirstByIsCurrentOrderByCommitDateTimeDesc(boolean isCurrent);


    @Query(
        value = "SELECT txid FROM dataset_metrics d WHERE d.job_id = :jobId ORDER BY d.commit_date_time DESC LIMIT 1",
        nativeQuery = true
    )
    Optional<UUID> getLatestDatasetMetricsRecordTxidForJobId(UUID jobId);


    List<DatasetMetrics> findDatasetMetricsByTxid(UUID txid);

    @Query(
        value = "SELECT job_id " + 
                "FROM dataset_metrics dm " + 
                "WHERE dm.is_forecast_recommendations_taken " + 
                "ORDER BY dm.commit_date_time DESC " + 
                "LIMIT 1",
        nativeQuery = true
    )
    public Optional<UUID> getLatestJobIdWithRecommendations();


}
