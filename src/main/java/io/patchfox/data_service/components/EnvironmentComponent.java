package io.patchfox.data_service.components;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.Getter;


@Getter
@Component
public class EnvironmentComponent {
    
    @Value("${spring.application.name}")
    String serviceName;

    @Value("${spring.kafka.request-topic}")
    String kafkaRequestTopicName;

    @Value("${spring.kafka.response-topic}")
    String kafkaResponseTopicName;

    @Value("${spring.kafka.request.client-id-prefix}")
    String kafkaRequestClientIdPrefix;

    @Value("${spring.kafka.response.client-id-prefix}")
    String kafkaResponseClientIdPrefix;

    @Value("${spring.kafka.group-name}")
    String kafkaGroupName;

    @Value("${custom.db.rest.query.valid-tables}")
    List<String> validDbTables;

}
