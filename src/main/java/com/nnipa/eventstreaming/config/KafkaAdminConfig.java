package com.nnipa.eventstreaming.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Admin Configuration
 * Configures the KafkaAdmin bean for topic management
 */
@Slf4j
@Configuration
public class KafkaAdminConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.admin.properties.request.timeout.ms:30000}")
    private String requestTimeoutMs;

    /**
     * Configure KafkaAdmin bean
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);

        log.info("Configuring KafkaAdmin with bootstrap servers: {}", bootstrapServers);

        return new KafkaAdmin(configs);
    }
}