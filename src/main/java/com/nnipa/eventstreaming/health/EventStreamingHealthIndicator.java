package com.nnipa.eventstreaming.health;

import com.nnipa.eventstreaming.service.SchemaRegistryService;
import com.nnipa.eventstreaming.service.TopicManagementService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Health Indicator for Event Streaming Service
 * Monitors Kafka, Schema Registry, and Redis health
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventStreamingHealthIndicator implements HealthIndicator {

    private final KafkaAdmin kafkaAdmin;
    private final SchemaRegistryService schemaRegistryService;
    private final TopicManagementService topicManagementService;
    private final RedisConnectionFactory redisConnectionFactory;

    @Override
    public Health health() {
        Map<String, Object> details = new HashMap<>();
        Health.Builder healthBuilder = new Health.Builder();

        try {
            // Check Kafka
            boolean kafkaHealthy = checkKafkaHealth(details);

            // Check Schema Registry
            boolean schemaRegistryHealthy = checkSchemaRegistryHealth(details);

            // Check Redis
            boolean redisHealthy = checkRedisHealth(details);

            // Check consumer lag
            checkConsumerLag(details);

            // Overall health
            if (kafkaHealthy && schemaRegistryHealthy && redisHealthy) {
                healthBuilder.up();
            } else {
                healthBuilder.down();
            }

        } catch (Exception e) {
            log.error("Health check failed", e);
            healthBuilder.down()
                    .withDetail("error", e.getMessage());
        }

        return healthBuilder.withDetails(details).build();
    }

    private boolean checkKafkaHealth(Map<String, Object> details) {
        try {
            AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
            DescribeClusterResult clusterResult = adminClient.describeCluster();

            // Get cluster info
            String clusterId = clusterResult.clusterId().get(5, TimeUnit.SECONDS);
            int nodeCount = clusterResult.nodes().get(5, TimeUnit.SECONDS).size();

            details.put("kafka.clusterId", clusterId);
            details.put("kafka.nodeCount", nodeCount);
            details.put("kafka.status", "UP");

            adminClient.close();

            return nodeCount > 0;

        } catch (Exception e) {
            log.error("Kafka health check failed", e);
            details.put("kafka.status", "DOWN");
            details.put("kafka.error", e.getMessage());
            return false;
        }
    }

    private boolean checkSchemaRegistryHealth(Map<String, Object> details) {
        try {
            boolean healthy = schemaRegistryService.isHealthy();

            if (healthy) {
                // Get schema statistics
                Map<String, Object> stats = schemaRegistryService.getSchemaStatistics();
                details.put("schemaRegistry.status", "UP");
                details.put("schemaRegistry.totalSubjects", stats.get("totalSubjects"));
                details.put("schemaRegistry.totalVersions", stats.get("totalVersions"));
            } else {
                details.put("schemaRegistry.status", "DOWN");
            }

            return healthy;

        } catch (Exception e) {
            log.error("Schema Registry health check failed", e);
            details.put("schemaRegistry.status", "DOWN");
            details.put("schemaRegistry.error", e.getMessage());
            return false;
        }
    }

    private boolean checkRedisHealth(Map<String, Object> details) {
        try {
            RedisConnection connection = redisConnectionFactory.getConnection();
            String pong = connection.ping();
            connection.close();

            details.put("redis.status", "UP");
            details.put("redis.response", pong);
            return "PONG".equals(pong);

        } catch (Exception e) {
            log.error("Redis health check failed", e);
            details.put("redis.status", "DOWN");
            details.put("redis.error", e.getMessage());
            return false;
        }
    }

    private void checkConsumerLag(Map<String, Object> details) {
        try {
            // Get overall consumer lag
            var healthStatus = topicManagementService.checkHealth();

            if (healthStatus.getLagTotal() > 0) {
                details.put("consumerLag.total", healthStatus.getLagTotal());

                // Add warning if lag is high
                if (healthStatus.getLagTotal() > 10000) {
                    details.put("consumerLag.warning", "High consumer lag detected");
                }

                // Add top lagging topics
                if (healthStatus.getLagByTopic() != null && !healthStatus.getLagByTopic().isEmpty()) {
                    details.put("consumerLag.byTopic", healthStatus.getLagByTopic());
                }
            }

        } catch (Exception e) {
            log.debug("Could not check consumer lag: {}", e.getMessage());
        }
    }
}