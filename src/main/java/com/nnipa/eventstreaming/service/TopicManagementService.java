package com.nnipa.eventstreaming.service;

import com.nnipa.eventstreaming.model.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Topic Management Service
 * Manages Kafka topics, consumer groups, and monitoring
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TopicManagementService {

    private final KafkaAdmin kafkaAdmin;
    private final SchemaRegistryService schemaRegistryService;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${event-streaming.topic-config.default-partitions:6}")
    private int defaultPartitions;

    @Value("${event-streaming.topic-config.default-replication-factor:3}")
    private short defaultReplicationFactor;

    @Value("${event-streaming.topic-config.retention-ms:604800000}")
    private String defaultRetentionMs;

    @Value("${event-streaming.topic-config.compression-type:snappy}")
    private String defaultCompressionType;

    @Value("${event-streaming.topic-config.min-in-sync-replicas:2}")
    private String defaultMinInSyncReplicas;

    private AdminClient adminClient;

    @PostConstruct
    public void init() {
        Map<String, Object> configs = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        this.adminClient = AdminClient.create(configs);
        log.info("Topic Management Service initialized with bootstrap servers: {}", bootstrapServers);
    }

    /**
     * Create a new topic
     */
    public void createTopic(TopicConfig config) {
        try {
            log.info("Creating topic: {}", config.getName());

            // Build topic configuration
            Map<String, String> topicConfigs = new HashMap<>();
            topicConfigs.put("retention.ms",
                    config.getRetentionMs() > 0 ? String.valueOf(config.getRetentionMs()) : defaultRetentionMs);
            topicConfigs.put("compression.type",
                    config.getCompressionType() != null ? config.getCompressionType() : defaultCompressionType);
            topicConfigs.put("min.insync.replicas",
                    config.getMinInSyncReplicas() > 0 ? String.valueOf(config.getMinInSyncReplicas()) : defaultMinInSyncReplicas);

            // Add additional configs if provided
            if (config.getAdditionalConfigs() != null) {
                topicConfigs.putAll(config.getAdditionalConfigs());
            }

            // Create the topic
            NewTopic newTopic = new NewTopic(
                    config.getName(),
                    config.getPartitions() > 0 ? config.getPartitions() : defaultPartitions,
                    config.getReplicationFactor() > 0 ? config.getReplicationFactor() : defaultReplicationFactor
            );
            newTopic.configs(topicConfigs);

            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
            result.all().get(30, TimeUnit.SECONDS);

            log.info("Topic created successfully: {}", config.getName());

        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                log.warn("Topic already exists: {}", config.getName());
            } else {
                log.error("Failed to create topic: {}", config.getName(), e);
                throw new TopicManagementException("Failed to create topic", e);
            }
        } catch (Exception e) {
            log.error("Failed to create topic: {}", config.getName(), e);
            throw new TopicManagementException("Failed to create topic", e);
        }
    }

    /**
     * Delete a topic
     */
    public void deleteTopic(String topicName) {
        try {
            log.warn("Deleting topic: {}", topicName);

            DeleteTopicsResult result = adminClient.deleteTopics(Collections.singletonList(topicName));
            result.all().get(30, TimeUnit.SECONDS);

            log.info("Topic deleted successfully: {}", topicName);

        } catch (Exception e) {
            log.error("Failed to delete topic: {}", topicName, e);
            throw new TopicManagementException("Failed to delete topic", e);
        }
    }

    /**
     * List all topics
     */
    public List<String> listTopics() {
        try {
            ListTopicsResult result = adminClient.listTopics();
            Set<String> topics = result.names().get(30, TimeUnit.SECONDS);

            // Filter out internal topics
            return topics.stream()
                    .filter(topic -> !topic.startsWith("_"))
                    .sorted()
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Failed to list topics", e);
            throw new TopicManagementException("Failed to list topics", e);
        }
    }

    /**
     * Get topic details
     */
    public Map<String, Object> getTopicDetails(String topicName) {
        try {
            Map<String, Object> details = new HashMap<>();
            details.put("name", topicName);

            // Get topic description
            DescribeTopicsResult describeResult = adminClient.describeTopics(
                    Collections.singletonList(topicName)
            );
            TopicDescription description = describeResult.all().get(30, TimeUnit.SECONDS).get(topicName);

            details.put("partitions", description.partitions().size());
            details.put("replicationFactor", description.partitions().get(0).replicas().size());
            details.put("isInternal", description.isInternal());

            // Get partition details
            List<Map<String, Object>> partitionDetails = new ArrayList<>();
            for (TopicPartitionInfo partition : description.partitions()) {
                Map<String, Object> partInfo = new HashMap<>();
                partInfo.put("partition", partition.partition());
                partInfo.put("leader", partition.leader() != null ? partition.leader().id() : null);
                partInfo.put("replicas", partition.replicas().stream()
                        .map(Node::id)
                        .collect(Collectors.toList()));
                partInfo.put("isr", partition.isr().stream()
                        .map(Node::id)
                        .collect(Collectors.toList()));
                partitionDetails.add(partInfo);
            }
            details.put("partitionDetails", partitionDetails);

            // Get topic configuration
            ConfigResource configResource = new ConfigResource(
                    ConfigResource.Type.TOPIC, topicName
            );
            DescribeConfigsResult configResult = adminClient.describeConfigs(
                    Collections.singletonList(configResource)
            );
            Config config = configResult.all().get(30, TimeUnit.SECONDS).get(configResource);

            Map<String, String> configs = new HashMap<>();
            config.entries().forEach(entry -> {
                if (!entry.isDefault()) {
                    configs.put(entry.name(), entry.value());
                }
            });
            details.put("configuration", configs);

            // Get schema information if available
            try {
                Map<String, SchemaInfo> schemas = schemaRegistryService.getSchemasForTopic(topicName);
                details.put("schemas", schemas);
            } catch (Exception e) {
                log.debug("No schema found for topic: {}", topicName);
            }

            return details;

        } catch (Exception e) {
            log.error("Failed to get topic details: {}", topicName, e);
            throw new TopicManagementException("Failed to get topic details", e);
        }
    }

    /**
     * Update topic configuration
     */
    public void updateTopicConfig(String topicName, Map<String, String> configs) {
        try {
            log.info("Updating topic configuration: {}", topicName);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

            Map<ConfigResource, Collection<AlterConfigOp>> updateConfigs = new HashMap<>();
            Collection<AlterConfigOp> ops = new ArrayList<>();

            for (Map.Entry<String, String> entry : configs.entrySet()) {
                ConfigEntry configEntry = new ConfigEntry(entry.getKey(), entry.getValue());
                AlterConfigOp op = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
                ops.add(op);
            }

            updateConfigs.put(resource, ops);

            AlterConfigsResult result = adminClient.incrementalAlterConfigs(updateConfigs);
            result.all().get(30, TimeUnit.SECONDS);

            log.info("Topic configuration updated successfully: {}", topicName);

        } catch (Exception e) {
            log.error("Failed to update topic configuration: {}", topicName, e);
            throw new TopicManagementException("Failed to update topic configuration", e);
        }
    }

    /**
     * Get consumer groups
     */
    public List<String> listConsumerGroups() {
        try {
            ListConsumerGroupsResult result = adminClient.listConsumerGroups();
            Collection<ConsumerGroupListing> groups = result.all().get(30, TimeUnit.SECONDS);

            return groups.stream()
                    .map(ConsumerGroupListing::groupId)
                    .sorted()
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Failed to list consumer groups", e);
            throw new TopicManagementException("Failed to list consumer groups", e);
        }
    }

    /**
     * Get consumer group details
     */
    public Map<String, Object> getConsumerGroupDetails(String groupId) {
        try {
            Map<String, Object> details = new HashMap<>();
            details.put("groupId", groupId);

            // Describe consumer group
            DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(
                    Collections.singletonList(groupId)
            );
            ConsumerGroupDescription description = describeResult.all()
                    .get(30, TimeUnit.SECONDS).get(groupId);

            details.put("state", description.state().toString());
            details.put("coordinator", description.coordinator().id());
            details.put("partitionAssignor", description.partitionAssignor());

            // Get members
            List<Map<String, Object>> members = new ArrayList<>();
            for (MemberDescription member : description.members()) {
                Map<String, Object> memberInfo = new HashMap<>();
                memberInfo.put("memberId", member.consumerId());
                memberInfo.put("clientId", member.clientId());
                memberInfo.put("host", member.host());

                List<String> assignments = member.assignment().topicPartitions().stream()
                        .map(tp -> tp.topic() + "-" + tp.partition())
                        .collect(Collectors.toList());
                memberInfo.put("assignments", assignments);

                members.add(memberInfo);
            }
            details.put("members", members);

            // Get consumer lag
            Map<TopicPartition, Long> lag = getConsumerLag(groupId);
            details.put("totalLag", lag.values().stream().mapToLong(Long::longValue).sum());
            details.put("lagByPartition", lag.entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> e.getKey().topic() + "-" + e.getKey().partition(),
                            Map.Entry::getValue
                    )));

            return details;

        } catch (Exception e) {
            log.error("Failed to get consumer group details: {}", groupId, e);
            throw new TopicManagementException("Failed to get consumer group details", e);
        }
    }

    /**
     * Get consumer lag for a group
     */
    public Map<TopicPartition, Long> getConsumerLag(String groupId) {
        try {
            Map<TopicPartition, Long> lagMap = new HashMap<>();

            // Get consumer group offsets
            ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> groupOffsets =
                    offsetsResult.partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);

            // Get end offsets for topics
            Set<TopicPartition> topicPartitions = groupOffsets.keySet();
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    getEndOffsets(topicPartitions);

            // Calculate lag
            for (TopicPartition tp : topicPartitions) {
                long consumerOffset = groupOffsets.get(tp).offset();
                long endOffset = endOffsets.get(tp).offset();
                long lag = endOffset - consumerOffset;
                lagMap.put(tp, lag);
            }

            return lagMap;

        } catch (Exception e) {
            log.error("Failed to get consumer lag for group: {}", groupId, e);
            return new HashMap<>();
        }
    }

    /**
     * Get end offsets for topic partitions
     */
    private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> getEndOffsets(
            Set<TopicPartition> partitions) throws Exception {

        Map<TopicPartition, OffsetSpec> offsetSpecs = partitions.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

        ListOffsetsResult result = adminClient.listOffsets(offsetSpecs);
        return result.all().get(30, TimeUnit.SECONDS);
    }

    /**
     * Reset consumer group offset
     */
    public void resetConsumerGroupOffset(String groupId, String topic, ResetOffsetStrategy strategy) {
        try {
            log.warn("Resetting consumer group offset - Group: {}, Topic: {}, Strategy: {}",
                    groupId, topic, strategy);

            // Get topic partitions
            DescribeTopicsResult topicResult = adminClient.describeTopics(Collections.singletonList(topic));
            TopicDescription topicDescription = topicResult.all().get(30, TimeUnit.SECONDS).get(topic);

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

            for (TopicPartitionInfo partition : topicDescription.partitions()) {
                TopicPartition tp = new TopicPartition(topic, partition.partition());

                long offset;
                switch (strategy) {
                    case EARLIEST:
                        offset = 0;
                        break;
                    case LATEST:
                        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                                getEndOffsets(Collections.singleton(tp));
                        offset = endOffsets.get(tp).offset();
                        break;
                    default:
                        offset = 0;
                }

                offsets.put(tp, new OffsetAndMetadata(offset));
            }

            // Reset offsets
            AlterConsumerGroupOffsetsResult result = adminClient.alterConsumerGroupOffsets(groupId, offsets);
            result.all().get(30, TimeUnit.SECONDS);

            log.info("Consumer group offset reset successfully - Group: {}, Topic: {}", groupId, topic);

        } catch (Exception e) {
            log.error("Failed to reset consumer group offset", e);
            throw new TopicManagementException("Failed to reset consumer group offset", e);
        }
    }

    /**
     * Get event statistics for topics
     */
    public Map<String, EventStatistics> getStatistics(List<String> topics) {
        Map<String, EventStatistics> statistics = new HashMap<>();

        if (topics == null || topics.isEmpty()) {
            topics = listTopics();
        }

        for (String topic : topics) {
            try {
                EventStatistics stats = getTopicStatistics(topic);
                statistics.put(topic, stats);
            } catch (Exception e) {
                log.error("Failed to get statistics for topic: {}", topic, e);
            }
        }

        return statistics;
    }

    /**
     * Get statistics for a single topic
     */
    private EventStatistics getTopicStatistics(String topic) throws Exception {
        // Get topic description
        DescribeTopicsResult describeResult = adminClient.describeTopics(Collections.singletonList(topic));
        TopicDescription description = describeResult.all().get(30, TimeUnit.SECONDS).get(topic);

        // Get message count (approximate)
        long totalMessages = 0;
        Set<TopicPartition> partitions = new HashSet<>();

        for (TopicPartitionInfo partition : description.partitions()) {
            TopicPartition tp = new TopicPartition(topic, partition.partition());
            partitions.add(tp);
        }

        // Get earliest and latest offsets
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
                getOffsets(partitions, OffsetSpec.earliest());
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                getOffsets(partitions, OffsetSpec.latest());

        for (TopicPartition tp : partitions) {
            long earliest = earliestOffsets.get(tp).offset();
            long latest = latestOffsets.get(tp).offset();
            totalMessages += (latest - earliest);
        }

        return EventStatistics.builder()
                .topic(topic)
                .totalEvents(totalMessages)
                .successfulEvents(totalMessages) // Approximate
                .failedEvents(0) // Would need to track separately
                .dlqEvents(0) // Would need to check DLQ topic
                .averageProcessingTimeMs(0) // Would need metrics
                .lastEventTime(Instant.now()) // Approximate
                .eventsByType(new HashMap<>()) // Would need to consume messages to determine
                .build();
    }

    /**
     * Get offsets for topic partitions
     */
    private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> getOffsets(
            Set<TopicPartition> partitions, OffsetSpec offsetSpec) throws Exception {

        Map<TopicPartition, OffsetSpec> offsetSpecs = partitions.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> offsetSpec));

        ListOffsetsResult result = adminClient.listOffsets(offsetSpecs);
        return result.all().get(30, TimeUnit.SECONDS);
    }

    /**
     * Check cluster health
     */
    public HealthStatus checkHealth() {
        HealthStatus.HealthStatusBuilder healthBuilder = HealthStatus.builder()
                .checkedAt(Instant.now());

        Map<String, String> errors = new HashMap<>();
        boolean healthy = true;

        try {
            // Check Kafka cluster
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            Collection<Node> nodes = clusterResult.nodes().get(5, TimeUnit.SECONDS);

            if (nodes.isEmpty()) {
                healthy = false;
                errors.put("kafka", "No brokers available");
                healthBuilder.kafkaStatus("UNHEALTHY");
            } else {
                healthBuilder.kafkaStatus("HEALTHY (" + nodes.size() + " brokers)");
            }

        } catch (Exception e) {
            healthy = false;
            errors.put("kafka", e.getMessage());
            healthBuilder.kafkaStatus("UNHEALTHY");
        }

        // Check Schema Registry
        try {
            if (schemaRegistryService.isHealthy()) {
                healthBuilder.schemaRegistryStatus("HEALTHY");
            } else {
                healthy = false;
                errors.put("schemaRegistry", "Not responding");
                healthBuilder.schemaRegistryStatus("UNHEALTHY");
            }
        } catch (Exception e) {
            healthy = false;
            errors.put("schemaRegistry", e.getMessage());
            healthBuilder.schemaRegistryStatus("UNHEALTHY");
        }

        // Check consumer lag
        try {
            Map<String, Long> lagByTopic = new HashMap<>();
            long totalLag = 0;

            List<String> groups = listConsumerGroups();
            for (String group : groups) {
                Map<TopicPartition, Long> groupLag = getConsumerLag(group);
                for (Map.Entry<TopicPartition, Long> entry : groupLag.entrySet()) {
                    String topic = entry.getKey().topic();
                    long lag = entry.getValue();
                    lagByTopic.merge(topic, lag, Long::sum);
                    totalLag += lag;
                }
            }

            healthBuilder.lagTotal(totalLag);
            healthBuilder.lagByTopic(lagByTopic);

            // Consider unhealthy if lag is too high
            if (totalLag > 100000) {
                errors.put("consumerLag", "High consumer lag: " + totalLag);
            }

        } catch (Exception e) {
            log.error("Failed to check consumer lag", e);
        }

        healthBuilder.healthy(healthy);
        healthBuilder.errors(errors);

        return healthBuilder.build();
    }

    /**
     * Create default topics for the platform
     */
    public void createDefaultTopics() {
        log.info("Creating default NNIPA topics...");

        List<TopicConfig> defaultTopics = Arrays.asList(
                // Tenant events
                TopicConfig.builder()
                        .name("nnipa.events.tenant.created")
                        .partitions(6)
                        .replicationFactor((short) 3)
                        .retentionMs(604800000L) // 7 days
                        .compressionType("snappy")
                        .minInSyncReplicas(2)
                        .build(),

                TopicConfig.builder()
                        .name("nnipa.events.tenant.updated")
                        .partitions(6)
                        .replicationFactor((short) 3)
                        .retentionMs(604800000L)
                        .compressionType("snappy")
                        .minInSyncReplicas(2)
                        .build(),

                // Auth events
                TopicConfig.builder()
                        .name("nnipa.events.auth.login")
                        .partitions(6)
                        .replicationFactor((short) 3)
                        .retentionMs(604800000L)
                        .compressionType("snappy")
                        .minInSyncReplicas(2)
                        .build(),

                TopicConfig.builder()
                        .name("nnipa.events.auth.logout")
                        .partitions(6)
                        .replicationFactor((short) 3)
                        .retentionMs(604800000L)
                        .compressionType("snappy")
                        .minInSyncReplicas(2)
                        .build(),

                // Command topics
                TopicConfig.builder()
                        .name("nnipa.commands.notification.send")
                        .partitions(6)
                        .replicationFactor((short) 3)
                        .retentionMs(604800000L)
                        .compressionType("snappy")
                        .minInSyncReplicas(2)
                        .build(),

                TopicConfig.builder()
                        .name("nnipa.commands.audit.log")
                        .partitions(6)
                        .replicationFactor((short) 3)
                        .retentionMs(2419200000L) // 28 days for audit
                        .compressionType("snappy")
                        .minInSyncReplicas(2)
                        .build(),

                // DLQ topics
                TopicConfig.builder()
                        .name("nnipa.dlq.events")
                        .partitions(3)
                        .replicationFactor((short) 3)
                        .retentionMs(1209600000L) // 14 days for DLQ
                        .compressionType("snappy")
                        .minInSyncReplicas(2)
                        .build()
        );

        for (TopicConfig topicConfig : defaultTopics) {
            try {
                createTopic(topicConfig);
            } catch (Exception e) {
                log.warn("Failed to create topic: {} - {}", topicConfig.getName(), e.getMessage());
            }
        }

        log.info("Default topics creation completed");
    }

    /**
     * Get topic metrics
     */
    public Map<String, Object> getTopicMetrics(String topic) {
        Map<String, Object> metrics = new HashMap<>();

        try {
            // Get basic topic info
            Map<String, Object> details = getTopicDetails(topic);
            metrics.put("partitions", details.get("partitions"));
            metrics.put("replicationFactor", details.get("replicationFactor"));

            // Get message rate (would need JMX or metrics reporter in production)
            metrics.put("messagesPerSecond", 0);
            metrics.put("bytesInPerSecond", 0);
            metrics.put("bytesOutPerSecond", 0);

            // Get storage size (would need to query brokers directly)
            metrics.put("sizeInBytes", 0);

            // Get consumer groups for this topic
            List<String> consumerGroups = getConsumerGroupsForTopic(topic);
            metrics.put("consumerGroups", consumerGroups);
            metrics.put("consumerGroupCount", consumerGroups.size());

        } catch (Exception e) {
            log.error("Failed to get metrics for topic: {}", topic, e);
        }

        return metrics;
    }

    /**
     * Get consumer groups subscribed to a topic
     */
    private List<String> getConsumerGroupsForTopic(String topic) {
        List<String> subscribedGroups = new ArrayList<>();

        try {
            List<String> allGroups = listConsumerGroups();

            for (String group : allGroups) {
                try {
                    ListConsumerGroupOffsetsResult offsetsResult =
                            adminClient.listConsumerGroupOffsets(group);
                    Map<TopicPartition, OffsetAndMetadata> offsets =
                            offsetsResult.partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);

                    boolean subscribedToTopic = offsets.keySet().stream()
                            .anyMatch(tp -> tp.topic().equals(topic));

                    if (subscribedToTopic) {
                        subscribedGroups.add(group);
                    }
                } catch (Exception e) {
                    log.debug("Error checking group {} for topic {}", group, topic);
                }
            }

        } catch (Exception e) {
            log.error("Failed to get consumer groups for topic: {}", topic, e);
        }

        return subscribedGroups;
    }

    /**
     * Enum for reset offset strategies
     */
    public enum ResetOffsetStrategy {
        EARLIEST,
        LATEST,
        TIMESTAMP,
        OFFSET
    }

    /**
     * Custom exception for topic management operations
     */
    public static class TopicManagementException extends RuntimeException {
        public TopicManagementException(String message) {
            super(message);
        }

        public TopicManagementException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}