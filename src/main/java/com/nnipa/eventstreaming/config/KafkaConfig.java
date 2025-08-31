package com.nnipa.eventstreaming.config;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Configuration for Event Streaming Service
 * Configures producers, consumers, admin client, and topics with Protobuf serialization
 */
@Slf4j
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${event-streaming.topic-config.default-partitions}")
    private int defaultPartitions;

    @Value("${event-streaming.topic-config.default-replication-factor}")
    private int defaultReplicationFactor;

    @Value("${event-streaming.topic-config.retention-ms}")
    private long retentionMs;

    @Value("${event-streaming.dlq.max-retries}")
    private int maxRetries;

    @Value("${event-streaming.dlq.retry-delay-ms}")
    private long retryDelayMs;

    // ============================================
    // Producer Configuration
    // ============================================

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        configs.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        configs.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.RETRIES_CONFIG, 3);
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        log.info("Configuring Kafka producer with bootstrap servers: {}", bootstrapServers);
        log.info("Schema Registry URL: {}", schemaRegistryUrl);

        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory());
        template.setObservationEnabled(true);
        return template;
    }

    // ============================================
    // Consumer Configuration
    // ============================================

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configs.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaProtobufDeserializer.class);
        configs.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Remove the problematic config
        // configs.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, true);
        // Use DynamicMessage for flexible protobuf handling
        configs.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
                "com.google.protobuf.DynamicMessage");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        configs.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        log.info("Configuring Kafka consumer with group id: {}", consumerGroupId);

        return new DefaultKafkaConsumerFactory<>(configs);
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Object>>
    kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setPollTimeout(3000);

        // Configure error handling with retry and DLQ
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new FixedBackOff(retryDelayMs, maxRetries)
        );
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    // ============================================
    // Admin Configuration
    // ============================================

    // Note: KafkaAdmin bean is defined in KafkaAdminConfig.java to avoid duplication

    // ============================================
    // Topic Creation
    // ============================================

    @Bean
    public NewTopic tenantCreatedTopic() {
        return TopicBuilder.name("nnipa.events.tenant.created")
                .partitions(defaultPartitions)
                .replicas(defaultReplicationFactor)
                .config("retention.ms", String.valueOf(retentionMs))
                .config("compression.type", "snappy")
                .config("min.insync.replicas", "2")
                .build();
    }

    @Bean
    public NewTopic tenantUpdatedTopic() {
        return TopicBuilder.name("nnipa.events.tenant.updated")
                .partitions(defaultPartitions)
                .replicas(defaultReplicationFactor)
                .config("retention.ms", String.valueOf(retentionMs))
                .config("compression.type", "snappy")
                .build();
    }

    @Bean
    public NewTopic userLoginTopic() {
        return TopicBuilder.name("nnipa.events.auth.login")
                .partitions(defaultPartitions)
                .replicas(defaultReplicationFactor)
                .config("retention.ms", String.valueOf(retentionMs))
                .config("compression.type", "snappy")
                .build();
    }

    @Bean
    public NewTopic sendNotificationTopic() {
        return TopicBuilder.name("nnipa.commands.notification.send")
                .partitions(defaultPartitions)
                .replicas(defaultReplicationFactor)
                .config("retention.ms", String.valueOf(retentionMs))
                .config("compression.type", "snappy")
                .build();
    }

    @Bean
    public NewTopic auditLogTopic() {
        return TopicBuilder.name("nnipa.commands.audit.log")
                .partitions(defaultPartitions)
                .replicas(defaultReplicationFactor)
                .config("retention.ms", String.valueOf(retentionMs * 4)) // Longer retention for audit
                .config("compression.type", "snappy")
                .config("min.insync.replicas", "2")
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name("nnipa.dlq.events")
                .partitions(3)
                .replicas(defaultReplicationFactor)
                .config("retention.ms", String.valueOf(retentionMs * 2)) // Longer retention for DLQ
                .config("compression.type", "snappy")
                .build();
    }
}