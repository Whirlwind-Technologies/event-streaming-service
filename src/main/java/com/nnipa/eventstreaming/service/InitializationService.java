package com.nnipa.eventstreaming.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

/**
 * Initialization Service
 * Handles startup initialization tasks for the Event Streaming Service
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InitializationService {

    private final TopicManagementService topicManagementService;
    private final SchemaRegistryService schemaRegistryService;

    /**
     * Initialize the event streaming infrastructure after application startup
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("===========================================");
        log.info("Initializing Event Streaming Infrastructure");
        log.info("===========================================");

        try {
            // Wait a bit for Kafka to be fully ready
            Thread.sleep(5000);

            // Create default topics
            initializeTopics();

            // Initialize schemas
            initializeSchemas();

            // Perform health check
            performHealthCheck();

            log.info("===========================================");
            log.info("Event Streaming Infrastructure Ready!");
            log.info("===========================================");

        } catch (Exception e) {
            log.error("Failed to initialize event streaming infrastructure", e);
            // Don't fail the application startup, just log the error
        }
    }

    /**
     * Initialize default topics
     */
    private void initializeTopics() {
        try {
            log.info("Creating default topics...");
            topicManagementService.createDefaultTopics();

            // List created topics
            var topics = topicManagementService.listTopics();
            log.info("Available topics: {}", topics);

        } catch (Exception e) {
            log.error("Failed to initialize topics", e);
        }
    }

    /**
     * Initialize schemas in Schema Registry
     */
    private void initializeSchemas() {
        try {
            log.info("Initializing schemas...");

            // Register tenant event schemas
            registerSchema("nnipa.events.tenant.created-value",
                    "com.nnipa.proto.tenant.TenantCreatedEvent");
            registerSchema("nnipa.events.tenant.updated-value",
                    "com.nnipa.proto.tenant.TenantUpdatedEvent");
            registerSchema("nnipa.events.tenant.activated-value",
                    "com.nnipa.proto.tenant.TenantActivatedEvent");
            registerSchema("nnipa.events.tenant.deactivated-value",
                    "com.nnipa.proto.tenant.TenantDeactivatedEvent");

            // Register auth event schemas
            registerSchema("nnipa.events.auth.login-value",
                    "com.nnipa.proto.auth.UserLoginEvent");
            registerSchema("nnipa.events.auth.logout-value",
                    "com.nnipa.proto.auth.UserLogoutEvent");
            registerSchema("nnipa.events.auth.login-failed-value",
                    "com.nnipa.proto.auth.LoginFailedEvent");
            registerSchema("nnipa.events.auth.password-changed-value",
                    "com.nnipa.proto.auth.PasswordChangedEvent");

            // Register command schemas
            registerSchema("nnipa.commands.notification.send-value",
                    "com.nnipa.proto.command.SendNotificationCommand");
            registerSchema("nnipa.commands.audit.log-value",
                    "com.nnipa.proto.command.AuditLogCommand");

            // List registered subjects
            var subjects = schemaRegistryService.getAllSubjects();
            log.info("Registered schema subjects: {}", subjects);

        } catch (Exception e) {
            log.error("Failed to initialize schemas", e);
        }
    }

    /**
     * Register a schema with Schema Registry
     */
    private void registerSchema(String subject, String protoType) {
        try {
            // For Protobuf, we would typically load the .proto file content
            // For now, we'll use a placeholder schema
            String schemaContent = String.format(
                    "syntax = \"proto3\";\n" +
                            "package com.nnipa.proto;\n" +
                            "message %s {}\n",
                    protoType.substring(protoType.lastIndexOf('.') + 1)
            );

            int schemaId = schemaRegistryService.registerSchema(subject, schemaContent);
            log.info("Registered schema: {} with ID: {}", subject, schemaId);

        } catch (Exception e) {
            log.debug("Schema already registered or failed to register: {} - {}",
                    subject, e.getMessage());
        }
    }

    /**
     * Perform health check on startup
     */
    private void performHealthCheck() {
        try {
            log.info("Performing health check...");

            var health = topicManagementService.checkHealth();

            log.info("Health Check Results:");
            log.info("  - Kafka: {}", health.getKafkaStatus());
            log.info("  - Schema Registry: {}", health.getSchemaRegistryStatus());
            log.info("  - Overall Status: {}", health.isHealthy() ? "HEALTHY" : "UNHEALTHY");

            if (!health.isHealthy() && health.getErrors() != null) {
                health.getErrors().forEach((key, value) ->
                        log.warn("  - {} Error: {}", key, value));
            }

        } catch (Exception e) {
            log.error("Health check failed", e);
        }
    }

    /**
     * Get initialization status
     */
    public InitializationStatus getStatus() {
        InitializationStatus status = new InitializationStatus();

        try {
            // Check topics
            var topics = topicManagementService.listTopics();
            status.setTopicsInitialized(!topics.isEmpty());
            status.setTopicCount(topics.size());

            // Check schemas
            var subjects = schemaRegistryService.getAllSubjects();
            status.setSchemasInitialized(!subjects.isEmpty());
            status.setSchemaCount(subjects.size());

            // Check health
            var health = topicManagementService.checkHealth();
            status.setHealthy(health.isHealthy());
            status.setKafkaStatus(health.getKafkaStatus());
            status.setSchemaRegistryStatus(health.getSchemaRegistryStatus());

        } catch (Exception e) {
            log.error("Failed to get initialization status", e);
            status.setHealthy(false);
        }

        return status;
    }

    /**
     * Initialization status DTO
     */
    public static class InitializationStatus {
        private boolean topicsInitialized;
        private int topicCount;
        private boolean schemasInitialized;
        private int schemaCount;
        private boolean healthy;
        private String kafkaStatus;
        private String schemaRegistryStatus;

        // Getters and setters
        public boolean isTopicsInitialized() { return topicsInitialized; }
        public void setTopicsInitialized(boolean topicsInitialized) {
            this.topicsInitialized = topicsInitialized;
        }

        public int getTopicCount() { return topicCount; }
        public void setTopicCount(int topicCount) { this.topicCount = topicCount; }

        public boolean isSchemasInitialized() { return schemasInitialized; }
        public void setSchemasInitialized(boolean schemasInitialized) {
            this.schemasInitialized = schemasInitialized;
        }

        public int getSchemaCount() { return schemaCount; }
        public void setSchemaCount(int schemaCount) { this.schemaCount = schemaCount; }

        public boolean isHealthy() { return healthy; }
        public void setHealthy(boolean healthy) { this.healthy = healthy; }

        public String getKafkaStatus() { return kafkaStatus; }
        public void setKafkaStatus(String kafkaStatus) { this.kafkaStatus = kafkaStatus; }

        public String getSchemaRegistryStatus() { return schemaRegistryStatus; }
        public void setSchemaRegistryStatus(String schemaRegistryStatus) {
            this.schemaRegistryStatus = schemaRegistryStatus;
        }
    }
}