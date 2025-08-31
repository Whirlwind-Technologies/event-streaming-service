package com.nnipa.eventstreaming.controller;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.nnipa.eventstreaming.enums.PartitionStrategy;
import com.nnipa.eventstreaming.model.*;
import com.nnipa.eventstreaming.service.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * REST Controller for Event Streaming Management
 * Provides endpoints for event publishing, replay, and monitoring
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
@Tag(name = "Event Streaming", description = "Event streaming management APIs")
public class EventStreamingController {

    private final EventPublisher eventPublisher;
    private final EventReplayService replayService;
    private final TopicManagementService topicService;
    private final SchemaRegistryService schemaService;
    private final InitializationService initializationService;
    private final JsonFormat.Parser jsonParser = JsonFormat.parser();
    private final JsonFormat.Printer jsonPrinter = JsonFormat.printer();

    /**
     * Publish an event (mainly for testing/admin purposes)
     * Production events should be published directly by services
     */
    @PostMapping("/publish")
    @Operation(summary = "Publish an event to a topic")
    public ResponseEntity<Map<String, Object>> publishEvent(
            @RequestParam String topic,
            @RequestParam(required = false) String key,
            @RequestBody Map<String, Object> eventData,
            @RequestParam(defaultValue = "TENANT_BASED") PartitionStrategy strategy) {

        try {
            log.info("Publishing event to topic: {} with strategy: {}", topic, strategy);

            // Convert JSON to appropriate Protobuf message
            // This is simplified - in production, you'd need proper type mapping
            String tenantId = (String) eventData.get("tenantId");
            String userId = (String) eventData.get("userId");

            CompletableFuture<Object> future = eventPublisher.publishEvent(
                    topic,
                    null, // Message would be constructed here
                    tenantId,
                    userId
            ).thenApply(result -> result);

            return ResponseEntity.accepted().body(Map.of(
                    "status", "accepted",
                    "topic", topic,
                    "key", key != null ? key : "auto-generated",
                    "timestamp", Instant.now().toString()
            ));

        } catch (Exception e) {
            log.error("Failed to publish event", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "error", "Failed to publish event",
                    "message", e.getMessage()
            ));
        }
    }

    /**
     * Replay events from a specific time range
     */
    @PostMapping("/replay")
    @Operation(summary = "Replay events from a time range")
    public ResponseEntity<EventReplayService.ReplayResult> replayEvents(
            @RequestParam String sourceTopic,
            @RequestParam long fromTimestamp,
            @RequestParam long toTimestamp,
            @RequestParam(required = false) String targetTopic) {

        log.info("Starting replay - Topic: {}, From: {}, To: {}",
                sourceTopic, Instant.ofEpochMilli(fromTimestamp), Instant.ofEpochMilli(toTimestamp));

        CompletableFuture<EventReplayService.ReplayResult> future = replayService.replayFromTimestamp(
                sourceTopic, fromTimestamp, toTimestamp, targetTopic, null
        );

        try {
            EventReplayService.ReplayResult result = future.get();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Replay failed", e);
            EventReplayService.ReplayResult errorResult = new EventReplayService.ReplayResult();
            errorResult.success = false;
            errorResult.errors.add(e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResult);
        }
    }

    /**
     * Get offset range for a topic partition
     */
    @GetMapping("/offsets")
    @Operation(summary = "Get offset range for a topic partition")
    public ResponseEntity<EventReplayService.OffsetRange> getOffsetRange(
            @RequestParam String topic,
            @RequestParam int partition) {

        try {
            EventReplayService.OffsetRange range = replayService.getOffsetRange(topic, partition);
            return ResponseEntity.ok(range);
        } catch (Exception e) {
            log.error("Failed to get offset range", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    /**
     * Create a new topic
     */
    @PostMapping("/topics")
    @Operation(summary = "Create a new Kafka topic")
    public ResponseEntity<Map<String, Object>> createTopic(@RequestBody TopicConfig config) {
        try {
            topicService.createTopic(config);
            return ResponseEntity.ok(Map.of(
                    "status", "created",
                    "topic", config.getName(),
                    "partitions", config.getPartitions(),
                    "replication", config.getReplicationFactor()
            ));
        } catch (Exception e) {
            log.error("Failed to create topic", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "error", "Failed to create topic",
                    "message", e.getMessage()
            ));
        }
    }

    /**
     * List all topics
     */
    @GetMapping("/topics")
    @Operation(summary = "List all Kafka topics")
    public ResponseEntity<List<String>> listTopics() {
        try {
            List<String> topics = topicService.listTopics();
            return ResponseEntity.ok(topics);
        } catch (Exception e) {
            log.error("Failed to list topics", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    /**
     * Get topic details
     */
    @GetMapping("/topics/{topic}")
    @Operation(summary = "Get topic details")
    public ResponseEntity<Map<String, Object>> getTopicDetails(@PathVariable String topic) {
        try {
            Map<String, Object> details = topicService.getTopicDetails(topic);
            return ResponseEntity.ok(details);
        } catch (Exception e) {
            log.error("Failed to get topic details", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "error", "Failed to get topic details",
                    "message", e.getMessage()
            ));
        }
    }

    /**
     * Delete a topic
     */
    @DeleteMapping("/topics/{topic}")
    @Operation(summary = "Delete a Kafka topic")
    public ResponseEntity<Map<String, Object>> deleteTopic(@PathVariable String topic) {
        try {
            topicService.deleteTopic(topic);
            return ResponseEntity.ok(Map.of(
                    "status", "deleted",
                    "topic", topic
            ));
        } catch (Exception e) {
            log.error("Failed to delete topic", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "error", "Failed to delete topic",
                    "message", e.getMessage()
            ));
        }
    }

    /**
     * Get schema for a subject
     */
    @GetMapping("/schemas/{subject}")
    @Operation(summary = "Get schema for a subject")
    public ResponseEntity<SchemaInfo> getSchema(
            @PathVariable String subject,
            @RequestParam(required = false) Integer version) {

        try {
            SchemaInfo schema = schemaService.getSchema(subject, version);
            return ResponseEntity.ok(schema);
        } catch (Exception e) {
            log.error("Failed to get schema", e);
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
    }

    /**
     * Register a new schema
     */
    @PostMapping("/schemas/{subject}")
    @Operation(summary = "Register a new schema")
    public ResponseEntity<Map<String, Object>> registerSchema(
            @PathVariable String subject,
            @RequestBody String schema) {

        try {
            int id = schemaService.registerSchema(subject, schema);
            return ResponseEntity.ok(Map.of(
                    "id", id,
                    "subject", subject,
                    "status", "registered"
            ));
        } catch (Exception e) {
            log.error("Failed to register schema", e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                    "error", "Failed to register schema",
                    "message", e.getMessage()
            ));
        }
    }

    /**
     * Get event statistics
     */
    @GetMapping("/statistics")
    @Operation(summary = "Get event statistics")
    public ResponseEntity<Map<String, EventStatistics>> getStatistics(
            @RequestParam(required = false) List<String> topics) {

        try {
            Map<String, EventStatistics> stats = topicService.getStatistics(topics);
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            log.error("Failed to get statistics", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    /**
     * Get initialization status
     */
    @GetMapping("/status")
    @Operation(summary = "Get event streaming service status")
    public ResponseEntity<InitializationService.InitializationStatus> getStatus() {
        InitializationService.InitializationStatus status = initializationService.getStatus();
        return ResponseEntity.ok(status);
    }

    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    @Operation(summary = "Check event streaming service health")
    public ResponseEntity<HealthStatus> getHealth() {
        try {
            HealthStatus health = topicService.checkHealth();
            HttpStatus status = health.isHealthy() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
            return ResponseEntity.status(status).body(health);
        } catch (Exception e) {
            log.error("Health check failed", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(
                    HealthStatus.builder()
                            .healthy(false)
                            .errors(Map.of("error", e.getMessage()))
                            .checkedAt(Instant.now())
                            .build()
            );
        }
    }
}