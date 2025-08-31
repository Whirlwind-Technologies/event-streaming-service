package com.nnipa.eventstreaming.service;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.nnipa.eventstreaming.metrics.EventStreamingMetrics;
import com.nnipa.eventstreaming.model.EventEnvelope;
import com.nnipa.eventstreaming.enums.PartitionStrategy;
import com.nnipa.proto.common.EventMetadata;
import com.nnipa.proto.common.Priority;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Event Publisher Service
 * Handles publishing of all events to Kafka with proper partitioning,
 * serialization, and error handling
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PartitionKeyExtractor partitionKeyExtractor;
    private final MeterRegistry meterRegistry;
    private final EventStreamingMetrics metrics;

    /**
     * Publish an event with metadata
     */
    @CircuitBreaker(name = "kafka-producer")
    @Retry(name = "kafka-producer")
    public CompletableFuture<SendResult<String, Object>> publishEvent(
            String topic,
            Message event,
            EventMetadata metadata,
            PartitionStrategy strategy) {

        Timer.Sample sample = Timer.start(meterRegistry);

        try {
            // Extract partition key based on strategy
            String partitionKey = partitionKeyExtractor.extractKey(event, metadata, strategy);

            // Create headers
            Headers headers = createHeaders(metadata);

            // Create producer record with headers
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                    topic,
                    null, // partition (let Kafka decide based on key)
                    partitionKey,
                    event,
                    headers
            );

            // Send to Kafka
            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(record);

            // Handle success
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Event published successfully - Topic: {}, Key: {}, EventId: {}",
                            topic, partitionKey, metadata.getEventId());
                    metrics.recordEventPublished(topic, true);
                    incrementCounter("events.published.success", topic);
                } else {
                    log.error("Failed to publish event - Topic: {}, EventId: {}",
                            topic, metadata.getEventId(), ex);
                    metrics.recordEventPublished(topic, false);
                    incrementCounter("events.published.failure", topic);
                }
                sample.stop(Timer.builder("events.publish.duration")
                        .tag("topic", topic)
                        .register(meterRegistry));
            });

            return future;

        } catch (Exception e) {
            log.error("Error publishing event to topic: {}", topic, e);
            incrementCounter("events.published.error", topic);
            throw new EventPublishException("Failed to publish event", e);
        }
    }

    /**
     * Publish event with auto-generated metadata
     */
    public CompletableFuture<SendResult<String, Object>> publishEvent(
            String topic,
            Message event,
            String tenantId,
            String userId) {

        EventMetadata metadata = createEventMetadata(tenantId, userId);
        return publishEvent(topic, event, metadata, PartitionStrategy.TENANT_BASED);
    }

    /**
     * Publish command message
     */
    @CircuitBreaker(name = "kafka-producer")
    @Retry(name = "kafka-producer")
    public CompletableFuture<SendResult<String, Object>> publishCommand(
            String topic,
            Message command,
            String tenantId,
            String userId,
            Priority priority) {

        EventMetadata metadata = EventMetadata.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setCorrelationId(UUID.randomUUID().toString())
                .setSourceService("event-streaming-service")
                .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                .setVersion(1)
                .setTenantId(tenantId)
                .setUserId(userId)
                .setPriority(priority)
                .setRetryCount(0)
                .build();

        return publishEvent(topic, command, metadata, PartitionStrategy.PRIORITY_BASED);
    }

    /**
     * Batch publish multiple events
     */
    public void publishBatch(String topic, Map<String, Message> events) {
        events.forEach((key, event) -> {
            try {
                kafkaTemplate.send(topic, key, event);
                log.debug("Batch event sent - Topic: {}, Key: {}", topic, key);
            } catch (Exception e) {
                log.error("Failed to send batch event - Topic: {}, Key: {}", topic, key, e);
            }
        });
    }

    /**
     * Publish to Dead Letter Queue
     */
    public void publishToDeadLetterQueue(
            Message originalEvent,
            String originalTopic,
            Exception error) {

        String dlqTopic = "nnipa.dlq." + originalTopic.replace("nnipa.events.", "");

        Headers headers = new RecordHeaders();
        headers.add("original.topic", originalTopic.getBytes(StandardCharsets.UTF_8));
        headers.add("error.message", error.getMessage().getBytes(StandardCharsets.UTF_8));
        headers.add("error.timestamp", String.valueOf(System.currentTimeMillis()).getBytes());
        headers.add("error.class", error.getClass().getName().getBytes(StandardCharsets.UTF_8));

        // Add stack trace (truncated)
        String stackTrace = getStackTrace(error);
        if (stackTrace != null && stackTrace.length() > 1000) {
            stackTrace = stackTrace.substring(0, 1000);
        }
        headers.add("error.stacktrace", stackTrace.getBytes(StandardCharsets.UTF_8));

        ProducerRecord<String, Object> dlqRecord = new ProducerRecord<>(
                dlqTopic,
                null,
                UUID.randomUUID().toString(),
                originalEvent,
                headers
        );

        kafkaTemplate.send(dlqRecord).whenComplete((result, ex) -> {
            if (ex == null) {
                log.warn("Event sent to DLQ - Topic: {}, Original Topic: {}", dlqTopic, originalTopic);
                metrics.recordDlqEvent(originalTopic);
                incrementCounter("events.dlq.sent", originalTopic);
            } else {
                log.error("Failed to send event to DLQ - Topic: {}", dlqTopic, ex);
                metrics.recordError(originalTopic, "dlq_publish_failed");
            }
        });
    }

    /**
     * Get stack trace as string
     */
    private String getStackTrace(Exception e) {
        if (e == null) return null;

        StringBuilder sb = new StringBuilder();
        sb.append(e.getClass().getName()).append(": ").append(e.getMessage()).append("\n");

        StackTraceElement[] elements = e.getStackTrace();
        int limit = Math.min(elements.length, 10); // Limit to 10 frames

        for (int i = 0; i < limit; i++) {
            sb.append("\tat ").append(elements[i].toString()).append("\n");
        }

        if (elements.length > limit) {
            sb.append("\t... ").append(elements.length - limit).append(" more\n");
        }

        return sb.toString();
    }

    // ============================================
    // Helper Methods
    // ============================================

    private EventMetadata createEventMetadata(String tenantId, String userId) {
        return EventMetadata.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setCorrelationId(UUID.randomUUID().toString())
                .setSourceService("event-streaming-service")
                .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                .setVersion(1)
                .setTenantId(tenantId != null ? tenantId : "")
                .setUserId(userId != null ? userId : "")
                .setPriority(Priority.PRIORITY_MEDIUM)
                .setRetryCount(0)
                .build();
    }

    private Headers createHeaders(EventMetadata metadata) {
        Headers headers = new RecordHeaders();
        headers.add("event.id", metadata.getEventId().getBytes(StandardCharsets.UTF_8));
        headers.add("correlation.id", metadata.getCorrelationId().getBytes(StandardCharsets.UTF_8));
        headers.add("source.service", metadata.getSourceService().getBytes(StandardCharsets.UTF_8));
        headers.add("tenant.id", metadata.getTenantId().getBytes(StandardCharsets.UTF_8));
        headers.add("user.id", metadata.getUserId().getBytes(StandardCharsets.UTF_8));
        headers.add("priority", metadata.getPriority().name().getBytes(StandardCharsets.UTF_8));
        headers.add("version", String.valueOf(metadata.getVersion()).getBytes());
        return headers;
    }

    private void incrementCounter(String name, String topic) {
        Counter.builder(name)
                .tag("topic", topic)
                .register(meterRegistry)
                .increment();
    }

    /**
     * Custom exception for event publishing failures
     */
    public static class EventPublishException extends RuntimeException {
        public EventPublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}