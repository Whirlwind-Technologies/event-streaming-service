package com.nnipa.eventstreaming.service;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.util.Timestamps;
import com.nnipa.proto.common.EventMetadata;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Base Event Consumer
 * Abstract base class for all event consumers providing common functionality
 * like error handling, metrics, and acknowledgment
 */
@Slf4j
@RequiredArgsConstructor
public abstract class BaseEventConsumer<T extends Message>
        implements AcknowledgingMessageListener<String, T> {

    protected final MeterRegistry meterRegistry;
    protected final EventPublisher eventPublisher;
    protected final RedisTemplate<String, String> redisTemplate;

    @Value("${event-streaming.dlq.max-retries:3}")
    private int maxRetries;

    @Value("${event-streaming.consumer.idempotency.ttl:3600}")
    private long idempotencyTtlSeconds;

    @Value("${event-streaming.consumer.idempotency.enabled:true}")
    private boolean idempotencyEnabled;

    /**
     * Process incoming message with acknowledgment
     */
    @Override
    public void onMessage(ConsumerRecord<String, T> record, Acknowledgment acknowledgment) {
        String topic = record.topic();
        String key = record.key();
        T event = record.value();
        Headers headers = record.headers();

        log.debug("Received event - Topic: {}, Key: {}, Partition: {}, Offset: {}",
                topic, key, record.partition(), record.offset());

        try {
            // Extract metadata from headers
            Map<String, String> headerMap = extractHeaders(headers);

            // Extract EventMetadata from the protobuf message
            EventMetadata eventMetadata = extractEventMetadata(event);

            // Validate event
            if (!validateEvent(event, headerMap, eventMetadata)) {
                log.warn("Event validation failed - Topic: {}, Key: {}", topic, key);
                handleInvalidEvent(event, topic, "Validation failed");
                acknowledgment.acknowledge();
                return;
            }

            // Check for duplicate processing (idempotency)
            String eventId = eventMetadata != null ? eventMetadata.getEventId() : headerMap.get("event.id");
            if (idempotencyEnabled && isDuplicateEvent(eventId)) {
                log.info("Duplicate event detected, skipping - EventId: {}", eventId);
                acknowledgment.acknowledge();
                incrementCounter("events.consumed.duplicate", topic);
                return;
            }

            // Process the event
            processEvent(event, headerMap);

            // Mark as processed for idempotency
            if (idempotencyEnabled && eventId != null) {
                markEventAsProcessed(eventId);
            }

            // Acknowledge the message
            acknowledgment.acknowledge();

            // Update metrics
            incrementCounter("events.consumed.success", topic);

        } catch (Exception e) {
            log.error("Error processing event - Topic: {}, Key: {}", topic, key, e);
            handleProcessingError(event, topic, e, acknowledgment, headers);
            incrementCounter("events.consumed.error", topic);
        }
    }

    /**
     * Abstract method to be implemented by concrete consumers
     */
    protected abstract void processEvent(T event, Map<String, String> headers) throws Exception;

    /**
     * Extract EventMetadata from protobuf message using reflection
     */
    protected EventMetadata extractEventMetadata(T event) {
        try {
            // Use reflection to find the metadata field
            Descriptors.Descriptor descriptor = event.getDescriptorForType();
            Descriptors.FieldDescriptor metadataField = descriptor.findFieldByName("metadata");

            if (metadataField != null) {
                Object metadataObj = event.getField(metadataField);
                if (metadataObj instanceof EventMetadata) {
                    return (EventMetadata) metadataObj;
                }
            }
        } catch (Exception e) {
            log.debug("Could not extract EventMetadata from event: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Validate the event before processing
     */
    protected boolean validateEvent(T event, Map<String, String> headers, EventMetadata metadata) {
        if (event == null) {
            log.error("Received null event");
            return false;
        }

        // Check for event ID (either in metadata or headers)
        String eventId = metadata != null ? metadata.getEventId() : headers.get("event.id");
        if (eventId == null || eventId.isEmpty()) {
            log.error("Event missing event.id");
            return false;
        }

        // Check if event is too old (optional)
        if (metadata != null && metadata.hasTimestamp()) {
            long eventTime = Timestamps.toMillis(metadata.getTimestamp());
            long age = System.currentTimeMillis() - eventTime;
            long maxAge = Duration.ofDays(7).toMillis(); // 7 days max age

            if (age > maxAge) {
                log.warn("Event is too old - EventId: {}, Age: {} ms", eventId, age);
                // You might want to handle old events differently
            }
        }

        // Additional validation can be added by subclasses
        return validateEventSpecific(event, headers);
    }

    /**
     * Hook for subclasses to add specific validation
     */
    protected boolean validateEventSpecific(T event, Map<String, String> headers) {
        return true;
    }

    /**
     * Check if event has already been processed (for idempotency)
     */
    protected boolean isDuplicateEvent(String eventId) {
        if (eventId == null || !idempotencyEnabled) {
            return false;
        }

        String key = getIdempotencyKey(eventId);
        Boolean exists = redisTemplate.hasKey(key);
        return Boolean.TRUE.equals(exists);
    }

    /**
     * Mark event as processed (for idempotency)
     */
    protected void markEventAsProcessed(String eventId) {
        if (eventId == null || !idempotencyEnabled) {
            return;
        }

        String key = getIdempotencyKey(eventId);
        redisTemplate.opsForValue().set(key, "processed", idempotencyTtlSeconds, TimeUnit.SECONDS);
        log.debug("Marked event as processed - EventId: {}, TTL: {}s", eventId, idempotencyTtlSeconds);
    }

    /**
     * Get idempotency key for Redis
     */
    private String getIdempotencyKey(String eventId) {
        return String.format("event:processed:%s:%s",
                this.getClass().getSimpleName(), eventId);
    }

    /**
     * Handle processing errors with retry logic
     */
    protected void handleProcessingError(T event, String topic, Exception error,
                                         Acknowledgment acknowledgment, Headers headers) {
        // Get retry count from headers or event metadata
        int retryCount = getRetryCount(event, headers);

        if (retryCount < maxRetries) {
            // Retry the event by not acknowledging (will be redelivered)
            log.warn("Retrying event - Topic: {}, Retry: {}/{}, Error: {}",
                    topic, retryCount + 1, maxRetries, error.getMessage());

            // Add retry count to headers for next attempt
            updateRetryCount(headers, retryCount + 1);

            // Optionally, you could republish to a retry topic with delay
            publishToRetryTopic(event, topic, retryCount + 1);

            // Acknowledge to move forward (since we're using retry topic)
            acknowledgment.acknowledge();
        } else {
            // Max retries exceeded, send to DLQ
            log.error("Max retries exceeded, sending to DLQ - Topic: {}, Retries: {}",
                    topic, retryCount);
            eventPublisher.publishToDeadLetterQueue(event, topic, error);
            acknowledgment.acknowledge(); // Acknowledge to move forward
        }
    }

    /**
     * Publish to retry topic with exponential backoff
     */
    private void publishToRetryTopic(T event, String originalTopic, int retryCount) {
        try {
            // Calculate delay with exponential backoff
            long delayMs = calculateBackoffDelay(retryCount);

            // Create retry topic name
            String retryTopic = originalTopic + ".retry." + retryCount;

            // Add delay header for delayed processing
            Map<String, String> headers = new HashMap<>();
            headers.put("retry.count", String.valueOf(retryCount));
            headers.put("retry.delay.ms", String.valueOf(delayMs));
            headers.put("original.topic", originalTopic);

            // Schedule republishing after delay (in production, use a delayed queue)
            log.info("Scheduling retry for event - Topic: {}, Delay: {}ms", retryTopic, delayMs);

            // For now, publish immediately (in production, use Kafka delayed messages or scheduled executor)
            // eventPublisher.publishEvent(retryTopic, event, extractEventMetadata(event), PartitionStrategy.TENANT_BASED);

        } catch (Exception e) {
            log.error("Failed to publish to retry topic", e);
        }
    }

    /**
     * Calculate exponential backoff delay
     */
    private long calculateBackoffDelay(int retryCount) {
        // Exponential backoff: 1s, 2s, 4s, 8s, etc.
        long baseDelay = 1000; // 1 second
        long maxDelay = 60000; // 60 seconds max
        long delay = Math.min(baseDelay * (long) Math.pow(2, retryCount - 1), maxDelay);

        // Add jitter to prevent thundering herd
        long jitter = (long) (Math.random() * delay * 0.1);
        return delay + jitter;
    }

    /**
     * Handle invalid events
     */
    protected void handleInvalidEvent(T event, String topic, String reason) {
        log.error("Invalid event received - Topic: {}, Reason: {}", topic, reason);
        eventPublisher.publishToDeadLetterQueue(event, topic,
                new IllegalArgumentException(reason));
    }

    /**
     * Extract headers to map
     */
    protected Map<String, String> extractHeaders(Headers headers) {
        Map<String, String> headerMap = new HashMap<>();
        for (Header header : headers) {
            String key = header.key();
            String value = new String(header.value(), StandardCharsets.UTF_8);
            headerMap.put(key, value);
        }
        return headerMap;
    }

    /**
     * Get retry count from event metadata or headers
     */
    protected int getRetryCount(T event, Headers kafkaHeaders) {
        // First, try to get from EventMetadata
        EventMetadata metadata = extractEventMetadata(event);
        if (metadata != null && metadata.getRetryCount() > 0) {
            return metadata.getRetryCount();
        }

        // Then, try to get from Kafka headers
        Header retryHeader = kafkaHeaders.lastHeader("retry.count");
        if (retryHeader != null) {
            try {
                String retryStr = new String(retryHeader.value(), StandardCharsets.UTF_8);
                return Integer.parseInt(retryStr);
            } catch (NumberFormatException e) {
                log.debug("Invalid retry count in header: {}", e.getMessage());
            }
        }

        // Default to 0
        return 0;
    }

    /**
     * Deprecated - use getRetryCount(T event, Headers headers) instead
     */
    @Deprecated
    protected int getRetryCount(T event) {
        EventMetadata metadata = extractEventMetadata(event);
        return metadata != null ? metadata.getRetryCount() : 0;
    }

    /**
     * Update retry count in headers
     */
    private void updateRetryCount(Headers headers, int newRetryCount) {
        // Remove old retry count header if exists
        headers.remove("retry.count");

        // Add new retry count
        headers.add("retry.count", String.valueOf(newRetryCount).getBytes(StandardCharsets.UTF_8));
        headers.add("retry.timestamp", String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Get max retries configuration
     */
    protected int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Update metrics counter
     */
    protected void incrementCounter(String name, String topic) {
        Counter.builder(name)
                .tag("topic", topic)
                .tag("consumer", this.getClass().getSimpleName())
                .register(meterRegistry)
                .increment();
    }

    /**
     * Get consumer group ID for this consumer
     */
    protected String getConsumerGroupId() {
        return this.getClass().getSimpleName().toLowerCase().replace("consumer", "-group");
    }

    /**
     * Handle consumer lifecycle events
     */
    public void onPartitionsAssigned(Map<String, Integer> assignments) {
        log.info("Partitions assigned to consumer: {}", assignments);
    }

    public void onPartitionsRevoked(Map<String, Integer> revoked) {
        log.info("Partitions revoked from consumer: {}", revoked);
    }

    /**
     * Graceful shutdown hook
     */
    public void shutdown() {
        log.info("Shutting down consumer: {}", this.getClass().getSimpleName());
        // Perform any cleanup needed
    }
}