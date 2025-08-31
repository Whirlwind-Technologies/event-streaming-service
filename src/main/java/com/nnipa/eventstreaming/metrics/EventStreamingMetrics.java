package com.nnipa.eventstreaming.metrics;

import io.micrometer.core.instrument.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Event Streaming Metrics
 * Provides comprehensive metrics for monitoring event streaming performance
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventStreamingMetrics {

    private final MeterRegistry meterRegistry;

    // Counters
    private final Map<String, Counter> eventCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> errorCounters = new ConcurrentHashMap<>();

    // Gauges
    private final AtomicLong activeConsumers = new AtomicLong(0);
    private final AtomicLong totalLag = new AtomicLong(0);

    // Timers
    private final Map<String, Timer> processingTimers = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        // Register gauges
        Gauge.builder("event.streaming.consumers.active", activeConsumers, AtomicLong::get)
                .description("Number of active consumers")
                .register(meterRegistry);

        Gauge.builder("event.streaming.lag.total", totalLag, AtomicLong::get)
                .description("Total consumer lag across all topics")
                .register(meterRegistry);

        log.info("Event streaming metrics initialized");
    }

    /**
     * Record event published
     */
    public void recordEventPublished(String topic, boolean success) {
        String key = success ? "published.success" : "published.failure";
        Counter counter = eventCounters.computeIfAbsent(
                key + "." + topic,
                k -> Counter.builder("event.streaming.events." + key)
                        .tag("topic", topic)
                        .description("Number of events " + key)
                        .register(meterRegistry)
        );
        counter.increment();
    }

    /**
     * Record event consumed
     */
    public void recordEventConsumed(String topic, String consumer, boolean success) {
        String key = success ? "consumed.success" : "consumed.failure";
        Counter counter = eventCounters.computeIfAbsent(
                key + "." + topic + "." + consumer,
                k -> Counter.builder("event.streaming.events." + key)
                        .tag("topic", topic)
                        .tag("consumer", consumer)
                        .description("Number of events " + key)
                        .register(meterRegistry)
        );
        counter.increment();
    }

    /**
     * Record event processing time
     */
    public Timer.Sample startTimer() {
        return Timer.start(meterRegistry);
    }

    public void recordProcessingTime(Timer.Sample sample, String topic, String operation) {
        Timer timer = processingTimers.computeIfAbsent(
                operation + "." + topic,
                k -> Timer.builder("event.streaming.processing.time")
                        .tag("topic", topic)
                        .tag("operation", operation)
                        .description("Event processing time")
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .register(meterRegistry)
        );
        sample.stop(timer);
    }

    /**
     * Record error
     */
    public void recordError(String topic, String errorType) {
        Counter counter = errorCounters.computeIfAbsent(
                errorType + "." + topic,
                k -> Counter.builder("event.streaming.errors")
                        .tag("topic", topic)
                        .tag("type", errorType)
                        .description("Number of errors")
                        .register(meterRegistry)
        );
        counter.increment();
    }

    /**
     * Update active consumers count
     */
    public void updateActiveConsumers(long count) {
        activeConsumers.set(count);
    }

    /**
     * Update total lag
     */
    public void updateTotalLag(long lag) {
        totalLag.set(lag);
    }

    /**
     * Record DLQ event
     */
    public void recordDlqEvent(String originalTopic) {
        Counter counter = eventCounters.computeIfAbsent(
                "dlq." + originalTopic,
                k -> Counter.builder("event.streaming.dlq.events")
                        .tag("original_topic", originalTopic)
                        .description("Number of events sent to DLQ")
                        .register(meterRegistry)
        );
        counter.increment();
    }

    /**
     * Record retry attempt
     */
    public void recordRetryAttempt(String topic, int retryCount) {
        Counter counter = eventCounters.computeIfAbsent(
                "retry." + topic + "." + retryCount,
                k -> Counter.builder("event.streaming.retries")
                        .tag("topic", topic)
                        .tag("retry_count", String.valueOf(retryCount))
                        .description("Number of retry attempts")
                        .register(meterRegistry)
        );
        counter.increment();
    }

    /**
     * Record schema registry operation
     */
    public void recordSchemaOperation(String operation, boolean success) {
        Counter counter = eventCounters.computeIfAbsent(
                "schema." + operation + "." + success,
                k -> Counter.builder("event.streaming.schema.operations")
                        .tag("operation", operation)
                        .tag("success", String.valueOf(success))
                        .description("Schema registry operations")
                        .register(meterRegistry)
        );
        counter.increment();
    }

    /**
     * Record topic operation
     */
    public void recordTopicOperation(String operation, boolean success) {
        Counter counter = eventCounters.computeIfAbsent(
                "topic." + operation + "." + success,
                k -> Counter.builder("event.streaming.topic.operations")
                        .tag("operation", operation)
                        .tag("success", String.valueOf(success))
                        .description("Topic management operations")
                        .register(meterRegistry)
        );
        counter.increment();
    }

    /**
     * Get metrics summary
     */
    public Map<String, Object> getMetricsSummary() {
        Map<String, Object> summary = new HashMap<>();

        // Add counter summaries
        long totalPublished = eventCounters.entrySet().stream()
                .filter(e -> e.getKey().startsWith("published.success"))
                .mapToLong(e -> (long) e.getValue().count())
                .sum();

        long totalConsumed = eventCounters.entrySet().stream()
                .filter(e -> e.getKey().startsWith("consumed.success"))
                .mapToLong(e -> (long) e.getValue().count())
                .sum();

        long totalErrors = errorCounters.values().stream()
                .mapToLong(c -> (long) c.count())
                .sum();

        long totalDlq = eventCounters.entrySet().stream()
                .filter(e -> e.getKey().startsWith("dlq"))
                .mapToLong(e -> (long) e.getValue().count())
                .sum();

        summary.put("totalEventsPublished", totalPublished);
        summary.put("totalEventsConsumed", totalConsumed);
        summary.put("totalErrors", totalErrors);
        summary.put("totalDlqEvents", totalDlq);
        summary.put("activeConsumers", activeConsumers.get());
        summary.put("totalLag", totalLag.get());

        return summary;
    }
}