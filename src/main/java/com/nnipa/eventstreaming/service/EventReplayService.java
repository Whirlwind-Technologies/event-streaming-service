package com.nnipa.eventstreaming.service;

import com.google.protobuf.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Event Replay Service
 * Provides capability to replay events from specific points in time
 * or offset ranges for recovery, debugging, or reprocessing
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventReplayService {

    private final ConsumerFactory<String, Object> consumerFactory;
    private final EventPublisher eventPublisher;
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

    @Value("${event-streaming.replay.batch-size:100}")
    private int batchSize;

    @Value("${event-streaming.replay.max-replay-duration:7d}")
    private String maxReplayDuration;

    /**
     * Replay events from a specific timestamp
     */
    public CompletableFuture<ReplayResult> replayFromTimestamp(
            String topic,
            long fromTimestamp,
            long toTimestamp,
            String targetTopic,
            ReplayFilter filter) {

        return CompletableFuture.supplyAsync(() -> {
            log.info("Starting replay - Topic: {}, From: {}, To: {}, Target: {}",
                    topic, Instant.ofEpochMilli(fromTimestamp),
                    Instant.ofEpochMilli(toTimestamp), targetTopic);

            KafkaConsumer<String, Object> consumer = createReplayConsumer();
            ReplayResult result = new ReplayResult();

            try {
                // Get all partitions for the topic
                List<PartitionInfo> partitions = consumer.partitionsFor(topic);
                List<TopicPartition> topicPartitions = partitions.stream()
                        .map(p -> new TopicPartition(topic, p.partition()))
                        .collect(Collectors.toList());

                // Assign partitions
                consumer.assign(topicPartitions);

                // Seek to timestamp for each partition
                Map<TopicPartition, Long> timestampMap = topicPartitions.stream()
                        .collect(Collectors.toMap(tp -> tp, tp -> fromTimestamp));

                Map<TopicPartition, OffsetAndTimestamp> offsetMap =
                        consumer.offsetsForTimes(timestampMap);

                // Seek to the found offsets
                offsetMap.forEach((partition, offsetAndTimestamp) -> {
                    if (offsetAndTimestamp != null) {
                        consumer.seek(partition, offsetAndTimestamp.offset());
                        log.debug("Seeking partition {} to offset {}",
                                partition.partition(), offsetAndTimestamp.offset());
                    } else {
                        consumer.seekToBeginning(Collections.singletonList(partition));
                        log.debug("No offset found for timestamp, seeking to beginning for partition {}",
                                partition.partition());
                    }
                });

                // Process events
                boolean continueReplay = true;
                while (continueReplay) {
                    ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(5));

                    if (records.isEmpty()) {
                        log.debug("No more records to replay");
                        break;
                    }

                    for (ConsumerRecord<String, Object> record : records) {
                        // Check if we've passed the end timestamp
                        if (record.timestamp() > toTimestamp) {
                            continueReplay = false;
                            break;
                        }

                        // Apply filter if provided
                        if (filter != null && !filter.shouldReplay(record)) {
                            result.skippedCount++;
                            continue;
                        }

                        // Replay the event
                        try {
                            replayEvent(record, targetTopic);
                            result.replayedCount++;
                        } catch (Exception e) {
                            log.error("Failed to replay event - Offset: {}, Partition: {}",
                                    record.offset(), record.partition(), e);
                            result.failedCount++;
                            result.errors.add(String.format("Failed at offset %d: %s",
                                    record.offset(), e.getMessage()));
                        }

                        // Update progress
                        if (result.replayedCount % batchSize == 0) {
                            log.info("Replay progress - Replayed: {}, Skipped: {}, Failed: {}",
                                    result.replayedCount, result.skippedCount, result.failedCount);
                        }
                    }
                }

                result.success = true;
                log.info("Replay completed - Replayed: {}, Skipped: {}, Failed: {}",
                        result.replayedCount, result.skippedCount, result.failedCount);

            } catch (Exception e) {
                log.error("Replay failed", e);
                result.success = false;
                result.errors.add("Replay failed: " + e.getMessage());
            } finally {
                consumer.close();
            }

            return result;
        }, executorService);
    }

    /**
     * Replay events from specific offset range
     */
    public CompletableFuture<ReplayResult> replayFromOffset(
            String topic,
            int partition,
            long fromOffset,
            long toOffset,
            String targetTopic) {

        return CompletableFuture.supplyAsync(() -> {
            log.info("Starting offset replay - Topic: {}, Partition: {}, From: {}, To: {}",
                    topic, partition, fromOffset, toOffset);

            KafkaConsumer<String, Object> consumer = createReplayConsumer();
            ReplayResult result = new ReplayResult();

            try {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                consumer.assign(Collections.singletonList(topicPartition));
                consumer.seek(topicPartition, fromOffset);

                boolean continueReplay = true;
                while (continueReplay) {
                    ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(5));

                    if (records.isEmpty()) {
                        break;
                    }

                    for (ConsumerRecord<String, Object> record : records) {
                        if (record.offset() > toOffset) {
                            continueReplay = false;
                            break;
                        }

                        try {
                            replayEvent(record, targetTopic);
                            result.replayedCount++;
                        } catch (Exception e) {
                            log.error("Failed to replay event at offset {}", record.offset(), e);
                            result.failedCount++;
                        }
                    }
                }

                result.success = true;

            } catch (Exception e) {
                log.error("Offset replay failed", e);
                result.success = false;
                result.errors.add("Replay failed: " + e.getMessage());
            } finally {
                consumer.close();
            }

            return result;
        }, executorService);
    }

    /**
     * Get the earliest and latest offsets for a topic
     */
    public OffsetRange getOffsetRange(String topic, int partition) {
        try (KafkaConsumer<String, Object> consumer = createReplayConsumer()) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.assign(Collections.singletonList(topicPartition));

            Map<TopicPartition, Long> beginningOffsets =
                    consumer.beginningOffsets(Collections.singletonList(topicPartition));
            Map<TopicPartition, Long> endOffsets =
                    consumer.endOffsets(Collections.singletonList(topicPartition));

            return new OffsetRange(
                    beginningOffsets.get(topicPartition),
                    endOffsets.get(topicPartition)
            );
        }
    }

    /**
     * Create a consumer for replay operations
     */
    private KafkaConsumer<String, Object> createReplayConsumer() {
        Map<String, Object> props = new HashMap<>(consumerFactory.getConfigurationProperties());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "replay-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchSize);

        return new KafkaConsumer<>(props);
    }

    /**
     * Replay a single event
     */
    private void replayEvent(ConsumerRecord<String, Object> record, String targetTopic) {
        String actualTarget = targetTopic != null ? targetTopic : record.topic() + ".replay";

        // Preserve original headers and add replay metadata
        var headers = record.headers();
        headers.add("replay.original.topic", record.topic().getBytes());
        headers.add("replay.original.offset", String.valueOf(record.offset()).getBytes());
        headers.add("replay.original.partition", String.valueOf(record.partition()).getBytes());
        headers.add("replay.timestamp", String.valueOf(System.currentTimeMillis()).getBytes());

        eventPublisher.publishEvent(
                actualTarget,
                (Message) record.value(),
                record.key(),
                null
        );
    }

    /**
     * Result of replay operation
     */
    public static class ReplayResult {
        public boolean success;
        public long replayedCount;
        public long skippedCount;
        public long failedCount;
        public List<String> errors = new ArrayList<>();
    }

    /**
     * Filter interface for selective replay
     */
    @FunctionalInterface
    public interface ReplayFilter {
        boolean shouldReplay(ConsumerRecord<String, Object> record);
    }

    /**
     * Offset range information
     */
    public static class OffsetRange {
        public final long earliestOffset;
        public final long latestOffset;

        public OffsetRange(long earliestOffset, long latestOffset) {
            this.earliestOffset = earliestOffset;
            this.latestOffset = latestOffset;
        }

        public long totalMessages() {
            return latestOffset - earliestOffset;
        }
    }
}