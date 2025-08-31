package com.nnipa.eventstreaming.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Error Handling Configuration for Kafka Consumers
 */
@Slf4j
@Configuration
public class ErrorHandlingConfig {

    /**
     * Configure default error handler with retry and DLQ
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // Configure exponential backoff
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(1000L); // 1 second
        backOff.setMultiplier(2.0); // Double each time
        backOff.setMaxInterval(30000L); // Max 30 seconds
        backOff.setMaxElapsedTime(180000L); // Give up after 3 minutes

        // Create DLQ recoverer
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> {
                    log.error("Sending to DLQ - Topic: {}, Partition: {}, Offset: {}",
                            record.topic(), record.partition(), record.offset(), exception);
                    // Route to DLQ topic
                    return new org.apache.kafka.common.TopicPartition(
                            "nnipa.dlq." + record.topic().replace("nnipa.events.", ""),
                            record.partition()
                    );
                }
        );

        // Create error handler
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Add retry listener
        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("Failed delivery attempt {} for record - Topic: {}, Partition: {}, Offset: {}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(), ex);
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.info("Successfully recovered record - Topic: {}, Partition: {}, Offset: {}",
                        record.topic(), record.partition(), record.offset());
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                log.error("Recovery failed for record - Topic: {}, Partition: {}, Offset: {}",
                        record.topic(), record.partition(), record.offset(), failure);
            }
        });

        // Configure which exceptions are retryable
        errorHandler.addRetryableExceptions(
                org.apache.kafka.common.errors.NetworkException.class,
                org.apache.kafka.common.errors.TimeoutException.class,
                org.springframework.kafka.listener.ListenerExecutionFailedException.class
        );

        // Configure which exceptions should not be retried
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                IllegalStateException.class,
                org.springframework.kafka.support.serializer.DeserializationException.class
        );

        return errorHandler;
    }

    /**
     * Consumer record recoverer for custom error handling
     */
    @Bean
    public ConsumerRecordRecoverer consumerRecordRecoverer() {
        return (record, exception) -> {
            log.error("Error processing record - Topic: {}, Partition: {}, Offset: {}",
                    record.topic(), record.partition(), record.offset(), exception);

            // Custom recovery logic
            if (exception.getCause() instanceof IllegalArgumentException) {
                // Skip invalid records
                log.warn("Skipping invalid record");
            } else {
                // Log and continue
                log.error("Unrecoverable error, skipping record", exception);
            }
        };
    }

    /**
     * Configure consumer error logging
     */
    @Bean
    public org.springframework.kafka.listener.KafkaListenerErrorHandler kafkaListenerErrorHandler() {
        return (message, exception) -> {
            log.error("Error in Kafka listener", exception);
            log.error("Message details: {}", message);

            // Return null to use default error handling
            // Or return a custom response
            return null;
        };
    }
}