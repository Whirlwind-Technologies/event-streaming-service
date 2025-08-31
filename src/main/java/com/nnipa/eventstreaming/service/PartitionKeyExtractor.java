package com.nnipa.eventstreaming.service;

import com.google.protobuf.Message;
import com.nnipa.eventstreaming.enums.PartitionStrategy;
import com.nnipa.proto.common.EventMetadata;
import com.nnipa.proto.common.Priority;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Partition Key Extractor
 * Determines the partition key based on the configured strategy
 * This ensures proper message routing and ordering guarantees
 */
@Slf4j
@Component
public class PartitionKeyExtractor {

    /**
     * Extract partition key based on strategy
     */
    public String extractKey(Message event, EventMetadata metadata, PartitionStrategy strategy) {
        String key;

        switch (strategy) {
            case TENANT_BASED:
                // Route all events for a tenant to the same partition
                key = metadata.getTenantId();
                if (key == null || key.isEmpty()) {
                    key = "default-tenant";
                }
                break;

            case USER_BASED:
                // Route all events for a user to the same partition
                key = metadata.getUserId();
                if (key == null || key.isEmpty()) {
                    key = metadata.getTenantId(); // Fallback to tenant
                }
                break;

            case CORRELATION_BASED:
                // Route related events to the same partition
                key = metadata.getCorrelationId();
                if (key == null || key.isEmpty()) {
                    key = metadata.getEventId();
                }
                break;

            case PRIORITY_BASED:
                // Route by priority level for priority queues
                Priority priority = metadata.getPriority();
                key = priority != null ? priority.name() : Priority.PRIORITY_MEDIUM.name();
                break;

            case EVENT_TYPE_BASED:
                // Route by event type (uses event class name)
                key = event.getClass().getSimpleName();
                break;

            case COMPOSITE:
                // Combine tenant and user for fine-grained partitioning
                String tenant = metadata.getTenantId();
                String user = metadata.getUserId();
                key = String.format("%s:%s",
                        tenant != null ? tenant : "default",
                        user != null ? user : "system");
                break;

            case ROUND_ROBIN:
                // Random key for round-robin distribution
                key = UUID.randomUUID().toString();
                break;

            case CUSTOM:
                // Custom logic based on event content
                key = extractCustomKey(event, metadata);
                break;

            default:
                // Default to tenant-based partitioning
                key = metadata.getTenantId();
                if (key == null || key.isEmpty()) {
                    key = UUID.randomUUID().toString();
                }
        }

        log.debug("Extracted partition key: {} using strategy: {}", key, strategy);
        return key;
    }

    /**
     * Custom key extraction logic
     * Override this method for domain-specific partitioning
     */
    private String extractCustomKey(Message event, EventMetadata metadata) {
        // Example: Extract key from specific event fields
        String eventType = event.getClass().getSimpleName();

        // Custom logic based on event type
        if (eventType.contains("Tenant")) {
            // For tenant events, use tenant ID
            return metadata.getTenantId();
        } else if (eventType.contains("User") || eventType.contains("Auth")) {
            // For user/auth events, combine tenant and user
            return String.format("%s:%s", metadata.getTenantId(), metadata.getUserId());
        } else if (eventType.contains("Notification")) {
            // For notifications, use user ID for user-specific ordering
            return metadata.getUserId();
        } else {
            // Default to correlation ID for related events
            return metadata.getCorrelationId();
        }
    }

    /**
     * Calculate partition number for explicit partition assignment
     * Used when you need to control exact partition placement
     */
    public int calculatePartition(String key, int numPartitions) {
        if (key == null || key.isEmpty()) {
            return 0;
        }

        // Use consistent hashing for stable partition assignment
        int hash = Math.abs(key.hashCode());
        int partition = hash % numPartitions;

        log.debug("Calculated partition {} for key {} (total partitions: {})",
                partition, key, numPartitions);

        return partition;
    }

    /**
     * Validate if a key would result in balanced partitioning
     */
    public boolean isBalancedKey(String key) {
        if (key == null || key.isEmpty()) {
            return false;
        }

        // Check if key has enough entropy for good distribution
        // Simple check: key should have reasonable length and variety
        return key.length() >= 8 && !key.matches("^[0-9]+$");
    }
}