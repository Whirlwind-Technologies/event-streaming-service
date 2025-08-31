package com.nnipa.eventstreaming.enums;

/**
 * Partition Strategy Enum
 */
public enum PartitionStrategy {
    TENANT_BASED,       // Partition by tenant ID
    USER_BASED,         // Partition by user ID
    CORRELATION_BASED,  // Partition by correlation ID
    PRIORITY_BASED,     // Partition by priority
    EVENT_TYPE_BASED,   // Partition by event type
    COMPOSITE,          // Combination of tenant and user
    ROUND_ROBIN,        // Round-robin distribution
    CUSTOM              // Custom partitioning logic
}