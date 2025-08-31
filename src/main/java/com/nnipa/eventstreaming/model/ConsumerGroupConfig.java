package com.nnipa.eventstreaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Consumer Group Configuration
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerGroupConfig {
    private String groupId;
    private String topic;
    private int consumerCount;
    private boolean autoCommit;
    private String offsetReset;
    private int maxPollRecords;
    private long sessionTimeoutMs;
    private long heartbeatIntervalMs;
}

