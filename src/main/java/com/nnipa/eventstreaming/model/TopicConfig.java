package com.nnipa.eventstreaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map; /**
 * Topic Configuration
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicConfig {
    private String name;
    private int partitions;
    private short replicationFactor;
    private long retentionMs;
    private String compressionType;
    private int minInSyncReplicas;
    private Map<String, String> additionalConfigs;
}
