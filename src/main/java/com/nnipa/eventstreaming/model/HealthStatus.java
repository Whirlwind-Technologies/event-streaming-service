package com.nnipa.eventstreaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map; /**
 * Health Status
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HealthStatus {
    private boolean healthy;
    private String kafkaStatus;
    private String schemaRegistryStatus;
    private long lagTotal;
    private Map<String, Long> lagByTopic;
    private Map<String, String> errors;
    private Instant checkedAt;
}
