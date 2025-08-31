package com.nnipa.eventstreaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map; /**
 * Event Statistics
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventStatistics {
    private String topic;
    private long totalEvents;
    private long successfulEvents;
    private long failedEvents;
    private long dlqEvents;
    private double averageProcessingTimeMs;
    private Instant lastEventTime;
    private Map<String, Long> eventsByType;
}
