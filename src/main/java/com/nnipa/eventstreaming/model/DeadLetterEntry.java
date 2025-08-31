package com.nnipa.eventstreaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map; /**
 * Dead Letter Queue Entry
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeadLetterEntry {
    private String originalTopic;
    private String eventId;
    private Object payload;
    private String errorMessage;
    private Instant failedAt;
    private int retryCount;
    private Map<String, String> headers;
}
