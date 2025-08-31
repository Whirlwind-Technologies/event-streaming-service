package com.nnipa.eventstreaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * Event Streaming Model Classes
 */

/**
 * Event Envelope for wrapping events with metadata
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventEnvelope {
    private String eventId;
    private String eventType;
    private String source;
    private String tenantId;
    private String userId;
    private Instant timestamp;
    private Object payload;
    private Map<String, String> headers;
    private int version;
}