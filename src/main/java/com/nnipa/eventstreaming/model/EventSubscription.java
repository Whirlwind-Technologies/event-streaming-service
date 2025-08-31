package com.nnipa.eventstreaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant; /**
 * Event Subscription
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventSubscription {
    private String subscriptionId;
    private String subscriberService;
    private String topic;
    private String eventType;
    private String filter;
    private boolean active;
    private Instant createdAt;
    private ConsumerGroupConfig consumerConfig;
}
