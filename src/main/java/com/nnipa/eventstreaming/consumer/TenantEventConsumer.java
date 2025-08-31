package com.nnipa.eventstreaming.consumer;

import com.nnipa.eventstreaming.service.BaseEventConsumer;
import com.nnipa.eventstreaming.service.EventPublisher;
import com.nnipa.proto.tenant.TenantCreatedEvent;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Example Consumer for Tenant Events
 * Demonstrates how to extend BaseEventConsumer for specific event types
 */
@Slf4j
@Component
public class TenantEventConsumer extends BaseEventConsumer<TenantCreatedEvent> {

    public TenantEventConsumer(
            MeterRegistry meterRegistry,
            EventPublisher eventPublisher,
            RedisTemplate<String, String> redisTemplate) {
        super(meterRegistry, eventPublisher, redisTemplate);
    }

    @Override
    @KafkaListener(
            topics = "nnipa.events.tenant.created",
            groupId = "event-streaming-service",
            containerFactory = "kafkaListenerContainerFactory",
            autoStartup = "${event-streaming.consumers.auto-start:false}"  // Make it optional
    )
    public void onMessage(org.apache.kafka.clients.consumer.ConsumerRecord<String, TenantCreatedEvent> record,
                          org.springframework.kafka.support.Acknowledgment acknowledgment) {
        super.onMessage(record, acknowledgment);
    }

    @Override
    protected void processEvent(TenantCreatedEvent event, Map<String, String> headers) throws Exception {
        log.info("Processing tenant created event for tenant: {}",
                event.getTenant().getTenantId());

        // Extract tenant information
        String tenantId = event.getTenant().getTenantId();
        String tenantName = event.getTenant().getName();
        String organizationType = event.getTenant().getOrganizationType().name();

        // Process the event (example: update cache, trigger workflows, etc.)
        updateTenantCache(tenantId, tenantName, organizationType);

        // Trigger any downstream processes
        triggerTenantProvisioning(tenantId);

        log.info("Successfully processed tenant created event for tenant: {}", tenantId);
    }

    @Override
    protected boolean validateEventSpecific(TenantCreatedEvent event, Map<String, String> headers) {
        // Add specific validation for tenant events
        if (event.getTenant() == null) {
            log.error("Tenant data is missing in the event");
            return false;
        }

        if (event.getTenant().getTenantId() == null || event.getTenant().getTenantId().isEmpty()) {
            log.error("Tenant ID is missing");
            return false;
        }

        return true;
    }

    private void updateTenantCache(String tenantId, String tenantName, String organizationType) {
        // Update Redis cache with tenant information
        try {
            String cacheKey = "tenant:" + tenantId;
            Map<String, String> tenantData = Map.of(
                    "name", tenantName,
                    "type", organizationType,
                    "processed_at", String.valueOf(System.currentTimeMillis())
            );

            // Store in Redis (example)
            log.debug("Updating tenant cache for: {}", tenantId);

        } catch (Exception e) {
            log.error("Failed to update tenant cache", e);
            throw new RuntimeException("Cache update failed", e);
        }
    }

    private void triggerTenantProvisioning(String tenantId) {
        // Trigger downstream provisioning processes
        log.debug("Triggering provisioning for tenant: {}", tenantId);
        // Implementation would depend on your provisioning service
    }
}