package com.nnipa.eventstreaming;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Event Streaming Service Application
 *
 * This service provides:
 * - Event publishing and subscription with Kafka
 * - Message queuing and routing with partitioning strategies
 * - Event replay and recovery capabilities
 * - Cross-service communication through events and commands
 * - Schema Registry integration with Protobuf
 * - Dead letter queue handling
 * - Consumer group management
 *
 * Integration Points:
 * - Tenant Management Service: Publishes tenant lifecycle events
 * - Auth Service: Publishes authentication events
 * - Notification Service: Consumes notification commands
 * - All Services: Subscribe to relevant events for their domain
 */
@Slf4j
@SpringBootApplication
@EnableKafka
@EnableCaching
@EnableAsync
@EnableScheduling
@ConfigurationPropertiesScan("com.nnipa.eventstreaming.config")
public class EventStreamingServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(EventStreamingServiceApplication.class, args);

		log.info("===========================================");
		log.info("NNIPA Event Streaming Service Started");
		log.info("===========================================");
		log.info("Core Capabilities:");
		log.info("- Event Publishing with Protobuf Serialization");
		log.info("- Event Subscription with Consumer Groups");
		log.info("- Message Routing with Partition Strategies");
		log.info("- Event Replay and Recovery");
		log.info("- Schema Registry Integration");
		log.info("- Dead Letter Queue Processing");
		log.info("===========================================");
		log.info("Supported Event Domains:");
		log.info("- Tenant Management Events");
		log.info("- Authentication Events");
		log.info("- Authorization Events");
		log.info("- Notification Events");
		log.info("- Audit Events");
		log.info("- Usage Tracking Events");
		log.info("===========================================");
		log.info("Schema Management:");
		log.info("- Protobuf Schema Serialization");
		log.info("- Schema Evolution Support");
		log.info("- Backward/Forward Compatibility");
		log.info("- Schema Versioning");
		log.info("===========================================");
		log.info("Service Integration:");
		log.info("- All services communicate through events");
		log.info("- No direct service-to-service calls");
		log.info("- Eventual consistency model");
		log.info("- Saga pattern for distributed transactions");
		log.info("===========================================");
	}
}