package com.nnipa.eventstreaming.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nnipa.eventstreaming.model.SchemaInfo;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Schema Registry Service
 * Manages Protobuf schemas in Confluent Schema Registry
 */
@Slf4j
@Service
public class SchemaRegistryService {

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${schema.registry.cache-size:1000}")
    private int cacheSize;

    @Value("${schema.registry.compatibility:FULL_TRANSITIVE}")
    private String defaultCompatibility;

    private SchemaRegistryClient schemaRegistryClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        this.schemaRegistryClient = new CachedSchemaRegistryClient(
                schemaRegistryUrl,
                cacheSize,
                Collections.singletonMap("schema.registry.url", schemaRegistryUrl)
        );
        log.info("Schema Registry Service initialized with URL: {}", schemaRegistryUrl);
    }

    /**
     * Register a new schema
     */
    @CircuitBreaker(name = "schema-registry")
    @Retry(name = "schema-registry")
    public int registerSchema(String subject, String schema) {
        try {
            log.info("Registering schema for subject: {}", subject);

            // Parse as Protobuf schema
            ProtobufSchema protobufSchema = new ProtobufSchema(schema);

            // Register the schema
            int schemaId = schemaRegistryClient.register(subject, protobufSchema);

            log.info("Schema registered successfully - Subject: {}, ID: {}", subject, schemaId);
            return schemaId;

        } catch (RestClientException | IOException e) {
            log.error("Failed to register schema for subject: {}", subject, e);
            throw new SchemaRegistryException("Failed to register schema", e);
        }
    }

    /**
     * Get schema by subject and version
     */
    @Cacheable(value = "schemas", key = "#subject + '-' + #version")
    @CircuitBreaker(name = "schema-registry")
    @Retry(name = "schema-registry")
    public SchemaInfo getSchema(String subject, Integer version) {
        log.debug("Fetching schema - Subject: {}, Version: {}", subject, version);

        // If no version specified, get latest
        if (version == null) {
            version = getLatestVersion(subject);
        }

        // Get schema metadata
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema schema =
                schemaRegistryClient.getByVersion(subject, version, false);

        // Build SchemaInfo
        return SchemaInfo.builder()
                .subject(subject)
                .version(version)
                .id(schema.getId())
                .schema(schema.getSchema())
                .schemaType(schema.getSchemaType())
                .compatibility(getCompatibility(subject))
                .build();

    }

    /**
     * Get latest schema version for a subject
     */
    @CircuitBreaker(name = "schema-registry")
    @Retry(name = "schema-registry")
    public int getLatestVersion(String subject) {
        try {
            List<Integer> versions = schemaRegistryClient.getAllVersions(subject);
            if (versions.isEmpty()) {
                throw new SchemaRegistryException("No versions found for subject: " + subject);
            }
            return Collections.max(versions);
        } catch (RestClientException | IOException e) {
            log.error("Failed to get latest version for subject: {}", subject, e);
            throw new SchemaRegistryException("Failed to get latest version", e);
        }
    }

    /**
     * Get all versions of a schema
     */
    @CircuitBreaker(name = "schema-registry")
    @Retry(name = "schema-registry")
    public List<Integer> getAllVersions(String subject) {
        try {
            return schemaRegistryClient.getAllVersions(subject);
        } catch (RestClientException | IOException e) {
            log.error("Failed to get versions for subject: {}", subject, e);
            throw new SchemaRegistryException("Failed to get versions", e);
        }
    }

    /**
     * List all subjects
     */
    @CircuitBreaker(name = "schema-registry")
    @Retry(name = "schema-registry")
    public List<String> getAllSubjects() {
        try {
            Collection<String> subjects = schemaRegistryClient.getAllSubjects();
            return new ArrayList<>(subjects);
        } catch (RestClientException | IOException e) {
            log.error("Failed to list subjects", e);
            throw new SchemaRegistryException("Failed to list subjects", e);
        }
    }

    /**
     * Delete a schema subject
     */
    @CircuitBreaker(name = "schema-registry")
    @Retry(name = "schema-registry")
    public void deleteSubject(String subject) {
        try {
            log.warn("Deleting schema subject: {}", subject);
            List<Integer> deletedVersions = schemaRegistryClient.deleteSubject(subject);
            log.info("Deleted schema subject: {} - Versions deleted: {}", subject, deletedVersions);
        } catch (RestClientException | IOException e) {
            log.error("Failed to delete subject: {}", subject, e);
            throw new SchemaRegistryException("Failed to delete subject", e);
        }
    }

    /**
     * Delete a specific schema version
     */
    @CircuitBreaker(name = "schema-registry")
    @Retry(name = "schema-registry")
    public void deleteSchemaVersion(String subject, int version) {
        try {
            log.warn("Deleting schema version - Subject: {}, Version: {}", subject, version);
            schemaRegistryClient.deleteSchemaVersion(subject, String.valueOf(version));
            log.info("Deleted schema version - Subject: {}, Version: {}", subject, version);
        } catch (RestClientException | IOException e) {
            log.error("Failed to delete schema version - Subject: {}, Version: {}",
                    subject, version, e);
            throw new SchemaRegistryException("Failed to delete schema version", e);
        }
    }

    /**
     * Set compatibility level for a subject
     */
    @CircuitBreaker(name = "schema-registry")
    @Retry(name = "schema-registry")
    public void setCompatibility(String subject, String compatibility) {
        try {
            log.info("Setting compatibility - Subject: {}, Level: {}", subject, compatibility);
            String result = schemaRegistryClient.updateCompatibility(subject, compatibility);
            log.info("Compatibility updated - Subject: {}, Result: {}", subject, result);
        } catch (RestClientException | IOException e) {
            log.error("Failed to set compatibility - Subject: {}, Level: {}",
                    subject, compatibility, e);
            throw new SchemaRegistryException("Failed to set compatibility", e);
        }
    }

    /**
     * Get compatibility level for a subject
     */
    @CircuitBreaker(name = "schema-registry")
    @Retry(name = "schema-registry")
    public String getCompatibility(String subject) {
        try {
            return schemaRegistryClient.getCompatibility(subject);
        } catch (RestClientException | IOException e) {
            log.debug("Failed to get compatibility for subject: {}, using default", subject);
            return defaultCompatibility;
        }
    }

    /**
     * Test compatibility of a new schema
     */
    @CircuitBreaker(name = "schema-registry")
    @Retry(name = "schema-registry")
    public boolean testCompatibility(String subject, String newSchema) {
        try {
            log.info("Testing schema compatibility for subject: {}", subject);

            ProtobufSchema protobufSchema = new ProtobufSchema(newSchema);

            // Test against all versions
            List<Integer> versions = getAllVersions(subject);
            for (Integer version : versions) {
                boolean isCompatible = schemaRegistryClient.testCompatibility(
                        subject, protobufSchema
                );
                if (!isCompatible) {
                    log.warn("Schema not compatible with version {} of subject {}",
                            version, subject);
                    return false;
                }
            }

            log.info("Schema is compatible with all versions of subject: {}", subject);
            return true;

        } catch (RestClientException | IOException e) {
            log.error("Failed to test compatibility for subject: {}", subject, e);
            throw new SchemaRegistryException("Failed to test compatibility", e);
        }
    }

    /**
     * Get schema by ID
     */
    @Cacheable(value = "schemas", key = "'id-' + #schemaId")
    @CircuitBreaker(name = "schema-registry")
    @Retry(name = "schema-registry")
    public SchemaInfo getSchemaById(int schemaId) {
        try {
            log.debug("Fetching schema by ID: {}", schemaId);

            io.confluent.kafka.schemaregistry.ParsedSchema schema =
                    schemaRegistryClient.getSchemaById(schemaId);

            return SchemaInfo.builder()
                    .id(schemaId)
                    .schema(schema.canonicalString())
                    .schemaType(schema.schemaType())
                    .build();

        } catch (RestClientException | IOException e) {
            log.error("Failed to get schema by ID: {}", schemaId, e);
            throw new SchemaRegistryException("Failed to get schema by ID", e);
        }
    }

    /**
     * Get all schemas for a topic
     */
    public Map<String, SchemaInfo> getSchemasForTopic(String topic) {
        Map<String, SchemaInfo> schemas = new HashMap<>();

        // Check for value schema
        String valueSubject = topic + "-value";
        try {
            SchemaInfo valueSchema = getSchema(valueSubject, null);
            schemas.put("value", valueSchema);
        } catch (Exception e) {
            log.debug("No value schema found for topic: {}", topic);
        }

        // Check for key schema
        String keySubject = topic + "-key";
        try {
            SchemaInfo keySchema = getSchema(keySubject, null);
            schemas.put("key", keySchema);
        } catch (Exception e) {
            log.debug("No key schema found for topic: {}", topic);
        }

        return schemas;
    }

    /**
     * Evolve schema with compatibility check
     */
    public int evolveSchema(String subject, String newSchema, boolean forceUpdate) {
        log.info("Evolving schema for subject: {}", subject);

        // Test compatibility first
        if (!forceUpdate) {
            boolean isCompatible = testCompatibility(subject, newSchema);
            if (!isCompatible) {
                throw new SchemaRegistryException(
                        "New schema is not compatible with existing versions"
                );
            }
        }

        // Register new version
        return registerSchema(subject, newSchema);
    }

    /**
     * Get schema statistics
     */
    public Map<String, Object> getSchemaStatistics() {
        try {
            List<String> subjects = getAllSubjects();

            Map<String, Object> stats = new HashMap<>();
            stats.put("totalSubjects", subjects.size());

            // Count by type
            Map<String, Long> byType = subjects.stream()
                    .collect(Collectors.groupingBy(
                            subject -> {
                                if (subject.endsWith("-value")) return "value";
                                else if (subject.endsWith("-key")) return "key";
                                else return "other";
                            },
                            Collectors.counting()
                    ));
            stats.put("byType", byType);

            // Version statistics
            int totalVersions = 0;
            int maxVersions = 0;
            for (String subject : subjects) {
                try {
                    List<Integer> versions = getAllVersions(subject);
                    totalVersions += versions.size();
                    maxVersions = Math.max(maxVersions, versions.size());
                } catch (Exception e) {
                    log.debug("Error getting versions for subject: {}", subject);
                }
            }
            stats.put("totalVersions", totalVersions);
            stats.put("maxVersionsPerSubject", maxVersions);
            stats.put("averageVersionsPerSubject",
                    subjects.isEmpty() ? 0 : (double) totalVersions / subjects.size());

            return stats;

        } catch (Exception e) {
            log.error("Failed to get schema statistics", e);
            return Collections.emptyMap();
        }
    }

    /**
     * Check Schema Registry health
     */
    public boolean isHealthy() {
        try {
            // Try to list subjects as a health check
            schemaRegistryClient.getAllSubjects();
            return true;
        } catch (Exception e) {
            log.error("Schema Registry health check failed", e);
            return false;
        }
    }

    /**
     * Custom exception for Schema Registry operations
     */
    public static class SchemaRegistryException extends RuntimeException {
        public SchemaRegistryException(String message) {
            super(message);
        }

        public SchemaRegistryException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}