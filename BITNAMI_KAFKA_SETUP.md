# NNIPA Bitnami Kafka Setup Guide

## Overview

This guide covers the setup and management of the NNIPA Event Streaming infrastructure using **Bitnami Kafka** images, which provide:

- **KRaft mode** (no Zookeeper dependency)
- **Lighter footprint** compared to Confluent images
- **Production-ready** configuration
- **Easy clustering** with built-in support

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Bitnami Kafka Cluster (KRaft)             │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────┐      ┌──────────┐      ┌──────────┐           │
│  │ Kafka-0  │      │ Kafka-1  │      │ Kafka-2  │           │
│  │  Node 0  │◄────►│  Node 1  │◄────►│  Node 2  │           │
│  │   9092   │      │   9094   │      │   9096   │           │
│  └──────────┘      └──────────┘      └──────────┘           │
│       ▲                 ▲                 ▲                 │
└───────┼─────────────────┼─────────────────┼─────────────────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          │
                    ┌─────▼──────┐
                    │  Schema    │
                    │  Registry  │
                    │    8081    │
                    └────────────┘
```

## Quick Start

### 1. Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- 8GB RAM minimum (16GB recommended)
- 20GB free disk space

### 2. Start the Cluster

```bash
# Make scripts executable
chmod +x scripts/*.sh

# Start the entire cluster
./scripts/kafka-manager.sh start

# Or start with the event streaming service
./scripts/start-kafka-cluster.sh --with-service
```

### 3. Verify Installation

```bash
# Check cluster health
./scripts/kafka-manager.sh health

# List topics
./scripts/kafka-manager.sh topics

# View cluster status
./scripts/kafka-manager.sh status
```

## 📋 Quick Commands

```bash
# Start the cluster
./scripts/kafka-manager.sh start

# Check health
./scripts/kafka-manager.sh health

# Create a topic
./scripts/kafka-manager.sh create-topic my-topic 6 3

# Produce a message
./scripts/kafka-manager.sh produce my-topic key1 "Hello World"

# Consume messages
./scripts/kafka-manager.sh consume my-topic

# View logs
./scripts/kafka-manager.sh logs kafka-0
```

## Service Endpoints

| Service | URL | Description |
|---------|-----|-------------|
| Kafka Broker 1 | `localhost:9092` | Primary Kafka broker |
| Kafka Broker 2 | `localhost:9094` | Secondary Kafka broker |
| Kafka Broker 3 | `localhost:9096` | Tertiary Kafka broker |
| Schema Registry | `http://localhost:8081` | Confluent Schema Registry |
| Kafka UI | `http://localhost:8080` | Web UI for Kafka management |
| Kafdrop | `http://localhost:9000` | Alternative Kafka UI |
| Redis | `localhost:6379` | Event streaming state store |
| Event Streaming Service | `http://localhost:8085` | REST API for events |

## Configuration

### Kafka Cluster Settings

The Bitnami Kafka cluster is configured with:

- **KRaft Mode**: No Zookeeper dependency
- **3 Brokers**: For high availability
- **Replication Factor**: 3 for all topics
- **Min In-Sync Replicas**: 2 for durability
- **Default Partitions**: 6 per topic
- **Compression**: Snappy for efficiency
- **Retention**: 7 days default

### Environment Variables

Key environment variables for Bitnami Kafka:

```yaml
# KRaft Configuration
KAFKA_CFG_NODE_ID: Unique node ID (0, 1, 2)
KAFKA_CFG_PROCESS_ROLES: controller,broker
KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: List of controller nodes
KAFKA_KRAFT_CLUSTER_ID: Unique cluster identifier

# Network Configuration
KAFKA_CFG_LISTENERS: Internal and external listeners
KAFKA_CFG_ADVERTISED_LISTENERS: Advertised broker addresses

# Replication Settings
KAFKA_CFG_DEFAULT_REPLICATION_FACTOR: 3
KAFKA_CFG_MIN_INSYNC_REPLICAS: 2
KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
```

## Topic Management

### Create a Topic

```bash
# Using the manager script
./scripts/kafka-manager.sh create-topic my-topic 6 3

# Or directly with Docker
docker exec nnipa-kafka-0 kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partitions 6 \
  --replication-factor 3
```

### List Topics

```bash
./scripts/kafka-manager.sh topics
```

### Describe a Topic

```bash
./scripts/kafka-manager.sh describe-topic nnipa.events.tenant.created
```

## Producer/Consumer Testing

### Produce a Test Message

```bash
# Using the manager script
./scripts/kafka-manager.sh produce my-topic key1 "Hello World"

# Or using Docker directly
echo "key1:Hello World" | docker exec -i nnipa-kafka-0 \
  kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --property "parse.key=true" \
  --property "key.separator=:"
```

### Consume Messages

```bash
# From beginning
./scripts/kafka-manager.sh consume my-topic test-group true

# From latest
./scripts/kafka-manager.sh consume my-topic test-group
```

## Consumer Group Management

### List Consumer Groups

```bash
./scripts/kafka-manager.sh groups
```

### Describe Consumer Group

```bash
./scripts/kafka-manager.sh describe-group event-streaming-service
```

### Reset Consumer Offset

```bash
# Reset to earliest
./scripts/kafka-manager.sh reset-offset my-group my-topic earliest

# Reset to latest
./scripts/kafka-manager.sh reset-offset my-group my-topic latest
```

## Monitoring

### Kafka UI

Access the Kafka UI at http://localhost:8080 for:
- Topic management
- Consumer group monitoring
- Message browsing
- Schema Registry integration

### Kafdrop

Alternative UI at http://localhost:9000 offering:
- Lightweight interface
- Topic browsing
- Consumer lag monitoring
- Protobuf message viewing

### Health Checks

```bash
# Check all services
./scripts/kafka-manager.sh health

# Check specific service
docker exec nnipa-kafka-0 kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

## Troubleshooting

### Common Issues

#### 1. Kafka Won't Start

```bash
# Check logs
./scripts/kafka-manager.sh logs kafka-0

# Clean and restart
./scripts/kafka-manager.sh clean
./scripts/kafka-manager.sh start
```

#### 2. Connection Refused

```bash
# Verify Kafka is listening
netstat -an | grep 9092

# Check Docker network
docker network inspect nnipa-network
```

#### 3. Schema Registry Issues

```bash
# Check Schema Registry health
curl http://localhost:8081/subjects

# View logs
docker logs nnipa-schema-registry
```

#### 4. Consumer Lag

```bash
# Check consumer lag
docker exec nnipa-kafka-0 kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group event-streaming-service \
  --describe
```

## Performance Tuning

### JVM Settings

Bitnami Kafka uses optimized JVM settings by default. To customize:

```yaml
environment:
  KAFKA_HEAP_OPTS: "-Xmx2G -Xms2G"
  KAFKA_JVM_PERFORMANCE_OPTS: "-XX:+UseG1GC -XX:MaxGCPauseMillis=20"
```

### OS Tuning

For production:

```bash
# Increase file descriptors
ulimit -n 100000

# Increase virtual memory
sysctl -w vm.max_map_count=262144

# Disable swap
swapoff -a
```

## Backup and Recovery

### Backup Topics

```bash
# Export topic data
docker exec nnipa-kafka-0 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning \
  --property print.key=true \
  --property key.separator="," \
  > topic-backup.txt
```

### Restore Topics

```bash
# Import topic data
cat topic-backup.txt | docker exec -i nnipa-kafka-0 \
  kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --property parse.key=true \
  --property key.separator=","
```

## Production Deployment

### Docker Swarm

```yaml
# Deploy as stack
docker stack deploy -c docker-compose-kafka.yml nnipa-kafka
```

### Kubernetes

```bash
# Use Bitnami Helm chart
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install nnipa-kafka bitnami/kafka \
  --set replicaCount=3 \
  --set kraft.enabled=true \
  --set persistence.enabled=true
```

## Maintenance

### Rolling Restart

```bash
# Restart brokers one by one
docker-compose -f docker-compose-kafka.yml restart kafka-0
sleep 30
docker-compose -f docker-compose-kafka.yml restart kafka-1
sleep 30
docker-compose -f docker-compose-kafka.yml restart kafka-2
```

### Upgrade Procedure

1. Backup configuration and data
2. Stop consumers
3. Update Docker image version
4. Perform rolling restart
5. Verify cluster health
6. Resume consumers

## Security Considerations

For production, enable:

1. **SASL Authentication**
```yaml
KAFKA_CFG_SASL_ENABLED_MECHANISMS: PLAIN,SCRAM-SHA-256
KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL: SCRAM-SHA-256
```

2. **SSL/TLS Encryption**
```yaml
KAFKA_CFG_SSL_KEYSTORE_LOCATION: /opt/bitnami/kafka/config/certs/kafka.keystore.jks
KAFKA_CFG_SSL_TRUSTSTORE_LOCATION: /opt/bitnami/kafka/config/certs/kafka.truststore.jks
```

3. **ACL Authorization**
```yaml
KAFKA_CFG_AUTHORIZER_CLASS_NAME: org.apache.kafka.metadata.authorizer.StandardAuthorizer
KAFKA_CFG_SUPER_USERS: User:admin
```

## Resources

- [Bitnami Kafka Documentation](https://github.com/bitnami/containers/tree/main/bitnami/kafka)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [KRaft Mode Guide](https://kafka.apache.org/documentation/#kraft)

## Support

For issues:
1. Check service logs: `./scripts/kafka-manager.sh logs <service>`
2. Verify health: `./scripts/kafka-manager.sh health`
3. Review this documentation
4. Contact the platform team