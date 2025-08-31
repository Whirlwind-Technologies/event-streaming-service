#!/bin/bash

# NNIPA Kafka Cluster Startup Script
# Using Bitnami Kafka with KRaft mode (no Zookeeper)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "Starting NNIPA Kafka Cluster (Bitnami)"
echo "=========================================="

# Create network if it doesn't exist
echo "Creating Docker network..."
docker network create nnipa-network 2>/dev/null || echo "Network already exists"

# Stop any existing containers
echo "Stopping existing containers..."
docker-compose -f "$PROJECT_ROOT/docker-compose-kafka.yml" down

# Clean up volumes if requested
if [ "$1" == "--clean" ]; then
    echo "Cleaning up volumes..."
    docker-compose -f "$PROJECT_ROOT/docker-compose-kafka.yml" down -v
    docker volume prune -f
fi

# Start Kafka cluster
echo "Starting Kafka cluster..."
docker-compose -f "$PROJECT_ROOT/docker-compose-kafka.yml" up -d kafka-0 kafka-1 kafka-2

# Wait for Kafka to be ready
echo "Waiting for Kafka cluster to be ready..."
sleep 10

# Check if Kafka is ready
MAX_ATTEMPTS=30
ATTEMPT=0
while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if docker exec nnipa-kafka-0 kafka-topics.sh --bootstrap-server localhost:9092 --list &>/dev/null; then
        echo "Kafka cluster is ready!"
        break
    fi
    echo "Waiting for Kafka... (attempt $((ATTEMPT+1))/$MAX_ATTEMPTS)"
    sleep 5
    ATTEMPT=$((ATTEMPT+1))
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo "Error: Kafka cluster failed to start"
    exit 1
fi

# Start Schema Registry
echo "Starting Schema Registry..."
docker-compose -f "$PROJECT_ROOT/docker-compose-kafka.yml" up -d schema-registry

# Wait for Schema Registry
sleep 10

# Start Redis
echo "Starting Redis..."
docker-compose -f "$PROJECT_ROOT/docker-compose-kafka.yml" up -d redis

# Initialize topics
echo "Initializing Kafka topics..."
docker-compose -f "$PROJECT_ROOT/docker-compose-kafka.yml" run --rm kafka-init

# Start monitoring tools
echo "Starting monitoring tools..."
docker-compose -f "$PROJECT_ROOT/docker-compose-kafka.yml" up -d kafka-ui kafdrop

# Start Event Streaming Service if requested
if [ "$2" == "--with-service" ]; then
    echo "Building and starting Event Streaming Service..."
    cd "$PROJECT_ROOT/event-streaming-service"
    mvn clean package -DskipTests
    docker-compose -f "$PROJECT_ROOT/docker-compose-kafka.yml" up -d event-streaming-service
fi

echo "=========================================="
echo "Kafka Cluster Started Successfully!"
echo "=========================================="
echo "Services:"
echo "  - Kafka Brokers: localhost:9092, localhost:9094, localhost:9096"
echo "  - Schema Registry: http://localhost:8081"
echo "  - Kafka UI: http://localhost:8080"
echo "  - Kafdrop: http://localhost:9000"
echo "  - Redis: localhost:6379"
if [ "$2" == "--with-service" ]; then
    echo "  - Event Streaming Service: http://localhost:8085"
fi
echo "=========================================="

# Show cluster status
echo "Cluster Status:"
docker-compose -f "$PROJECT_ROOT/docker-compose-kafka.yml" ps

# Verify topics
echo ""
echo "Created Topics:"
docker exec nnipa-kafka-0 kafka-topics.sh --bootstrap-server localhost:9092 --list

echo ""
echo "To view logs:"
echo "  docker-compose -f docker-compose-kafka.yml logs -f kafka-0"
echo ""
echo "To stop the cluster:"
echo "  docker-compose -f docker-compose-kafka.yml down"