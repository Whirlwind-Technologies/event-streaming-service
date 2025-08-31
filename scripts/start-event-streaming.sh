#!/bin/bash

# Start Event Streaming Service
# Production-ready startup script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "=========================================="
echo "Starting NNIPA Event Streaming Service"
echo "=========================================="

# 1. Check prerequisites
echo "Checking prerequisites..."

# Check Kafka
if ! nc -zv localhost 9092 2>/dev/null; then
    echo -e "${YELLOW}⚠${NC} Kafka not running. Starting Kafka cluster..."
    "$SCRIPT_DIR/kafka-manager.sh" start
    sleep 15
fi

# Check Redis (optional but recommended)
if ! nc -zv localhost 6379 2>/dev/null; then
    echo -e "${YELLOW}⚠${NC} Redis not running. Starting Redis..."
    docker run -d --name nnipa-redis -p 6379:6379 redis:7-alpine
    sleep 5
fi

# 2. Build the service
echo ""
echo "Building Event Streaming Service..."
cd "$PROJECT_ROOT/event-streaming-service"

# Clean and package
mvn clean package -DskipTests

# 3. Set environment variables
export JAVA_OPTS="-Xms512m -Xmx1024m -XX:+UseG1GC"
export KAFKA_BROKERS="localhost:9092"
export SCHEMA_REGISTRY_URL="http://localhost:8081"
export REDIS_HOST="localhost"
export REDIS_PORT="6379"

# 4. Start the service
echo ""
echo "Starting Event Streaming Service..."
echo "=========================================="

# Run with proper logging
java $JAVA_OPTS \
    -Dspring.profiles.active=${PROFILE:-default} \
    -Dlogging.file.name=logs/event-streaming-service.log \
    -jar target/event-streaming-service-*.jar &

SERVICE_PID=$!
echo "Service PID: $SERVICE_PID"

# 5. Wait for startup
echo "Waiting for service to start (30 seconds)..."
sleep 30

# 6. Verify startup
echo ""
echo "Verifying service status..."

if ps -p $SERVICE_PID > /dev/null; then
    echo -e "${GREEN}✓${NC} Service is running (PID: $SERVICE_PID)"

    # Check health endpoint
    if curl -s http://localhost:8085/event-streaming/actuator/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Health endpoint is responding"

        # Get health details
        health=$(curl -s http://localhost:8085/event-streaming/actuator/health)
        echo ""
        echo "Health Status:"
        echo "$health" | python3 -m json.tool | head -20
    else
        echo -e "${YELLOW}⚠${NC} Health endpoint not yet available"
    fi

    # Show service URLs
    echo ""
    echo "=========================================="
    echo "Service URLs:"
    echo "=========================================="
    echo "Health:   http://localhost:8085/event-streaming/actuator/health"
    echo "Metrics:  http://localhost:8085/event-streaming/actuator/metrics"
    echo "API Docs: http://localhost:8085/event-streaming/swagger-ui.html"
    echo "Kafka UI: http://localhost:8080"
    echo ""
    echo "Logs:     tail -f $PROJECT_ROOT/event-streaming-service/logs/event-streaming-service.log"
    echo "Stop:     kill $SERVICE_PID"
    echo ""

    # Save PID for later
    echo $SERVICE_PID > /tmp/event-streaming-service.pid
    echo "PID saved to: /tmp/event-streaming-service.pid"

else
    echo -e "${RED}✗${NC} Service failed to start"
    echo "Check logs: tail -f logs/event-streaming-service.log"
    exit 1
fi

echo "=========================================="
echo -e "${GREEN}Event Streaming Service Started Successfully!${NC}"
echo "=========================================="