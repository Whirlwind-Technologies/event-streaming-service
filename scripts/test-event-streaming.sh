#!/bin/bash

# Test Event Streaming Service
# This script verifies the event streaming service is working correctly

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=========================================="
echo "Testing Event Streaming Service"
echo "=========================================="

# Function to check service
check_service() {
    local service=$1
    local port=$2
    local endpoint=$3

    echo -n "Checking $service... "

    if curl -s -f "http://localhost:$port$endpoint" > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Running"
        return 0
    else
        echo -e "${RED}✗${NC} Not running"
        return 1
    fi
}

# 1. Check if Kafka is running
echo "1. Checking Kafka Cluster..."
if docker ps | grep -q "nnipa-kafka"; then
    echo -e "${GREEN}✓${NC} Kafka is running"
else
    echo -e "${RED}✗${NC} Kafka is not running"
    echo "Starting Kafka cluster..."
    ./scripts/kafka-manager.sh start
    sleep 10
fi

# 2. Check Schema Registry
echo ""
echo "2. Checking Schema Registry..."
check_service "Schema Registry" 8081 "/subjects"

# 3. Check Redis
echo ""
echo "3. Checking Redis..."
if docker ps | grep -q "nnipa-redis"; then
    echo -e "${GREEN}✓${NC} Redis is running"
else
    echo -e "${RED}✗${NC} Redis is not running"
fi

# 4. Build Event Streaming Service
echo ""
echo "4. Building Event Streaming Service..."
cd "$PROJECT_ROOT/event-streaming-service"
mvn clean package -DskipTests

# 5. Start Event Streaming Service
echo ""
echo "5. Starting Event Streaming Service..."
java -jar target/*.jar &
SERVICE_PID=$!
echo "Service PID: $SERVICE_PID"

# Wait for service to start
echo "Waiting for service to start..."
sleep 30

# 6. Check Event Streaming Service Health
echo ""
echo "6. Checking Event Streaming Service..."
if check_service "Event Streaming Service" 8085 "/event-streaming/actuator/health"; then
    # Get detailed health
    echo ""
    echo "Health Status:"
    curl -s http://localhost:8085/event-streaming/actuator/health | python3 -m json.tool
fi

# 7. Check Topics
echo ""
echo "7. Checking Kafka Topics..."
docker exec nnipa-kafka-0 kafka-topics.sh --bootstrap-server localhost:9092 --list | head -10

# 8. Test Event Publishing
echo ""
echo "8. Testing Event Publishing..."
curl -X POST http://localhost:8085/event-streaming/api/v1/events/publish \
    -H "Content-Type: application/json" \
    -d '{
        "topic": "test.topic",
        "key": "test-key",
        "eventData": {
            "tenantId": "test-tenant",
            "userId": "test-user",
            "message": "Test event"
        }
    }'

echo ""
echo ""
echo "=========================================="
echo "Test Results Summary"
echo "=========================================="

# Summary
if check_service "Event Streaming Service" 8085 "/event-streaming/actuator/health" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Event Streaming Service is operational"
    echo ""
    echo "Access points:"
    echo "  • Health: http://localhost:8085/event-streaming/actuator/health"
    echo "  • Metrics: http://localhost:8085/event-streaming/actuator/metrics"
    echo "  • API Docs: http://localhost:8085/event-streaming/swagger-ui.html"
    echo "  • Kafka UI: http://localhost:8080"
    echo ""
    echo "Service PID: $SERVICE_PID"
    echo "To stop: kill $SERVICE_PID"
else
    echo -e "${RED}✗${NC} Event Streaming Service failed to start"
    echo "Check logs: tail -f logs/event-streaming-service.log"
fi

echo "=========================================="