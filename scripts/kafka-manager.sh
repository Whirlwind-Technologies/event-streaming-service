#!/bin/bash

# NNIPA Kafka Manager Script
# Comprehensive management for Bitnami Kafka cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose-kafka.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Functions
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_header() {
    echo ""
    echo "=========================================="
    echo "$1"
    echo "=========================================="
}

# Check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
}

# Start Kafka cluster
start_cluster() {
    print_header "Starting NNIPA Kafka Cluster"

    # Create network
    docker network create nnipa-network 2>/dev/null || print_warning "Network already exists"

    # Start Kafka nodes
    print_success "Starting Kafka brokers..."
    docker-compose -f "$COMPOSE_FILE" up -d kafka-0 kafka-1 kafka-2

    # Wait for Kafka to be ready
    print_success "Waiting for Kafka cluster..."
    sleep 15

    # Start Schema Registry
    print_success "Starting Schema Registry..."
    docker-compose -f "$COMPOSE_FILE" up -d schema-registry

    # Initialize topics
    print_success "Creating topics..."
    docker-compose -f "$COMPOSE_FILE" run --rm kafka-init

    # Start monitoring
    print_success "Starting monitoring tools..."
    docker-compose -f "$COMPOSE_FILE" up -d kafka-ui kafdrop

    print_header "Cluster Started Successfully!"
    show_status
}

# Stop Kafka cluster
stop_cluster() {
    print_header "Stopping NNIPA Kafka Cluster"
    docker-compose -f "$COMPOSE_FILE" down
    print_success "Cluster stopped"
}

# Clean all data
clean_cluster() {
    print_header "Cleaning NNIPA Kafka Cluster"
    print_warning "This will delete all data!"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose -f "$COMPOSE_FILE" down -v
        docker volume prune -f
        print_success "Cluster cleaned"
    else
        print_warning "Cancelled"
    fi
}

# Show cluster status
show_status() {
    print_header "Cluster Status"
    docker-compose -f "$COMPOSE_FILE" ps

    echo ""
    echo "Service URLs:"
    echo "  • Kafka Brokers: localhost:9092, localhost:9094, localhost:9096"
    echo "  • Schema Registry: http://localhost:8081"
    echo "  • Kafka UI: http://localhost:8080"
    echo "  • Kafdrop: http://localhost:9000"
}

# List topics
list_topics() {
    print_header "Kafka Topics"
    docker exec nnipa-kafka-0 kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --list | sort
}

# Create a topic
create_topic() {
    local topic_name=$1
    local partitions=${2:-6}
    local replication=${3:-3}

    print_header "Creating Topic: $topic_name"
    docker exec nnipa-kafka-0 kafka-topics.sh \
        --create \
        --bootstrap-server localhost:9092 \
        --topic "$topic_name" \
        --partitions "$partitions" \
        --replication-factor "$replication" \
        --config retention.ms=604800000 \
        --config compression.type=snappy \
        --config min.insync.replicas=2

    print_success "Topic created: $topic_name"
}

# Delete a topic
delete_topic() {
    local topic_name=$1

    print_header "Deleting Topic: $topic_name"
    print_warning "This will delete all messages in the topic!"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker exec nnipa-kafka-0 kafka-topics.sh \
            --delete \
            --bootstrap-server localhost:9092 \
            --topic "$topic_name"
        print_success "Topic deleted: $topic_name"
    else
        print_warning "Cancelled"
    fi
}

# Describe a topic
describe_topic() {
    local topic_name=$1

    print_header "Topic Details: $topic_name"
    docker exec nnipa-kafka-0 kafka-topics.sh \
        --describe \
        --bootstrap-server localhost:9092 \
        --topic "$topic_name"
}

# List consumer groups
list_consumer_groups() {
    print_header "Consumer Groups"
    docker exec nnipa-kafka-0 kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 \
        --list
}

# Describe consumer group
describe_consumer_group() {
    local group_name=$1

    print_header "Consumer Group: $group_name"
    docker exec nnipa-kafka-0 kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 \
        --group "$group_name" \
        --describe
}

# Reset consumer group offset
reset_consumer_offset() {
    local group_name=$1
    local topic=$2
    local reset_to=${3:-earliest}  # earliest, latest, or specific offset

    print_header "Resetting Consumer Offset"
    echo "Group: $group_name"
    echo "Topic: $topic"
    echo "Reset to: $reset_to"

    docker exec nnipa-kafka-0 kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 \
        --group "$group_name" \
        --topic "$topic" \
        --reset-offsets \
        --to-$reset_to \
        --execute

    print_success "Offset reset completed"
}

# Produce test message
produce_message() {
    local topic=$1
    local key=$2
    local value=$3

    print_header "Producing Message"
    echo "Topic: $topic"
    echo "Key: $key"
    echo "Value: $value"

    echo "$key:$value" | docker exec -i nnipa-kafka-0 kafka-console-producer.sh \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --property "parse.key=true" \
        --property "key.separator=:"

    print_success "Message sent"
}

# Consume messages
consume_messages() {
    local topic=$1
    local group=${2:-console-consumer-$$}
    local from_beginning=${3:-false}

    print_header "Consuming Messages"
    echo "Topic: $topic"
    echo "Group: $group"
    echo "Press Ctrl+C to stop..."
    echo ""

    local beginning_flag=""
    if [ "$from_beginning" == "true" ]; then
        beginning_flag="--from-beginning"
    fi

    docker exec -it nnipa-kafka-0 kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --group "$group" \
        $beginning_flag \
        --property print.key=true \
        --property key.separator=" : "
}

# Show logs
show_logs() {
    local service=$1
    docker-compose -f "$COMPOSE_FILE" logs -f "$service"
}

# Health check
health_check() {
    print_header "Health Check"

    # Check Kafka
    if docker exec nnipa-kafka-0 kafka-broker-api-versions.sh --bootstrap-server localhost:9092 &>/dev/null; then
        print_success "Kafka cluster is healthy"
    else
        print_error "Kafka cluster is not responding"
    fi

    # Check Schema Registry
    if curl -s http://localhost:8081/subjects &>/dev/null; then
        print_success "Schema Registry is healthy"
    else
        print_error "Schema Registry is not responding"
    fi
}

# Show help
show_help() {
    cat << EOF
NNIPA Kafka Manager

Usage: $0 [command] [options]

Commands:
  start                 Start the Kafka cluster
  stop                  Stop the Kafka cluster
  restart               Restart the Kafka cluster
  clean                 Clean all data (WARNING: destructive)
  status                Show cluster status
  health                Check cluster health

  topics                List all topics
  create-topic          Create a new topic
                       Usage: $0 create-topic <name> [partitions] [replication]
  delete-topic          Delete a topic
                       Usage: $0 delete-topic <name>
  describe-topic        Describe a topic
                       Usage: $0 describe-topic <name>

  groups                List consumer groups
  describe-group        Describe consumer group
                       Usage: $0 describe-group <name>
  reset-offset          Reset consumer group offset
                       Usage: $0 reset-offset <group> <topic> [earliest|latest]

  produce               Produce a test message
                       Usage: $0 produce <topic> <key> <value>
  consume               Consume messages from topic
                       Usage: $0 consume <topic> [group] [from-beginning]

  logs                  Show service logs
                       Usage: $0 logs <service>

  help                  Show this help message

Examples:
  $0 start                                    # Start cluster
  $0 create-topic my-topic 3 2               # Create topic with 3 partitions, 2 replicas
  $0 produce my-topic key1 "Hello World"     # Send a message
  $0 consume my-topic my-group true          # Consume from beginning
  $0 reset-offset my-group my-topic earliest # Reset offset to beginning
  $0 logs kafka-0                            # Show kafka-0 logs

EOF
}

# Main script logic
check_docker

case "$1" in
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    restart)
        stop_cluster
        sleep 5
        start_cluster
        ;;
    clean)
        clean_cluster
        ;;
    status)
        show_status
        ;;
    health)
        health_check
        ;;
    topics)
        list_topics
        ;;
    create-topic)
        create_topic "$2" "$3" "$4"
        ;;
    delete-topic)
        delete_topic "$2"
        ;;
    describe-topic)
        describe_topic "$2"
        ;;
    groups)
        list_consumer_groups
        ;;
    describe-group)
        describe_consumer_group "$2"
        ;;
    reset-offset)
        reset_consumer_offset "$2" "$3" "$4"
        ;;
    produce)
        produce_message "$2" "$3" "$4"
        ;;
    consume)
        consume_messages "$2" "$3" "$4"
        ;;
    logs)
        show_logs "$2"
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac