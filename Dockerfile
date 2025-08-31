# Build stage
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /app

# Copy and install the local nnipa-protos dependency first
COPY nnipa-protos-1.0.0.jar /tmp/
RUN mvn install:install-file \
    -Dfile=/tmp/nnipa-protos-1.0.0.jar \
    -DgroupId=com.nnipa \
    -DartifactId=nnipa-protos \
    -Dversion=1.0.0 \
    -Dpackaging=jar \
    -DgeneratePom=true

# Copy pom.xml and download dependencies
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source code and build
COPY src ./src
RUN mvn clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:21-jre-alpine

# Install curl for health checks
RUN apk add --no-cache curl

# Create non-root user
RUN addgroup -g 1001 -S appuser && \
    adduser -u 1001 -S appuser -G appuser

# Create directories
RUN mkdir -p /app/logs && \
    chown -R appuser:appuser /app

WORKDIR /app

# Copy JAR from builder
COPY --from=builder --chown=appuser:appuser /app/target/*.jar app.jar

# Switch to non-root user
USER appuser

# JVM options for container environment
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 \
    -XX:+UseG1GC \
    -XX:+UseStringDeduplication \
    -Djava.security.egd=file:/dev/./urandom"

# Expose port
EXPOSE 4601

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:4601/event-streaming/actuator/health || exit 1

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]