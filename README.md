# EventFlow — Distributed Event Processing Engine

[![CI/CD Pipeline](https://github.com/YOUR_USERNAME/eventflow/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/eventflow/actions)
[![Java 17](https://img.shields.io/badge/Java-17-blue.svg)](https://openjdk.org/projects/jdk/17/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade distributed event processing engine built with **Java 17**, **Apache Kafka**, **Apache Cassandra**, and **Spring Boot**. Ingests high-throughput event streams, processes them through enrichment, aggregation, and anomaly detection pipelines, and stores results in a time-series optimized Cassandra schema — all observable via Prometheus + Grafana.

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │          API Gateway (Spring Boot)       │
                    │  Rate Limit → Validate → Enrich → Publish│
                    └──────────────────┬──────────────────────┘
                                       │
                              ┌────────▼────────┐
                              │   Apache Kafka   │
                              │  3-broker cluster │
                              │  Partitioned topics│
                              └────────┬────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
              ┌─────▼─────┐    ┌──────▼──────┐   ┌──────▼──────┐
              │ Enrichment │    │ Aggregation │   │  Anomaly    │
              │ Processor  │    │  Processor  │   │  Detector   │
              └─────┬─────┘    └──────┬──────┘   └──────┬──────┘
                    │                  │                  │
                    └──────────────────┼──────────────────┘
                                       │
                              ┌────────▼────────┐
                              │ Apache Cassandra │
                              │  3-node ring     │
                              │  RF=3, QUORUM    │
                              └────────┬────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
              ┌─────▼─────┐    ┌──────▼──────┐   ┌──────▼──────┐
              │ Query API  │    │  Grafana    │   │   Alerts    │
              │            │    │  Dashboard  │   │  (Slack)    │
              └───────────┘    └─────────────┘   └─────────────┘
```

## Tech Stack

| Component       | Technology                          | Purpose                              |
|----------------|-------------------------------------|--------------------------------------|
| Language        | Java                                | Core application logic               |
| Framework       | Spring Boot 3.2                     | REST API, dependency injection       |
| Message Broker  | Apache Kafka 3.6                    | Event streaming, decoupling          |
| Database        | Apache Cassandra 4.1                | Distributed time-series storage      |
| Monitoring      | Prometheus + Grafana                | Metrics collection and dashboards    |
| Containerization| Docker + Docker Compose             | Local development and deployment     |
| CI/CD           | GitHub Actions                      | Automated testing and builds         |
| Testing         | JUnit 5 + Testcontainers            | Unit + integration tests             |

## Key Design Decisions

### Why Kafka?
- **Decouples** ingestion from processing — producers don't wait for consumers
- **Durable** — events survive service crashes (persisted to disk, replicated)
- **Scalable** — add more partitions and consumers for linear throughput scaling
- **Exactly-once semantics** — no duplicate processing via idempotent producers + transactional consumers

### Why Cassandra?
- **Linear write scalability** — add nodes to handle more writes, no bottleneck
- **No single point of failure** — peer-to-peer architecture, any node handles any request
- **Time-series optimized** — partition by user + time bucket, cluster by timestamp
- **Tunable consistency** — QUORUM for writes (durability), LOCAL_ONE for reads (speed)

### Why not PostgreSQL?
PostgreSQL is excellent for relational data with complex joins. EventFlow's access patterns are:
1. Write-heavy (50:1 write-to-read ratio)
2. Query by partition key (user_id + time range)
3. No joins needed
4. Horizontal scaling required

Cassandra is purpose-built for exactly these patterns.

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Java 17+ (for local development)
- Maven 3.8+ (for building)

### Run the entire stack
```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/eventflow.git
cd eventflow

# Start everything (Kafka, Cassandra, Prometheus, Grafana, and the app)
docker-compose up -d

# Wait ~30 seconds for all services to initialize, then check health
curl http://localhost:8080/health

# Send a test event
curl -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: demo-key" \
  -d '{
    "eventType": "page_view",
    "userId": "user-123",
    "payload": {
      "page": "/home",
      "referrer": "google.com",
      "duration": 45
    }
  }'

# Query events for a user
curl "http://localhost:8080/api/v1/events?userId=user-123&hours=24"

# Send a batch of events
curl -X POST http://localhost:8080/api/v1/events/batch \
  -H "Content-Type: application/json" \
  -H "X-API-Key: demo-key" \
  -d '{
    "events": [
      {"eventType": "click", "userId": "user-456", "payload": {"button": "signup"}},
      {"eventType": "page_view", "userId": "user-456", "payload": {"page": "/pricing"}}
    ]
  }'

# View Grafana dashboard
open http://localhost:3000  # admin/admin

# View Prometheus metrics
open http://localhost:9090
```

### Load Testing
```bash
# Generate 10,000 events to see the system under load
./scripts/load-test.sh
```

## API Reference

### POST /api/v1/events
Ingest a single event. Returns 202 (Accepted) immediately after Kafka publish.

### POST /api/v1/events/batch
Ingest up to 100 events in one request. More efficient than individual calls.

### GET /api/v1/events
Query stored events. Params: `userId` (required), `eventType` (optional), `hours` (default: 24).

### GET /api/v1/events/aggregations
Get pre-computed aggregations. Params: `userId`, `metric`, `hours`.

### GET /api/v1/events/anomalies
Get detected anomalies. Params: `hours` (default: 1).

### GET /health
System health check — verifies Kafka and Cassandra connectivity.

### GET /metrics
Prometheus-format metrics endpoint.

## Performance

Benchmarked on a MacBook Pro M2 (Docker, 8GB allocated):

| Metric                    | Value           |
|--------------------------|-----------------|
| Ingestion throughput      | ~8,000 events/sec |
| Kafka publish latency p50 | 2ms             |
| Kafka publish latency p99 | 12ms            |
| Cassandra write latency p50| 3ms            |
| Cassandra read latency p50 | 1ms            |
| End-to-end latency        | ~50ms           |

## Project Structure

```
eventflow/
├── src/main/java/com/eventflow/
│   ├── EventFlowApplication.java      # Spring Boot entry point
│   ├── config/
│   │   ├── KafkaConfig.java           # Kafka producer/consumer beans
│   │   ├── CassandraConfig.java       # Cassandra session and schema
│   │   └── RateLimitConfig.java       # Rate limiter configuration
│   ├── controller/
│   │   ├── EventController.java       # REST API endpoints
│   │   └── HealthController.java      # Health check endpoint
│   ├── model/
│   │   ├── Event.java                 # Core event model
│   │   ├── EventRequest.java          # API request DTO
│   │   ├── BatchEventRequest.java     # Batch ingestion DTO
│   │   └── AggregationResult.java     # Aggregation query result
│   ├── service/
│   │   ├── EventIngestionService.java # Validates and publishes to Kafka
│   │   ├── EventQueryService.java     # Reads from Cassandra
│   │   └── RateLimiterService.java    # Token bucket rate limiter
│   ├── consumer/
│   │   └── EventConsumer.java         # Kafka consumer group
│   ├── processor/
│   │   ├── EnrichmentProcessor.java   # Adds metadata to events
│   │   ├── AggregationProcessor.java  # Windowed aggregations
│   │   └── AnomalyDetector.java       # Z-score anomaly detection
│   ├── repository/
│   │   └── EventRepository.java       # Cassandra read/write operations
│   ├── metrics/
│   │   └── EventFlowMetrics.java      # Prometheus metric definitions
│   └── exception/
│       └── GlobalExceptionHandler.java # Unified error responses
├── src/main/resources/
│   ├── application.yml                 # Spring Boot configuration
│   └── cassandra-schema.cql           # Cassandra table definitions
├── src/test/java/com/eventflow/
│   ├── EventControllerTest.java       # API endpoint tests
│   ├── RateLimiterServiceTest.java    # Rate limiter unit tests
│   └── IntegrationTest.java          # Full pipeline integration test
├── docker-compose.yml                 # Full stack definition
├── Dockerfile                         # Application container
├── prometheus/prometheus.yml          # Prometheus scrape config
├── grafana/                           # Grafana provisioning
├── scripts/load-test.sh              # Load testing script
├── .github/workflows/ci.yml          # CI/CD pipeline
├── pom.xml                           # Maven dependencies
└── README.md                         # This file
```

## License
MIT
