# StreamFeature: Real-Time Feature Store Engine

> *Built from first principles. No shortcuts. Ship the engine, not the wrapper.*

A **production-grade real-time feature store** with incremental SQL materialized views, exactly-once Kafka ingestion, point-in-time correctness, and sub-10ms gRPC serving.

---

## Architecture

```
Producers → Kafka (3 brokers, KRaft) → Apache Flink (SQL, RocksDB state)
                                              ↓                  ↓
                                       Redis Cluster       MinIO Parquet
                                       (Online Store)     (Offline Store)
                                              ↓
                                        gRPC Server (Go)   ← Feature Registry (PostgreSQL)
                                              ↓
                                      ML Training / Inference
```

## Quick Start

### 1. Prerequisites
```bash
brew install go maven protobuf protoc-gen-go protoc-gen-go-grpc
# Docker + Docker Compose must be installed
```

### 2. Set API Key (for AI Dashboard)
```bash
cp .env.example .env
# Edit .env and add: OPENAI_API_KEY=sk-...
```

### 3. Start All Infrastructure
```bash
cd feature-store-engine
make infra-up
make health-check
```

### 4. Register a Feature View
```bash
make register-feature file=features/user_engagement.yaml
```

### 5. Build & Start Streaming Job
```bash
make flink-build
make flink-run feature_view=user_engagement
```

### 6. Produce Test Events
```bash
make produce-test-events count=100000 out_of_order_ratio=0.1
```

### 7. Start gRPC API Server
```bash
make api-build
make api-run
```

### 8. Query Features (grpcurl)
```bash
grpcurl -plaintext -proto proto/feature_service.proto \
  -d '{"feature_names":["click_count_1h"],"entities":[{"entity_type":"user","entity_id":"user_123"}]}' \
  localhost:50051 featurestore.FeatureService/GetOnlineFeatures
```

### 9. Generate Training Dataset (PIT-correct)
```bash
make pit-dataset entities=e2e-tests/fixtures/users.csv \
  features=click_count_1h,avg_session_duration_24h output=train.csv
```

### 10. Open Dashboards
```bash
make dashboard-up
# Grafana:    http://localhost:3000  (admin/admin123)
# Flink UI:   http://localhost:8082
# Kafka UI:   http://localhost:8080
# Jaeger:     http://localhost:16686
# MinIO:      http://localhost:9001  (minioadmin/minioadmin123)
```

### 11. Start AI Dashboard
```bash
make ai-dashboard
# → http://localhost:8888
```

---

## Project Structure

```
feature-store-engine/
├── docker-compose.yml       # All infra (Kafka, Redis, Postgres, MinIO, Flink, monitoring)
├── Makefile                 # All commands
├── proto/                   # Protobuf definitions
│   └── feature_service.proto
├── schemas/                 # Avro schemas
│   └── user_events.avsc
├── features/                # Feature view definitions (YAML)
│   └── user_engagement.yaml
├── flink-jobs/              # Apache Flink streaming jobs (Java/Maven)
│   ├── pom.xml
│   └── src/main/java/com/featurestore/
│       └── jobs/            # FeatureJobRunner, YAML parser, sink implementations
├── api-server/              # gRPC feature serving (Go)
│   ├── cmd/server/main.go
│   └── internal/
│       ├── store/           # Redis Cluster client + LRU cache + circuit breaker
│       ├── registry/        # PostgreSQL feature registry
│       └── handlers/        # gRPC handlers + interceptors
├── producer/                # Kafka event producer (Go)
│   └── cmd/producer/main.go
├── pit-engine/              # Point-in-time join engine (Go)
│   └── cmd/main.go
├── registry/                # Feature registry API + migrations
│   └── migrations/001_init.sql
├── observability/           # Prometheus + Grafana + Jaeger config
│   ├── prometheus/
│   └── grafana/
├── ai-dashboard/            # AI monitoring dashboard (FastAPI + OpenAI)
│   ├── main.py
│   └── requirements.txt
└── e2e-tests/               # Integration tests (Go)
    ├── tests/
    └── fixtures/
```

---

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Message Broker | Apache Kafka (KRaft) | 7.6.x (Confluent) |
| Stream Processor | Apache Flink | 1.18.1 LTS |
| State Backend | RocksDB (embedded) | Flink-managed |
| Online Store | Redis Cluster | 7.2 LTS |
| Offline Store | Parquet on MinIO | Latest |
| Feature Registry | PostgreSQL | 16 LTS |
| API Server | Go + gRPC | 1.22 LTS |
| AI Dashboard | FastAPI + OpenAI | GPT-4o-mini |
| Observability | Prometheus + Grafana + Jaeger | Latest stable |

---

## SLA Targets

| Metric | Target |
|--------|--------|
| End-to-end latency (event → feature) | < 5s |
| Serving p50 latency | < 2ms |
| Serving p99 latency | < 10ms |
| Throughput (serving) | > 10k RPS |
| Throughput (ingestion) | > 50k events/sec |
| Availability | > 99.9% |
| Data consistency | 100% (PIT-correct) |

---

## Run All Tests

```bash
make test-all        # Unit + integration
make load-test rps=10000 duration=5m
make verify-exactly-once
```

---

## Environment Variables

```bash
# Copy and configure
cp .env.example .env
```

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | — | Required for AI dashboard |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `REDIS_ADDRS` | `localhost:7001,...` | Redis cluster nodes |
| `POSTGRES_DSN` | see .env.example | PostgreSQL connection string |
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO S3 endpoint |
