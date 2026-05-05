# StreamFeature: Architecture Deep Dive

> *Built from first principles. No shortcuts. Ship the engine, not the wrapper.*

## Table of Contents

1. [System Overview](#system-overview)
2. [Data Flow](#data-flow)
3. [Component Architecture](#component-architecture)
4. [Incremental View Maintenance](#incremental-view-maintenance)
5. [Point-in-Time Correctness](#point-in-time-correctness)
6. [Exactly-Once Guarantees](#exactly-once-guarantees)
7. [Backfill Strategy](#backfill-strategy)
8. [Serving Layer Design](#serving-layer-design)
9. [State Management](#state-management)
10. [Observability Architecture](#observability-architecture)
11. [Design Decisions & Trade-offs](#design-decisions--trade-offs)

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    StreamFeature Architecture                         │
│                                                                       │
│  ┌──────────┐    ┌──────────────────┐    ┌─────────────────────────┐│
│  │Producers │───▶│  Kafka Cluster   │───▶│   Apache Flink 1.18     ││
│  │  (Go)    │    │  3 brokers KRaft │    │   SQL / Table API       ││
│  └──────────┘    │  RF=3, EOS txns  │    │   RocksDB state backend ││
│                  └──────────────────┘    └───────────┬─────────────┘│
│                           │                          │               │
│                  ┌────────▼──────┐        ┌──────────▼──────────┐   │
│                  │Schema Registry│        │                     │   │
│                  │  (Confluent)  │        ├── Redis Cluster ────┤   │
│                  └───────────────┘        │   6 nodes (online)  │   │
│                                           │                     │   │
│                                           ├── MinIO/Parquet ────┤   │
│                                           │   (offline store)   │   │
│                                           └──────────┬──────────┘   │
│                                                      │               │
│  ┌────────────────────────────────────────┐          │               │
│  │         gRPC API Server (Go)           │◀─────────┘               │
│  │  • Connection pool to Redis Cluster    │                          │
│  │  • Local LRU cache (hot entities)      │     ┌─────────────────┐ │
│  │  • Circuit breaker (gobreaker)         │◀────│Feature Registry │ │
│  │  • HMGET pipeline batching             │     │  (PostgreSQL 16)│ │
│  │  • Parallel goroutines per entity      │     └─────────────────┘ │
│  └─────────────────┬──────────────────────┘                          │
│                    │                                                  │
│  ┌─────────────────▼──────────────────────┐                          │
│  │           ML Clients                   │                          │
│  │  Training (PIT engine) / Inference     │                          │
│  └────────────────────────────────────────┘                          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Streaming Path (Online Features)

```
Event occurs
    │
    ▼
Producer (Go)
    ├─ Avro-serialize with Schema Registry wire format
    ├─ Partition by entity_id (per-entity ordering)
    └─ Publish with Kafka transaction (exactly-once)
    │
    ▼
Kafka Topic (user_events)
    ├─ 3 replicas, min ISR=2
    └─ Retention: 7 days
    │
    ▼
Flink SQL Job
    ├─ Kafka source with Avro-Confluent deserialization
    ├─ Assign event_time watermarks (-5s tolerance)
    ├─ Window operations:
    │   ├─ TUMBLE(1h): click_count_1h, purchase_count_24h
    │   ├─ HOP(1h/24h): avg_session_duration_24h
    │   └─ Global: total_clicks_lifetime
    └─ EXACTLY_ONCE checkpoint to MinIO every 60s
    │
    ├── Online Sink ──▶ Redis Cluster
    │                   HSET feat:user:{id} {feature} {binary_float64}
    │                   EXPIRE {ttl_seconds}
    │
    └── Offline Sink ─▶ MinIO Parquet
                        s3a://feature-offline-store/{view}/
                        Partitioned by: feature_name/dt
```

### Serving Path (Inference)

```
gRPC GetOnlineFeatures(entities=[user_123], features=[click_count_1h])
    │
    ▼
Feature Registry validation (in-memory, loaded at startup)
    │
    ▼
Local LRU cache check (1s TTL, 10k entries)
    ├─ HIT  → return cached value (sub-microsecond)
    └─ MISS → Redis HMGET pipeline
                │
                ├─ Circuit breaker check (gobreaker)
                ├─ Parallel goroutine per entity
                └─ Binary float64 decode (IEEE 754 big-endian)
                │
                ▼
            Return FeatureValue with status, event_time
```

### Training Path (PIT-Correct Dataset)

```
Training request: (entity_id, lookup_timestamp, label)
    │
    ▼
PIT Engine (pit-engine/cmd/main.go)
    │
    ├─ Scan MinIO Parquet: s3a://feature-offline-store/{view}/
    ├─ Filter: event_time <= lookup_timestamp  ← STRICT leakage prevention
    ├─ Select: MAX(ingestion_time) per (entity_id, feature_name, window_end)
    └─ Return most recent value before lookup_timestamp
    │
    ▼
Training dataset (CSV/Parquet)
    ├─ entity_id, timestamp, label
    └─ feature_1, feature_2, ... (all PIT-correct)
```

---

## Component Architecture

### Kafka Cluster (3-Broker KRaft)

- **Mode**: KRaft (no ZooKeeper) — Kafka 3.7+
- **Replication**: Factor 3, min ISR=2
- **Exactly-once**: Producer transactions + `read_committed` consumers
- **Partitioning**: By `entity_id` key — guarantees per-entity event ordering
- **Schema**: Confluent Schema Registry (Avro) with backward compatibility

### Apache Flink 1.18 (Incremental MV Engine)

- **Execution mode**: Streaming (online), Batch (backfill)
- **State backend**: EmbeddedRocksDB with `incremental=true`
- **Checkpointing**: EXACTLY_ONCE, 60s interval, externalized to MinIO
- **Restart strategy**: Fixed-delay (10 attempts, 10s delay)
- **Watermark**: `event_time - INTERVAL '5' SECOND`
- **State TTL**: Configurable per feature view (prevents unbounded growth)

### Redis Cluster (Online Store)

- **Topology**: 6 nodes — 3 masters + 3 replicas
- **Persistence**: AOF (appendfsync=everysec) + RDB snapshots
- **Key format**: `feat:{entity_type}:{entity_id}`
- **Value encoding**: IEEE 754 float64, big-endian binary (8 bytes)
  - Why: avoids string parsing overhead; consistent between Go (encoding/binary)
    and Java (ByteBuffer.putDouble)
- **TTL**: Set per-feature on every write; prevents stale data accumulation
- **Failover**: Redis Cluster automatic failover < 5s

### MinIO / Parquet (Offline Store)

- **Format**: Apache Parquet + Snappy compression
- **Schema**: `(entity_id, feature_name, feature_value, event_time, ingestion_time, window_end, dt)`
- **Partitioning**: `feature_name / dt` — enables partition pruning on PIT queries
- **Buckets**:
  - `feature-offline-store/`: Feature log (append-only per Flink job)
  - `flink-checkpoints/`: Incremental RocksDB state snapshots
  - `flink-savepoints/`: Manual savepoints for controlled restarts

### PostgreSQL 16 (Feature Registry)

- **Tables**:
  - `feature_views`: Feature definitions (SQL, entities, TTL, owner, version)
  - `feature_lineage`: Upstream source tracking
  - `feature_versions`: Immutable version history
  - `feature_serving_stats`: Usage metrics per service
  - `feature_acls`: Access control (read/write/admin per service)
- **Pattern**: Loaded into memory on server startup → zero-latency lookups
- **Indexes**: GIN on JSONB columns (entities, tags) for flexible filtering

---

## Incremental View Maintenance

The core insight: **never recompute from scratch**. Flink maintains incremental state.

### Tumbling Windows (click_count_1h)

```
Event stream:  [u1,t=0:01] [u1,t=0:15] [u1,t=0:45] [u1,t=1:02] ...
                                         ↑                          ↑
                                    Window close               Window close
                                  (emit count=3)             (emit count=1)

State (RocksDB):
  Key: (user_1, window_2024-01-01T00:00:00Z)
  Value: {count: 3, window_end: 2024-01-01T01:00:00Z}

On window close:
  1. Emit row to Redis sink (HSET feat:user:u1 click_count_1h "3.0")
  2. State entry expires per TTL
```

### Hopping Windows (avg_session_duration_24h)

```
Window size: 24h, hop: 1h
At any point in time: 24 overlapping windows per user

State (RocksDB): 24 partial aggregates per user
On hop boundary: emit updated average for all windows containing that hop
```

### Watermarks & Late Data

```
Flink watermark = max(event_time seen) - 5 seconds

Event arrives 3s late  → event_time within watermark → processed correctly ✅
Event arrives 10s late → event_time before watermark → routed to side output ⚠️
                                                        (logged, not lost)
```

---

## Point-in-Time Correctness

The fundamental guarantee: **no feature value with `event_time > lookup_timestamp`**.

```python
# Wrong (causes training/serving skew):
feature_value = offline_store.get_latest(entity_id, feature_name)

# Right (PIT-correct):
feature_value = offline_store.get_as_of(
    entity_id, feature_name,
    as_of_timestamp=training_row.timestamp  # STRICT: event_time <= this
)
```

### Why This Matters

If a model trains on features that include "future" data relative to the label's
timestamp, it will appear to perform well in training but fail in production
where only historical features are available.

The `pit-engine` enforces this by scanning all Parquet rows and **discarding**
any row where `event_time > lookup_timestamp`, maintaining a counter of blocked
rows for audit.

---

## Exactly-Once Guarantees

Three-layer defense:

| Layer | Mechanism | Guarantee |
|-------|-----------|-----------|
| Kafka Producer | `transactional.id` + `enable.idempotence=true` | No duplicate messages in Kafka |
| Flink→Redis | Two-phase commit: checkpoint barrier + HSET atomicity | No partial window writes |
| Offline Store | Deduplication via `ROW_NUMBER() OVER (PARTITION BY entity_id, feature_name, window_end)` | Idempotent re-runs |

### Failure Recovery

```
1. Flink job crashes mid-checkpoint
   → RocksDB state rolled back to last committed checkpoint
   → Kafka offsets reset to checkpoint position
   → Re-processing from checkpoint (duplicate Redis writes absorbed by HSET)

2. Redis master fails
   → Redis Cluster auto-promotes replica (<5s)
   → In-flight gRPC requests: circuit breaker opens, returns cached values
   → New master accepts writes after promotion

3. Backfill running concurrently with streaming
   → Both write same (entity_id, feature_name) key via idempotent HSET
   → Streaming job always has the most recent value (higher ingestion_time)
   → Offline store: ROW_NUMBER deduplication keeps latest ingestion_time
```

---

## Backfill Strategy

Run historical recomputation **without stopping the stream**:

```
Time:      ─────────────────────────────────────────────────────▶
                                                                now
           [─────── Historical (Parquet) ───────][── Streaming ──]
                           │                           │
                     Batch Flink job            Streaming Flink job
                    (RuntimeMode.BATCH)        (RuntimeMode.STREAMING)
                           │                           │
                           └──────────┬────────────────┘
                                      │
                                 Redis Cluster
                              (idempotent HSET)
                                      │
                                 MinIO Parquet
                            (deduplication applied)
```

**Deduplication key**: `(entity_id, feature_name, window_end)`
**Strategy**: `ROW_NUMBER() OVER (... ORDER BY ingestion_time DESC)` keeps latest

---

## Serving Layer Design

### Latency Budget (p99 < 10ms)

| Operation | Typical Time |
|-----------|-------------|
| gRPC framework overhead | ~0.1ms |
| Feature registry lookup (in-memory) | ~0.01ms |
| LRU cache check | ~0.01ms |
| Redis HMGET (cluster, 1 hop) | ~0.5–2ms |
| Protobuf serialization | ~0.1ms |
| **Total p50** | **~1ms** |
| **Total p99** | **~5ms** |

### Connection Architecture

```
gRPC Server
    │
    ├─ Redis Cluster Client (go-redis/v9)
    │   ├─ Pool: 100 connections, 20 min-idle
    │   ├─ RouteByLatency: true (reads prefer closest node)
    │   ├─ ReadTimeout: 5ms (fail fast)
    │   └─ WriteTimeout: 5ms
    │
    ├─ Local LRU Cache (hashicorp/golang-lru/v2)
    │   ├─ Size: 10,000 entities
    │   ├─ TTL: 1s (prevents stale data accumulation)
    │   └─ Hit rate target: >70% for hot entities
    │
    └─ Circuit Breaker (sony/gobreaker)
        ├─ Opens after: 50% failure rate over 5+ requests in 60s
        ├─ Half-open: 5 probe requests after 30s
        └─ On open: returns cached values (graceful degradation)
```

---

## State Management

### RocksDB Configuration

```
state.backend: rocksdb
state.backend.incremental: true          # only upload delta to S3
state.backend.rocksdb.localdir: /tmp/flink-rocksdb
table.exec.state.ttl: {max_feature_ttl}  # auto-clean expired window state
```

### State Size Estimation

```
Per user, per feature:
  - Tumbling window (1h): 1 partial aggregate = ~100 bytes
  - Hopping window (24h/1h): 24 partial aggregates = ~2.4 KB
  - Global: 1 running aggregate = ~50 bytes

10,000 users × 5 features × avg 500 bytes = ~25 MB RocksDB state
Incremental checkpoint to S3: only changed SST files (~1–5 MB/checkpoint)
```

---

## Observability Architecture

```
Flink / Redis / Go Server
         │
         │ Prometheus /metrics endpoint
         ▼
    Prometheus (scrape every 5–15s)
         │
         ├─ Alertmanager → PagerDuty / Slack
         │
         ▼
    Grafana Dashboard (auto-refresh 5s)
         │
         ▼
    AI Dashboard (FastAPI + GPT-4o-mini)
         ├─ Polls Prometheus every 5s
         ├─ Auto-analysis on SLA breach
         └─ Natural language query interface
```

### Key Metrics

| Metric | PromQL |
|--------|--------|
| Serving p99 | `histogram_quantile(0.99, rate(featurestore_serving_latency_seconds_bucket[5m]))` |
| Feature freshness | `max(featurestore_feature_freshness_seconds)` |
| Cache hit rate | `rate(cache_hits_total[5m]) / (rate(cache_hits_total[5m]) + rate(cache_misses_total[5m]))` |
| Circuit breaker | `featurestore_circuit_breaker_state{name="redis"}` |
| Kafka consumer lag | `kafka_consumer_group_lag` |

---

## Design Decisions & Trade-offs

### Why Flink over Spark Structured Streaming?

| Aspect | Flink | Spark SS |
|--------|-------|----------|
| Latency | Sub-second | Micro-batch (1–10s) |
| State backend | Native RocksDB | External (Delta) |
| Exactly-once | Native | Requires external coordination |
| SQL support | Full (including session windows) | Limited |

**Decision**: Flink for streaming, Python/DuckDB for ad-hoc offline queries.

### Why Redis Cluster over Cassandra?

- **Latency**: Redis p50 ~0.5ms vs Cassandra ~2–5ms
- **Cluster failover**: Redis <5s vs Cassandra eventual consistency
- **Cost**: Redis Cluster is simpler to operate locally
- **Trade-off**: Redis memory cost; mitigated by TTLs and binary encoding

### Why Binary float64 in Redis?

- `HSET feat:user:u1 click_count_1h "42.5"` (string) = 10+ bytes + parsing
- `HSET feat:user:u1 click_count_1h <8_bytes>` (binary) = 8 bytes, zero-copy decode
- ~20% reduction in Redis memory, ~30% faster reads
- Same encoding in Go (`encoding/binary`) and Java (`ByteBuffer.putDouble`)

### Why Parquet + MinIO over a Database for Offline Store?

- **Cost**: Parquet on object storage is ~100× cheaper than RDS
- **PIT queries**: Column pruning + partition pruning makes PIT scans efficient
- **Flink native**: Flink writes Parquet directly without an extra ETL layer
- **Trade-off**: No ACID updates; solved by deduplication + ROW_NUMBER pattern
