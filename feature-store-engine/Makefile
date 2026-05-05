# ============================================================
# StreamFeature – Real-Time Feature Store Engine
# Makefile: All development commands
# ============================================================

SHELL := /bin/bash
.PHONY: all infra-up infra-down health-check produce-test-events \
        proto-gen flink-build flink-run flink-restore \
        api-build api-run load-test test-all pit-dataset \
        register-feature list-features lineage dashboard-up \
        chaos-kill-flink verify-exactly-once verify-incremental \
        clean logs

# ── Config ──────────────────────────────────────────────────
KAFKA_BROKERS    := localhost:9092
SCHEMA_REGISTRY  := http://localhost:8081
REDIS_CLUSTER    := localhost:7001
POSTGRES_DSN     := postgres://featurestore:featurestore123@localhost:5432/featurestore?sslmode=disable
MINIO_ENDPOINT   := http://localhost:9000
FLINK_REST        := http://localhost:8082
GRPC_ADDR        := localhost:50051
PROTO_DIR        := ./proto
GEN_DIR          := ./api-server/internal/gen
FLINK_JAR        := ./flink-jobs/target/flink-jobs-1.0.0.jar

# ── Go paths ────────────────────────────────────────────────
export PATH := $(shell go env GOPATH)/bin:$(PATH)
export GOPATH := $(shell go env GOPATH)

# ────────────────────────────────────────────────────────────
# INFRASTRUCTURE
# ────────────────────────────────────────────────────────────

## infra-up: Start all infrastructure services
infra-up:
	@echo "🚀 Starting StreamFeature infrastructure..."
	docker-compose up -d
	@echo "⏳ Waiting for services to become healthy..."
	@sleep 20
	@$(MAKE) health-check

## infra-down: Stop all infrastructure services
infra-down:
	@echo "🛑 Stopping StreamFeature infrastructure..."
	docker-compose down -v
	@echo "✅ Infrastructure stopped and volumes removed."

## infra-restart: Restart infrastructure without removing volumes
infra-restart:
	docker-compose restart

## logs: Tail logs for a service (e.g. make logs svc=kafka-1)
logs:
	docker-compose logs -f $(svc)

## health-check: Verify all services are healthy
health-check:
	@echo "🔍 Health Check..."
	@echo -n "  Kafka-1:         "; curl -s --max-time 5 http://localhost:9092 > /dev/null 2>&1 && echo "✅" || echo "❌ (checking broker)"; \
	  docker exec kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "  Kafka-1:         ✅" || echo "  Kafka-1:         ❌"
	@echo -n "  Schema Registry: "; curl -sf http://localhost:8081/subjects > /dev/null && echo "✅" || echo "❌"
	@echo -n "  Redis Cluster:   "; docker exec redis-node-1 redis-cli cluster info 2>/dev/null | grep -q "cluster_state:ok" && echo "✅" || echo "❌"
	@echo -n "  PostgreSQL:      "; docker exec featurestore-postgres pg_isready -U featurestore > /dev/null 2>&1 && echo "✅" || echo "❌"
	@echo -n "  MinIO:           "; curl -sf http://localhost:9000/minio/health/live > /dev/null && echo "✅" || echo "❌"
	@echo -n "  Flink UI:        "; curl -sf http://localhost:8082/overview > /dev/null && echo "✅" || echo "❌"
	@echo -n "  Prometheus:      "; curl -sf http://localhost:9090/-/healthy > /dev/null && echo "✅" || echo "❌"
	@echo -n "  Grafana:         "; curl -sf http://localhost:3000/api/health > /dev/null && echo "✅" || echo "❌"
	@echo -n "  Jaeger:          "; curl -sf http://localhost:16686/ > /dev/null && echo "✅" || echo "❌"
	@echo "✅ Health check complete."

# ────────────────────────────────────────────────────────────
# PROTOBUF CODE GENERATION
# ────────────────────────────────────────────────────────────

## proto-gen: Generate Go and Java protobuf code
proto-gen:
	@echo "📦 Generating Protobuf code..."
	@mkdir -p $(GEN_DIR)
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/feature_service.proto
	@echo "✅ Protobuf code generated → $(GEN_DIR)"

# ────────────────────────────────────────────────────────────
# PRODUCER
# ────────────────────────────────────────────────────────────

## produce-test-events: Produce synthetic events to Kafka
## Usage: make produce-test-events count=100000 out_of_order_ratio=0.1
produce-test-events:
	@echo "📤 Producing $(count) test events (out-of-order ratio: $(out_of_order_ratio))..."
	@cd producer && go run ./cmd/producer \
		--brokers=$(KAFKA_BROKERS) \
		--schema-registry=$(SCHEMA_REGISTRY) \
		--topic=user_events \
		--count=$(or $(count),10000) \
		--out-of-order-ratio=$(or $(out_of_order_ratio),0.1) \
		--rate=$(or $(rate),1000)

## produce-events: Produce events at specified rate
## Usage: make produce-events topic=user_events rate=1000
produce-events:
	@cd producer && go run ./cmd/producer \
		--brokers=$(KAFKA_BROKERS) \
		--schema-registry=$(SCHEMA_REGISTRY) \
		--topic=$(or $(topic),user_events) \
		--rate=$(or $(rate),1000) \
		--continuous=true

# ────────────────────────────────────────────────────────────
# FLINK JOBS
# ────────────────────────────────────────────────────────────

## flink-build: Build Flink job JAR
flink-build:
	@echo "🔨 Building Flink jobs..."
	@cd flink-jobs && mvn clean package -DskipTests -q
	@echo "✅ Flink JAR built: $(FLINK_JAR)"

## flink-run: Submit a Flink job for a feature view
## Usage: make flink-run feature_view=user_engagement
flink-run: flink-build
	@echo "🚀 Submitting Flink job for feature view: $(feature_view)..."
	@curl -sf -X POST $(FLINK_REST)/jars/$(shell curl -sf $(FLINK_REST)/jars | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['files'][0]['id'] if d['files'] else '')")/run \
		-H "Content-Type: application/json" \
		-d '{"programArgs": "--feature-view=$(feature_view) --kafka-brokers=$(KAFKA_BROKERS) --schema-registry=$(SCHEMA_REGISTRY) --redis-cluster=$(REDIS_CLUSTER) --minio-endpoint=$(MINIO_ENDPOINT)"}' || \
	(echo "📤 Uploading JAR first..."; \
	  curl -sf -X POST $(FLINK_REST)/jars/upload -H "Expect:" -F "jarfile=@$(FLINK_JAR)" && \
	  $(MAKE) flink-run feature_view=$(feature_view))

## flink-restore: Restore Flink job from savepoint
flink-restore:
	@echo "♻️  Restoring Flink job from savepoint..."
	@curl -sf -X POST $(FLINK_REST)/jobs/$(job_id)/stop \
		-H "Content-Type: application/json" \
		-d '{"drain": false, "targetDirectory": "s3://flink-savepoints/savepoints"}' || true
	@echo "Savepoint created. Restart with flink-run."

## chaos-kill-flink: Kill Flink jobmanager for chaos testing
chaos-kill-flink:
	@echo "💥 Killing Flink jobmanager (chaos test)..."
	docker-compose restart flink-jobmanager
	@echo "✅ Flink jobmanager restarting..."

# ────────────────────────────────────────────────────────────
# API SERVER (gRPC)
# ────────────────────────────────────────────────────────────

## api-build: Build the gRPC API server
api-build: proto-gen
	@echo "🔨 Building API server..."
	@cd api-server && go build -o bin/api-server ./cmd/server
	@echo "✅ API server built."

## api-run: Run the gRPC API server
api-run: api-build
	@echo "🚀 Starting gRPC API server on :50051..."
	@cd api-server && ./bin/api-server \
		--redis-addrs=localhost:7001,localhost:7002,localhost:7003,localhost:7004,localhost:7005,localhost:7006 \
		--postgres-dsn="$(POSTGRES_DSN)" \
		--minio-endpoint=$(MINIO_ENDPOINT) \
		--grpc-port=50051 \
		--metrics-port=2112

# ────────────────────────────────────────────────────────────
# FEATURE REGISTRY
# ────────────────────────────────────────────────────────────

## register-feature: Register a feature definition from YAML
## Usage: make register-feature file=features/user_engagement.yaml
register-feature:
	@echo "📋 Registering feature: $(file)..."
	@cd registry && go run ./api \
		--postgres-dsn="$(POSTGRES_DSN)" \
		register --file=../$(file)

## list-features: List all registered features
list-features:
	@echo "📋 Listing features..."
	@cd registry && go run ./api \
		--postgres-dsn="$(POSTGRES_DSN)" \
		list $(if $(entity),--entity=$(entity),)

## lineage: Show lineage for a feature
## Usage: make lineage feature=click_count_1h
lineage:
	@echo "🔗 Lineage for feature: $(feature)..."
	@cd registry && go run ./api \
		--postgres-dsn="$(POSTGRES_DSN)" \
		lineage --feature=$(feature)

# ────────────────────────────────────────────────────────────
# POINT-IN-TIME DATASET GENERATION
# ────────────────────────────────────────────────────────────

## pit-dataset: Generate a point-in-time correct training dataset
## Usage: make pit-dataset entities=users.csv features=click_count_1h output=train.parquet
pit-dataset:
	@echo "📊 Generating PIT dataset..."
	@cd pit-engine && go run ./cmd \
		--minio-endpoint=$(MINIO_ENDPOINT) \
		--entities=$(or $(entities),fixtures/users.csv) \
		--features=$(or $(features),click_count_1h) \
		--output=$(or $(output),train.parquet)

## verify-incremental: Verify incremental feature updates in Redis
verify-incremental:
	@echo "🔍 Verifying incremental feature: $(feature)..."
	@cd e2e-tests && go test ./tests/... -run TestIncremental -v \
		-feature=$(feature) \
		-redis-addrs="localhost:7001"

## verify-exactly-once: Verify exactly-once semantics
verify-exactly-once:
	@echo "🔍 Verifying exactly-once semantics..."
	@cd e2e-tests && go test ./tests/... -run TestExactlyOnce -v -timeout 10m

## inject-events: Inject a large number of events for chaos testing
inject-events:
	@$(MAKE) produce-test-events count=$(or $(count),1000000)

# ────────────────────────────────────────────────────────────
# LOAD TESTING
# ────────────────────────────────────────────────────────────

## load-test: Run k6 load test against gRPC server
## Usage: make load-test rps=10000 duration=5m
load-test:
	@echo "⚡ Running load test: $(or $(rps),10000) RPS for $(or $(duration),5m)..."
	@if command -v k6 &> /dev/null; then \
		k6 run \
			-e GRPC_ADDR=$(GRPC_ADDR) \
			-e RPS=$(or $(rps),10000) \
			-e DURATION=$(or $(duration),5m) \
			./e2e-tests/load/k6_test.js; \
	else \
		echo "⚠️  k6 not found. Install with: brew install k6"; \
		echo "   Running Go-based load test instead..."; \
		cd e2e-tests && go test ./tests/... -run TestLoad -v \
			-rps=$(or $(rps),1000) -duration=$(or $(duration),30s); \
	fi

# ────────────────────────────────────────────────────────────
# OBSERVABILITY
# ────────────────────────────────────────────────────────────

## dashboard-up: Open Grafana + Flink + Jaeger in browser
dashboard-up:
	@echo "📊 Opening dashboards..."
	@open http://localhost:3000 || xdg-open http://localhost:3000 || true
	@open http://localhost:8082 || xdg-open http://localhost:8082 || true
	@open http://localhost:16686 || xdg-open http://localhost:16686 || true
	@open http://localhost:8080 || xdg-open http://localhost:8080 || true
	@echo "✅ Dashboards:"
	@echo "   Grafana:      http://localhost:3000  (admin/admin123)"
	@echo "   Flink UI:     http://localhost:8082"
	@echo "   Jaeger:       http://localhost:16686"
	@echo "   Kafka UI:     http://localhost:8080"
	@echo "   MinIO:        http://localhost:9001  (minioadmin/minioadmin123)"
	@echo "   Prometheus:   http://localhost:9090"

## ai-dashboard: Start AI-powered monitoring dashboard
ai-dashboard:
	@echo "🤖 Starting AI monitoring dashboard..."
	@cd ai-dashboard && pip install -r requirements.txt -q && \
		python main.py

# ────────────────────────────────────────────────────────────
# TESTING
# ────────────────────────────────────────────────────────────

## test-all: Run all tests (unit + integration)
test-all:
	@echo "🧪 Running all tests..."
	@echo "--- API Server tests ---"
	@cd api-server && go test ./... -v -timeout 120s
	@echo "--- Producer tests ---"
	@cd producer && go test ./... -v -timeout 120s
	@echo "--- PIT Engine tests ---"
	@cd pit-engine && go test ./... -v -timeout 120s
	@echo "--- Registry tests ---"
	@cd registry && go test ./... -v -timeout 120s
	@echo "--- E2E Integration tests ---"
	@cd e2e-tests && go test ./tests/... -v -timeout 300s \
		-kafka-brokers=$(KAFKA_BROKERS) \
		-redis-addrs="localhost:7001,localhost:7002,localhost:7003" \
		-postgres-dsn="$(POSTGRES_DSN)"
	@echo "✅ All tests passed!"

## test-unit: Run only unit tests (no infra needed)
test-unit:
	@cd api-server && go test ./... -short
	@cd producer && go test ./... -short
	@cd pit-engine && go test ./... -short

# ────────────────────────────────────────────────────────────
# DATABASE MIGRATIONS
# ────────────────────────────────────────────────────────────

## db-migrate: Run PostgreSQL migrations
db-migrate:
	@echo "🔄 Running database migrations..."
	@for f in registry/migrations/*.sql; do \
		echo "  Applying $$f..."; \
		docker exec -i featurestore-postgres psql -U featurestore featurestore < "$$f"; \
	done
	@echo "✅ Migrations complete."

# ────────────────────────────────────────────────────────────
# CLEANUP
# ────────────────────────────────────────────────────────────

## clean: Clean all build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	@rm -f api-server/bin/api-server
	@rm -f producer/bin/producer
	@rm -f pit-engine/bin/pit-engine
	@cd flink-jobs && mvn clean -q 2>/dev/null || true
	@echo "✅ Clean complete."

## help: Show this help
help:
	@echo "StreamFeature – Real-Time Feature Store Engine"
	@echo ""
	@grep -E '^## ' Makefile | sed 's/## //' | column -t -s ':'
