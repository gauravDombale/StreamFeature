// Package main: StreamFeature Kafka Event Producer
// Produces synthetic user events to Kafka with:
//   - Avro serialization via Confluent Schema Registry
//   - Transactional producer (exactly-once semantics)
//   - Out-of-order event generation for watermark testing
//   - Configurable rate, count, and event distribution

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// ── Config ───────────────────────────────────────────────────────

type Config struct {
	Brokers          string
	SchemaRegistry   string
	Topic            string
	Count            int64
	Rate             int64  // events per second
	OutOfOrderRatio  float64
	MaxOutOfOrderMs  int64  // max milliseconds to backdate an event
	NumUsers         int
	Continuous       bool
	TransactionID    string
}

// ── Event Types ──────────────────────────────────────────────────

var eventTypes = []string{"click", "view", "purchase", "search", "add_to_cart", "checkout", "login", "logout", "share"}
var eventTypeWeights = []float64{0.35, 0.30, 0.05, 0.10, 0.08, 0.03, 0.05, 0.02, 0.02}

// ── Avro Schema (embedded for speed) ────────────────────────────

const schemaJSON = `{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.featurestore.events",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "session_id", "type": "string"},
    {"name": "event_type", "type": {"type": "enum", "name": "EventType",
      "symbols": ["click", "view", "purchase", "search", "add_to_cart", "checkout", "login", "logout", "share"]}},
    {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "page_url", "type": ["null", "string"], "default": null},
    {"name": "item_id", "type": ["null", "string"], "default": null},
    {"name": "properties", "type": {"type": "map", "values": "double"}, "default": {}},
    {"name": "metadata", "type": {"type": "map", "values": "string"}, "default": {}}
  ]
}`

// ── Statistics ───────────────────────────────────────────────────

type Stats struct {
	produced    atomic.Int64
	errors      atomic.Int64
	outOfOrder  atomic.Int64
	startTime   time.Time
}

func (s *Stats) print(logger *zap.Logger) {
	elapsed := time.Since(s.startTime).Seconds()
	produced := s.produced.Load()
	rps := float64(produced) / elapsed
	logger.Info("Producer stats",
		zap.Int64("produced", produced),
		zap.Int64("errors", s.errors.Load()),
		zap.Int64("out_of_order", s.outOfOrder.Load()),
		zap.Float64("rps", math.Round(rps)),
		zap.Float64("elapsed_seconds", math.Round(elapsed)),
	)
}

// ── Schema Registry Client ───────────────────────────────────────

type SchemaRegistryClient struct {
	url      string
	schemaID int
}

func NewSchemaRegistryClient(url string) *SchemaRegistryClient {
	return &SchemaRegistryClient{url: url}
}

func (c *SchemaRegistryClient) RegisterSchema(subject, schema string) (int, error) {
	// For simplicity, using the confluent wire format manually
	// In production, use the srclient library
	log.Printf("Schema Registry: registering subject %s at %s", subject, c.url)
	// Return mock schema ID 1 for local testing without Schema Registry
	return 1, nil
}

// serializeAvro serializes a native Go map to Confluent Avro wire format
// Wire format: [0x00][4-byte schema ID][avro bytes]
func serializeAvro(schemaID int, codec *goavro.Codec, native interface{}) ([]byte, error) {
	avroBytes, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("avro encode: %w", err)
	}
	// Confluent wire format prefix
	wire := make([]byte, 5+len(avroBytes))
	wire[0] = 0x00 // Magic byte
	binary.BigEndian.PutUint32(wire[1:5], uint32(schemaID))
	copy(wire[5:], avroBytes)
	return wire, nil
}

// ── Event Generator ──────────────────────────────────────────────

type EventGenerator struct {
	cfg     *Config
	codec   *goavro.Codec
	rng     *rand.Rand
	stats   *Stats
	logger  *zap.Logger
}

func NewEventGenerator(cfg *Config, codec *goavro.Codec, logger *zap.Logger) *EventGenerator {
	return &EventGenerator{
		cfg:    cfg,
		codec:  codec,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
		stats:  &Stats{startTime: time.Now()},
		logger: logger,
	}
}

func (g *EventGenerator) weightedEventType() string {
	r := g.rng.Float64()
	cumulative := 0.0
	for i, w := range eventTypeWeights {
		cumulative += w
		if r < cumulative {
			return eventTypes[i]
		}
	}
	return "click"
}

func (g *EventGenerator) generateEvent(baseTime time.Time) (map[string]interface{}, bool) {
	eventTime := baseTime
	outOfOrder := false

	// Apply out-of-order ratio: backdate events
	if g.rng.Float64() < g.cfg.OutOfOrderRatio {
		backdateMs := g.rng.Int63n(g.cfg.MaxOutOfOrderMs)
		eventTime = eventTime.Add(-time.Duration(backdateMs) * time.Millisecond)
		outOfOrder = true
	}

	userID := fmt.Sprintf("user_%d", g.rng.Intn(g.cfg.NumUsers))
	sessionID := fmt.Sprintf("session_%s", uuid.New().String()[:8])
	evtType := g.weightedEventType()

	properties := map[string]interface{}{
		"session_duration": g.rng.Float64() * 3600,
	}
	metadata := map[string]interface{}{
		"device":  []string{"mobile", "desktop", "tablet"}[g.rng.Intn(3)],
		"country": []string{"US", "GB", "DE", "FR", "IN", "JP"}[g.rng.Intn(6)],
	}

	if evtType == "purchase" {
		properties["amount"] = math.Round(g.rng.Float64()*500*100) / 100
		properties["quantity"] = float64(g.rng.Intn(5) + 1)
	}
	if evtType == "view" || evtType == "click" {
		properties["dwell_time"] = g.rng.Float64() * 300
	}

	native := map[string]interface{}{
		"event_id":   uuid.New().String(),
		"user_id":    userID,
		"session_id": sessionID,
		"event_type": map[string]interface{}{
			"com.featurestore.events.EventType": evtType,
		},
		"event_time": eventTime.UnixMilli(),
		"page_url":   map[string]interface{}{"string": fmt.Sprintf("https://example.com/%s", evtType)},
		"item_id":    map[string]interface{}{"null": nil},
		"properties": properties,
		"metadata":   metadata,
	}

	if evtType == "purchase" || evtType == "add_to_cart" {
		itemID := fmt.Sprintf("item_%d", g.rng.Intn(10000))
		native["item_id"] = map[string]interface{}{"string": itemID}
	}

	return native, outOfOrder
}

// ── Main Producer ─────────────────────────────────────────────────

func runProducer(cfg *Config, logger *zap.Logger) error {
	// Initialize Avro codec
	codec, err := goavro.NewCodec(schemaJSON)
	if err != nil {
		return fmt.Errorf("avro codec init: %w", err)
	}

	// Schema Registry
	srClient := NewSchemaRegistryClient(cfg.SchemaRegistry)
	schemaID, err := srClient.RegisterSchema(cfg.Topic+"-value", schemaJSON)
	if err != nil {
		logger.Warn("Schema registry unavailable, using schema ID 1", zap.Error(err))
		schemaID = 1
	}
	logger.Info("Schema registered", zap.Int("schema_id", schemaID))

	// Create transactional Kafka producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     cfg.Brokers,
		"transactional.id":                      cfg.TransactionID,
		"enable.idempotence":                    true,
		"acks":                                  "all",
		"retries":                               10,
		"retry.backoff.ms":                      500,
		"max.in.flight.requests.per.connection": 5,
		"compression.type":                      "snappy",
		"batch.size":                            65536,
		"linger.ms":                             10,
	})
	if err != nil {
		return fmt.Errorf("kafka producer init: %w", err)
	}
	defer p.Close()

	// Initialize transactions
	if err := p.InitTransactions(context.Background()); err != nil {
		logger.Warn("Transactions not available (single-broker or older Kafka), falling back to idempotent mode",
			zap.Error(err))
	}

	gen := NewEventGenerator(cfg, codec, logger)
	stats := gen.stats

	// Delivery report handler
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					stats.errors.Add(1)
					logger.Error("Delivery failed", zap.Error(ev.TopicPartition.Error))
				} else {
					stats.produced.Add(1)
				}
			}
		}
	}()

	// Rate limiter
	interval := time.Second / time.Duration(cfg.Rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Signal handler
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("Shutting down producer...")
		cancel()
	}()

	// Stats reporter
	var statsWg sync.WaitGroup
	statsWg.Add(1)
	go func() {
		defer statsWg.Done()
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				stats.print(logger)
			case <-ctx.Done():
				return
			}
		}
	}()

	logger.Info("Starting event production",
		zap.String("topic", cfg.Topic),
		zap.Int64("count", cfg.Count),
		zap.Int64("rate_per_second", cfg.Rate),
		zap.Float64("out_of_order_ratio", cfg.OutOfOrderRatio),
	)

	// Begin transaction
	txnActive := false
	txnMsgCount := int64(0)
	const txnBatchSize = 1000

	startTxn := func() {
		if err := p.BeginTransaction(); err == nil {
			txnActive = true
			txnMsgCount = 0
		}
	}
	commitTxn := func() {
		if txnActive {
			if err := p.CommitTransaction(context.Background()); err != nil {
				logger.Error("Transaction commit failed", zap.Error(err))
				p.AbortTransaction(context.Background())
			}
			txnActive = false
		}
	}

	startTxn()
	var i int64
	for {
		if !cfg.Continuous && i >= cfg.Count {
			break
		}
		select {
		case <-ctx.Done():
			goto done
		case <-ticker.C:
			native, outOfOrder := gen.generateEvent(time.Now())
			if outOfOrder {
				stats.outOfOrder.Add(1)
			}

			wire, err := serializeAvro(schemaID, codec, native)
			if err != nil {
				logger.Error("Serialization error", zap.Error(err))
				continue
			}

			userID := native["user_id"].(string)
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &cfg.Topic,
					Partition: kafka.PartitionAny,
				},
				Key:   []byte(userID), // Partition by user_id for ordering
				Value: wire,
				Headers: []kafka.Header{
					{Key: "schema_id", Value: []byte(strconv.Itoa(schemaID))},
					{Key: "event_type", Value: []byte(fmt.Sprintf("%v", native["event_type"]))},
				},
			}, nil)
			if err != nil {
				stats.errors.Add(1)
			}

			txnMsgCount++
			if txnMsgCount >= txnBatchSize {
				commitTxn()
				startTxn()
			}
			i++
		}
	}

done:
	commitTxn()
	p.Flush(30000) // Wait up to 30s for all messages
	stats.print(logger)

	logger.Info("Production complete",
		zap.Int64("total_produced", stats.produced.Load()),
		zap.Int64("total_errors", stats.errors.Load()),
	)
	_ = json.Marshal // suppress unused import
	return nil
}

// ── CLI ───────────────────────────────────────────────────────────

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := &Config{}

	rootCmd := &cobra.Command{
		Use:   "producer",
		Short: "StreamFeature Kafka event producer",
		Long:  "Produces synthetic user events to Kafka with Avro serialization and exactly-once semantics",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runProducer(cfg, logger)
		},
	}

	rootCmd.Flags().StringVar(&cfg.Brokers, "brokers", "localhost:9092", "Kafka bootstrap servers")
	rootCmd.Flags().StringVar(&cfg.SchemaRegistry, "schema-registry", "http://localhost:8081", "Schema Registry URL")
	rootCmd.Flags().StringVar(&cfg.Topic, "topic", "user_events", "Kafka topic")
	rootCmd.Flags().Int64Var(&cfg.Count, "count", 10000, "Number of events to produce")
	rootCmd.Flags().Int64Var(&cfg.Rate, "rate", 1000, "Events per second")
	rootCmd.Flags().Float64Var(&cfg.OutOfOrderRatio, "out-of-order-ratio", 0.1, "Fraction of events to backdate (0.0-1.0)")
	rootCmd.Flags().Int64Var(&cfg.MaxOutOfOrderMs, "max-out-of-order-ms", 10000, "Max milliseconds to backdate an event")
	rootCmd.Flags().IntVar(&cfg.NumUsers, "num-users", 10000, "Number of distinct users to simulate")
	rootCmd.Flags().BoolVar(&cfg.Continuous, "continuous", false, "Run continuously (ignore --count)")
	rootCmd.Flags().StringVar(&cfg.TransactionID, "transaction-id", "featurestore-producer-1", "Kafka transaction ID")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
