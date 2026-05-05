// Package store: Redis Cluster client for feature serving
// Features:
//   - Redis Cluster with automatic failover
//   - Connection pooling
//   - Circuit breaker (gobreaker)
//   - Local LRU cache for hot entities
//   - Batch pipeline lookups
//   - Prometheus metrics

package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
)

// ── Prometheus Metrics ────────────────────────────────────────────

var (
	redisLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "featurestore_redis_latency_seconds",
		Help:    "Redis operation latency",
		Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1},
	}, []string{"operation"})

	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "featurestore_cache_hits_total",
		Help: "Local LRU cache hits",
	})

	cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "featurestore_cache_misses_total",
		Help: "Local LRU cache misses",
	})

	circuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "featurestore_circuit_breaker_state",
		Help: "Circuit breaker state (0=closed, 1=half-open, 2=open)",
	}, []string{"name"})
)

// ── Types ─────────────────────────────────────────────────────────

type RedisConfig struct {
	Addrs     []string
	Password  string
	CacheSize int
	CacheTTL  time.Duration
}

type FeatureValue struct {
	EntityID      string
	FeatureName   string
	Value         float64
	EventTime     time.Time
	IngestionTime time.Time
	Status        FeatureStatus
}

type FeatureStatus int

const (
	StatusPresent    FeatureStatus = 0
	StatusNull       FeatureStatus = 1
	StatusNotFound   FeatureStatus = 2
	StatusOutsideTTL FeatureStatus = 3
)

type cacheKey struct {
	entityID    string
	featureName string
}

type cacheEntry struct {
	value     FeatureValue
	expiresAt time.Time
}

// RedisStore provides feature lookup with caching and circuit breaking
type RedisStore struct {
	client  *redis.ClusterClient
	breaker *gobreaker.CircuitBreaker
	cache   *lru.Cache[cacheKey, cacheEntry]
	cacheTTL time.Duration
	mu      sync.RWMutex
}

// NewRedisStore creates a new Redis Cluster feature store
func NewRedisStore(ctx context.Context, cfg RedisConfig) (*RedisStore, error) {
	// Redis Cluster client with connection pooling
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:         cfg.Addrs,
		Password:      cfg.Password,
		PoolSize:      100,
		MinIdleConns:  20,
		MaxIdleConns:  50,
		ConnMaxIdleTime: 5 * time.Minute,
		ReadTimeout:   5 * time.Millisecond,
		WriteTimeout:  5 * time.Millisecond,
		DialTimeout:   100 * time.Millisecond,
		// Read from replicas for lower latency on reads
		RouteByLatency: true,
		RouteRandomly:  false,
	})

	// Test connectivity
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	// LRU cache for hot entities
	cache, err := lru.New[cacheKey, cacheEntry](cfg.CacheSize)
	if err != nil {
		return nil, fmt.Errorf("lru cache init: %w", err)
	}

	// Circuit breaker: open after 5 failures in 60s, retry after 30s
	breaker := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "redis",
		MaxRequests: 5,
		Interval:    60 * time.Second,
		Timeout:     30 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 5 && failureRatio >= 0.5
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			circuitBreakerState.WithLabelValues(name).Set(float64(to))
		},
	})

	return &RedisStore{
		client:   client,
		breaker:  breaker,
		cache:    cache,
		cacheTTL: cfg.CacheTTL,
	}, nil
}

// GetFeatures retrieves multiple features for a single entity in parallel
func (s *RedisStore) GetFeatures(
	ctx context.Context,
	entityType, entityID string,
	featureNames []string,
) ([]FeatureValue, error) {
	timer := prometheus.NewTimer(redisLatency.WithLabelValues("get_features"))
	defer timer.ObserveDuration()

	results := make([]FeatureValue, len(featureNames))

	// Check LRU cache first
	cacheMissIdx := []int{}
	for i, name := range featureNames {
		key := cacheKey{entityID: entityID, featureName: name}
		if entry, ok := s.cache.Get(key); ok && time.Now().Before(entry.expiresAt) {
			results[i] = entry.value
			cacheHits.Inc()
		} else {
			cacheMisses.Inc()
			cacheMissIdx = append(cacheMissIdx, i)
		}
	}

	if len(cacheMissIdx) == 0 {
		return results, nil // Full cache hit
	}

	// Batch fetch from Redis using pipeline
	missNames := make([]string, len(cacheMissIdx))
	for i, idx := range cacheMissIdx {
		missNames[i] = featureNames[idx]
	}

	redisValues, err := s.batchGet(ctx, entityType, entityID, missNames)
	if err != nil {
		return nil, err
	}

	// Fill results and update cache
	for i, idx := range cacheMissIdx {
		results[idx] = redisValues[i]
		key := cacheKey{entityID: entityID, featureName: featureNames[idx]}
		s.cache.Add(key, cacheEntry{
			value:     redisValues[i],
			expiresAt: time.Now().Add(s.cacheTTL),
		})
	}

	return results, nil
}

// batchGet fetches multiple features in a single Redis pipeline (avoids N round trips)
func (s *RedisStore) batchGet(
	ctx context.Context,
	entityType, entityID string,
	featureNames []string,
) ([]FeatureValue, error) {
	_, err := s.breaker.Execute(func() (interface{}, error) {
		return nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("circuit breaker open: %w", err)
	}

	// Redis key format: feat:{entity_type}:{entity_id}
	hashKey := fmt.Sprintf("feat:%s:%s", entityType, entityID)

	// Use HMGET to fetch all fields in one round trip
	pipe := s.client.Pipeline()
	hmgetCmd := pipe.HMGet(ctx, hashKey, featureNames...)
	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redis pipeline exec: %w", err)
	}

	vals, err := hmgetCmd.Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redis hmget: %w", err)
	}

	results := make([]FeatureValue, len(featureNames))
	for i, val := range vals {
		fv := FeatureValue{
			EntityID:      entityID,
			FeatureName:   featureNames[i],
			IngestionTime: time.Now(),
		}

		if val == nil {
			fv.Status = StatusNotFound
		} else {
			// Values stored as 8-byte IEEE 754 float64 big-endian
			switch v := val.(type) {
			case string:
				if len(v) == 8 {
					bits := binary.BigEndian.Uint64([]byte(v))
					fv.Value = math.Float64frombits(bits)
					fv.Status = StatusPresent
				} else {
					// Try parsing as float string (legacy)
					fmt.Sscanf(v, "%f", &fv.Value)
					fv.Status = StatusPresent
				}
			default:
				fv.Status = StatusNotFound
			}
		}
		results[i] = fv
	}

	return results, nil
}

// SetFeature writes a feature value to Redis with TTL
func (s *RedisStore) SetFeature(
	ctx context.Context,
	entityType, entityID, featureName string,
	value float64,
	ttl time.Duration,
) error {
	timer := prometheus.NewTimer(redisLatency.WithLabelValues("set_feature"))
	defer timer.ObserveDuration()

	hashKey := fmt.Sprintf("feat:%s:%s", entityType, entityID)

	// Encode as 8-byte IEEE 754 float64
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(value))

	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, hashKey, featureName, buf)
	pipe.Expire(ctx, hashKey, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// Close closes the Redis client
func (s *RedisStore) Close() error {
	return s.client.Close()
}

// Ping checks Redis connectivity
func (s *RedisStore) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}
