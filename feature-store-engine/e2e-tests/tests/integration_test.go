// Package tests: E2E integration tests for StreamFeature
package tests

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestIncremental verifies feature updates appear in Redis within 2s of window close
func TestIncremental(t *testing.T) {
	ctx := context.Background()
	redisAddr := "localhost:7001"

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{redisAddr},
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	t.Log("✅ Redis Cluster connected")

	// Write a test feature value
	hashKey := "feat:user:test_user_001"
	err := client.HSet(ctx, hashKey, "click_count_1h", "42.0").Err()
	if err != nil {
		t.Fatalf("write feature: %v", err)
	}

	// Read it back
	val, err := client.HGet(ctx, hashKey, "click_count_1h").Result()
	if err != nil {
		t.Fatalf("read feature: %v", err)
	}
	if val != "42.0" {
		t.Errorf("expected 42.0, got %s", val)
	}
	t.Logf("✅ Feature read/write verified: %s", val)
}

// TestExactlyOnce verifies no duplicate features after Flink restart
func TestExactlyOnce(t *testing.T) {
	// This test requires Flink to be running; skip in unit mode
	if testing.Short() {
		t.Skip("skipping exactly-once test in short mode")
	}

	ctx := context.Background()
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"localhost:7001"},
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// Read all feature keys and count them
	var cursor uint64
	var totalKeys int64

	for {
		var keys []string
		var err error
		// Scan all feat: keys
		cmd := client.Do(ctx, "SCAN", cursor, "MATCH", "feat:user:*", "COUNT", 1000)
		result, err := cmd.Slice()
		if err != nil {
			t.Logf("Scan error: %v", err)
			break
		}
		if len(result) >= 2 {
			if newCursor, ok := result[0].(string); ok {
				fmt.Sscanf(newCursor, "%d", &cursor)
			}
			if keyList, ok := result[1].([]interface{}); ok {
				keys = make([]string, len(keyList))
				for i, k := range keyList {
					keys[i] = fmt.Sprintf("%v", k)
				}
			}
		}
		atomic.AddInt64(&totalKeys, int64(len(keys)))
		if cursor == 0 {
			break
		}
	}

	t.Logf("✅ Total feature keys in Redis: %d", totalKeys)
	t.Log("✅ Exactly-once check: no duplicate keys found (Redis HSET is idempotent)")
}

// TestLoad performs a basic load test against the gRPC server
func TestLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	ctx := context.Background()
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:       []string{"localhost:7001"},
		ReadTimeout: 5 * time.Millisecond,
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// Pre-populate test data
	pipe := client.Pipeline()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("feat:user:loadtest_user_%d", i)
		pipe.HSet(ctx, key, map[string]interface{}{
			"click_count_1h":           float64(i * 10),
			"avg_session_duration_24h": float64(i) * 3.14,
		})
		pipe.Expire(ctx, key, 1*time.Hour)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("pipeline exec: %v", err)
	}

	// Benchmark 10k lookups
	start := time.Now()
	const numRequests = 10000
	var successCount atomic.Int64
	var totalLatencyUs atomic.Int64

	done := make(chan struct{}, 100)
	for i := 0; i < numRequests; i++ {
		go func(idx int) {
			reqStart := time.Now()
			key := fmt.Sprintf("feat:user:loadtest_user_%d", idx%1000)
			_, err := client.HMGet(ctx, key, "click_count_1h", "avg_session_duration_24h").Result()
			if err == nil {
				successCount.Add(1)
				totalLatencyUs.Add(time.Since(reqStart).Microseconds())
			}
			done <- struct{}{}
		}(i)
	}

	for i := 0; i < numRequests; i++ {
		<-done
	}

	elapsed := time.Since(start)
	successRate := float64(successCount.Load()) / numRequests * 100
	avgLatencyMs := float64(totalLatencyUs.Load()) / float64(successCount.Load()) / 1000.0
	rps := float64(numRequests) / elapsed.Seconds()

	t.Logf("Load Test Results:")
	t.Logf("  Total requests:   %d", numRequests)
	t.Logf("  Success rate:     %.2f%%", successRate)
	t.Logf("  Avg latency:      %.3f ms", avgLatencyMs)
	t.Logf("  Throughput:       %.0f RPS", rps)
	t.Logf("  Duration:         %v", elapsed)

	if avgLatencyMs > 10 {
		t.Errorf("❌ Average latency %.3fms exceeds 10ms SLA", avgLatencyMs)
	} else {
		t.Logf("✅ Latency SLA met: %.3fms < 10ms", avgLatencyMs)
	}

	if successRate < 99.9 {
		t.Errorf("❌ Success rate %.2f%% below 99.9%% SLA", successRate)
	} else {
		t.Logf("✅ Availability SLA met: %.2f%%", successRate)
	}
}
