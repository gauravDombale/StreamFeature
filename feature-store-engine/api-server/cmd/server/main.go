// Package main: StreamFeature gRPC API Server
// Serves features with p99 < 10ms at 10k RPS via Redis Cluster

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/featurestore/api-server/internal/handlers"
	"github.com/featurestore/api-server/internal/registry"
	"github.com/featurestore/api-server/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// ServerConfig holds all configuration for the API server
type ServerConfig struct {
	RedisAddrs    []string
	PostgresDSN   string
	MinioEndpoint string
	GRPCPort      int
	MetricsPort   int
	CacheSize     int
	CacheTTL      time.Duration
}

func runServer(cfg *ServerConfig, logger *zap.Logger) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── Redis Store ───────────────────────────────────────────────
	logger.Info("Connecting to Redis Cluster", zap.Strings("addrs", cfg.RedisAddrs))
	redisStore, err := store.NewRedisStore(ctx, store.RedisConfig{
		Addrs:    cfg.RedisAddrs,
		CacheSize: cfg.CacheSize,
		CacheTTL:  cfg.CacheTTL,
	})
	if err != nil {
		return fmt.Errorf("redis store init: %w", err)
	}
	defer redisStore.Close()
	logger.Info("Redis Cluster connected")

	// ── Feature Registry ──────────────────────────────────────────
	logger.Info("Connecting to PostgreSQL registry", zap.String("dsn", cfg.PostgresDSN))
	reg, err := registry.NewPostgresRegistry(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("registry init: %w", err)
	}
	defer reg.Close()
	logger.Info("Feature registry connected")

	// ── gRPC Server ───────────────────────────────────────────────
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(16*1024*1024),
		grpc.MaxSendMsgSize(16*1024*1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Second,
			MaxConnectionAge:      30 * time.Second,
			MaxConnectionAgeGrace: 5 * time.Second,
			Time:                  5 * time.Second,
			Timeout:               1 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.ChainUnaryInterceptor(
			handlers.LoggingInterceptor(logger),
			handlers.MetricsInterceptor(),
			handlers.RecoveryInterceptor(logger),
		),
		grpc.ChainStreamInterceptor(
			handlers.StreamLoggingInterceptor(logger),
		),
	)

	// Register feature service handler
	featureHandler := handlers.NewFeatureServiceHandler(redisStore, reg, logger)
	handlers.RegisterFeatureServiceServer(grpcServer, featureHandler)

	// Register registry service handler
	registryHandler := handlers.NewRegistryServiceHandler(reg, logger)
	handlers.RegisterRegistryServiceServer(grpcServer, registryHandler)

	// Enable gRPC reflection (for grpcurl)
	reflection.Register(grpcServer)

	// ── Start gRPC listener ───────────────────────────────────────
	grpcAddr := fmt.Sprintf(":%d", cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("gRPC listen: %w", err)
	}

	go func() {
		logger.Info("gRPC server starting", zap.String("addr", grpcAddr))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	// ── Prometheus Metrics Server ─────────────────────────────────
	metricsAddr := fmt.Sprintf(":%d", cfg.MetricsPort)
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	metricsServer := &http.Server{
		Addr:    metricsAddr,
		Handler: metricsMux,
	}
	go func() {
		logger.Info("Metrics server starting", zap.String("addr", metricsAddr))
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics server error", zap.Error(err))
		}
	}()

	logger.Info("StreamFeature API server ready",
		zap.String("grpc", grpcAddr),
		zap.String("metrics", metricsAddr),
	)

	// ── Graceful Shutdown ─────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down gracefully...")
	grpcServer.GracefulStop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	metricsServer.Shutdown(shutdownCtx)
	logger.Info("Server stopped")

	return nil
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := &ServerConfig{}
	var redisAddrs string

	rootCmd := &cobra.Command{
		Use:   "api-server",
		Short: "StreamFeature gRPC API Server",
		Long:  "Low-latency gRPC serving layer for real-time feature retrieval (p99 < 10ms)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.RedisAddrs = strings.Split(redisAddrs, ",")
			return runServer(cfg, logger)
		},
	}

	rootCmd.Flags().StringVar(&redisAddrs, "redis-addrs",
		"localhost:7001,localhost:7002,localhost:7003,localhost:7004,localhost:7005,localhost:7006",
		"Redis cluster node addresses (comma-separated)")
	rootCmd.Flags().StringVar(&cfg.PostgresDSN, "postgres-dsn",
		"postgres://featurestore:featurestore123@localhost:5432/featurestore?sslmode=disable",
		"PostgreSQL DSN for feature registry")
	rootCmd.Flags().StringVar(&cfg.MinioEndpoint, "minio-endpoint",
		"http://localhost:9000", "MinIO S3 endpoint")
	rootCmd.Flags().IntVar(&cfg.GRPCPort, "grpc-port", 50051, "gRPC server port")
	rootCmd.Flags().IntVar(&cfg.MetricsPort, "metrics-port", 2112, "Prometheus metrics port")
	rootCmd.Flags().IntVar(&cfg.CacheSize, "cache-size", 10000, "Local LRU cache size (entities)")
	rootCmd.Flags().DurationVar(&cfg.CacheTTL, "cache-ttl", 1*time.Second, "Local cache TTL")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
