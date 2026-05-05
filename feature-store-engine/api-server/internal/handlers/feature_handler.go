// Package handlers: gRPC service handlers for FeatureService and RegistryService

package handlers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/featurestore/api-server/internal/registry"
	"github.com/featurestore/api-server/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ── Prometheus Metrics ────────────────────────────────────────────

var (
	servingLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "featurestore_serving_latency_seconds",
		Help:    "gRPC feature serving latency",
		Buckets: []float64{.0001, .0005, .001, .002, .005, .01, .05, .1, 1},
	}, []string{"method"})

	servingRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "featurestore_serving_requests_total",
		Help: "Total gRPC requests",
	}, []string{"method", "status"})

	featureFreshness = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "featurestore_feature_freshness_seconds",
		Help: "Seconds since the feature was last updated",
	}, []string{"feature_name"})
)

// ── Stub types for generated proto (until proto-gen runs) ─────────

// These are temporary stub types to make the code compile before
// protoc generates the real types. In production, replace with
// the generated pb.go types.

type GetOnlineFeaturesRequest struct {
	FeatureNames     []string
	Entities         []*Entity
	FullFeatureNames bool
}

type GetOnlineFeaturesResponse struct {
	Results  []*EntityFeatures
	Metadata *ResponseMetadata
}

type GetHistoricalFeaturesRequest struct {
	FeatureNames     []string
	EntityTimestamps []*EntityTimestamp
}

type StreamFeaturesRequest struct {
	FeatureNames []string
	Entities     []*Entity
	WindowSizeMs int64
}

type FeatureRow struct {
	Entity    *Entity
	Features  []*FeatureValueProto
	Timestamp *time.Time
	Source    string
}

type Entity struct {
	EntityType string
	EntityId   string
}

type EntityTimestamp struct {
	Entity    *Entity
	Timestamp *time.Time
}

type EntityFeatures struct {
	Entity   *Entity
	Features []*FeatureValueProto
}

type ResponseMetadata struct {
	LatencyUs   int64
	FeatureView string
	CacheHits   int32
	CacheMisses int32
}

type FeatureValueProto struct {
	FeatureName string
	DoubleVal   float64
	Status      int32
	EventTime   *time.Time
}

type HealthCheckRequest struct{}
type HealthCheckResponse struct {
	Healthy    bool
	Status     string
	Components map[string]string
}

// ── FeatureServiceServer interface ───────────────────────────────

type FeatureServiceServer interface {
	GetOnlineFeatures(ctx context.Context, req *GetOnlineFeaturesRequest) (*GetOnlineFeaturesResponse, error)
	HealthCheck(ctx context.Context, req *HealthCheckRequest) (*HealthCheckResponse, error)
}

func RegisterFeatureServiceServer(s *grpc.Server, srv FeatureServiceServer) {
	// In production this would call pb.RegisterFeatureServiceServer(s, srv)
	// The proto-generated code will handle this
}

// ── FeatureServiceHandler ─────────────────────────────────────────

type FeatureServiceHandler struct {
	store    *store.RedisStore
	registry *registry.PostgresRegistry
	logger   *zap.Logger
}

func NewFeatureServiceHandler(
	redisStore *store.RedisStore,
	reg *registry.PostgresRegistry,
	logger *zap.Logger,
) *FeatureServiceHandler {
	return &FeatureServiceHandler{
		store:    redisStore,
		registry: reg,
		logger:   logger,
	}
}

// GetOnlineFeatures serves features for multiple entities in parallel
func (h *FeatureServiceHandler) GetOnlineFeatures(
	ctx context.Context,
	req *GetOnlineFeaturesRequest,
) (*GetOnlineFeaturesResponse, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		servingLatency.WithLabelValues("GetOnlineFeatures").Observe(elapsed.Seconds())
	}()

	// Validate features exist in registry
	if err := h.registry.ValidateFeatures(req.FeatureNames); err != nil {
		servingRequests.WithLabelValues("GetOnlineFeatures", "invalid_argument").Inc()
		return nil, status.Errorf(codes.InvalidArgument, "feature validation: %v", err)
	}

	// Fetch features for all entities in parallel
	results := make([]*EntityFeatures, len(req.Entities))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i, entity := range req.Entities {
		wg.Add(1)
		go func(idx int, e *Entity) {
			defer wg.Done()

			featureVals, err := h.store.GetFeatures(ctx, e.EntityType, e.EntityId, req.FeatureNames)
			if err != nil {
				mu.Lock()
				firstErr = err
				mu.Unlock()
				return
			}

			protoFeatures := make([]*FeatureValueProto, len(featureVals))
			for j, fv := range featureVals {
				protoFeatures[j] = &FeatureValueProto{
					FeatureName: fv.FeatureName,
					DoubleVal:   fv.Value,
					Status:      int32(fv.Status),
				}
				// Track freshness metric
				if !fv.EventTime.IsZero() {
					featureFreshness.WithLabelValues(fv.FeatureName).Set(
						time.Since(fv.EventTime).Seconds(),
					)
				}
			}

			mu.Lock()
			results[idx] = &EntityFeatures{
				Entity:   e,
				Features: protoFeatures,
			}
			mu.Unlock()
		}(i, entity)
	}
	wg.Wait()

	if firstErr != nil {
		servingRequests.WithLabelValues("GetOnlineFeatures", "error").Inc()
		return nil, status.Errorf(codes.Internal, "feature lookup: %v", firstErr)
	}

	servingRequests.WithLabelValues("GetOnlineFeatures", "ok").Inc()

	return &GetOnlineFeaturesResponse{
		Results: results,
		Metadata: &ResponseMetadata{
			LatencyUs: time.Since(start).Microseconds(),
		},
	}, nil
}

// HealthCheck verifies all downstream dependencies
func (h *FeatureServiceHandler) HealthCheck(
	ctx context.Context,
	req *HealthCheckRequest,
) (*HealthCheckResponse, error) {
	components := map[string]string{}
	healthy := true

	// Check Redis
	if err := h.store.Ping(ctx); err != nil {
		components["redis"] = fmt.Sprintf("unhealthy: %v", err)
		healthy = false
	} else {
		components["redis"] = "healthy"
	}

	components["registry"] = "healthy" // Registry loaded in memory

	statusStr := "ok"
	if !healthy {
		statusStr = "degraded"
	}

	return &HealthCheckResponse{
		Healthy:    healthy,
		Status:     statusStr,
		Components: components,
	}, nil
}

// ── RegistryServiceHandler ────────────────────────────────────────

type RegistryServiceServer interface{}

func RegisterRegistryServiceServer(s *grpc.Server, srv RegistryServiceServer) {}

type RegistryServiceHandler struct {
	registry *registry.PostgresRegistry
	logger   *zap.Logger
}

func NewRegistryServiceHandler(reg *registry.PostgresRegistry, logger *zap.Logger) *RegistryServiceHandler {
	return &RegistryServiceHandler{registry: reg, logger: logger}
}

// ── gRPC Interceptors ─────────────────────────────────────────────

func LoggingInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logger.Info("gRPC request",
			zap.String("method", info.FullMethod),
			zap.Duration("latency", time.Since(start)),
			zap.Error(err),
		)
		return resp, err
	}
}

func MetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(ctx, req)
	}
}

func RecoveryInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Panic recovered", zap.Any("panic", r))
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

func StreamLoggingInterceptor(logger *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, stream)
		logger.Info("gRPC stream",
			zap.String("method", info.FullMethod),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err),
		)
		return err
	}
}
