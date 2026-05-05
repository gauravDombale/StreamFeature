// Package registry: PostgreSQL-backed feature registry
// Manages feature view definitions, lineage, and schema validation

package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Types ──────────────────────────────────────────────────────────

type FeatureView struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	SQLDefinition string            `json:"sql_definition"`
	Entities      []string          `json:"entities"`
	TTLSeconds    int               `json:"ttl_seconds"`
	Owner         string            `json:"owner"`
	CreatedAt     time.Time         `json:"created_at"`
	Tags          map[string]string `json:"tags"`
	Version       string            `json:"version"`
}

type FeatureLineage struct {
	ChildFeature     string `json:"child_feature"`
	ParentSource     string `json:"parent_source"`
	TransformationType string `json:"transformation_type"`
}

// PostgresRegistry provides feature definition CRUD operations
type PostgresRegistry struct {
	pool *pgxpool.Pool
	// In-memory cache of feature views (loaded on startup)
	featureViews map[string]*FeatureView
}

// NewPostgresRegistry creates and initializes a new PostgreSQL registry
func NewPostgresRegistry(ctx context.Context, dsn string) (*PostgresRegistry, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}

	config.MaxConns = 20
	config.MinConns = 5
	config.MaxConnIdleTime = 5 * time.Minute
	config.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("postgres pool init: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}

	reg := &PostgresRegistry{
		pool:         pool,
		featureViews: make(map[string]*FeatureView),
	}

	// Load all feature views into memory on startup
	if err := reg.loadAll(ctx); err != nil {
		return nil, fmt.Errorf("load feature views: %w", err)
	}

	return reg, nil
}

// loadAll loads all feature views from PostgreSQL into memory
func (r *PostgresRegistry) loadAll(ctx context.Context) error {
	rows, err := r.pool.Query(ctx, `
		SELECT id, name, sql_definition, entities, ttl_seconds, owner, created_at, tags, version
		FROM feature_views
		ORDER BY created_at
	`)
	if err != nil {
		return fmt.Errorf("query feature views: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		fv := &FeatureView{}
		var entitiesJSON, tagsJSON []byte
		err := rows.Scan(
			&fv.ID, &fv.Name, &fv.SQLDefinition,
			&entitiesJSON, &fv.TTLSeconds, &fv.Owner,
			&fv.CreatedAt, &tagsJSON, &fv.Version,
		)
		if err != nil {
			return fmt.Errorf("scan feature view: %w", err)
		}
		json.Unmarshal(entitiesJSON, &fv.Entities)
		json.Unmarshal(tagsJSON, &fv.Tags)
		r.featureViews[fv.Name] = fv
	}
	return rows.Err()
}

// GetFeatureView returns a cached feature view by name
func (r *PostgresRegistry) GetFeatureView(name string) (*FeatureView, error) {
	fv, ok := r.featureViews[name]
	if !ok {
		return nil, fmt.Errorf("feature view not found: %s", name)
	}
	return fv, nil
}

// RegisterFeatureView inserts a new feature view into PostgreSQL
func (r *PostgresRegistry) RegisterFeatureView(ctx context.Context, fv *FeatureView) error {
	entitiesJSON, _ := json.Marshal(fv.Entities)
	tagsJSON, _ := json.Marshal(fv.Tags)

	_, err := r.pool.Exec(ctx, `
		INSERT INTO feature_views (name, sql_definition, entities, ttl_seconds, owner, tags, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (name) DO UPDATE
		SET sql_definition = EXCLUDED.sql_definition,
		    version = EXCLUDED.version,
		    tags = EXCLUDED.tags
	`, fv.Name, fv.SQLDefinition, entitiesJSON, fv.TTLSeconds, fv.Owner, tagsJSON, fv.Version)
	if err != nil {
		return fmt.Errorf("insert feature view: %w", err)
	}

	// Update in-memory cache
	r.featureViews[fv.Name] = fv
	return nil
}

// ListFeatureViews returns all registered feature views, optionally filtered by entity
func (r *PostgresRegistry) ListFeatureViews(ctx context.Context, entityFilter string) ([]*FeatureView, error) {
	var views []*FeatureView
	for _, fv := range r.featureViews {
		if entityFilter == "" {
			views = append(views, fv)
		} else {
			for _, e := range fv.Entities {
				if e == entityFilter {
					views = append(views, fv)
					break
				}
			}
		}
	}
	return views, nil
}

// GetLineage returns the data lineage for a feature
func (r *PostgresRegistry) GetLineage(ctx context.Context, featureName string) ([]FeatureLineage, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT fl.child_feature_name, fl.parent_source, fl.transformation_type
		FROM feature_lineage fl
		JOIN feature_views fv ON fl.child_feature_id = fv.id
		WHERE fv.name = $1
	`, featureName)
	if err != nil {
		return nil, fmt.Errorf("query lineage: %w", err)
	}
	defer rows.Close()

	var lineage []FeatureLineage
	for rows.Next() {
		var l FeatureLineage
		if err := rows.Scan(&l.ChildFeature, &l.ParentSource, &l.TransformationType); err != nil {
			return nil, err
		}
		lineage = append(lineage, l)
	}
	return lineage, rows.Err()
}

// ValidateFeatures checks that all requested features exist in the registry
func (r *PostgresRegistry) ValidateFeatures(featureNames []string) error {
	for _, name := range featureNames {
		found := false
		for _, fv := range r.featureViews {
			if fv.Name == name {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unknown feature: %s", name)
		}
	}
	return nil
}

// Close closes the connection pool
func (r *PostgresRegistry) Close() {
	r.pool.Close()
}
