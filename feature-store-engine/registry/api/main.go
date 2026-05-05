// Package main: Feature Registry HTTP REST API
// Provides CRUD, lineage, versioning, and impact analysis for feature views

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Config ────────────────────────────────────────────────────────

var (
	postgresDSN = getenv("POSTGRES_DSN",
		"postgres://featurestore:featurestore123@localhost:5432/featurestore?sslmode=disable")
	listenAddr = getenv("REGISTRY_ADDR", ":8090")
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ── Types ─────────────────────────────────────────────────────────

type FeatureView struct {
	ID            string            `json:"id,omitempty"`
	Name          string            `json:"name"`
	SQLDefinition string            `json:"sql_definition"`
	Entities      []string          `json:"entities"`
	TTLSeconds    int               `json:"ttl_seconds"`
	Owner         string            `json:"owner"`
	Version       string            `json:"version"`
	Tags          map[string]string `json:"tags,omitempty"`
	CreatedAt     time.Time         `json:"created_at,omitempty"`
	IsActive      bool              `json:"is_active"`
}

type Lineage struct {
	ChildFeature       string `json:"child_feature"`
	ParentSource       string `json:"parent_source"`
	TransformationType string `json:"transformation_type"`
}

type ImpactAnalysis struct {
	ChangedSource    string        `json:"changed_source"`
	AffectedFeatures []string      `json:"affected_features"`
	AffectedOwners   []string      `json:"affected_owners"`
	TotalImpact      int           `json:"total_impact"`
	Details          []FeatureView `json:"details"`
}

// ── Server ────────────────────────────────────────────────────────

type Server struct {
	db *pgxpool.Pool
}

func NewServer(ctx context.Context) (*Server, error) {
	pool, err := pgxpool.New(ctx, postgresDSN)
	if err != nil {
		return nil, fmt.Errorf("db connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("db ping: %w", err)
	}
	return &Server{db: pool}, nil
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// Feature Views CRUD
	mux.HandleFunc("GET /api/v1/features", s.listFeatures)
	mux.HandleFunc("POST /api/v1/features", s.registerFeature)
	mux.HandleFunc("GET /api/v1/features/{name}", s.getFeature)
	mux.HandleFunc("DELETE /api/v1/features/{name}", s.deleteFeature)

	// Lineage & Impact Analysis
	mux.HandleFunc("GET /api/v1/features/{name}/lineage", s.getLineage)
	mux.HandleFunc("GET /api/v1/impact", s.impactAnalysis)

	// Versioning
	mux.HandleFunc("GET /api/v1/features/{name}/versions", s.listVersions)

	// Health
	mux.HandleFunc("GET /health", s.health)

	return withLogging(withCORS(mux))
}

// ── Handlers ──────────────────────────────────────────────────────

// GET /api/v1/features?entity=user
// Lists all feature views, optionally filtered by entity type
func (s *Server) listFeatures(w http.ResponseWriter, r *http.Request) {
	entity := r.URL.Query().Get("entity")

	query := `
		SELECT id, name, sql_definition, entities, ttl_seconds, owner, version, tags, created_at, is_active
		FROM feature_views
		WHERE ($1 = '' OR entities ? $1) AND is_active = true
		ORDER BY name`

	rows, err := s.db.Query(r.Context(), query, entity)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var views []FeatureView
	for rows.Next() {
		var fv FeatureView
		var entitiesJSON, tagsJSON []byte
		if err := rows.Scan(&fv.ID, &fv.Name, &fv.SQLDefinition, &entitiesJSON,
			&fv.TTLSeconds, &fv.Owner, &fv.Version, &tagsJSON, &fv.CreatedAt, &fv.IsActive); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.Unmarshal(entitiesJSON, &fv.Entities)
		json.Unmarshal(tagsJSON, &fv.Tags)
		views = append(views, fv)
	}
	respondJSON(w, http.StatusOK, map[string]any{"features": views, "count": len(views)})
}

// POST /api/v1/features — Register or update a feature view
func (s *Server) registerFeature(w http.ResponseWriter, r *http.Request) {
	var fv FeatureView
	if err := json.NewDecoder(r.Body).Decode(&fv); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if fv.Name == "" || fv.SQLDefinition == "" {
		http.Error(w, "name and sql_definition are required", http.StatusBadRequest)
		return
	}

	entitiesJSON, _ := json.Marshal(fv.Entities)
	tagsJSON, _ := json.Marshal(fv.Tags)

	var id string
	err := s.db.QueryRow(r.Context(), `
		INSERT INTO feature_views (name, sql_definition, entities, ttl_seconds, owner, version, tags)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (name) DO UPDATE
		SET sql_definition = EXCLUDED.sql_definition,
		    version = EXCLUDED.version,
		    tags = EXCLUDED.tags,
		    updated_at = CURRENT_TIMESTAMP
		RETURNING id`,
		fv.Name, fv.SQLDefinition, entitiesJSON,
		fv.TTLSeconds, fv.Owner, fv.Version, tagsJSON,
	).Scan(&id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Record version history
	s.db.Exec(r.Context(), `
		INSERT INTO feature_versions (feature_view_id, version, sql_definition, created_by)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (feature_view_id, version) DO NOTHING`,
		id, fv.Version, fv.SQLDefinition, fv.Owner,
	)

	fv.ID = id
	respondJSON(w, http.StatusCreated, fv)
}

// GET /api/v1/features/{name}
func (s *Server) getFeature(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var fv FeatureView
	var entitiesJSON, tagsJSON []byte

	err := s.db.QueryRow(r.Context(), `
		SELECT id, name, sql_definition, entities, ttl_seconds, owner, version, tags, created_at, is_active
		FROM feature_views WHERE name = $1`, name,
	).Scan(&fv.ID, &fv.Name, &fv.SQLDefinition, &entitiesJSON,
		&fv.TTLSeconds, &fv.Owner, &fv.Version, &tagsJSON, &fv.CreatedAt, &fv.IsActive)

	if err != nil {
		http.Error(w, "feature not found: "+name, http.StatusNotFound)
		return
	}
	json.Unmarshal(entitiesJSON, &fv.Entities)
	json.Unmarshal(tagsJSON, &fv.Tags)
	respondJSON(w, http.StatusOK, fv)
}

// DELETE /api/v1/features/{name} — Soft delete (set is_active=false)
func (s *Server) deleteFeature(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cmd, err := s.db.Exec(r.Context(),
		`UPDATE feature_views SET is_active = false WHERE name = $1`, name)
	if err != nil || cmd.RowsAffected() == 0 {
		http.Error(w, "feature not found: "+name, http.StatusNotFound)
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "deactivated", "name": name})
}

// GET /api/v1/features/{name}/lineage
// Returns upstream sources that feed this feature
func (s *Server) getLineage(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	rows, err := s.db.Query(r.Context(), `
		SELECT fl.child_feature_name, fl.parent_source, fl.transformation_type
		FROM feature_lineage fl
		JOIN feature_views fv ON fl.child_feature_id = fv.id
		WHERE fv.name = $1`, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var lineage []Lineage
	for rows.Next() {
		var l Lineage
		rows.Scan(&l.ChildFeature, &l.ParentSource, &l.TransformationType)
		lineage = append(lineage, l)
	}
	respondJSON(w, http.StatusOK, map[string]any{"feature": name, "lineage": lineage})
}

// GET /api/v1/impact?source=kafka:user_events
// Impact analysis: "If I change this source, which features break?"
func (s *Server) impactAnalysis(w http.ResponseWriter, r *http.Request) {
	source := r.URL.Query().Get("source")
	if source == "" {
		http.Error(w, "source query param required (e.g. ?source=kafka:user_events)", http.StatusBadRequest)
		return
	}

	rows, err := s.db.Query(r.Context(), `
		SELECT DISTINCT fv.name, fv.owner, fv.sql_definition, fv.entities,
		                fv.ttl_seconds, fv.version, fv.tags, fv.created_at, fv.is_active
		FROM feature_lineage fl
		JOIN feature_views fv ON fl.child_feature_id = fv.id
		WHERE fl.parent_source ILIKE $1
		ORDER BY fv.name`, "%"+source+"%")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	analysis := ImpactAnalysis{ChangedSource: source}
	ownerSet := map[string]bool{}

	for rows.Next() {
		var fv FeatureView
		var entitiesJSON, tagsJSON []byte
		rows.Scan(&fv.Name, &fv.Owner, &fv.SQLDefinition, &entitiesJSON,
			&fv.TTLSeconds, &fv.Version, &tagsJSON, &fv.CreatedAt, &fv.IsActive)
		json.Unmarshal(entitiesJSON, &fv.Entities)
		json.Unmarshal(tagsJSON, &fv.Tags)

		analysis.AffectedFeatures = append(analysis.AffectedFeatures, fv.Name)
		analysis.Details = append(analysis.Details, fv)
		if fv.Owner != "" {
			ownerSet[fv.Owner] = true
		}
	}

	for owner := range ownerSet {
		analysis.AffectedOwners = append(analysis.AffectedOwners, owner)
	}
	analysis.TotalImpact = len(analysis.AffectedFeatures)

	respondJSON(w, http.StatusOK, analysis)
}

// GET /api/v1/features/{name}/versions
func (s *Server) listVersions(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	rows, err := s.db.Query(r.Context(), `
		SELECT fv2.version, fv2.created_at, fv2.created_by, fv2.change_summary
		FROM feature_versions fv2
		JOIN feature_views fv ON fv2.feature_view_id = fv.id
		WHERE fv.name = $1
		ORDER BY fv2.created_at DESC`, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type Version struct {
		Version       string    `json:"version"`
		CreatedAt     time.Time `json:"created_at"`
		CreatedBy     string    `json:"created_by"`
		ChangeSummary string    `json:"change_summary"`
	}
	var versions []Version
	for rows.Next() {
		var v Version
		rows.Scan(&v.Version, &v.CreatedAt, &v.CreatedBy, &v.ChangeSummary)
		versions = append(versions, v)
	}
	respondJSON(w, http.StatusOK, map[string]any{"feature": name, "versions": versions})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Ping(r.Context()); err != nil {
		http.Error(w, `{"status":"unhealthy","db":"error"}`, http.StatusServiceUnavailable)
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok", "db": "healthy"})
}

// ── Middleware ─────────────────────────────────────────────────────

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %v", r.Method, r.URL.Path, time.Since(start))
	})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func respondJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

// ── Main ──────────────────────────────────────────────────────────

func main() {
	ctx := context.Background()
	srv, err := NewServer(ctx)
	if err != nil {
		log.Fatalf("server init: %v", err)
	}

	log.Printf("🗂️  Feature Registry API listening on %s", listenAddr)
	log.Printf("   GET  /api/v1/features")
	log.Printf("   POST /api/v1/features")
	log.Printf("   GET  /api/v1/features/{name}/lineage")
	log.Printf("   GET  /api/v1/impact?source=kafka:user_events")
	log.Printf("   GET  /api/v1/features/{name}/versions")

	if err := http.ListenAndServe(listenAddr, srv.Routes()); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

var _ = strings.TrimSpace // keep import
