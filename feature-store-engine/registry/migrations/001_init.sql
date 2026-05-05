-- StreamFeature: PostgreSQL Schema Migrations
-- Feature Registry & Metadata Store
-- Run order: 001_init.sql

-- ─────────────────────────────────────────────────────────────────
-- Extensions
-- ─────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";   -- Full-text search on feature names

-- ─────────────────────────────────────────────────────────────────
-- Feature Views Table
-- Stores all registered feature view definitions
-- ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS feature_views (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(255) UNIQUE NOT NULL,
    sql_definition  TEXT NOT NULL,
    entities        JSONB NOT NULL DEFAULT '[]',
    ttl_seconds     INT DEFAULT 86400,
    owner           VARCHAR(255),
    version         VARCHAR(50) DEFAULT '1.0.0',
    tags            JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active       BOOLEAN DEFAULT TRUE,
    
    CONSTRAINT feature_views_name_check CHECK (name ~ '^[a-z][a-z0-9_]*$'),
    CONSTRAINT feature_views_ttl_check  CHECK (ttl_seconds > 0)
);

CREATE INDEX IF NOT EXISTS idx_feature_views_name ON feature_views(name);
CREATE INDEX IF NOT EXISTS idx_feature_views_owner ON feature_views(owner);
CREATE INDEX IF NOT EXISTS idx_feature_views_entities ON feature_views USING gin(entities);
CREATE INDEX IF NOT EXISTS idx_feature_views_tags ON feature_views USING gin(tags);

-- ─────────────────────────────────────────────────────────────────
-- Feature Lineage Table
-- Tracks data lineage: which sources feed which features
-- ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS feature_lineage (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    child_feature_id    UUID REFERENCES feature_views(id) ON DELETE CASCADE,
    child_feature_name  VARCHAR(255),
    parent_source       VARCHAR(255) NOT NULL,  -- e.g. "kafka:user_events"
    transformation_type VARCHAR(50) NOT NULL,   -- e.g. "tumbling_window", "hopping_window"
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_lineage_child ON feature_lineage(child_feature_id);
CREATE INDEX IF NOT EXISTS idx_lineage_parent ON feature_lineage(parent_source);

-- ─────────────────────────────────────────────────────────────────
-- Feature Versions Table
-- Immutable record of all feature definition versions
-- ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS feature_versions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feature_view_id UUID REFERENCES feature_views(id) ON DELETE CASCADE,
    version         VARCHAR(50) NOT NULL,
    sql_definition  TEXT NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by      VARCHAR(255),
    change_summary  TEXT,
    
    UNIQUE (feature_view_id, version)
);

CREATE INDEX IF NOT EXISTS idx_feature_versions_view ON feature_versions(feature_view_id);

-- ─────────────────────────────────────────────────────────────────
-- Feature Serving Stats Table
-- Tracks usage patterns for ACL enforcement and optimization
-- ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS feature_serving_stats (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feature_name    VARCHAR(255) NOT NULL,
    service_name    VARCHAR(255) NOT NULL,
    request_count   BIGINT DEFAULT 0,
    error_count     BIGINT DEFAULT 0,
    p50_latency_us  FLOAT,
    p99_latency_us  FLOAT,
    recorded_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_serving_stats_feature ON feature_serving_stats(feature_name);
CREATE INDEX IF NOT EXISTS idx_serving_stats_time ON feature_serving_stats(recorded_at);

-- ─────────────────────────────────────────────────────────────────
-- Feature ACLs Table
-- Access control: which services can access which features
-- ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS feature_acls (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feature_view_id UUID REFERENCES feature_views(id) ON DELETE CASCADE,
    service_name    VARCHAR(255) NOT NULL,
    permission      VARCHAR(20) NOT NULL DEFAULT 'read',  -- read | write | admin
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE (feature_view_id, service_name)
);

-- ─────────────────────────────────────────────────────────────────
-- Trigger: Update updated_at on feature_views changes
-- ─────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_feature_views_updated_at
    BEFORE UPDATE ON feature_views
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ─────────────────────────────────────────────────────────────────
-- Seed Data: Register initial feature views
-- ─────────────────────────────────────────────────────────────────

INSERT INTO feature_views (name, sql_definition, entities, ttl_seconds, owner, version, tags)
VALUES (
    'user_engagement',
    'SELECT user_id, COUNT(*) AS click_count_1h FROM user_events WHERE event_type = ''click'' GROUP BY user_id, TUMBLE(event_time, INTERVAL ''1'' HOUR)',
    '["user_id"]'::jsonb,
    86400,
    'ml-platform-team',
    '1.0.0',
    '{"domain": "engagement", "team": "ml-platform"}'::jsonb
) ON CONFLICT (name) DO NOTHING;

INSERT INTO feature_lineage (child_feature_id, child_feature_name, parent_source, transformation_type)
SELECT id, 'click_count_1h', 'kafka:user_events', 'tumbling_window'
FROM feature_views WHERE name = 'user_engagement'
ON CONFLICT DO NOTHING;

-- ─────────────────────────────────────────────────────────────────
-- Views for easy querying
-- ─────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW feature_catalog AS
SELECT
    fv.id,
    fv.name,
    fv.owner,
    fv.ttl_seconds,
    fv.version,
    fv.entities,
    fv.tags,
    fv.is_active,
    fv.created_at,
    COUNT(fl.id) AS upstream_sources
FROM feature_views fv
LEFT JOIN feature_lineage fl ON fl.child_feature_id = fv.id
GROUP BY fv.id, fv.name, fv.owner, fv.ttl_seconds, fv.version, fv.entities, fv.tags, fv.is_active, fv.created_at;

-- ─────────────────────────────────────────────────────────────────
-- Helpful queries (commented for reference)
-- ─────────────────────────────────────────────────────────────────

-- List all features:
-- SELECT * FROM feature_catalog;

-- Get lineage for a feature:
-- SELECT fl.parent_source, fl.transformation_type
-- FROM feature_lineage fl
-- JOIN feature_views fv ON fl.child_feature_id = fv.id
-- WHERE fv.name = 'user_engagement';

-- Impact analysis: what breaks if kafka:user_events changes?
-- SELECT DISTINCT fv.name, fl.transformation_type
-- FROM feature_lineage fl
-- JOIN feature_views fv ON fl.child_feature_id = fv.id
-- WHERE fl.parent_source = 'kafka:user_events';
