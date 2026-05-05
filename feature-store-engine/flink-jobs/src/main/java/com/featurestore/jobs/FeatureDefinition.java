package com.featurestore.jobs;

/**
 * FeatureDefinition – A single feature within a FeatureViewDefinition.
 * Parsed from the 'features' section of the YAML file.
 */
public class FeatureDefinition {

    private String name;
    private String description;
    private String sql;
    private String aggregation;    // tumbling_window | hopping_window | session_window | global
    private String windowSize;     // e.g. "1h", "24h"
    private String hopSize;        // e.g. "1h" (for hopping windows)
    private String sessionGap;     // e.g. "30m" (for session windows)
    private String ttl;            // e.g. "24h", "7d"
    private String outputType;     // int64 | double | string
    private String redisKeyTemplate;

    // ── Getters & Setters ─────────────────────────────────────────

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getSql() { return sql; }
    public void setSql(String sql) { this.sql = sql; }

    public String getAggregation() { return aggregation; }
    public void setAggregation(String aggregation) { this.aggregation = aggregation; }

    public String getWindowSize() { return windowSize; }
    public void setWindowSize(String windowSize) { this.windowSize = windowSize; }

    public String getHopSize() { return hopSize; }
    public void setHopSize(String hopSize) { this.hopSize = hopSize; }

    public String getSessionGap() { return sessionGap; }
    public void setSessionGap(String sessionGap) { this.sessionGap = sessionGap; }

    public String getTtl() { return ttl; }
    public void setTtl(String ttl) { this.ttl = ttl; }

    public String getOutputType() { return outputType; }
    public void setOutputType(String outputType) { this.outputType = outputType; }

    public String getRedisKeyTemplate() { return redisKeyTemplate; }
    public void setRedisKeyTemplate(String redisKeyTemplate) { this.redisKeyTemplate = redisKeyTemplate; }

    // ── Computed ───────────────────────────────────────────────────

    public boolean isTumblingWindow() { return "tumbling_window".equals(aggregation); }
    public boolean isHoppingWindow()  { return "hopping_window".equals(aggregation); }
    public boolean isSessionWindow()  { return "session_window".equals(aggregation); }
    public boolean isGlobal()         { return "global".equals(aggregation); }

    @Override
    public String toString() {
        return String.format("Feature{name=%s, aggregation=%s, ttl=%s}", name, aggregation, ttl);
    }
}
