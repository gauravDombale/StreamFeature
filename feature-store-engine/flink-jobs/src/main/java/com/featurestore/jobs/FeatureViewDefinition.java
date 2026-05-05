package com.featurestore.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * FeatureViewDefinition – Parsed representation of a feature view YAML file.
 *
 * Maps to the YAML structure defined in features/user_engagement.yaml
 */
public class FeatureViewDefinition {

    private String featureView;
    private String version;
    private String owner;
    private String description;
    private List<EntityDefinition> entities;
    private SourceDefinition source;
    private List<FeatureDefinition> features;
    private SinksDefinition sinks;
    private CheckpointingDefinition checkpointing;

    // ── Loader ──────────────────────────────────────────────────

    public static FeatureViewDefinition loadFromFile(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        FeatureViewDefinition def = mapper.readValue(new File(path), FeatureViewDefinition.class);
        return def;
    }

    // ── Computed properties ──────────────────────────────────────

    public String getName() { return featureView; }

    /** Returns the maximum TTL across all features in milliseconds */
    public long getMaxTtlMs() {
        return features.stream()
            .mapToLong(f -> parseTtlMs(f.getTtl()))
            .max()
            .orElse(86_400_000L);
    }

    private static long parseTtlMs(String ttl) {
        if (ttl == null) return 86_400_000L;
        if (ttl.endsWith("h")) return Long.parseLong(ttl.replace("h", "")) * 3_600_000L;
        if (ttl.endsWith("d")) return Long.parseLong(ttl.replace("d", "")) * 86_400_000L;
        if (ttl.endsWith("m")) return Long.parseLong(ttl.replace("m", "")) * 60_000L;
        return Long.parseLong(ttl) * 1000L;
    }

    // ── Getters ──────────────────────────────────────────────────

    public String getFeatureView() { return featureView; }
    public void setFeatureView(String featureView) { this.featureView = featureView; }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public List<EntityDefinition> getEntities() { return entities; }
    public void setEntities(List<EntityDefinition> entities) { this.entities = entities; }

    public SourceDefinition getSource() { return source; }
    public void setSource(SourceDefinition source) { this.source = source; }

    public List<FeatureDefinition> getFeatures() { return features; }
    public void setFeatures(List<FeatureDefinition> features) { this.features = features; }

    public SinksDefinition getSinks() { return sinks; }
    public void setSinks(SinksDefinition sinks) { this.sinks = sinks; }

    public CheckpointingDefinition getCheckpointing() { return checkpointing; }
    public void setCheckpointing(CheckpointingDefinition checkpointing) { this.checkpointing = checkpointing; }

    // ── Nested Classes ────────────────────────────────────────────

    public static class EntityDefinition {
        private String name;
        private String type;
        private String description;
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
    }

    public static class SourceDefinition {
        private String type;
        private String topic;
        private String schemaRegistry;
        private String schema;
        private String format;
        private String watermark;
        private String bootstrapServers;
        private String consumerGroup;
        private String startupMode;

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getSchemaRegistry() { return schemaRegistry; }
        public void setSchemaRegistry(String schemaRegistry) { this.schemaRegistry = schemaRegistry; }
        public String getWatermark() { return watermark; }
        public void setWatermark(String watermark) { this.watermark = watermark; }
        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
        public String getConsumerGroup() { return consumerGroup; }
        public void setConsumerGroup(String consumerGroup) { this.consumerGroup = consumerGroup; }
    }

    public static class SinksDefinition {
        private Map<String, Object> online;
        private Map<String, Object> offline;
        public Map<String, Object> getOnline() { return online; }
        public void setOnline(Map<String, Object> online) { this.online = online; }
        public Map<String, Object> getOffline() { return offline; }
        public void setOffline(Map<String, Object> offline) { this.offline = offline; }
    }

    public static class CheckpointingDefinition {
        private long intervalMs;
        private String mode;
        private String storage;
        public long getIntervalMs() { return intervalMs; }
        public void setIntervalMs(long intervalMs) { this.intervalMs = intervalMs; }
        public String getMode() { return mode; }
        public void setMode(String mode) { this.mode = mode; }
        public String getStorage() { return storage; }
        public void setStorage(String storage) { this.storage = storage; }
    }
}
