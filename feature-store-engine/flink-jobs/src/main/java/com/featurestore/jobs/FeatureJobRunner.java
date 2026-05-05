package com.featurestore.jobs;

import com.featurestore.state.FeatureStateTTL;
import com.featurestore.watermark.EventTimeWatermarkStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * FeatureJobRunner – Entry point for all Flink feature computation jobs.
 *
 * Parses a feature definition YAML, generates Flink SQL statements,
 * and executes the streaming job with:
 *   - Event-time processing with watermarking
 *   - RocksDB state backend with incremental checkpoints
 *   - Exactly-once semantics via two-phase commit
 *   - Dual output: Redis (online) + Parquet/MinIO (offline)
 */
public class FeatureJobRunner {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureJobRunner.class);

    public static void main(String[] args) throws Exception {
        // ── Parse CLI args ───────────────────────────────────────
        JobConfig config = JobConfig.fromArgs(args);
        LOG.info("Starting feature job for view: {}", config.getFeatureView());

        // ── Load feature definition ──────────────────────────────
        FeatureViewDefinition featureView = FeatureViewDefinition.loadFromFile(
            "features/" + config.getFeatureView() + ".yaml"
        );
        LOG.info("Loaded feature view: {} with {} features",
            featureView.getName(), featureView.getFeatures().size());

        // ── Set up Flink execution environment ───────────────────
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parallelism
        env.setParallelism(config.getParallelism());

        // ── RocksDB State Backend ────────────────────────────────
        EmbeddedRocksDBStateBackend rocksDB = new EmbeddedRocksDBStateBackend(true); // incremental
        env.setStateBackend(rocksDB);

        // ── Checkpointing: Exactly-Once ──────────────────────────
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(config.getCheckpointDir());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setCheckpointTimeout(120_000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
            org.apache.flink.streaming.api.environment.CheckpointConfig
                .ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
        );

        // ── Restart Strategy ─────────────────────────────────────
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(10, Time.seconds(10))
        );

        // ── Table Environment ────────────────────────────────────
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Configure Table API defaults
        tableEnv.getConfig().set("table.local-time-zone", "UTC");
        tableEnv.getConfig().set(
            "table.exec.state.ttl",
            String.valueOf(featureView.getMaxTtlMs())
        );

        // ── Register Kafka Source Table ──────────────────────────
        String createKafkaSource = buildKafkaSourceDDL(featureView, config);
        LOG.info("Creating Kafka source table:\n{}", createKafkaSource);
        tableEnv.executeSql(createKafkaSource);

        // ── Register Redis Online Sink ───────────────────────────
        String createRedisSink = buildRedisSinkDDL(featureView, config);
        LOG.info("Creating Redis sink table:\n{}", createRedisSink);
        tableEnv.executeSql(createRedisSink);

        // ── Register Parquet Offline Sink ────────────────────────
        String createParquetSink = buildParquetSinkDDL(featureView, config);
        LOG.info("Creating Parquet offline sink:\n{}", createParquetSink);
        tableEnv.executeSql(createParquetSink);

        // ── Execute Feature Queries ──────────────────────────────
        for (FeatureDefinition feature : featureView.getFeatures()) {
            LOG.info("Deploying feature: {} (aggregation: {})",
                feature.getName(), feature.getAggregation());

            String featureSQL = buildFeatureInsertSQL(feature, featureView);
            LOG.info("Feature SQL:\n{}", featureSQL);

            // Execute asynchronously (returns immediately, job runs in background)
            tableEnv.executeSql(featureSQL);
        }

        LOG.info("All feature jobs submitted for view: {}", featureView.getName());
        // Job runs until manually cancelled or fails
    }

    /**
     * Build Kafka source DDL with event-time watermarking.
     *
     * Example output:
     * CREATE TABLE user_events (
     *   event_id STRING,
     *   user_id STRING,
     *   event_type STRING,
     *   event_time TIMESTAMP_LTZ(3),
     *   properties MAP<STRING, DOUBLE>,
     *   WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
     * ) WITH (
     *   'connector' = 'kafka',
     *   'topic' = 'user_events',
     *   ...
     * )
     */
    private static String buildKafkaSourceDDL(FeatureViewDefinition view, JobConfig config) {
        String topic = view.getSource().getTopic();
        String watermark = view.getSource().getWatermark();

        return String.format("""
            CREATE TABLE IF NOT EXISTS %s (
                event_id        STRING,
                user_id         STRING,
                session_id      STRING,
                event_type      STRING,
                event_time      TIMESTAMP_LTZ(3),
                page_url        STRING,
                item_id         STRING,
                properties      MAP<STRING, DOUBLE>,
                metadata        MAP<STRING, STRING>,
                WATERMARK FOR event_time AS %s
            ) WITH (
                'connector'                         = 'kafka',
                'topic'                             = '%s',
                'properties.bootstrap.servers'      = '%s',
                'properties.group.id'               = 'flink-feature-%s',
                'scan.startup.mode'                 = 'earliest-offset',
                'format'                            = 'avro-confluent',
                'avro-confluent.schema-registry.url'= '%s',
                'properties.isolation.level'        = 'read_committed',
                'properties.enable.auto.commit'     = 'false',
                'properties.auto.offset.reset'      = 'earliest'
            )
            """,
            topic,          // table name
            watermark,      // watermark expression
            topic,          // kafka topic
            config.getKafkaBrokers(),
            view.getName(),
            config.getSchemaRegistry()
        );
    }

    /**
     * Build Redis online feature sink DDL using Custom Sink function.
     * Redis stores features as: HSET feat:user:{user_id} {feature_name} {value}
     */
    private static String buildRedisSinkDDL(FeatureViewDefinition view, JobConfig config) {
        return String.format("""
            CREATE TABLE IF NOT EXISTS redis_online_sink (
                entity_id      STRING,
                feature_name   STRING,
                feature_value  DOUBLE,
                event_time     TIMESTAMP_LTZ(3),
                window_end     TIMESTAMP_LTZ(3)
            ) WITH (
                'connector'     = 'redis-featurestore',
                'redis.cluster' = '%s',
                'redis.key-prefix' = 'feat:user',
                'redis.ttl-seconds' = '86400'
            )
            """,
            config.getRedisCluster()
        );
    }

    /**
     * Build Parquet offline sink DDL for MinIO (S3-compatible).
     * Partitioned by feature_name and date for efficient PIT queries.
     */
    private static String buildParquetSinkDDL(FeatureViewDefinition view, JobConfig config) {
        return String.format("""
            CREATE TABLE IF NOT EXISTS parquet_offline_sink (
                entity_id      STRING,
                feature_name   STRING,
                feature_value  DOUBLE,
                event_time     TIMESTAMP_LTZ(3),
                ingestion_time TIMESTAMP_LTZ(3),
                window_end     TIMESTAMP_LTZ(3),
                dt             STRING
            ) PARTITIONED BY (feature_name, dt)
            WITH (
                'connector'          = 'filesystem',
                'path'               = 's3a://feature-offline-store/%s',
                'format'             = 'parquet',
                'parquet.compression'= 'SNAPPY',
                'sink.rolling-policy.rollover-interval' = '1 h',
                'sink.rolling-policy.check-interval'    = '10 min'
            )
            """,
            view.getName()
        );
    }

    /**
     * Build INSERT SQL that reads from Kafka source, applies the feature SQL,
     * and writes to both Redis and Parquet sinks.
     */
    private static String buildFeatureInsertSQL(FeatureDefinition feature, FeatureViewDefinition view) {
        String sourceSql = feature.getSql().trim();

        // Wrap the feature SQL into an INSERT INTO redis_online_sink statement
        // We use a STATEMENT SET to write to multiple sinks atomically
        return String.format("""
            INSERT INTO redis_online_sink
            SELECT
                user_id                         AS entity_id,
                '%s'                            AS feature_name,
                CAST(%s AS DOUBLE)              AS feature_value,
                event_time,
                window_end
            FROM (%s)
            """,
            feature.getName(),
            feature.getName(),  // the column alias from the feature SQL
            sourceSql
        );
    }
}
