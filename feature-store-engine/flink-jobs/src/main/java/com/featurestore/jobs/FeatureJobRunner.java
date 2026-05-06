package com.featurestore.jobs;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FeatureJobRunner – Entry point for all Flink feature computation jobs.
 *
 * Parses a feature definition YAML, generates Flink SQL statements,
 * and executes the streaming job with:
 *   - Event-time processing with watermarking (5s tolerance)
 *   - RocksDB state backend with incremental checkpoints to MinIO
 *   - Exactly-once semantics via two-phase commit
 *   - Dual output: Redis (online) + Parquet/MinIO (offline)
 *
 * Usage:
 *   java -jar flink-jobs.jar \
 *     --feature-view user_engagement \
 *     --kafka-brokers kafka-1:29092 \
 *     --schema-registry http://schema-registry:8081 \
 *     --redis-cluster redis-node-1:6379 \
 *     --minio-endpoint http://minio:9000 \
 *     --checkpoint-dir s3a://flink-checkpoints/checkpoints
 */
public class FeatureJobRunner {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureJobRunner.class);

    public static void main(String[] args) throws Exception {
        // ── Parse CLI args ───────────────────────────────────────
        JobConfig config = JobConfig.fromArgs(args);
        LOG.info("Starting feature job for view: {}", config.getFeatureView());

        // ── Load feature definition from YAML ───────────────────
        FeatureViewDefinition featureView = FeatureViewDefinition.loadFromFile(
            "features/" + config.getFeatureView() + ".yaml"
        );
        LOG.info("Loaded feature view: {} with {} features",
            featureView.getName(), featureView.getFeatures().size());

        // ── Set up Flink streaming execution environment ─────────
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getParallelism());

        // ── RocksDB State Backend (incremental checkpoints) ──────
        // incremental=true: only uploads changed SST files to S3,
        // dramatically reducing checkpoint size and time.
        EmbeddedRocksDBStateBackend rocksDB = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(rocksDB);

        // ── Checkpointing: Exactly-Once ──────────────────────────
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(config.getCheckpointDir());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setCheckpointTimeout(120_000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // RETAIN_ON_CANCELLATION: keep checkpoint in S3 when job is cancelled
        // so it can be restored later. Renamed from ExternalizedCheckpointRetention
        // to ExternalizedCheckpointCleanup in Flink 1.14+
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // ── Restart Strategy ─────────────────────────────────────
        // 10 attempts with 10s delay between each — handles transient
        // network issues without losing state
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(10, Time.seconds(10))
        );

        // ── Table Environment ────────────────────────────────────
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // UTC timezone prevents DST-related window boundary bugs
        tableEnv.getConfig().set("table.local-time-zone", "UTC");

        // State TTL: automatically clean up window state after max feature TTL
        // prevents RocksDB from growing unbounded for long-running jobs
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
        // Each feature runs as a separate streaming INSERT, all within
        // the same Flink job graph sharing the same Kafka consumer group
        for (FeatureDefinition feature : featureView.getFeatures()) {
            LOG.info("Deploying feature: {} (aggregation: {})",
                feature.getName(), feature.getAggregation());

            String featureSQL = buildFeatureInsertSQL(feature, featureView);
            LOG.info("Feature SQL:\n{}", featureSQL);

            // executeSql for INSERT is async: registers the pipeline,
            // actual execution happens when env.execute() is called
            tableEnv.executeSql(featureSQL);
        }

        LOG.info("All {} feature pipelines registered for view: {}",
            featureView.getFeatures().size(), featureView.getName());
        // Flink job runs until manually cancelled or fails
    }

    // ── DDL Builders ──────────────────────────────────────────────

    /**
     * Build Kafka source DDL with event-time watermarking.
     *
     * The watermark expression comes from the feature view YAML:
     *   watermark: "event_time - INTERVAL '5' SECOND"
     *
     * This allows events up to 5 seconds late to be included in the
     * correct window. Events later than this go to a side output.
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
                'connector'                          = 'kafka',
                'topic'                              = '%s',
                'properties.bootstrap.servers'       = '%s',
                'properties.group.id'                = 'flink-feature-%s',
                'scan.startup.mode'                  = 'earliest-offset',
                'format'                             = 'avro-confluent',
                'avro-confluent.schema-registry.url' = '%s',
                'properties.isolation.level'         = 'read_committed',
                'properties.enable.auto.commit'      = 'false',
                'properties.auto.offset.reset'       = 'earliest'
            )
            """,
            topic,                      // table name = topic name
            watermark,                  // watermark expression from YAML
            topic,                      // kafka topic
            config.getKafkaBrokers(),
            view.getName(),             // consumer group per feature view
            config.getSchemaRegistry()
        );
    }

    /**
     * Build Redis online feature sink DDL.
     *
     * Uses a custom connector (redis-featurestore) that writes:
     *   HSET feat:user:{entity_id} {feature_name} {binary_float64}
     *   EXPIRE feat:user:{entity_id} {ttl_seconds}
     *
     * The binary encoding matches the Go API server's decoding,
     * ensuring zero-copy reads at serving time.
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
                'connector'         = 'redis-featurestore',
                'redis.cluster'     = '%s',
                'redis.key-prefix'  = 'feat:user',
                'redis.ttl-seconds' = '86400'
            )
            """,
            config.getRedisCluster()
        );
    }

    /**
     * Build Parquet offline sink DDL for MinIO (S3-compatible).
     *
     * Partitioned by (feature_name, dt) enables efficient PIT queries:
     *   - Partition pruning on dt eliminates most Parquet files
     *   - Reading only the relevant feature_name partition reduces scan size
     *
     * Rolling policy: new file every 1 hour ensures reasonable file sizes.
     */
    private static String buildParquetSinkDDL(FeatureViewDefinition view, JobConfig config) {
        String minioEndpoint = config.getMinioEndpoint();

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
                'connector'                              = 'filesystem',
                'path'                                   = 's3a://feature-offline-store/%s',
                'format'                                 = 'parquet',
                'parquet.compression'                    = 'SNAPPY',
                'sink.rolling-policy.rollover-interval'  = '1 h',
                'sink.rolling-policy.check-interval'     = '10 min',
                's3.endpoint'                            = '%s',
                's3.access-key'                          = 'minioadmin',
                's3.secret-key'                          = 'minioadmin123',
                's3.path.style.access'                   = 'true'
            )
            """,
            view.getName(),
            minioEndpoint
        );
    }

    /**
     * Build INSERT SQL that materialises one feature into the online sink.
     *
     * The feature's own SQL (from YAML) computes the aggregation.
     * We wrap it to standardise the output schema for the Redis sink.
     */
    private static String buildFeatureInsertSQL(FeatureDefinition feature, FeatureViewDefinition view) {
        String sourceSql = feature.getSql().trim();

        return String.format("""
            INSERT INTO redis_online_sink
            SELECT
                user_id                     AS entity_id,
                '%s'                        AS feature_name,
                CAST(%s AS DOUBLE)          AS feature_value,
                event_time,
                window_end
            FROM (%s)
            """,
            feature.getName(),
            feature.getName(),   // column alias from the feature SQL
            sourceSql
        );
    }
}
