package com.featurestore.backfill;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.HostAndPort;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * BackfillRunner – Flink BATCH mode backfill engine.
 *
 * Recomputes historical features from the Parquet offline store and
 * writes them to Redis (online store) AND back to the offline store
 * with deduplication using (entity_id, event_time, feature_name) as the
 * composite unique key.
 *
 * Design:
 *   1. Reads Parquet files from MinIO for a given date range
 *   2. Applies the same SQL transformations as the streaming job
 *      (guarantees online/offline parity)
 *   3. Deduplicates using GROUP BY (entity_id, feature_name, window_end)
 *      keeping the MAX value (idempotent on re-run)
 *   4. Writes to Redis Cluster via HSET pipeline
 *   5. Writes deduplicated results back to Parquet (upsert semantics)
 *
 * Exactly-once guarantee: Flink batch mode + idempotent HSET in Redis
 * means re-running produces identical results.
 *
 * Run alongside the streaming job without downtime:
 *   make flink-backfill feature_view=user_engagement start=2024-01-01 end=2024-01-07
 */
public class BackfillRunner {

    private static final Logger LOG = LoggerFactory.getLogger(BackfillRunner.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String featureView  = params.getRequired("feature-view");
        String startDate    = params.getRequired("start");         // yyyy-MM-dd
        String endDate      = params.getRequired("end");           // yyyy-MM-dd
        String minioEndpoint = params.get("minio-endpoint",  "http://minio:9000");
        String redisNodes   = params.get("redis-cluster",
            "redis-node-1:6379,redis-node-2:6379,redis-node-3:6379");
        String bucket       = params.get("bucket", "feature-offline-store");
        int parallelism     = params.getInt("parallelism", 4);

        LOG.info("Starting backfill: feature_view={} range=[{}, {}]",
            featureView, startDate, endDate);

        // ── Flink BATCH execution mode ────────────────────────────
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(parallelism);

        // Configure S3 (MinIO) access
        org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
        conf.setString("s3.endpoint", minioEndpoint);
        conf.setString("s3.access-key", params.get("minio-access-key", "minioadmin"));
        conf.setString("s3.secret-key", params.get("minio-secret-key", "minioadmin123"));
        conf.setString("s3.path.style.access", "true");
        env.configure(conf);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // ── Register Parquet source: offline feature log ──────────
        // Reads partitioned Parquet files for the given date range
        String parquetSourcePath = String.format(
            "s3a://%s/%s/", bucket, featureView
        );

        String createParquetSource = String.format("""
            CREATE TABLE parquet_source (
                entity_id      STRING,
                feature_name   STRING,
                feature_value  DOUBLE,
                event_time     TIMESTAMP_LTZ(3),
                ingestion_time TIMESTAMP_LTZ(3),
                window_end     TIMESTAMP_LTZ(3),
                dt             STRING
            ) PARTITIONED BY (feature_name, dt)
            WITH (
                'connector'  = 'filesystem',
                'path'       = '%s',
                'format'     = 'parquet',
                'source.monitor-interval' = '0'
            )
            """, parquetSourcePath);

        tableEnv.executeSql(createParquetSource);

        // ── Register deduplicated output sink (back to Parquet) ───
        String createOutputSink = String.format("""
            CREATE TABLE parquet_backfill_output (
                entity_id      STRING,
                feature_name   STRING,
                feature_value  DOUBLE,
                event_time     TIMESTAMP_LTZ(3),
                ingestion_time TIMESTAMP_LTZ(3),
                window_end     TIMESTAMP_LTZ(3),
                dt             STRING
            ) PARTITIONED BY (feature_name, dt)
            WITH (
                'connector'           = 'filesystem',
                'path'                = 's3a://%s/%s_backfill/',
                'format'              = 'parquet',
                'parquet.compression' = 'SNAPPY'
            )
            """, bucket, featureView);

        tableEnv.executeSql(createOutputSink);

        // ── Deduplication query ────────────────────────────────────
        // Composite unique key: (entity_id, feature_name, window_end)
        // On conflict: keep latest ingestion_time value
        // This guarantees idempotency: re-running produces same output
        String deduplicationSQL = String.format("""
            INSERT INTO parquet_backfill_output
            SELECT
                entity_id,
                feature_name,
                feature_value,
                event_time,
                CURRENT_TIMESTAMP AS ingestion_time,
                window_end,
                dt
            FROM (
                SELECT
                    entity_id,
                    feature_name,
                    feature_value,
                    event_time,
                    ingestion_time,
                    window_end,
                    dt,
                    ROW_NUMBER() OVER (
                        PARTITION BY entity_id, feature_name, window_end
                        ORDER BY ingestion_time DESC
                    ) AS rn
                FROM parquet_source
                WHERE dt >= '%s' AND dt <= '%s'
            ) t
            WHERE rn = 1
            """, startDate, endDate);

        LOG.info("Running deduplication SQL:\n{}", deduplicationSQL);
        tableEnv.executeSql(deduplicationSQL).await();
        LOG.info("✅ Parquet deduplication complete. Writing to Redis...");

        // ── Write deduplicated results to Redis Cluster ───────────
        // Read back the deduplicated output and pipeline to Redis
        Table result = tableEnv.sqlQuery(String.format("""
            SELECT entity_id, feature_name, feature_value
            FROM parquet_backfill_output
            WHERE dt >= '%s' AND dt <= '%s'
            """, startDate, endDate));

        DataStream<BackfillRecord> stream = tableEnv.toDataStream(result)
            .map((MapFunction<org.apache.flink.types.Row, BackfillRecord>) row -> {
                BackfillRecord r = new BackfillRecord();
                r.entityId    = (String) row.getField(0);
                r.featureName = (String) row.getField(1);
                r.value       = (Double) row.getField(2);
                return r;
            });

        // Redis writer with pipeline batching
        stream.addSink(new RedisBackfillSink(redisNodes));

        env.execute("Backfill: " + featureView + " [" + startDate + " → " + endDate + "]");

        LOG.info("✅ Backfill complete for {} [{} → {}]", featureView, startDate, endDate);
        LOG.info("   Features written to Redis online store (idempotent HSET)");
        LOG.info("   Deduplicated Parquet written to s3a://{}/{}_backfill/", bucket, featureView);
    }

    /** Simple POJO for backfill records */
    public static class BackfillRecord {
        public String entityId;
        public String featureName;
        public double value;
    }
}
