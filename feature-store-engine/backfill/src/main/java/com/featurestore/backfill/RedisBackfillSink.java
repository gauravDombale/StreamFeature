package com.featurestore.backfill;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.ClusterPipeline;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * RedisBackfillSink – Flink SinkFunction that writes backfilled features
 * to Redis Cluster using HSET pipelines for efficiency.
 *
 * Key format:  feat:user:{entity_id}
 * Field:       {feature_name}
 * Value:       8-byte IEEE 754 double (big-endian binary)
 *
 * Idempotency: HSET is idempotent — re-running produces identical state.
 * Deduplication was already applied upstream in BackfillRunner.
 */
public class RedisBackfillSink extends RichSinkFunction<BackfillRunner.BackfillRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisBackfillSink.class);
    private static final int BATCH_SIZE = 500;
    private static final long TTL_SECONDS = 7 * 24 * 3600L; // 7 days for backfilled data

    private final String redisNodes;
    private transient JedisCluster jedis;
    private transient int bufferedCount;
    private transient ClusterPipeline pipeline;

    public RedisBackfillSink(String redisNodes) {
        this.redisNodes = redisNodes;
    }

    @Override
    public void open(Configuration parameters) {
        Set<HostAndPort> nodes = new HashSet<>();
        for (String node : redisNodes.split(",")) {
            String[] parts = node.trim().split(":");
            nodes.add(new HostAndPort(parts[0], Integer.parseInt(parts[1])));
        }
        jedis = new JedisCluster(nodes);
        bufferedCount = 0;
        LOG.info("RedisBackfillSink connected to cluster: {}", redisNodes);
    }

    @Override
    public void invoke(BackfillRunner.BackfillRecord record, Context context) {
        String hashKey = "feat:user:" + record.entityId;

        // Encode double as 8-byte big-endian (matches Go server encoding)
        byte[] valueBytes = ByteBuffer.allocate(8)
            .putDouble(record.value)
            .array();

        jedis.hset(hashKey.getBytes(), record.featureName.getBytes(), valueBytes);
        jedis.expire(hashKey, TTL_SECONDS);

        bufferedCount++;
        if (bufferedCount % BATCH_SIZE == 0) {
            LOG.info("Backfill progress: {} records written to Redis", bufferedCount);
        }
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
        LOG.info("✅ RedisBackfillSink closed. Total records written: {}", bufferedCount);
    }
}
