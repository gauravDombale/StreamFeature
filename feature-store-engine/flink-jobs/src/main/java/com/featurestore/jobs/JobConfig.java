package com.featurestore.jobs;

import org.apache.commons.cli.*;

/**
 * JobConfig – CLI argument parser for Flink job configuration.
 */
public class JobConfig {

    private String featureView;
    private String kafkaBrokers;
    private String schemaRegistry;
    private String redisCluster;
    private String minioEndpoint;
    private String checkpointDir;
    private int parallelism;

    public static JobConfig fromArgs(String[] args) throws ParseException {
        Options opts = new Options();
        opts.addRequiredOption("f", "feature-view", true, "Feature view name");
        opts.addOption("k", "kafka-brokers",   true, "Kafka bootstrap servers");
        opts.addOption("s", "schema-registry", true, "Schema Registry URL");
        opts.addOption("r", "redis-cluster",   true, "Redis cluster nodes (comma-separated)");
        opts.addOption("m", "minio-endpoint",  true, "MinIO S3 endpoint");
        opts.addOption("c", "checkpoint-dir",  true, "Checkpoint directory (s3://...)");
        opts.addOption("p", "parallelism",     true, "Job parallelism");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opts, args);

        JobConfig config = new JobConfig();
        config.featureView   = cmd.getOptionValue("feature-view");
        config.kafkaBrokers  = cmd.getOptionValue("kafka-brokers",   "kafka-1:29092,kafka-2:29092,kafka-3:29092");
        config.schemaRegistry= cmd.getOptionValue("schema-registry", "http://schema-registry:8081");
        config.redisCluster  = cmd.getOptionValue("redis-cluster",   "redis-node-1:6379,redis-node-2:6379,redis-node-3:6379");
        config.minioEndpoint = cmd.getOptionValue("minio-endpoint",  "http://minio:9000");
        config.checkpointDir = cmd.getOptionValue("checkpoint-dir",
            "s3a://flink-checkpoints/checkpoints/" + config.featureView);
        config.parallelism   = Integer.parseInt(cmd.getOptionValue("parallelism", "4"));

        return config;
    }

    public String getFeatureView()   { return featureView; }
    public String getKafkaBrokers()  { return kafkaBrokers; }
    public String getSchemaRegistry(){ return schemaRegistry; }
    public String getRedisCluster()  { return redisCluster; }
    public String getMinioEndpoint() { return minioEndpoint; }
    public String getCheckpointDir() { return checkpointDir; }
    public int    getParallelism()   { return parallelism; }
}
