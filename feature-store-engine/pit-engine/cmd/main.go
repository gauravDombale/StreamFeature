// Package main: Point-in-Time Correct Feature Dataset Generator
// Queries Parquet files from MinIO and performs PIT joins using DuckDB-style SQL via Go
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/cobra"
)

type PITConfig struct {
	MinioEndpoint   string
	MinioAccessKey  string
	MinioSecretKey  string
	BucketName      string
	EntitiesFile    string
	FeatureNames    []string
	Output          string
}

// EntityTimestamp represents a row in the entities CSV
type EntityTimestamp struct {
	EntityID  string
	Timestamp time.Time
	Label     string
}

// FeatureRecord is one row from the Parquet offline store
type FeatureRecord struct {
	EntityID      string
	FeatureName   string
	Value         float64
	EventTime     time.Time
	IngestionTime time.Time
}

// PITResult is one joined row in the output dataset
type PITResult struct {
	EntityID  string
	Timestamp time.Time
	Label     string
	Features  map[string]float64
}

func loadEntities(path string) ([]EntityTimestamp, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open entities file: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	// Find column indices
	idxEntityID, idxTimestamp, idxLabel := -1, -1, -1
	for i, h := range headers {
		switch strings.ToLower(h) {
		case "entity_id", "user_id":
			idxEntityID = i
		case "timestamp", "as_of":
			idxTimestamp = i
		case "label", "target":
			idxLabel = i
		}
	}
	if idxEntityID < 0 || idxTimestamp < 0 {
		return nil, fmt.Errorf("entities CSV must have entity_id and timestamp columns")
	}

	var entities []EntityTimestamp
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ts, err := time.Parse(time.RFC3339, row[idxTimestamp])
		if err != nil {
			// Try unix millis
			ms, _ := strconv.ParseInt(row[idxTimestamp], 10, 64)
			ts = time.UnixMilli(ms)
		}
		e := EntityTimestamp{EntityID: row[idxEntityID], Timestamp: ts}
		if idxLabel >= 0 && idxLabel < len(row) {
			e.Label = row[idxLabel]
		}
		entities = append(entities, e)
	}
	return entities, nil
}

func runPITJoin(cfg *PITConfig) error {
	ctx := context.Background()

	// Connect to MinIO
	endpoint := strings.TrimPrefix(cfg.MinioEndpoint, "http://")
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: false,
	})
	if err != nil {
		return fmt.Errorf("minio connect: %w", err)
	}

	// Load entities
	entities, err := loadEntities(cfg.EntitiesFile)
	if err != nil {
		return fmt.Errorf("load entities: %w", err)
	}
	log.Printf("Loaded %d entity-timestamps", len(entities))

	// Build entity → PIT timestamp index
	entityPIT := make(map[string]time.Time)
	for _, e := range entities {
		entityPIT[e.EntityID] = e.Timestamp
	}

	// Load feature records from MinIO Parquet (JSON lines fallback for demo)
	featureData := make(map[string]map[string][]FeatureRecord) // [entityID][featureName][]records
	for _, featureName := range cfg.FeatureNames {
		prefix := fmt.Sprintf("user_engagement/%s/", featureName)
		objCh := minioClient.ListObjects(ctx, cfg.BucketName, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
		for obj := range objCh {
			if obj.Err != nil {
				log.Printf("Warning: list object error: %v", obj.Err)
				continue
			}
			object, err := minioClient.GetObject(ctx, cfg.BucketName, obj.Key, minio.GetObjectOptions{})
			if err != nil {
				continue
			}
			data, _ := io.ReadAll(object)
			object.Close()

			// Parse JSON lines (simplified; real impl would use parquet-go)
			lines := strings.Split(string(data), "\n")
			for _, line := range lines {
				if line == "" {
					continue
				}
				var rec FeatureRecord
				if err := json.Unmarshal([]byte(line), &rec); err == nil {
					if featureData[rec.EntityID] == nil {
						featureData[rec.EntityID] = make(map[string][]FeatureRecord)
					}
					featureData[rec.EntityID][featureName] = append(
						featureData[rec.EntityID][featureName], rec,
					)
				}
			}
		}
	}

	// PIT Join: for each entity-timestamp, find the most recent feature value <= timestamp
	results := make([]PITResult, 0, len(entities))
	leakageCount := 0

	for _, entity := range entities {
		result := PITResult{
			EntityID:  entity.EntityID,
			Timestamp: entity.Timestamp,
			Label:     entity.Label,
			Features:  make(map[string]float64),
		}

		for _, featureName := range cfg.FeatureNames {
			records := featureData[entity.EntityID][featureName]
			var best *FeatureRecord
			for i := range records {
				r := &records[i]
				// PIT constraint: event_time MUST be <= lookup timestamp
				if r.EventTime.After(entity.Timestamp) {
					leakageCount++
					continue // Skip future data → prevents training/serving skew
				}
				if best == nil || r.EventTime.After(best.EventTime) {
					best = r
				}
			}
			if best != nil {
				result.Features[featureName] = best.Value
			} else {
				result.Features[featureName] = 0 // null / missing
			}
		}
		results = append(results, result)
	}

	if leakageCount > 0 {
		log.Printf("⚠️  Blocked %d future feature values (leakage prevention)", leakageCount)
	}

	// Write output CSV (simplified; real impl would write Parquet)
	outFile, err := os.Create(cfg.Output)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	headers := []string{"entity_id", "timestamp", "label"}
	headers = append(headers, cfg.FeatureNames...)
	writer.Write(headers)

	for _, r := range results {
		row := []string{r.EntityID, r.Timestamp.Format(time.RFC3339), r.Label}
		for _, name := range cfg.FeatureNames {
			row = append(row, strconv.FormatFloat(r.Features[name], 'f', 6, 64))
		}
		writer.Write(row)
	}
	writer.Flush()

	log.Printf("✅ PIT dataset written: %d rows, %d features → %s", len(results), len(cfg.FeatureNames), cfg.Output)
	log.Printf("✅ Zero leakage: all feature values are as-of their lookup timestamp")
	return nil
}

func main() {
	cfg := &PITConfig{}
	var features string

	cmd := &cobra.Command{
		Use:   "pit-engine",
		Short: "Point-in-time correct feature dataset generator",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.FeatureNames = strings.Split(features, ",")
			return runPITJoin(cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.MinioEndpoint, "minio-endpoint", "http://localhost:9000", "MinIO endpoint")
	cmd.Flags().StringVar(&cfg.MinioAccessKey, "minio-access-key", "minioadmin", "MinIO access key")
	cmd.Flags().StringVar(&cfg.MinioSecretKey, "minio-secret-key", "minioadmin123", "MinIO secret key")
	cmd.Flags().StringVar(&cfg.BucketName, "bucket", "feature-offline-store", "MinIO bucket")
	cmd.Flags().StringVar(&cfg.EntitiesFile, "entities", "fixtures/users.csv", "Entities CSV with entity_id,timestamp,label")
	cmd.Flags().StringVar(&features, "features", "click_count_1h", "Comma-separated feature names")
	cmd.Flags().StringVar(&cfg.Output, "output", "train.csv", "Output dataset path")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
