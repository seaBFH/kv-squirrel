package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// KeyData represents a Redis key with all its metadata
type KeyData struct {
	Key   string        `json:"key"`
	Type  string        `json:"type"`
	TTL   time.Duration `json:"ttl"`
	Value interface{}   `json:"value"`
	Dump  []byte        `json:"dump"` // Using DUMP for complex types
}

// Config holds the tool configuration
type Config struct {
	SourceAddrs []string
	SourceUser  string
	SourcePass  string
	TargetAddrs []string
	TargetUser  string
	TargetPass  string
	Pattern     string
	OutputFile  string
	InputFile   string
	BatchSize   int64
	UseRDBDump  bool // Use DUMP/RESTORE for accurate replication
}

func main() {
	config := parseFlags()

	if config.InputFile == "" {
		// Export mode
		log.Println("=== Export Mode ===")
		if err := exportKeys(config); err != nil {
			log.Fatalf("Export failed: %v", err)
		}
		log.Printf("✓ Export completed successfully to %s\n", config.OutputFile)
	} else {
		// Import mode
		log.Println("=== Import Mode ===")
		if err := importKeys(config); err != nil {
			log.Fatalf("Import failed: %v", err)
		}
		log.Println("✓ Import completed successfully")
	}
}

func parseFlags() *Config {
	config := &Config{}

	// Source cluster flags
	sourceAddrs := flag.String("source-addrs", "localhost:7000,localhost:7001", "Source cluster addresses (comma-separated)")
	flag.StringVar(&config.SourceUser, "source-user", "", "Source cluster username (ACL)")
	flag.StringVar(&config.SourcePass, "source-pass", "", "Source cluster password")

	// Target cluster flags
	targetAddrs := flag.String("target-addrs", "localhost:8000,localhost:8001", "Target cluster addresses (comma-separated)")
	flag.StringVar(&config.TargetUser, "target-user", "", "Target cluster username (ACL)")
	flag.StringVar(&config.TargetPass, "target-pass", "", "Target cluster password")

	// Operation flags
	flag.StringVar(&config.Pattern, "pattern", "*", "Key pattern to match (glob-style)")
	flag.StringVar(&config.OutputFile, "output", "redis-dump.json", "Output file for export")
	flag.StringVar(&config.InputFile, "input", "", "Input file for import (if set, runs import mode)")
	flag.Int64Var(&config.BatchSize, "batch", 1000, "Batch size for scanning")
	flag.BoolVar(&config.UseRDBDump, "use-dump", true, "Use DUMP/RESTORE commands (recommended)")

	flag.Parse()

	// Parse addresses
	config.SourceAddrs = parseAddresses(*sourceAddrs)
	config.TargetAddrs = parseAddresses(*targetAddrs)

	return config
}

func parseAddresses(addrs string) []string {
	var result []string
	current := ""
	for _, char := range addrs {
		if char == ',' {
			if current != "" {
				result = append(result, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

// exportKeys scans the source cluster and exports matching keys
func exportKeys(config *Config) error {
	ctx := context.Background()

	// Connect to source cluster
	sourceClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        config.SourceAddrs,
		Username:     config.SourceUser,
		Password:     config.SourcePass,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})
	defer sourceClient.Close()

	// Test connection
	if err := sourceClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to source cluster:  %w", err)
	}

	log.Printf("✓ Connected to source cluster:   %v\n", config.SourceAddrs)
	if config.SourceUser != "" {
		log.Printf("  Using username: %s\n", config.SourceUser)
	}

	// Collect all keys from all master nodes using sync.Map
	var allKeys sync.Map
	var totalKeys int

	err := sourceClient.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		log.Printf("Scanning master node:  %s\n", master.Options().Addr)

		iter := master.Scan(ctx, 0, config.Pattern, config.BatchSize).Iterator()
		nodeKeyCount := 0

		for iter.Next(ctx) {
			key := iter.Val()

			// LoadOrStore is atomic and returns true if the key was actually stored (was new)
			if _, loaded := allKeys.LoadOrStore(key, true); !loaded {
				nodeKeyCount++
			}
		}

		if err := iter.Err(); err != nil {
			return fmt.Errorf("scan error on %s:  %w", master.Options().Addr, err)
		}

		log.Printf("  Found %d keys on this node\n", nodeKeyCount)
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to scan cluster:  %w", err)
	}

	// Convert sync.Map to slice
	keys := make([]string, 0)
	allKeys.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(string))
		totalKeys++
		return true
	})

	log.Printf("✓ Total unique keys found: %d\n", totalKeys)

	if len(keys) == 0 {
		log.Println("⚠ No keys found matching pattern.  Nothing to export.")
		return nil
	}

	// Export key data
	keyDataList := make([]KeyData, 0, len(keys))
	exported := 0
	failed := 0

	log.Println("Exporting key data...")

	for i, key := range keys {
		if (i+1)%100 == 0 {
			log.Printf("  Progress: %d/%d keys\n", i+1, len(keys))
		}

		keyData, err := exportKey(ctx, sourceClient, key, config.UseRDBDump)
		if err != nil {
			log.Printf("  ⚠ Failed to export key %s: %v\n", key, err)
			failed++
			continue
		}

		keyDataList = append(keyDataList, *keyData)
		exported++
	}

	log.Printf("✓ Successfully exported:   %d keys\n", exported)
	if failed > 0 {
		log.Printf("⚠ Failed to export:  %d keys\n", failed)
	}

	// Write to file
	file, err := os.Create(config.OutputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(keyDataList); err != nil {
		return fmt.Errorf("failed to write JSON:   %w", err)
	}

	return nil
}

// exportKey exports a single key with all its data
func exportKey(ctx context.Context, client redis.UniversalClient, key string, useDump bool) (*KeyData, error) {
	keyData := &KeyData{
		Key: key,
	}

	// Get TTL
	ttl, err := client.TTL(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get TTL:   %w", err)
	}
	keyData.TTL = ttl

	// Get type
	keyType, err := client.Type(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get type:  %w", err)
	}
	keyData.Type = keyType

	if useDump {
		// Use DUMP command for accurate serialization
		dump, err := client.Dump(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to dump key: %w", err)
		}
		keyData.Dump = []byte(dump)
	} else {
		// Fallback: export by type (less reliable for complex types)
		value, err := exportValueByType(ctx, client, key, keyType)
		if err != nil {
			return nil, fmt.Errorf("failed to export value:   %w", err)
		}
		keyData.Value = value
	}

	return keyData, nil
}

// exportValueByType exports value based on Redis type
func exportValueByType(ctx context.Context, client redis.UniversalClient, key, keyType string) (interface{}, error) {
	switch keyType {
	case "string":
		return client.Get(ctx, key).Result()

	case "list":
		return client.LRange(ctx, key, 0, -1).Result()

	case "set":
		return client.SMembers(ctx, key).Result()

	case "zset":
		return client.ZRangeWithScores(ctx, key, 0, -1).Result()

	case "hash":
		return client.HGetAll(ctx, key).Result()

	default:
		return nil, fmt.Errorf("unsupported type:   %s", keyType)
	}
}

// importKeys reads from file and imports to target cluster
func importKeys(config *Config) error {
	ctx := context.Background()

	// Read from file
	file, err := os.Open(config.InputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	var keyDataList []KeyData
	if err := json.NewDecoder(file).Decode(&keyDataList); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	log.Printf("✓ Loaded %d keys from %s\n", len(keyDataList), config.InputFile)

	if len(keyDataList) == 0 {
		log.Println("⚠ No keys to import")
		return nil
	}

	// Connect to target cluster
	targetClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        config.TargetAddrs,
		Username:     config.TargetUser,
		Password:     config.TargetPass,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})
	defer targetClient.Close()

	// Test connection
	if err := targetClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to target cluster:  %w", err)
	}

	log.Printf("✓ Connected to target cluster: %v\n", config.TargetAddrs)
	if config.TargetUser != "" {
		log.Printf("  Using username: %s\n", config.TargetUser)
	}

	// Import keys
	imported := 0
	failed := 0

	log.Println("Importing keys...")

	for i, keyData := range keyDataList {
		if (i+1)%100 == 0 {
			log.Printf("  Progress: %d/%d keys\n", i+1, len(keyDataList))
		}

		if err := importKey(ctx, targetClient, &keyData, config.UseRDBDump); err != nil {
			log.Printf("  ⚠ Failed to import key %s: %v\n", keyData.Key, err)
			failed++
			continue
		}

		imported++
	}

	log.Printf("✓ Successfully imported:   %d keys\n", imported)
	if failed > 0 {
		log.Printf("⚠ Failed to import:  %d keys\n", failed)
	}

	return nil
}

// importKey imports a single key
func importKey(ctx context.Context, client redis.UniversalClient, keyData *KeyData, useDump bool) error {
	if useDump && len(keyData.Dump) > 0 {
		// Use RESTORE command
		ttl := keyData.TTL
		if ttl < 0 {
			ttl = 0 // No expiration
		}

		return client.RestoreReplace(ctx, keyData.Key, ttl, string(keyData.Dump)).Err()
	}

	// Fallback:  import by type
	return importValueByType(ctx, client, keyData)
}

// importValueByType imports value based on Redis type
func importValueByType(ctx context.Context, client redis.UniversalClient, keyData *KeyData) error {
	key := keyData.Key

	switch keyData.Type {
	case "string":
		val, ok := keyData.Value.(string)
		if !ok {
			return fmt.Errorf("invalid string value")
		}
		if err := client.Set(ctx, key, val, keyData.TTL).Err(); err != nil {
			return err
		}

	case "list":
		vals, ok := keyData.Value.([]interface{})
		if !ok {
			return fmt.Errorf("invalid list value")
		}
		for _, v := range vals {
			if err := client.RPush(ctx, key, v).Err(); err != nil {
				return err
			}
		}
		if keyData.TTL > 0 {
			client.Expire(ctx, key, keyData.TTL)
		}

	case "set":
		vals, ok := keyData.Value.([]interface{})
		if !ok {
			return fmt.Errorf("invalid set value")
		}
		for _, v := range vals {
			if err := client.SAdd(ctx, key, v).Err(); err != nil {
				return err
			}
		}
		if keyData.TTL > 0 {
			client.Expire(ctx, key, keyData.TTL)
		}

	case "hash":
		vals, ok := keyData.Value.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid hash value")
		}
		if err := client.HSet(ctx, key, vals).Err(); err != nil {
			return err
		}
		if keyData.TTL > 0 {
			client.Expire(ctx, key, keyData.TTL)
		}

	case "zset":
		vals, ok := keyData.Value.([]interface{})
		if !ok {
			return fmt.Errorf("invalid zset value")
		}
		members := make([]redis.Z, 0, len(vals))
		for _, v := range vals {
			zval := v.(map[string]interface{})
			members = append(members, redis.Z{
				Score:  zval["Score"].(float64),
				Member: zval["Member"],
			})
		}
		if err := client.ZAdd(ctx, key, members...).Err(); err != nil {
			return err
		}
		if keyData.TTL > 0 {
			client.Expire(ctx, key, keyData.TTL)
		}

	default:
		return fmt.Errorf("unsupported type:  %s", keyData.Type)
	}

	return nil
}
