package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

type GeneratorConfig struct {
	Addrs       []string
	Password    string
	KeyPrefix   string
	Count       int
	DataTypes   []string
	MinTTL      int
	MaxTTL      int
	BatchSize   int
	StringSize  int
	ListSize    int
	SetSize     int
	HashFields  int
	ZSetMembers int
}

var (
	// Sample data for generating realistic test data
	firstNames = []string{"John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"}
	lastNames  = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
	cities     = []string{"NewYork", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Mumbai", "Beijing", "Moscow"}
	products   = []string{"Laptop", "Phone", "Tablet", "Camera", "Headphones", "Watch", "Keyboard", "Mouse", "Monitor", "Speaker"}
	statuses   = []string{"active", "pending", "inactive", "suspended", "verified", "processing", "completed", "failed"}
)

func main() {
	config := parseFlags()

	ctx := context.Background()

	// Connect to Redis cluster
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    config.Addrs,
		Password: config.Password,
	})
	defer client.Close()

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	log.Printf("✓ Connected to Redis cluster:  %v\n", config.Addrs)
	log.Printf("Generating %d keys with prefix '%s'\n", config.Count, config.KeyPrefix)

	rand.Seed(time.Now().UnixNano())

	generated := 0
	failed := 0
	startTime := time.Now()

	for i := 0; i < config.Count; i++ {
		if (i+1)%100 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(i+1) / elapsed.Seconds()
			log.Printf("Progress: %d/%d keys (%.0f keys/sec)\n", i+1, config.Count, rate)
		}

		// Randomly select a data type
		dataType := config.DataTypes[rand.Intn(len(config.DataTypes))]

		var err error
		switch dataType {
		case "string":
			err = generateString(ctx, client, config, i)
		case "list":
			err = generateList(ctx, client, config, i)
		case "set":
			err = generateSet(ctx, client, config, i)
		case "hash":
			err = generateHash(ctx, client, config, i)
		case "zset":
			err = generateZSet(ctx, client, config, i)
		}

		if err != nil {
			log.Printf("⚠ Failed to generate key %d: %v\n", i, err)
			failed++
		} else {
			generated++
		}
	}

	elapsed := time.Since(startTime)
	log.Printf("\n✓ Generation completed in %v\n", elapsed)
	log.Printf("  Successfully generated: %d keys\n", generated)
	log.Printf("  Failed:  %d keys\n", failed)
	log.Printf("  Average rate: %.0f keys/sec\n", float64(generated)/elapsed.Seconds())
}

func parseFlags() *GeneratorConfig {
	config := &GeneratorConfig{}

	addrs := flag.String("addrs", "localhost:7000,localhost:7001,localhost:7002", "Redis cluster addresses")
	flag.StringVar(&config.Password, "password", "", "Redis password")
	flag.StringVar(&config.KeyPrefix, "prefix", "test", "Key prefix (e.g., 'user', 'session', 'product')")
	flag.IntVar(&config.Count, "count", 1000, "Number of keys to generate")
	dataTypes := flag.String("types", "string,list,set,hash,zset", "Data types to generate (comma-separated)")
	flag.IntVar(&config.MinTTL, "min-ttl", 0, "Minimum TTL in seconds (0 = no expiration)")
	flag.IntVar(&config.MaxTTL, "max-ttl", 0, "Maximum TTL in seconds (0 = no expiration)")
	flag.IntVar(&config.StringSize, "string-size", 100, "Size of string values in bytes")
	flag.IntVar(&config.ListSize, "list-size", 10, "Number of elements in lists")
	flag.IntVar(&config.SetSize, "set-size", 10, "Number of elements in sets")
	flag.IntVar(&config.HashFields, "hash-fields", 5, "Number of fields in hashes")
	flag.IntVar(&config.ZSetMembers, "zset-members", 10, "Number of members in sorted sets")

	flag.Parse()

	config.Addrs = parseAddresses(*addrs)
	config.DataTypes = parseAddresses(*dataTypes)

	return config
}

func parseAddresses(input string) []string {
	var result []string
	current := ""
	for _, char := range input {
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

func generateString(ctx context.Context, client redis.UniversalClient, config *GeneratorConfig, index int) error {
	key := fmt.Sprintf("%s:string:%d", config.KeyPrefix, index)

	// Generate different types of string data
	var value string
	switch rand.Intn(5) {
	case 0: // User data
		value = fmt.Sprintf(`{"id":%d,"name": "%s %s","email":"%s@example.com","age":%d,"city":"%s"}`,
			index,
			firstNames[rand.Intn(len(firstNames))],
			lastNames[rand.Intn(len(lastNames))],
			randomString(8),
			20+rand.Intn(50),
			cities[rand.Intn(len(cities))])
	case 1: // Session token
		value = fmt.Sprintf("session_%s_%d", randomString(32), time.Now().Unix())
	case 2: // Counter
		value = fmt.Sprintf("%d", rand.Intn(10000))
	case 3: // Status
		value = statuses[rand.Intn(len(statuses))]
	default: // Random string
		value = randomString(config.StringSize)
	}

	ttl := randomTTL(config)
	return client.Set(ctx, key, value, ttl).Err()
}

func generateList(ctx context.Context, client redis.UniversalClient, config *GeneratorConfig, index int) error {
	key := fmt.Sprintf("%s:list:%d", config.KeyPrefix, index)

	// Generate list items
	items := make([]interface{}, config.ListSize)
	for i := 0; i < config.ListSize; i++ {
		items[i] = fmt.Sprintf("item_%d_%s", i, randomString(10))
	}

	if err := client.RPush(ctx, key, items...).Err(); err != nil {
		return err
	}

	ttl := randomTTL(config)
	if ttl > 0 {
		return client.Expire(ctx, key, ttl).Err()
	}
	return nil
}

func generateSet(ctx context.Context, client redis.UniversalClient, config *GeneratorConfig, index int) error {
	key := fmt.Sprintf("%s:set:%d", config.KeyPrefix, index)

	// Generate unique set members
	members := make([]interface{}, config.SetSize)
	for i := 0; i < config.SetSize; i++ {
		members[i] = fmt.Sprintf("%s_%d", products[rand.Intn(len(products))], rand.Intn(1000))
	}

	if err := client.SAdd(ctx, key, members...).Err(); err != nil {
		return err
	}

	ttl := randomTTL(config)
	if ttl > 0 {
		return client.Expire(ctx, key, ttl).Err()
	}
	return nil
}

func generateHash(ctx context.Context, client redis.UniversalClient, config *GeneratorConfig, index int) error {
	key := fmt.Sprintf("%s:hash:%d", config.KeyPrefix, index)

	// Generate hash fields
	fields := make(map[string]interface{})

	// Common hash patterns
	switch rand.Intn(3) {
	case 0: // User profile
		fields["id"] = fmt.Sprintf("%d", index)
		fields["username"] = fmt.Sprintf("%s%d", randomString(8), index)
		fields["email"] = fmt.Sprintf("%s@example.com", randomString(10))
		fields["created_at"] = time.Now().Unix()
		fields["status"] = statuses[rand.Intn(len(statuses))]
		fields["login_count"] = rand.Intn(1000)
	case 1: // Product
		fields["product_id"] = fmt.Sprintf("PROD_%d", index)
		fields["name"] = products[rand.Intn(len(products))]
		fields["price"] = fmt.Sprintf("%.2f", 10.0+rand.Float64()*990.0)
		fields["stock"] = rand.Intn(500)
		fields["category"] = fmt.Sprintf("cat_%d", rand.Intn(10))
	default: // Generic
		for i := 0; i < config.HashFields; i++ {
			fields[fmt.Sprintf("field_%d", i)] = randomString(20)
		}
	}

	if err := client.HSet(ctx, key, fields).Err(); err != nil {
		return err
	}

	ttl := randomTTL(config)
	if ttl > 0 {
		return client.Expire(ctx, key, ttl).Err()
	}
	return nil
}

func generateZSet(ctx context.Context, client redis.UniversalClient, config *GeneratorConfig, index int) error {
	key := fmt.Sprintf("%s:zset:%d", config.KeyPrefix, index)

	// Generate sorted set members with scores
	members := make([]redis.Z, config.ZSetMembers)
	for i := 0; i < config.ZSetMembers; i++ {
		members[i] = redis.Z{
			Score:  float64(rand.Intn(1000)),
			Member: fmt.Sprintf("%s:%d", randomString(10), i),
		}
	}

	if err := client.ZAdd(ctx, key, members...).Err(); err != nil {
		return err
	}

	ttl := randomTTL(config)
	if ttl > 0 {
		return client.Expire(ctx, key, ttl).Err()
	}
	return nil
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func randomTTL(config *GeneratorConfig) time.Duration {
	if config.MaxTTL == 0 {
		return 0
	}

	ttlSeconds := config.MinTTL
	if config.MaxTTL > config.MinTTL {
		ttlSeconds += rand.Intn(config.MaxTTL - config.MinTTL)
	}

	return time.Duration(ttlSeconds) * time.Second
}
