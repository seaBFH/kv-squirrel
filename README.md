# KV squirrel

## Overview

redis migration tool from one redis cluster to another.

## kv-squirrel usage

### Export keys from source cluster
```bash
# Export all keys matching pattern "user: *"
./kv-squirrel \
  -source-addrs "172.38.0.11:6379,172.38.0.12:6379,172.38.0.13:6379,172.38.0.14:6379,172.38.0.15:6379,172.38.0.16:6379" \
  -source-pass "your-password" \
  -pattern "user:*" \
  -output "users-export.json"

./kv-squirrel \
  -source-addrs "172.38.0.11:6379,172.38.0.12:6379,172.38.0.13:6379,172.38.0.14:6379,172.38.0.15:6379,172.38.0.16:6379" \
  -pattern "user:*" \
  -output "users-export.json"

# Export all keys
./kv-squirrel \
  -source-addrs "localhost:7000,localhost:7001" \
  -pattern "*" \
  -output "full-dump.json"

```

### Import keys to target cluster

```bash
# Import from file
./kv-squirrel \
  -target-addrs "172.38.0.11:6379,172.38.0.12:6379,172.38.0.13:6379,172.38.0.14:6379,172.38.0.15:6379,172.38.0.16:6379" \
  -target-pass "target-password" \
  -input "users-export.json"

./kv-squirrel \
  -target-addrs "172.38.0.11:6379,172.38.0.12:6379,172.38.0.13:6379,172.38.0.14:6379,172.38.0.15:6379,172.38.0.16:6379" \
  -input "./ipcache-export.json"
```

## kv-random-gen usage

```
# Generate 10,000 mixed keys
./kv-random-gen \
  -addrs "172.38.0.11:6379,172.38.0.12:6379,172.38.0.13:6379,172.38.0.14:6379,172.38.0.15:6379,172.38.0.16:6379" \
  -prefix "user" \
  -count 10000 \
  -types "string"

# Generate with TTL (expire in 1-24 hours)
./kv-random-gen \
  -addrs "localhost:7000" \
  -prefix "session" \
  -count 5000 \
  -types "string" \
  -min-ttl 3600 \
  -max-ttl 86400

# Generate only strings for testing
./kv-random-gen \
  -prefix "test" \
  -count 1000 \
  -types "string" \
  -string-size 200

# Generate complex data structures
./kv-random-gen \
  -prefix "product" \
  -count 2000 \
  -types "hash,zset" \
  -hash-fields 10 \
  -zset-members 20

# Build and run as binary
go build -o kv-random-gen ./cmd/kv-random-gen/main.go
./kv-random-gen -prefix "data" -count 50000
```
