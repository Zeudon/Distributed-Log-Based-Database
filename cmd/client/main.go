// Command client is a CLI tool for interacting with the distributed KV database.
//
// Usage:
//
//	client set   <key> <value>
//	client get   <key>
//	client delete <key>
//
// The client resolves the correct partition using FNV-1a hashing and contacts
// the matching partition's replicas starting with the last known leader.

package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/Zeudon/Distributed-Log-Based-Database/internal/partition"
	"github.com/Zeudon/Distributed-Log-Based-Database/internal/rpc"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	args := os.Args[1:]
	if len(args) < 2 {
		printUsage()
		os.Exit(1)
	}

	partitionCount := getEnvIntDefault("PARTITION_COUNT", 3)
	partitions := buildPartitions(partitionCount)

	cli := rpc.NewClient()
	defer cli.Close()

	router := partition.NewRouter(partitions, cli)

	command := strings.ToLower(args[0])
	switch command {
	case "get":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: client get <key>")
			os.Exit(1)
		}
		key := args[1]
		value, found, err := router.Get(key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}
		if !found {
			fmt.Printf("(nil)\n")
		} else {
			fmt.Printf("%s\n", value)
		}

	case "set":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "Usage: client set <key> <value>")
			os.Exit(1)
		}
		key, value := args[1], args[2]
		if err := router.Set(key, value); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "delete", "del":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: client delete <key>")
			os.Exit(1)
		}
		key := args[1]
		if err := router.Delete(key); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

// buildPartitions reads partition replica addresses from environment variables.
// Expected format: PARTITION_0_ADDRS=host1:7000,host2:7000,host3:7000
func buildPartitions(count int) []partition.PartitionInfo {
	parts := make([]partition.PartitionInfo, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("PARTITION_%d_ADDRS", i)
		val := os.Getenv(key)
		if val == "" {
			slog.Warn("No addresses configured for partition",
				"partition", i,
				"envVar", key)
			parts[i] = partition.PartitionInfo{ID: i}
			continue
		}
		addrs := strings.Split(val, ",")
		var cleaned []string
		for _, a := range addrs {
			a = strings.TrimSpace(a)
			if a != "" {
				cleaned = append(cleaned, a)
			}
		}
		parts[i] = partition.PartitionInfo{
			ID:               i,
			ReplicaAddresses: cleaned,
		}
	}
	return parts
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `Usage:
  client get    <key>
  client set    <key> <value>
  client delete <key>

Environment:
  PARTITION_COUNT     number of partitions (default: 3)
  PARTITION_0_ADDRS   comma-separated replica addresses for partition 0
  PARTITION_1_ADDRS   comma-separated replica addresses for partition 1
  PARTITION_2_ADDRS   comma-separated replica addresses for partition 2`)
}

func getEnvIntDefault(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
