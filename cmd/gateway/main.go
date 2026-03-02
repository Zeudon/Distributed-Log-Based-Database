// cmd/gateway is the entry point for the API gateway binary.
// It reads partition HTTP addresses from environment variables and starts
// a stateless REST gateway that routes requests to the correct partition
// or fans out to all partitions for aggregate queries.
//
// Required environment variables:
//
//	PARTITION_COUNT            — number of partitions (default: 3)
//	PARTITION_<N>_HTTP_ADDRS  — comma-separated HTTP addresses for partition N
//	                             e.g. PARTITION_0_HTTP_ADDRS=node-p0-r0:8080,...
//	GATEWAY_PORT               — port the gateway listens on (default: 9090)
package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/Zeudon/Distributed-Log-Based-Database/internal/gateway"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	partitionCount := envIntDefault("PARTITION_COUNT", 3)
	gatewayPort := envIntDefault("GATEWAY_PORT", 9090)

	partitions := make([]gateway.PartitionConfig, partitionCount)
	for i := 0; i < partitionCount; i++ {
		key := fmt.Sprintf("PARTITION_%d_HTTP_ADDRS", i)
		raw := os.Getenv(key)
		if raw == "" {
			slog.Error("required env var not set", "var", key)
			os.Exit(1)
		}
		addrs := splitAddrs(raw)
		partitions[i] = gateway.PartitionConfig{ID: i, HTTPAddrs: addrs}
		slog.Info("Partition configured", "id", i, "addrs", addrs)
	}

	gw := gateway.New(partitions, gatewayPort)
	slog.Info("Gateway starting", "port", gatewayPort)
	if err := gw.Listen(); err != nil {
		slog.Error("Gateway stopped", "err", err)
		os.Exit(1)
	}
}

func envIntDefault(key string, def int) int {
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

func splitAddrs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
