package config

import (
	"os"
	"strconv"
	"strings"
)

// ClusterConfig holds the configuration for a single node in the cluster.
// All values are populated from environment variables so Docker Compose
// can inject them at container start time.
type ClusterConfig struct {
	// NodeID is a unique integer identifier for this node (0-based).
	NodeID int

	// PartitionID is the shard this node belongs to (0, 1, or 2).
	PartitionID int

	// PartitionCount is the total number of partitions in the cluster.
	PartitionCount int

	// ReplicaCount is the number of replicas per partition (Raft group size).
	ReplicaCount int

	// PeerAddresses are the host:port addresses of the other replicas in the
	// same Raft group (i.e. the other nodes in the same partition).
	PeerAddresses []string

	// SelfAddress is the host:port this node listens on for RPC.
	SelfAddress string

	// DataDir is the directory where WAL, snapshots, and Raft state are stored.
	DataDir string

	// RPCPort is the TCP port this node listens on.
	RPCPort int

	// SnapshotThreshold is the number of log entries after which a snapshot
	// is triggered automatically.
	SnapshotThreshold int

	// ElectionTimeoutMinMs and ElectionTimeoutMaxMs define the random range
	// for the Raft election timeout (milliseconds).
	ElectionTimeoutMinMs int
	ElectionTimeoutMaxMs int

	// HeartbeatIntervalMs is how often (ms) the Raft leader sends heartbeats.
	HeartbeatIntervalMs int

	// HTTPPort is the TCP port this node's REST HTTP server listens on.
	HTTPPort int
}

// LoadFromEnv reads all configuration values from environment variables.
// Required variables: NODE_ID, PARTITION_ID, SELF_ADDR, PEERS, DATA_DIR.
// Optional variables use sensible defaults.
func LoadFromEnv() ClusterConfig {
	cfg := ClusterConfig{
		NodeID:               mustGetEnvInt("NODE_ID"),
		PartitionID:          mustGetEnvInt("PARTITION_ID"),
		PartitionCount:       getEnvIntDefault("PARTITION_COUNT", 3),
		ReplicaCount:         getEnvIntDefault("REPLICA_COUNT", 3),
		SelfAddress:          mustGetEnv("SELF_ADDR"),
		PeerAddresses:        splitAddresses(getEnvDefault("PEERS", "")),
		DataDir:              getEnvDefault("DATA_DIR", "/data"),
		RPCPort:              getEnvIntDefault("RPC_PORT", 7000),
		SnapshotThreshold:    getEnvIntDefault("SNAPSHOT_THRESHOLD", 1000),
		ElectionTimeoutMinMs: getEnvIntDefault("ELECTION_TIMEOUT_MIN_MS", 150),
		ElectionTimeoutMaxMs: getEnvIntDefault("ELECTION_TIMEOUT_MAX_MS", 300),
		HeartbeatIntervalMs:  getEnvIntDefault("HEARTBEAT_INTERVAL_MS", 50),
		HTTPPort:             getEnvIntDefault("HTTP_PORT", 8080),
	}
	return cfg
}

// splitAddresses splits a comma-separated list of addresses, filtering blanks.
func splitAddresses(s string) []string {
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	var addrs []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			addrs = append(addrs, p)
		}
	}
	return addrs
}

func mustGetEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic("required environment variable not set: " + key)
	}
	return v
}

func mustGetEnvInt(key string) int {
	v := mustGetEnv(key)
	n, err := strconv.Atoi(v)
	if err != nil {
		panic("environment variable " + key + " must be an integer, got: " + v)
	}
	return n
}

func getEnvDefault(key, defaultVal string) string {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	return v
}

func getEnvIntDefault(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return n
}
