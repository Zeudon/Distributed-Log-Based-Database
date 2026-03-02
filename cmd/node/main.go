// Command node is the main entry point for a database node.
// All configuration is provided via environment variables (injected by Docker
// Compose at container start time).
//
// Required environment variables:
//
//	NODE_ID        — unique integer ID for this node (e.g. 0, 1, 2)
//	PARTITION_ID   — which partition shard this node belongs to (0, 1, or 2)
//	SELF_ADDR      — host:port this node listens on (e.g. node-p0-r0:7000)
//	PEERS          — comma-separated host:port of other replicas in this partition
//	DATA_DIR       — directory to store WAL, snapshot, Raft state
//
// Optional environment variables (with defaults):
//
//	PARTITION_COUNT      (default 3)
//	REPLICA_COUNT        (default 3)
//	RPC_PORT             (default 7000)
//	SNAPSHOT_THRESHOLD   (default 1000)
//	ELECTION_TIMEOUT_MIN_MS (default 150)
//	ELECTION_TIMEOUT_MAX_MS (default 300)
//	HEARTBEAT_INTERVAL_MS   (default 50)
package main

import (
	"log/slog"
	"os"

	"github.com/Zeudon/Distributed-Log-Based-Database/config"
	"github.com/Zeudon/Distributed-Log-Based-Database/internal/node"
)

func main() {
	// Structured JSON logging to stdout (visible via docker logs)
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	cfg := config.LoadFromEnv()

	slog.Info("Starting database node",
		"nodeID", cfg.NodeID,
		"partitionID", cfg.PartitionID,
		"selfAddr", cfg.SelfAddress,
		"peers", cfg.PeerAddresses,
		"dataDir", cfg.DataDir)

	n, err := node.NewNode(cfg)
	if err != nil {
		slog.Error("Failed to initialise node", "err", err)
		os.Exit(1)
	}

	if err := n.Start(); err != nil {
		slog.Error("Node exited with error", "err", err)
		os.Exit(1)
	}
}
