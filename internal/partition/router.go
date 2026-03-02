package partition

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

	"github.com/Zeudon/Distributed-Log-Based-Database/internal/rpc"
	"github.com/Zeudon/Distributed-Log-Based-Database/proto"
)

// PartitionInfo describes a single partition's replica set and current known
// leader address.
type PartitionInfo struct {
	ID               int
	ReplicaAddresses []string // all replica addresses (including current leader)
	LeaderAddr       string   // last known leader address; empty = unknown
}

// Router routes client requests to the correct partition and leader.
// It maintains a list of PartitionInfo structs and uses FNV-1a hashing to
// determine the target partition for a key.
type Router struct {
	mu         sync.RWMutex
	partitions []PartitionInfo
	rpcCli     *rpc.Client
}

// NewRouter creates a Router.
// partitions must have one PartitionInfo per shard (index == PartitionID).
func NewRouter(partitions []PartitionInfo, cli *rpc.Client) *Router {
	return &Router{partitions: partitions, rpcCli: cli}
}

// GetPartitionID returns the partition ID responsible for key using FNV-1a.
func GetPartitionID(key string, partitionCount int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()) % partitionCount
}

// Get sends a GetRequest to the correct partition leader.
func (rt *Router) Get(key string) (string, bool, error) {
	pid := GetPartitionID(key, len(rt.partitions))
	addr, err := rt.leaderAddr(pid)
	if err != nil {
		return "", false, err
	}

	req := proto.GetRequest{Key: key}

	retries := 5
	for i := 0; i < retries; i++ {
		replyType, payload, err := rt.rpcCli.Call(addr, proto.MsgGet, req)
		if err != nil {
			return "", false, fmt.Errorf("router get: rpc: %w", err)
		}
		if replyType == proto.MsgError {
			var errResp proto.ErrorResponse
			if err := rpc.DecodePayload(payload, &errResp); err != nil {
				return "", false, fmt.Errorf("router get: decode error resp: %w", err)
			}
			if errResp.ErrCode == proto.ErrNotLeader && errResp.LeaderAddr != "" {
				slog.Debug("Router: redirecting to leader",
					"partition", pid,
					"newLeader", errResp.LeaderAddr)
				rt.updateLeader(pid, errResp.LeaderAddr)
				addr = errResp.LeaderAddr
				continue
			}
			return "", false, fmt.Errorf("router get: %s", errResp.Message)
		}
		var resp proto.GetResponse
		if err := rpc.DecodePayload(payload, &resp); err != nil {
			return "", false, fmt.Errorf("router get: decode resp: %w", err)
		}
		return resp.Value, resp.Found, nil
	}
	return "", false, fmt.Errorf("router get: too many redirects")
}

// Set sends a SetRequest to the correct partition leader.
func (rt *Router) Set(key, value string) error {
	pid := GetPartitionID(key, len(rt.partitions))
	addr, err := rt.leaderAddr(pid)
	if err != nil {
		return err
	}

	req := proto.SetRequest{Key: key, Value: value}

	retries := 5
	for i := 0; i < retries; i++ {
		replyType, payload, err := rt.rpcCli.Call(addr, proto.MsgSet, req)
		if err != nil {
			return fmt.Errorf("router set: rpc: %w", err)
		}
		if replyType == proto.MsgError {
			var errResp proto.ErrorResponse
			if err := rpc.DecodePayload(payload, &errResp); err != nil {
				return fmt.Errorf("router set: decode error resp: %w", err)
			}
			if errResp.ErrCode == proto.ErrNotLeader && errResp.LeaderAddr != "" {
				rt.updateLeader(pid, errResp.LeaderAddr)
				addr = errResp.LeaderAddr
				continue
			}
			return fmt.Errorf("router set: %s", errResp.Message)
		}
		var resp proto.SetResponse
		if err := rpc.DecodePayload(payload, &resp); err != nil {
			return fmt.Errorf("router set: decode resp: %w", err)
		}
		if !resp.Success {
			return fmt.Errorf("router set: server returned failure")
		}
		return nil
	}
	return fmt.Errorf("router set: too many redirects")
}

// Delete sends a DeleteRequest to the correct partition leader.
func (rt *Router) Delete(key string) error {
	pid := GetPartitionID(key, len(rt.partitions))
	addr, err := rt.leaderAddr(pid)
	if err != nil {
		return err
	}

	req := proto.DeleteRequest{Key: key}

	retries := 5
	for i := 0; i < retries; i++ {
		replyType, payload, err := rt.rpcCli.Call(addr, proto.MsgDelete, req)
		if err != nil {
			return fmt.Errorf("router delete: rpc: %w", err)
		}
		if replyType == proto.MsgError {
			var errResp proto.ErrorResponse
			if err := rpc.DecodePayload(payload, &errResp); err != nil {
				return fmt.Errorf("router delete: decode error resp: %w", err)
			}
			if errResp.ErrCode == proto.ErrNotLeader && errResp.LeaderAddr != "" {
				rt.updateLeader(pid, errResp.LeaderAddr)
				addr = errResp.LeaderAddr
				continue
			}
			return fmt.Errorf("router delete: %s", errResp.Message)
		}
		return nil
	}
	return fmt.Errorf("router delete: too many redirects")
}

// UpdateLeader updates the known leader address for a partition.
func (rt *Router) UpdateLeader(partitionID int, leaderAddr string) {
	rt.updateLeader(partitionID, leaderAddr)
}

// leaderAddr returns the known leader address for the given partition,
// falling back to any replica address if the leader is unknown.
func (rt *Router) leaderAddr(partitionID int) (string, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	if partitionID >= len(rt.partitions) {
		return "", fmt.Errorf("partition %d out of range", partitionID)
	}
	p := rt.partitions[partitionID]
	if p.LeaderAddr != "" {
		return p.LeaderAddr, nil
	}
	if len(p.ReplicaAddresses) == 0 {
		return "", fmt.Errorf("no replicas configured for partition %d", partitionID)
	}
	return p.ReplicaAddresses[0], nil
}

// updateLeader updates the cached leader address for a partition.
func (rt *Router) updateLeader(partitionID int, addr string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if partitionID < len(rt.partitions) {
		rt.partitions[partitionID].LeaderAddr = addr
	}
}
