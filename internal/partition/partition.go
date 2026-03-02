// Implements partition identification and key-to-partition
// routing for the distributed key-value database.
//
// The cluster is divided into PartitionCount (=3) independent shards.
// Each shard is a separate Raft consensus group with ReplicaCount (=3) nodes.
// The partition for a given key is determined by FNV-1a hash mod PartitionCount.
package partition
