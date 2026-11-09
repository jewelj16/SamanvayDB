// Package router resolves keys to shards and shards to nodes.
//
// Sharding decision (documented trade-off): SamanvayDB uses HASH-based
// sharding, shardId(key) = fnv1a64(key) mod NUM_SHARDS, rather than
// range-based sharding. Hashing gives even load distribution with no hot
// ranges and almost no implementation complexity (no split/merge logic,
// no range metadata). The cost is that adjacent keys scatter across shards,
// making range scans expensive — an acceptable cost because SamanvayDB's API
// deliberately has no range scans. NUM_SHARDS is fixed at cluster creation;
// consistent hashing (so that changing the shard count moves only a
// proportional slice of keys) is documented future work.
package router

import "hash/fnv"

// ShardFor maps a key to a shard ID in [0, numShards).
func ShardFor(key []byte, numShards int32) int32 {
	if numShards <= 1 {
		return 0
	}
	h := fnv.New64a() // reuse a well-distributed library hash; never hand-roll one
	h.Write(key)
	return int32(h.Sum64() % uint64(numShards))
}
