package router

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Shard states.
const (
	StateStable    = "STABLE"
	StateMigrating = "MIGRATING"
)

// NodeInfo is a cluster member's addresses.
type NodeInfo struct {
	ID       string `json:"id"`
	Addr     string `json:"addr"`                // gRPC (KV + Raft + Admin)
	HTTPAddr string `json:"http_addr,omitempty"` // /healthz, /metrics, /status
}

// ShardMeta is cluster-wide metadata about one shard: which nodes form its
// Raft group, and its migration state. It is metadata ABOUT shards, kept
// strictly separate from the sharded data itself.
type ShardMeta struct {
	ShardID  int32    `json:"shard_id"`
	Replicas []string `json:"replicas"`
	State    string   `json:"state"`
	// Migration bookkeeping (empty unless State == MIGRATING).
	SourceGroup    []string `json:"source_group,omitempty"`
	TargetGroup    []string `json:"target_group,omitempty"`
	MigrationPhase string   `json:"migration_phase,omitempty"`
}

// ShardMap is the versioned cluster topology. Every update bumps Version;
// consumers compare versions to detect staleness. It is stored in the
// metadata Raft group (shard.MetaShardID) and updated only by
// compare-and-swap, so concurrent updaters cannot clobber each other.
type ShardMap struct {
	Version      uint64      `json:"version"`
	NumShards    int32       `json:"num_shards"`
	Nodes        []NodeInfo  `json:"nodes"`
	Shards       []ShardMeta `json:"shards"`
	MetaReplicas []string    `json:"meta_replicas"`
}

// InitialShardMap derives version 1 of the map deterministically from the
// cluster's bootstrap topology. Every node computes the identical map.
//
// Default placement: replica j of shard i lives on node (i*rf + j) mod N,
// which gives disjoint replica sets whenever N == shards*rf (e.g. the 9-node,
// 3-shard, RF=3 demo topology) and spreads replicas round-robin otherwise.
func InitialShardMap(nodes []NodeInfo, numShards int32, rf int, placement map[int32][]string, metaReplicas []string) (*ShardMap, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("router: no nodes")
	}
	if numShards < 1 {
		return nil, fmt.Errorf("router: num_shards must be >= 1")
	}
	if rf < 1 {
		rf = 1
	}
	if rf > len(nodes) {
		return nil, fmt.Errorf("router: replication factor %d exceeds node count %d", rf, len(nodes))
	}
	m := &ShardMap{Version: 1, NumShards: numShards, Nodes: append([]NodeInfo(nil), nodes...)}
	for i := int32(0); i < numShards; i++ {
		meta := ShardMeta{ShardID: i, State: StateStable}
		if p, ok := placement[i]; ok {
			meta.Replicas = append([]string(nil), p...)
		} else {
			for j := 0; j < rf; j++ {
				meta.Replicas = append(meta.Replicas, nodes[(int(i)*rf+j)%len(nodes)].ID)
			}
		}
		sort.Strings(meta.Replicas)
		m.Shards = append(m.Shards, meta)
	}
	if len(metaReplicas) == 0 {
		for i := 0; i < len(nodes) && i < 3; i++ {
			metaReplicas = append(metaReplicas, nodes[i].ID)
		}
	}
	m.MetaReplicas = append([]string(nil), metaReplicas...)
	sort.Strings(m.MetaReplicas)
	return m, m.Validate()
}

// Validate checks internal consistency.
func (m *ShardMap) Validate() error {
	known := map[string]bool{}
	for _, n := range m.Nodes {
		if n.ID == "" || n.Addr == "" {
			return fmt.Errorf("router: node with empty id/addr")
		}
		known[n.ID] = true
	}
	if int32(len(m.Shards)) != m.NumShards {
		return fmt.Errorf("router: %d shards listed, num_shards=%d", len(m.Shards), m.NumShards)
	}
	for i, s := range m.Shards {
		if s.ShardID != int32(i) {
			return fmt.Errorf("router: shard at position %d has id %d", i, s.ShardID)
		}
		if len(s.Replicas) == 0 {
			return fmt.Errorf("router: shard %d has no replicas", i)
		}
		for _, r := range append(append([]string{}, s.Replicas...), s.TargetGroup...) {
			if !known[r] {
				return fmt.Errorf("router: shard %d references unknown node %q", i, r)
			}
		}
	}
	for _, r := range m.MetaReplicas {
		if !known[r] {
			return fmt.Errorf("router: meta group references unknown node %q", r)
		}
	}
	return nil
}

// ShardFor maps key to its shard under this map.
func (m *ShardMap) ShardFor(key []byte) int32 { return ShardFor(key, m.NumShards) }

// Shard returns the metadata for shard id.
func (m *ShardMap) Shard(id int32) (ShardMeta, bool) {
	if id < 0 || int(id) >= len(m.Shards) {
		return ShardMeta{}, false
	}
	return m.Shards[id], true
}

// Node returns a node's addresses.
func (m *ShardMap) Node(id string) (NodeInfo, bool) {
	for _, n := range m.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return NodeInfo{}, false
}

// ShardsOn returns the shards whose replica set includes node.
func (m *ShardMap) ShardsOn(node string) []int32 {
	var out []int32
	for _, s := range m.Shards {
		for _, r := range s.Replicas {
			if r == node {
				out = append(out, s.ShardID)
				break
			}
		}
	}
	return out
}

// ReplicaCounts returns node -> number of shard replicas it hosts.
func (m *ShardMap) ReplicaCounts() map[string]int {
	out := map[string]int{}
	for _, n := range m.Nodes {
		out[n.ID] = 0
	}
	for _, s := range m.Shards {
		for _, r := range s.Replicas {
			out[r]++
		}
	}
	return out
}

// Clone returns a deep copy.
func (m *ShardMap) Clone() *ShardMap {
	b, _ := json.Marshal(m)
	var c ShardMap
	json.Unmarshal(b, &c)
	return &c
}

// Encode serializes the map (deterministically).
func (m *ShardMap) Encode() []byte {
	b, err := json.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("router: encode shard map: %v", err))
	}
	return b
}

// DecodeShardMap parses an encoded map.
func DecodeShardMap(b []byte) (*ShardMap, error) {
	var m ShardMap
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("router: decode shard map: %w", err)
	}
	return &m, nil
}
