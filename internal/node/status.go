package node

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"samanvaydb/internal/membership"
	"samanvaydb/internal/raft"
	"samanvaydb/internal/router"
	"samanvaydb/internal/shard"
	"samanvaydb/proto/adminpb"
)

func roleName(s raft.State) string { return strings.ToLower(s.String()) }

func ids(in []raft.NodeID) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}
	return out
}

// localStatus reports every replica hosted on this node.
func (n *Node) localStatus() *adminpb.NodeStatusResponse {
	resp := &adminpb.NodeStatusResponse{Node: &adminpb.NodeInfo{NodeId: n.id, Up: true, Addr: n.Addr(), HttpAddr: n.HTTPAddr()}}
	for _, id := range n.manager.IDs() {
		s, ok := n.manager.Get(id)
		if !ok {
			continue
		}
		st, keys := s.Status()
		role := roleName(st.State)
		if mem, ok := st.Config.Get(st.ID); ok && mem.Learner {
			role = "learner"
		} else if !st.Config.Contains(st.ID) {
			role = "detached"
		}
		ls := &adminpb.LocalShardStatus{
			ShardId: id, Role: role, Term: st.Term, Leader: string(st.Leader),
			CommitIndex: st.CommitIndex, AppliedIndex: st.LastApplied, LastLogIndex: st.LastLogIndex,
			SnapshotIndex: st.SnapshotIndex, Voters: ids(st.Config.Voters()), Learners: ids(st.Config.Learners()),
			KeyCount: uint64(keys),
		}
		for p, pr := range st.Progress {
			lag := uint64(0)
			if st.LastLogIndex > pr.Match {
				lag = st.LastLogIndex - pr.Match
			}
			r := "follower"
			if pr.Learner {
				r = "learner"
			}
			ls.Followers = append(ls.Followers, &adminpb.ReplicaInfo{NodeId: string(p), Role: r, MatchIndex: pr.Match, Lag: lag, Up: pr.Recent})
		}
		sort.Slice(ls.Followers, func(i, j int) bool { return ls.Followers[i].NodeId < ls.Followers[j].NodeId })
		resp.Node.HostedShards = append(resp.Node.HostedShards, id)
		resp.Shards = append(resp.Shards, ls)
	}
	return resp
}

// nodeStatus fetches NodeStatus from id (locally for self).
func (n *Node) nodeStatus(ctx context.Context, id string) (*adminpb.NodeStatusResponse, error) {
	if id == n.id {
		return n.localStatus(), nil
	}
	cl, err := n.adminClient(id)
	if err != nil {
		return nil, err
	}
	return cl.NodeStatus(ctx, &adminpb.NodeStatusRequest{})
}

// gather queries NodeStatus on every node in parallel.
func (n *Node) gather(ctx context.Context, m *router.ShardMap, only []string) map[string]*adminpb.NodeStatusResponse {
	targets := only
	if targets == nil {
		for _, ni := range m.Nodes {
			targets = append(targets, ni.ID)
		}
	}
	var mu sync.Mutex
	out := map[string]*adminpb.NodeStatusResponse{}
	var wg sync.WaitGroup
	for _, id := range targets {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
			defer cancel()
			if r, err := n.nodeStatus(cctx, id); err == nil {
				mu.Lock()
				out[id] = r
				mu.Unlock()
			}
		}(id)
	}
	wg.Wait()
	return out
}

func findShard(r *adminpb.NodeStatusResponse, id int32) *adminpb.LocalShardStatus {
	if r == nil {
		return nil
	}
	for _, s := range r.Shards {
		if s.ShardId == id {
			return s
		}
	}
	return nil
}

// shardInfo aggregates one shard's status from every replica's report.
func shardInfo(id int32, replicas []string, state string, reports map[string]*adminpb.NodeStatusResponse) *adminpb.ShardInfo {
	info := &adminpb.ShardInfo{ShardId: id, Replicas: replicas, State: state}
	var leader *adminpb.LocalShardStatus
	candidates := append([]string(nil), replicas...)
	for nid, r := range reports { // learners/targets not yet in the map
		if s := findShard(r, id); s != nil && !contains(candidates, nid) {
			candidates = append(candidates, nid)
		}
	}
	for _, nid := range candidates {
		s := findShard(reports[nid], id)
		if s != nil && s.Role == "leader" && (leader == nil || s.Term > leader.Term) {
			leader, info.Leader = s, nid
		}
	}
	if leader != nil {
		info.Term, info.CommittedIndex, info.Learners = leader.Term, leader.CommitIndex, leader.Learners
	}
	sort.Strings(candidates)
	for _, nid := range candidates {
		ri := &adminpb.ReplicaInfo{NodeId: nid, Role: "down"}
		if s := findShard(reports[nid], id); s != nil {
			ri.Role, ri.Up, ri.AppliedIndex = s.Role, true, s.AppliedIndex
			if info.Term == 0 {
				info.Term = s.Term
			}
		} else if _, up := reports[nid]; up {
			ri.Role, ri.Up = "absent", true
		}
		if leader != nil {
			if nid == info.Leader {
				ri.MatchIndex = leader.LastLogIndex
			}
			for _, f := range leader.Followers {
				if f.NodeId == nid {
					ri.MatchIndex, ri.Lag = f.MatchIndex, f.Lag
				}
			}
		}
		info.ReplicaStatus = append(info.ReplicaStatus, ri)
	}
	return info
}

// clusterStatus builds the cluster-wide view: every shard's leader, term,
// committed index, replica roles and lag, plus every node's liveness.
func (n *Node) clusterStatus(ctx context.Context) (*adminpb.ClusterStatusResponse, error) {
	m, err := n.readShardMap(ctx)
	if err != nil {
		m = n.cachedMap()
	}
	reports := n.gather(ctx, m, nil)
	resp := &adminpb.ClusterStatusResponse{ShardMapVersion: m.Version}
	resp.Shards = append(resp.Shards, shardInfo(shard.MetaShardID, m.MetaReplicas, router.StateStable, reports))
	for _, s := range m.Shards {
		resp.Shards = append(resp.Shards, shardInfo(s.ShardID, s.Replicas, s.State, reports))
	}
	for _, ni := range m.Nodes {
		info := &adminpb.NodeInfo{NodeId: ni.ID, Addr: ni.Addr, HttpAddr: ni.HTTPAddr}
		if r, ok := reports[ni.ID]; ok {
			info.Up, info.HostedShards = true, r.Node.HostedShards
		}
		resp.Nodes = append(resp.Nodes, info)
	}
	return resp, nil
}

// shardProgress finds a shard's leader and returns its replication view.
func (n *Node) shardProgress(ctx context.Context, id int32) (membership.Progress, error) {
	m := n.cachedMap()
	var cands []string
	if id == shard.MetaShardID {
		cands = m.MetaReplicas
	} else if meta, ok := m.Shard(id); ok {
		cands = append(append([]string(nil), meta.Replicas...), meta.TargetGroup...)
	}
	if s, ok := n.manager.Get(id); ok {
		st := s.Raft().Status()
		for _, mem := range st.Config.Members {
			if !contains(cands, string(mem.ID)) {
				cands = append(cands, string(mem.ID))
			}
		}
	}
	reports := n.gather(ctx, m, cands)
	var best *adminpb.LocalShardStatus
	var bestID string
	for nid, r := range reports {
		if s := findShard(r, id); s != nil && s.Role == "leader" && (best == nil || s.Term > best.Term) {
			best, bestID = s, nid
		}
	}
	if best == nil {
		return membership.Progress{}, errNoLeader(id)
	}
	p := membership.Progress{Leader: bestID, LastIndex: best.LastLogIndex, Match: map[string]uint64{},
		Voters: best.Voters, Learners: best.Learners}
	for _, f := range best.Followers {
		p.Match[f.NodeId] = f.MatchIndex
	}
	return p, nil
}

func errNoLeader(id int32) error {
	if id == shard.MetaShardID {
		return fmt.Errorf("no leader found for the metadata group")
	}
	return fmt.Errorf("no leader found for shard %d", id)
}
