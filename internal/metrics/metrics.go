// Package metrics exposes Prometheus metrics for a SamanvayDB node at
// /metrics. Each node owns its own registry (so several in-process nodes in
// a test never collide), and the metric vocabulary is exactly the one the
// architecture already uses:
//
//	Request-level  samanvaydb_requests_total{op,code}
//	               samanvaydb_request_duration_seconds{op}   (p50/p95/p99 via histogram_quantile)
//	Raft           samanvaydb_raft_term{shard}
//	               samanvaydb_raft_is_leader{shard}
//	               samanvaydb_raft_elections_total{shard}
//	               samanvaydb_raft_leader_changes_total{shard}
//	               samanvaydb_raft_seconds_since_leader_contact{shard}
//	               samanvaydb_raft_replication_lag_entries{shard,peer}   (leader only)
//	               samanvaydb_raft_committed_entries_total{shard}
//	               samanvaydb_raft_commit_index{shard} / _applied_index / _log_entries
//	               samanvaydb_raft_snapshots_total{shard,kind}
//	               samanvaydb_raft_membership_changes_total{shard}
//	               samanvaydb_raft_apply_lag_seconds{shard}
//	Storage        samanvaydb_storage_keys{shard}
//	Cluster        samanvaydb_cluster_shards_hosted
//	               samanvaydb_cluster_peers_reachable / samanvaydb_cluster_peers_total
//
// The storage engine is an in-memory map, so the LSM-specific metrics from
// the original plan (MemTable size, SSTable counts, compaction runs, bloom
// false positives) have no counterpart; their durability analogues are the
// Raft log size and snapshot counters above.
package metrics

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"samanvaydb/internal/raft"
)

// ShardStatus is the per-shard state sampled at scrape time.
type ShardStatus struct {
	ShardID int32
	Raft    raft.Status
	Keys    int
}

// StatusFunc returns the current state of every hosted shard.
type StatusFunc func() []ShardStatus

// PeerFunc returns (reachable, total) peer nodes from this node's view.
type PeerFunc func() (int, int)

// Metrics holds a node's collectors.
type Metrics struct {
	Registry *prometheus.Registry

	Requests      *prometheus.CounterVec
	Latency       *prometheus.HistogramVec
	Elections     *prometheus.CounterVec
	LeaderChanges *prometheus.CounterVec
	Committed     *prometheus.CounterVec
	Snapshots     *prometheus.CounterVec
	ConfigChanges *prometheus.CounterVec
	ApplyLag      *prometheus.HistogramVec

	mu         sync.Mutex
	lastLeader map[int32]raft.NodeID
	lastCommit map[int32]uint64
}

// New creates the metrics for a node. status and peers are sampled on every
// scrape (either may be nil).
func New(nodeID string, status StatusFunc, peers PeerFunc) *Metrics {
	reg := prometheus.NewRegistry()
	constLabels := prometheus.Labels{"node_id": nodeID}
	m := &Metrics{
		Registry: reg,
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "samanvaydb_requests_total", Help: "Client requests by operation and result code.",
			ConstLabels: constLabels}, []string{"op", "code"}),
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "samanvaydb_request_duration_seconds", Help: "Client request latency.",
			ConstLabels: constLabels,
			Buckets:     []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}}, []string{"op"}),
		Elections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "samanvaydb_raft_elections_total", Help: "Elections started by this replica.",
			ConstLabels: constLabels}, []string{"shard"}),
		LeaderChanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "samanvaydb_raft_leader_changes_total", Help: "Leader changes observed by this replica.",
			ConstLabels: constLabels}, []string{"shard"}),
		Committed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "samanvaydb_raft_committed_entries_total", Help: "Log entries committed (rate() gives entries/sec).",
			ConstLabels: constLabels}, []string{"shard"}),
		Snapshots: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "samanvaydb_raft_snapshots_total", Help: "Snapshots created locally or installed from a leader.",
			ConstLabels: constLabels}, []string{"shard", "kind"}),
		ConfigChanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "samanvaydb_raft_membership_changes_total", Help: "Committed membership changes.",
			ConstLabels: constLabels}, []string{"shard"}),
		ApplyLag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "samanvaydb_raft_apply_lag_seconds", Help: "Time from leader propose to apply on this replica.",
			ConstLabels: constLabels,
			Buckets:     []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}}, []string{"shard"}),
		lastLeader: map[int32]raft.NodeID{},
		lastCommit: map[int32]uint64{},
	}
	reg.MustRegister(m.Requests, m.Latency, m.Elections, m.LeaderChanges, m.Committed,
		m.Snapshots, m.ConfigChanges, m.ApplyLag,
		prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	reg.MustRegister(&stateCollector{status: status, peers: peers, constLabels: constLabels})
	return m
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// ObserveRequest records one client request.
func (m *Metrics) ObserveRequest(op, code string, d time.Duration) {
	m.Requests.WithLabelValues(op, code).Inc()
	m.Latency.WithLabelValues(op).Observe(d.Seconds())
}

// ObserveApplyLag records the apply lag of one entry on shard.
func (m *Metrics) ObserveApplyLag(shard int32, d time.Duration) {
	m.ApplyLag.WithLabelValues(shardLabel(shard)).Observe(d.Seconds())
}

func shardLabel(id int32) string {
	if id < 0 {
		return "meta"
	}
	return fmt.Sprint(id)
}

// RaftHooks returns hooks that keep shard's Raft counters current. They are
// called under the Raft lock and only touch counters.
func (m *Metrics) RaftHooks(shard int32) raft.Hooks {
	label := shardLabel(shard)
	return raft.Hooks{
		OnElectionStarted: func(uint64) { m.Elections.WithLabelValues(label).Inc() },
		OnStateChange: func(_ uint64, _ raft.State, leader raft.NodeID) {
			if leader == "" {
				return
			}
			m.mu.Lock()
			prev, seen := m.lastLeader[shard]
			m.lastLeader[shard] = leader
			m.mu.Unlock()
			if seen && prev != leader {
				m.LeaderChanges.WithLabelValues(label).Inc()
			}
		},
		OnCommit: func(idx uint64) {
			m.mu.Lock()
			prev := m.lastCommit[shard]
			m.lastCommit[shard] = idx
			m.mu.Unlock()
			if idx > prev {
				m.Committed.WithLabelValues(label).Add(float64(idx - prev))
			}
		},
		OnSnapshot: func(_, _ uint64, installed bool) {
			kind := "created"
			if installed {
				kind = "installed"
			}
			m.Snapshots.WithLabelValues(label, kind).Inc()
		},
		OnConfigChange: func(_ uint64, _ raft.Membership, committed bool) {
			if committed {
				m.ConfigChanges.WithLabelValues(label).Inc()
			}
		},
	}
}

// MergeHooks chains two hook sets.
func MergeHooks(a, b raft.Hooks) raft.Hooks {
	return raft.Hooks{
		OnStateChange: func(t uint64, s raft.State, l raft.NodeID) {
			if a.OnStateChange != nil {
				a.OnStateChange(t, s, l)
			}
			if b.OnStateChange != nil {
				b.OnStateChange(t, s, l)
			}
		},
		OnElectionStarted: func(t uint64) {
			if a.OnElectionStarted != nil {
				a.OnElectionStarted(t)
			}
			if b.OnElectionStarted != nil {
				b.OnElectionStarted(t)
			}
		},
		OnSnapshot: func(i, t uint64, inst bool) {
			if a.OnSnapshot != nil {
				a.OnSnapshot(i, t, inst)
			}
			if b.OnSnapshot != nil {
				b.OnSnapshot(i, t, inst)
			}
		},
		OnConfigChange: func(i uint64, mem raft.Membership, c bool) {
			if a.OnConfigChange != nil {
				a.OnConfigChange(i, mem, c)
			}
			if b.OnConfigChange != nil {
				b.OnConfigChange(i, mem, c)
			}
		},
		OnCommit: func(i uint64) {
			if a.OnCommit != nil {
				a.OnCommit(i)
			}
			if b.OnCommit != nil {
				b.OnCommit(i)
			}
		},
	}
}

// stateCollector samples gauges from live shard state at scrape time.
type stateCollector struct {
	status      StatusFunc
	peers       PeerFunc
	constLabels prometheus.Labels
}

var (
	descTerm        = []string{"samanvaydb_raft_term", "Current Raft term."}
	descIsLeader    = []string{"samanvaydb_raft_is_leader", "1 if this replica is the shard leader."}
	descSinceLeader = []string{"samanvaydb_raft_seconds_since_leader_contact", "Seconds since this replica last heard from a leader."}
	descCommit      = []string{"samanvaydb_raft_commit_index", "Raft commit index."}
	descApplied     = []string{"samanvaydb_raft_applied_index", "Raft last-applied index."}
	descLogEntries  = []string{"samanvaydb_raft_log_entries", "Entries in the Raft log since the last snapshot (the effective WAL size)."}
	descKeys        = []string{"samanvaydb_storage_keys", "Keys stored in this replica's engine."}
	descLag         = []string{"samanvaydb_raft_replication_lag_entries", "Leader-observed replication lag per follower, in entries."}
	descHosted      = []string{"samanvaydb_cluster_shards_hosted", "Shard replicas hosted on this node."}
	descPeersUp     = []string{"samanvaydb_cluster_peers_reachable", "Peer nodes reachable from this node."}
	descPeersTotal  = []string{"samanvaydb_cluster_peers_total", "Peer nodes known to this node."}
)

func (c *stateCollector) desc(d []string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(d[0], d[1], labels, c.constLabels)
}

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range [][]string{descTerm, descIsLeader, descSinceLeader, descCommit, descApplied, descLogEntries, descKeys} {
		ch <- c.desc(d, "shard")
	}
	ch <- c.desc(descLag, "shard", "peer")
	ch <- c.desc(descHosted)
	ch <- c.desc(descPeersUp)
	ch <- c.desc(descPeersTotal)
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	var statuses []ShardStatus
	if c.status != nil {
		statuses = c.status()
	}
	now := time.Now()
	for _, s := range statuses {
		l := shardLabel(s.ShardID)
		g := func(d []string, v float64) {
			ch <- prometheus.MustNewConstMetric(c.desc(d, "shard"), prometheus.GaugeValue, v, l)
		}
		g(descTerm, float64(s.Raft.Term))
		leader := 0.0
		if s.Raft.State == raft.Leader {
			leader = 1
		}
		g(descIsLeader, leader)
		since := -1.0
		if !s.Raft.LastLeaderContact.IsZero() {
			since = now.Sub(s.Raft.LastLeaderContact).Seconds()
		}
		g(descSinceLeader, since)
		g(descCommit, float64(s.Raft.CommitIndex))
		g(descApplied, float64(s.Raft.LastApplied))
		g(descLogEntries, float64(s.Raft.LastLogIndex-s.Raft.SnapshotIndex))
		g(descKeys, float64(s.Keys))
		for peer, p := range s.Raft.Progress {
			lag := float64(0)
			if s.Raft.LastLogIndex > p.Match {
				lag = float64(s.Raft.LastLogIndex - p.Match)
			}
			ch <- prometheus.MustNewConstMetric(c.desc(descLag, "shard", "peer"), prometheus.GaugeValue, lag, l, string(peer))
		}
	}
	ch <- prometheus.MustNewConstMetric(c.desc(descHosted), prometheus.GaugeValue, float64(len(statuses)))
	if c.peers != nil {
		up, total := c.peers()
		ch <- prometheus.MustNewConstMetric(c.desc(descPeersUp), prometheus.GaugeValue, float64(up))
		ch <- prometheus.MustNewConstMetric(c.desc(descPeersTotal), prometheus.GaugeValue, float64(total))
	}
}
