// Package testcluster runs a multi-node SamanvayDB cluster inside one test
// process: every node is a full internal/node.Node with real gRPC servers on
// localhost ports and real on-disk Raft logs, so integration tests exercise
// the same code paths as a deployed cluster (routing, redirects, Raft over
// gRPC, metadata group, admin API). Kill stops a node abruptly; Restart
// brings it back from its data directory on the same address.
package testcluster

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"samanvaydb/internal/config"
	"samanvaydb/internal/logging"
	"samanvaydb/internal/node"
	"samanvaydb/internal/raft"
	"samanvaydb/internal/router"
	"samanvaydb/internal/shard"
	"samanvaydb/pkg/client"
)

// Options configures a test cluster.
type Options struct {
	Nodes        int
	Shards       int32
	RF           int
	MetaReplicas []string
	// Raft timing; zero values use fast test defaults
	// (election 150–300ms, heartbeat 30ms, snapshot every 500 entries).
	Raft config.RaftConfig
	// Sync enables fsync (off by default: an in-process "kill" never loses
	// the OS page cache, so it does not change test outcomes).
	Sync           bool
	ExpiryInterval time.Duration
	LogLevel       slog.Level
}

// Cluster is a running in-process cluster.
type Cluster struct {
	t    testing.TB
	opts Options
	dir  string

	mu     sync.Mutex
	cfgs   map[string]*config.Config
	nodes  map[string]*node.Node
	order  []string
	logOut *os.File
}

func (o *Options) defaults() {
	if o.Nodes == 0 {
		o.Nodes = 3
	}
	if o.Shards == 0 {
		o.Shards = 1
	}
	if o.RF == 0 {
		o.RF = o.Nodes
		if o.RF > 3 {
			o.RF = 3
		}
	}
	if o.Raft.ElectionTimeoutMinMs == 0 {
		o.Raft.ElectionTimeoutMinMs = 150
		o.Raft.ElectionTimeoutMaxMs = 300
	}
	if o.Raft.HeartbeatIntervalMs == 0 {
		o.Raft.HeartbeatIntervalMs = 30
	}
	if o.Raft.SnapshotThreshold == 0 {
		o.Raft.SnapshotThreshold = 500
	}
	o.Raft.NoSync = !o.Sync
	if o.ExpiryInterval == 0 {
		o.ExpiryInterval = 100 * time.Millisecond
	}
	if o.LogLevel == 0 {
		o.LogLevel = slog.LevelError + 4
	}
}

func listen() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }

// Start launches the cluster and waits until every shard (and the metadata
// group) has a leader.
func Start(t testing.TB, opts Options) *Cluster {
	t.Helper()
	opts.defaults()
	c := &Cluster{t: t, opts: opts, dir: t.TempDir(), cfgs: map[string]*config.Config{}, nodes: map[string]*node.Node{}}
	if lvl := os.Getenv("SAMANVAY_TEST_LOG"); lvl != "" {
		c.opts.LogLevel = slog.LevelInfo
	}

	lis := map[string][2]net.Listener{}
	var entries []config.NodeEntry
	for i := 1; i <= opts.Nodes; i++ {
		id := fmt.Sprintf("node-%d", i)
		g, err := listen()
		if err != nil {
			t.Fatal(err)
		}
		h, err := listen()
		if err != nil {
			t.Fatal(err)
		}
		lis[id] = [2]net.Listener{g, h}
		entries = append(entries, config.NodeEntry{ID: id, Addr: g.Addr().String(), HTTPAddr: h.Addr().String()})
		c.order = append(c.order, id)
	}
	for _, e := range entries {
		cfg := &config.Config{
			NodeID: e.ID, ListenAddr: e.Addr, HTTPAddr: e.HTTPAddr,
			DataDir: filepath.Join(c.dir, e.ID),
			Cluster: config.ClusterConfig{Nodes: entries, NumShards: opts.Shards, ReplicationFactor: opts.RF, MetaReplicas: opts.MetaReplicas},
			Raft:    opts.Raft,
		}
		cfg.ApplyDefaults()
		c.cfgs[e.ID] = cfg
	}
	var wg sync.WaitGroup
	var startErr error
	var emu sync.Mutex
	for _, id := range c.order {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := c.startNode(id, lis[id][0], lis[id][1]); err != nil {
				emu.Lock()
				startErr = err
				emu.Unlock()
			}
		}(id)
	}
	wg.Wait()
	if startErr != nil {
		c.Stop()
		t.Fatal(startErr)
	}
	t.Cleanup(c.Stop)
	if err := c.WaitReady(15 * time.Second); err != nil {
		t.Fatal(err)
	}
	return c
}

func (c *Cluster) startNode(id string, g, h net.Listener) error {
	cfg := c.cfgs[id]
	logger := logging.NewLogger(logging.Options{NodeID: id, Level: c.opts.LogLevel, Output: os.Stderr})
	n, err := node.New(node.Options{Config: cfg, Logger: logger, Listener: g, HTTPListener: h, ExpiryInterval: c.opts.ExpiryInterval})
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
	return nil
}

// relisten binds addr again (retrying while the old socket is released).
func relisten(addr string) (net.Listener, error) {
	var err error
	for i := 0; i < 50; i++ {
		var l net.Listener
		if l, err = net.Listen("tcp", addr); err == nil {
			return l, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, err
}

// Node returns a live node.
func (c *Cluster) Node(id string) *node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// IDs returns every node ID ever started (live or killed), in order.
func (c *Cluster) IDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.order...)
}

// Live returns the IDs of running nodes.
func (c *Cluster) Live() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, id := range c.order {
		if c.nodes[id] != nil {
			out = append(out, id)
		}
	}
	return out
}

// Addr returns a node's gRPC address.
func (c *Cluster) Addr(id string) string { return c.cfgs[id].ListenAddr }

// HTTPAddr returns a node's HTTP address.
func (c *Cluster) HTTPAddr(id string) string { return c.cfgs[id].HTTPAddr }

// Seeds returns the gRPC addresses of every configured node.
func (c *Cluster) Seeds() []string {
	var out []string
	for _, id := range c.IDs() {
		out = append(out, c.Addr(id))
	}
	return out
}

// Kill stops a node abruptly (its data directory is kept).
func (c *Cluster) Kill(id string) {
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	if n != nil {
		n.Stop()
	}
}

// Restart brings a killed node back from its data directory.
func (c *Cluster) Restart(id string) error {
	cfg := c.cfgs[id]
	g, err := relisten(cfg.ListenAddr)
	if err != nil {
		return err
	}
	h, err := relisten(cfg.HTTPAddr)
	if err != nil {
		g.Close()
		return err
	}
	return c.startNode(id, g, h)
}

// AddNode starts a brand-new node in join mode (hosting nothing until the
// admin API adds replicas to it) and returns its addresses.
func (c *Cluster) AddNode(id string) (router.NodeInfo, error) {
	g, err := listen()
	if err != nil {
		return router.NodeInfo{}, err
	}
	h, err := listen()
	if err != nil {
		return router.NodeInfo{}, err
	}
	first := c.cfgs[c.order[0]]
	cfg := &config.Config{
		NodeID: id, ListenAddr: g.Addr().String(), HTTPAddr: h.Addr().String(),
		DataDir: filepath.Join(c.dir, id), Join: true,
		Cluster: first.Cluster, Raft: c.opts.Raft,
	}
	c.mu.Lock()
	c.cfgs[id] = cfg
	c.order = append(c.order, id)
	c.mu.Unlock()
	if err := c.startNode(id, g, h); err != nil {
		return router.NodeInfo{}, err
	}
	return router.NodeInfo{ID: id, Addr: cfg.ListenAddr, HTTPAddr: cfg.HTTPAddr}, nil
}

// Stop shuts down every node.
func (c *Cluster) Stop() {
	for _, id := range c.Live() {
		c.Kill(id)
	}
}

// Client returns a client connected to the cluster.
func (c *Cluster) Client() *client.Client {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cl, err := client.New(ctx, client.Config{Seeds: c.Seeds()})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { cl.Close() })
	return cl
}

// ShardLeader returns the live node currently leading shard id.
func (c *Cluster) ShardLeader(id int32) (string, uint64, bool) {
	var best string
	var bestTerm uint64
	for _, nid := range c.Live() {
		n := c.Node(nid)
		if n == nil {
			continue
		}
		if s, ok := n.Manager().Get(id); ok {
			st := s.Raft().Status()
			if st.State == raft.Leader && st.Term >= bestTerm {
				best, bestTerm = nid, st.Term
			}
		}
	}
	return best, bestTerm, best != ""
}

// WaitLeader waits for shard id to have a leader.
func (c *Cluster) WaitLeader(id int32, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l, _, ok := c.ShardLeader(id); ok {
			return l, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "", fmt.Errorf("testcluster: shard %d has no leader after %v", id, timeout)
}

// WaitReady waits until the metadata group and every shard have leaders.
func (c *Cluster) WaitReady(timeout time.Duration) error {
	for _, id := range append([]int32{shard.MetaShardID}, c.shardIDs()...) {
		if _, err := c.WaitLeader(id, timeout); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) shardIDs() []int32 {
	var out []int32
	for i := int32(0); i < c.opts.Shards; i++ {
		out = append(out, i)
	}
	return out
}

// Replicas returns the live nodes hosting a replica of shard id.
func (c *Cluster) Replicas(id int32) []string {
	var out []string
	for _, nid := range c.Live() {
		if _, ok := c.Node(nid).Manager().Get(id); ok {
			out = append(out, nid)
		}
	}
	sort.Strings(out)
	return out
}

// Partition applies a network partition on every live node.
func (c *Cluster) Partition(a, b []string) {
	for _, id := range c.Live() {
		c.Node(id).Faults().Partition(a, b)
	}
}

// Heal removes every network fault on every live node.
func (c *Cluster) Heal() {
	for _, id := range c.Live() {
		c.Node(id).Faults().Clear()
	}
}

// WaitConverged waits until every live replica of shard id has applied the
// leader's commit index.
func (c *Cluster) WaitConverged(id int32, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		l, _, ok := c.ShardLeader(id)
		if ok {
			target := c.Node(l).Manager()
			s, _ := target.Get(id)
			commit := s.Raft().Status().CommitIndex
			all := true
			for _, nid := range c.Replicas(id) {
				r, ok := c.Node(nid).Manager().Get(id)
				if !ok || r.Raft().Status().LastApplied < commit {
					all = false
				}
			}
			if all {
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("testcluster: shard %d replicas did not converge within %v", id, timeout)
}
