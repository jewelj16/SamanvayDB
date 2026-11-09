// Package node assembles a complete SamanvayDB server process:
//
//	Client -> KV gRPC (kvserver) -> Router (key -> shard) -> Shard (Raft group)
//	       -> FSM -> storage.Engine (map)
//
// plus the Raft transport (shard-ID dispatch over shared gRPC connections),
// the Admin API, the metadata Raft group that stores the shard map, the
// /healthz /metrics /status HTTP endpoints, and the network fault injector.
// cmd/samanvaydb-server is a thin main() around this package, and the
// in-process integration tests start several Nodes directly.
package node

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"samanvaydb/internal/config"
	"samanvaydb/internal/faultinjector"
	"samanvaydb/internal/kvserver"
	"samanvaydb/internal/logging"
	"samanvaydb/internal/metrics"
	"samanvaydb/internal/raft"
	"samanvaydb/internal/router"
	"samanvaydb/internal/server"
	"samanvaydb/internal/shard"
	"samanvaydb/internal/transport"
	"samanvaydb/proto/adminpb"
	"samanvaydb/proto/kvpb"
	"samanvaydb/proto/raftpb"
)

// Options configures a Node.
type Options struct {
	Config *config.Config
	Logger *logging.Logger
	// Listener / HTTPListener are optional pre-bound listeners (tests bind
	// port 0 first so every node's address is known up front).
	Listener     net.Listener
	HTTPListener net.Listener
	// ExpiryInterval overrides the TTL/lock expiry tick (default 500ms).
	ExpiryInterval time.Duration
	// DefaultRequestTimeout bounds client requests that carry no deadline.
	DefaultRequestTimeout time.Duration
}

// Node is one SamanvayDB server.
type Node struct {
	cfg    *config.Config
	id     string
	logger *logging.Logger
	opts   Options

	book    *transport.AddressBook
	pool    *transport.Pool
	faults  *faultinjector.Rules
	rclient *transport.Client
	manager *shard.Manager
	metrics *metrics.Metrics

	grpcSrv  *grpc.Server
	lis      net.Listener
	health   *server.HealthServer
	httpLis  net.Listener
	initial  *router.ShardMap
	smapMu   sync.RWMutex
	smap     *router.ShardMap
	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// New builds and starts a node: it recovers (or bootstraps) every hosted
// shard replica, then starts serving gRPC and HTTP.
func New(opts Options) (*Node, error) {
	cfg := opts.Config
	if cfg == nil {
		return nil, fmt.Errorf("node: config required")
	}
	if opts.Logger == nil {
		opts.Logger = logging.NewLogger(logging.Options{NodeID: cfg.NodeID})
	}
	if opts.DefaultRequestTimeout == 0 {
		opts.DefaultRequestTimeout = 5 * time.Second
	}
	n := &Node{cfg: cfg, id: cfg.NodeID, logger: opts.Logger, opts: opts, stopCh: make(chan struct{})}

	var nodes []router.NodeInfo
	for _, e := range cfg.Cluster.Nodes {
		nodes = append(nodes, router.NodeInfo{ID: e.ID, Addr: e.Addr, HTTPAddr: e.HTTPAddr})
	}
	initial, err := router.InitialShardMap(nodes, cfg.Cluster.NumShards, cfg.Cluster.ReplicationFactor,
		cfg.Cluster.Placement, cfg.Cluster.MetaReplicas)
	if err != nil {
		return nil, fmt.Errorf("node: initial shard map: %w", err)
	}
	n.initial, n.smap = initial, initial

	n.book = transport.NewAddressBook()
	for _, ni := range initial.Nodes {
		n.book.Set(ni.ID, ni.Addr)
	}
	n.book.Resolve = func(id string) (string, bool) {
		if ni, ok := n.cachedMap().Node(id); ok {
			return ni.Addr, true
		}
		return "", false
	}
	n.faults = faultinjector.NewRules(n.id, 0)
	n.pool = transport.NewPool()
	n.rclient = transport.NewClient(n.id, n.book, n.pool, n.faults)
	n.metrics = metrics.New(n.id, n.shardStatuses, n.peerReachability)

	emin, emax := cfg.Raft.ElectionTimeouts()
	n.manager = shard.NewManager(shard.ManagerConfig{
		NodeID:  raft.NodeID(n.id),
		DataDir: cfg.DataDir,
		Logger:  n.logger,
		Configure: func(c shard.Config) shard.Config {
			id := c.ShardID
			c.Transport = n.rclient.ForShard(id)
			c.ElectionTimeoutMin, c.ElectionTimeoutMax = emin, emax
			c.HeartbeatInterval = cfg.Raft.Heartbeat()
			c.SnapshotThreshold = cfg.Raft.Snapshot()
			c.NoSync = cfg.Raft.NoSync
			c.ExpiryInterval = opts.ExpiryInterval
			c.Hooks = metrics.MergeHooks(n.metrics.RaftHooks(id), raft.Hooks{
				OnConfigChange: func(_ uint64, m raft.Membership, _ bool) {
					for _, mem := range m.Members { // learn new members' addresses
						n.book.Set(string(mem.ID), mem.Addr)
					}
				},
			})
			c.OnApplyLag = func(d time.Duration) { n.metrics.ObserveApplyLag(id, d) }
			return c
		},
	})
	if err := n.openShards(); err != nil {
		n.manager.Close()
		return nil, err
	}
	if err := n.serve(); err != nil {
		n.manager.Close()
		return nil, err
	}
	n.wg.Add(1)
	go n.refreshLoop()
	n.health.SetReady()
	n.logger.Info("node ready", "addr", n.Addr(), "http_addr", n.HTTPAddr(), "shards", fmt.Sprint(n.manager.IDs()))
	return n, nil
}

// membershipFor builds a shard's bootstrap Raft membership from the map.
func (n *Node) membershipFor(m *router.ShardMap, replicas []string) *raft.Membership {
	mem := raft.Membership{}
	for _, r := range replicas {
		addr := ""
		if ni, ok := m.Node(r); ok {
			addr = ni.Addr
		}
		mem.Members = append(mem.Members, raft.Member{ID: raft.NodeID(r), Addr: addr})
	}
	return &mem
}

func contains(list []string, x string) bool {
	for _, v := range list {
		if v == x {
			return true
		}
	}
	return false
}

// openShards recovers the shards recorded in the node manifest, or — on
// first boot — bootstraps the ones the initial topology assigns to us.
func (n *Node) openShards() error {
	ids, ok, err := n.manager.LoadManifest()
	if err != nil {
		return err
	}
	bootFor := func(id int32) *raft.Membership {
		if id == shard.MetaShardID {
			if contains(n.initial.MetaReplicas, n.id) {
				return n.membershipFor(n.initial, n.initial.MetaReplicas)
			}
			return nil
		}
		if meta, ok := n.initial.Shard(id); ok && contains(meta.Replicas, n.id) {
			return n.membershipFor(n.initial, meta.Replicas)
		}
		return nil
	}
	if !ok {
		if n.cfg.Join {
			return nil // a joining node hosts nothing until replicas are added
		}
		ids = nil
		if contains(n.initial.MetaReplicas, n.id) {
			ids = append(ids, shard.MetaShardID)
		}
		ids = append(ids, n.initial.ShardsOn(n.id)...)
	}
	for _, id := range ids {
		boot := bootFor(id)
		if _, err := n.manager.Open(id, boot, boot == nil); err != nil {
			return fmt.Errorf("node: open shard %d: %w", id, err)
		}
	}
	return nil
}

func (n *Node) serve() error {
	lis := n.opts.Listener
	if lis == nil {
		var err error
		lis, err = net.Listen("tcp", n.cfg.ListenAddr)
		if err != nil {
			return fmt.Errorf("node: listen %s: %w", n.cfg.ListenAddr, err)
		}
	}
	n.lis = lis
	n.grpcSrv = grpc.NewServer(grpc.MaxRecvMsgSize(transport.MaxMessageSize), grpc.MaxSendMsgSize(transport.MaxMessageSize))
	kvpb.RegisterKVServer(n.grpcSrv, kvserver.NewWithBackend(n, n.logger, n.metrics.ObserveRequest))
	raftpb.RegisterRaftServer(n.grpcSrv, transport.NewServer(n.id, func(id int32) (raft.RPCHandler, bool) {
		s, ok := n.manager.Get(id)
		if !ok {
			return nil, false
		}
		return s.Raft(), true
	}, n.faults))
	adminpb.RegisterAdminServer(n.grpcSrv, &adminServer{n: n})
	go n.grpcSrv.Serve(lis)

	n.health = server.NewHealthServer(n.cfg.HTTPAddr, n.id, n.logger)
	n.health.Handle("/metrics", n.metrics.Handler())
	n.health.Handle("/status", http.HandlerFunc(n.handleStatus))
	httpLis := n.opts.HTTPListener
	if httpLis == nil && n.cfg.HTTPAddr != "" {
		var err error
		httpLis, err = net.Listen("tcp", n.cfg.HTTPAddr)
		if err != nil {
			n.grpcSrv.Stop()
			return fmt.Errorf("node: listen http %s: %w", n.cfg.HTTPAddr, err)
		}
	}
	if httpLis != nil {
		n.httpLis = httpLis
		n.health.StartOn(httpLis)
	}
	return nil
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	var msg proto.Message
	if r.URL.Query().Get("scope") == "node" {
		msg = n.localStatus()
	} else {
		cs, err := n.clusterStatus(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		msg = cs
	}
	b, _ := protojson.MarshalOptions{Indent: "  ", EmitUnpopulated: true}.Marshal(msg)
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

// refreshLoop keeps the cached shard map (and so the address book) current.
func (n *Node) refreshLoop() {
	defer n.wg.Done()
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			n.readShardMap(ctx)
			cancel()
		}
	}
}

// Stop shuts the node down. Every acknowledged write is already durable
// (fsynced Raft log), so Stop and a crash are equivalent for correctness.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		n.grpcSrv.Stop()
		n.manager.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		n.health.Shutdown(ctx)
		cancel()
		n.wg.Wait()
		n.pool.Close()
		n.logger.Info("node stopped")
	})
}

// ID returns the node ID.
func (n *Node) ID() string { return n.id }

// Addr returns the gRPC address.
func (n *Node) Addr() string { return n.lis.Addr().String() }

// HTTPAddr returns the HTTP address ("" if none).
func (n *Node) HTTPAddr() string {
	if n.httpLis == nil {
		return ""
	}
	return n.httpLis.Addr().String()
}

// Manager exposes the shard manager (tests, status).
func (n *Node) Manager() *shard.Manager { return n.manager }

// Faults exposes the network fault rules (tests).
func (n *Node) Faults() *faultinjector.Rules { return n.faults }

// Metrics exposes the node's metrics.
func (n *Node) Metrics() *metrics.Metrics { return n.metrics }

func (n *Node) cachedMap() *router.ShardMap {
	n.smapMu.RLock()
	defer n.smapMu.RUnlock()
	return n.smap
}

func (n *Node) setCachedMap(m *router.ShardMap) {
	n.smapMu.Lock()
	defer n.smapMu.Unlock()
	if m.Version >= n.smap.Version {
		n.smap = m
		for _, ni := range m.Nodes {
			n.book.Set(ni.ID, ni.Addr)
		}
	}
}

func (n *Node) shardStatuses() []metrics.ShardStatus {
	var out []metrics.ShardStatus
	for _, id := range n.manager.IDs() {
		if s, ok := n.manager.Get(id); ok {
			st, keys := s.Status()
			out = append(out, metrics.ShardStatus{ShardID: id, Raft: st, Keys: keys})
		}
	}
	return out
}

// peerReachability estimates how many peer nodes this node currently hears
// from, using Raft contact information across all hosted groups.
func (n *Node) peerReachability() (int, int) {
	m := n.cachedMap()
	reach := map[string]bool{}
	for _, s := range n.shardStatuses() {
		if s.Raft.State == raft.Leader {
			for p, pr := range s.Raft.Progress {
				if pr.Recent {
					reach[string(p)] = true
				}
			}
		} else if s.Raft.Leader != "" && time.Since(s.Raft.LastLeaderContact) < 2*time.Second {
			reach[string(s.Raft.Leader)] = true
		}
	}
	total := 0
	for _, ni := range m.Nodes {
		if ni.ID != n.id {
			total++
		}
	}
	delete(reach, n.id)
	return len(reach), total
}
