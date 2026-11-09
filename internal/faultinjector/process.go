package faultinjector

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// LocalCluster manages a cluster of real samanvaydb-server OS processes on
// this machine. It is the process backend of FaultInjector and the engine
// behind `kvctl cluster start/stop`, `kvctl node kill/restart/add`.
//
// Its state file (<dir>/cluster.json) records only what is needed to manage
// the processes (binary, ports, PIDs, config paths). Topology questions are
// always answered by the cluster's Admin API, never from this file.
type LocalCluster struct {
	Dir    string
	mu     sync.Mutex
	State  LocalState
	procs  map[string]*os.Process
	Admin  *AdminNetwork
	waitCh map[string]chan struct{}
}

// LocalNode is one managed process.
type LocalNode struct {
	ID       string `json:"id"`
	Addr     string `json:"addr"`
	HTTPAddr string `json:"http_addr"`
	Config   string `json:"config"`
	DataDir  string `json:"data_dir"`
	Log      string `json:"log"`
	PID      int    `json:"pid"`
	Join     bool   `json:"join,omitempty"`
}

// LocalState is persisted in cluster.json.
type LocalState struct {
	Binary    string      `json:"binary"`
	Shards    int32       `json:"shards"`
	RF        int         `json:"replication_factor"`
	Nodes     []LocalNode `json:"nodes"`
	SeedNodes []string    `json:"seed_nodes"` // the bootstrap topology
}

// LocalOptions configures CreateLocal.
type LocalOptions struct {
	Dir      string // state/data directory (e.g. ./.samanvay-local)
	Binary   string // path to samanvaydb-server
	Nodes    int
	Shards   int32
	RF       int
	BasePort int // 0 = pick free ports
	// Extra raft settings written into every node config.
	Raft map[string]any
}

type yamlNode struct {
	ID       string `yaml:"id"`
	Addr     string `yaml:"addr"`
	HTTPAddr string `yaml:"http_addr"`
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// CreateLocal writes configs for a new cluster and starts every node.
func CreateLocal(o LocalOptions) (*LocalCluster, error) {
	if o.Nodes < 1 {
		return nil, fmt.Errorf("faultinjector: need at least one node")
	}
	if o.RF == 0 {
		o.RF = 3
		if o.Nodes < 3 {
			o.RF = o.Nodes
		}
	}
	if o.Shards == 0 {
		o.Shards = 1
	}
	abs, err := filepath.Abs(o.Dir)
	if err != nil {
		return nil, err
	}
	o.Dir = abs
	if _, err := os.Stat(filepath.Join(o.Dir, "cluster.json")); err == nil {
		return nil, fmt.Errorf("faultinjector: a cluster already exists in %s (stop it or remove the directory)", o.Dir)
	}
	if err := os.MkdirAll(o.Dir, 0o755); err != nil {
		return nil, err
	}
	bin, err := filepath.Abs(o.Binary)
	if err != nil {
		return nil, err
	}
	lc := &LocalCluster{Dir: o.Dir, State: LocalState{Binary: bin, Shards: o.Shards, RF: o.RF}}
	var topo []yamlNode
	for i := 1; i <= o.Nodes; i++ {
		var gp, hp int
		if o.BasePort > 0 {
			gp, hp = o.BasePort+i, o.BasePort+1000+i
		} else {
			if gp, err = freePort(); err != nil {
				return nil, err
			}
			if hp, err = freePort(); err != nil {
				return nil, err
			}
		}
		id := fmt.Sprintf("node-%d", i)
		topo = append(topo, yamlNode{ID: id, Addr: fmt.Sprintf("127.0.0.1:%d", gp), HTTPAddr: fmt.Sprintf("127.0.0.1:%d", hp)})
		lc.State.SeedNodes = append(lc.State.SeedNodes, id)
	}
	for _, n := range topo {
		ln, err := lc.writeConfig(n, topo, false, o.Raft)
		if err != nil {
			return nil, err
		}
		lc.State.Nodes = append(lc.State.Nodes, ln)
	}
	lc.init()
	for _, n := range lc.State.Nodes {
		if err := lc.start(n.ID); err != nil {
			return lc, err
		}
	}
	return lc, lc.save()
}

func (lc *LocalCluster) writeConfig(n yamlNode, topo []yamlNode, join bool, raftCfg map[string]any) (LocalNode, error) {
	dir := filepath.Join(lc.Dir, n.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return LocalNode{}, err
	}
	cfg := map[string]any{
		"node_id":     n.ID,
		"listen_addr": n.Addr,
		"http_addr":   n.HTTPAddr,
		"data_dir":    filepath.ToSlash(filepath.Join(dir, "data")),
		"join":        join,
		"cluster": map[string]any{
			"num_shards":         lc.State.Shards,
			"replication_factor": lc.State.RF,
			"nodes":              topo,
		},
	}
	if len(raftCfg) > 0 {
		cfg["raft"] = raftCfg
	}
	b, err := yaml.Marshal(cfg)
	if err != nil {
		return LocalNode{}, err
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return LocalNode{}, err
	}
	return LocalNode{ID: n.ID, Addr: n.Addr, HTTPAddr: n.HTTPAddr, Config: path,
		DataDir: filepath.Join(dir, "data"), Log: filepath.Join(dir, "node.log"), Join: join}, nil
}

// OpenLocal loads a cluster created earlier (e.g. by a previous kvctl run).
func OpenLocal(dir string) (*LocalCluster, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(filepath.Join(abs, "cluster.json"))
	if err != nil {
		return nil, fmt.Errorf("faultinjector: no local cluster in %s (run `kvctl cluster start` first): %w", abs, err)
	}
	lc := &LocalCluster{Dir: abs}
	if err := json.Unmarshal(b, &lc.State); err != nil {
		return nil, err
	}
	lc.init()
	return lc, nil
}

func (lc *LocalCluster) init() {
	lc.procs = map[string]*os.Process{}
	lc.waitCh = map[string]chan struct{}{}
	lc.Admin = &AdminNetwork{Nodes: lc.Addrs}
}

func (lc *LocalCluster) save() error {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	b, _ := json.MarshalIndent(lc.State, "", "  ")
	tmp := filepath.Join(lc.Dir, "cluster.json.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(lc.Dir, "cluster.json"))
}

// Addrs returns nodeID -> gRPC address for every managed node.
func (lc *LocalCluster) Addrs() map[string]string {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	out := map[string]string{}
	for _, n := range lc.State.Nodes {
		out[n.ID] = n.Addr
	}
	return out
}

// Seeds returns every node's gRPC address.
func (lc *LocalCluster) Seeds() []string {
	var out []string
	for _, a := range lc.Addrs() {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

// NodeIDs returns the managed node IDs.
func (lc *LocalCluster) NodeIDs() []string {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	var out []string
	for _, n := range lc.State.Nodes {
		out = append(out, n.ID)
	}
	return out
}

func (lc *LocalCluster) node(id string) (*LocalNode, error) {
	for i := range lc.State.Nodes {
		if lc.State.Nodes[i].ID == id {
			return &lc.State.Nodes[i], nil
		}
	}
	return nil, fmt.Errorf("faultinjector: unknown node %q", id)
}

func (lc *LocalCluster) start(id string) error {
	lc.mu.Lock()
	n, err := lc.node(id)
	if err != nil {
		lc.mu.Unlock()
		return err
	}
	logf, err := os.OpenFile(n.Log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		lc.mu.Unlock()
		return err
	}
	cmd := exec.Command(lc.State.Binary, "-config", n.Config)
	cmd.Stdout, cmd.Stderr = logf, logf
	detach(cmd)
	if err := cmd.Start(); err != nil {
		lc.mu.Unlock()
		logf.Close()
		return fmt.Errorf("faultinjector: start %s: %w", id, err)
	}
	n.PID = cmd.Process.Pid
	lc.procs[id] = cmd.Process
	done := make(chan struct{})
	lc.waitCh[id] = done
	httpAddr := n.HTTPAddr
	lc.mu.Unlock()
	go func() { cmd.Wait(); logf.Close(); close(done) }()
	return waitHealthy(httpAddr, 15*time.Second)
}

func waitHealthy(httpAddr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	cl := http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		if resp, err := cl.Get("http://" + httpAddr + "/healthz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("faultinjector: node at %s not healthy after %v", httpAddr, timeout)
}

// Alive reports whether the node's HTTP endpoint answers.
func (lc *LocalCluster) Alive(id string) bool {
	lc.mu.Lock()
	n, err := lc.node(id)
	lc.mu.Unlock()
	if err != nil {
		return false
	}
	cl := http.Client{Timeout: 300 * time.Millisecond}
	resp, err := cl.Get("http://" + n.HTTPAddr + "/healthz")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return true
}

// KillNode crashes a node: SIGKILL (Unix) / TerminateProcess (Windows) —
// no graceful shutdown. It returns once the process is really gone.
func (lc *LocalCluster) KillNode(id string) error {
	lc.mu.Lock()
	n, err := lc.node(id)
	if err != nil {
		lc.mu.Unlock()
		return err
	}
	p := lc.procs[id]
	done := lc.waitCh[id]
	pid := n.PID
	lc.mu.Unlock()
	if p == nil {
		if pid == 0 {
			return fmt.Errorf("faultinjector: %s is not running", id)
		}
		if p, err = os.FindProcess(pid); err != nil {
			return err
		}
	}
	if err := p.Kill(); err != nil && !strings.Contains(err.Error(), "finished") {
		return fmt.Errorf("faultinjector: kill %s (pid %d): %w", id, pid, err)
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			return fmt.Errorf("faultinjector: %s (pid %d) did not exit", id, pid)
		}
	} else {
		deadline := time.Now().Add(5 * time.Second)
		for lc.Alive(id) && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
		}
	}
	lc.mu.Lock()
	n.PID = 0
	delete(lc.procs, id)
	lc.mu.Unlock()
	return lc.save()
}

// RestartNode starts a killed node again from its data directory.
func (lc *LocalCluster) RestartNode(id string) error {
	if lc.Alive(id) {
		return fmt.Errorf("faultinjector: %s is already running", id)
	}
	if err := lc.start(id); err != nil {
		return err
	}
	return lc.save()
}

// AddNode creates and starts a new node in join mode (it hosts nothing
// until the admin API adds replicas to it).
func (lc *LocalCluster) AddNode(id string) (LocalNode, error) {
	gp, err := freePort()
	if err != nil {
		return LocalNode{}, err
	}
	hp, err := freePort()
	if err != nil {
		return LocalNode{}, err
	}
	var topo []yamlNode
	for _, n := range lc.State.Nodes {
		if contains(lc.State.SeedNodes, n.ID) {
			topo = append(topo, yamlNode{ID: n.ID, Addr: n.Addr, HTTPAddr: n.HTTPAddr})
		}
	}
	ln, err := lc.writeConfig(yamlNode{ID: id, Addr: fmt.Sprintf("127.0.0.1:%d", gp), HTTPAddr: fmt.Sprintf("127.0.0.1:%d", hp)}, topo, true, nil)
	if err != nil {
		return LocalNode{}, err
	}
	lc.mu.Lock()
	lc.State.Nodes = append(lc.State.Nodes, ln)
	lc.mu.Unlock()
	if err := lc.start(id); err != nil {
		return ln, err
	}
	return ln, lc.save()
}

// StopAll kills every node and removes the state file (data is kept in Dir
// unless purge).
func (lc *LocalCluster) StopAll(purge bool) error {
	for _, id := range lc.NodeIDs() {
		lc.mu.Lock()
		n, _ := lc.node(id)
		running := n.PID != 0
		lc.mu.Unlock()
		if running {
			_ = lc.KillNode(id)
		}
	}
	lc.Admin.Close()
	if purge {
		return os.RemoveAll(lc.Dir)
	}
	return os.Remove(filepath.Join(lc.Dir, "cluster.json"))
}

// FaultInjector surface: process faults here, network faults via Admin.

func (lc *LocalCluster) CreatePartition(a, b []string) error { return lc.Admin.CreatePartition(a, b) }
func (lc *LocalCluster) HealPartition() error                { return lc.Admin.HealPartition() }
func (lc *LocalCluster) DelayMessages(from, to string, d time.Duration) error {
	return lc.Admin.DelayMessages(from, to, d)
}
func (lc *LocalCluster) DropMessages(from, to string, rate float64) error {
	return lc.Admin.DropMessages(from, to, rate)
}
func (lc *LocalCluster) SlowNode(id string, d time.Duration) error { return lc.Admin.SlowNode(id, d) }

var (
	_ FaultInjector = (*LocalCluster)(nil)
	_ SlowInjector  = (*LocalCluster)(nil)
)
