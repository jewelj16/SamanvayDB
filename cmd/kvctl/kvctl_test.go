package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"samanvaydb/internal/testcluster"
)

// run executes kvctl in-process and returns stdout and the exit code.
func run(t *testing.T, seeds []string, args ...string) (string, int) {
	t.Helper()
	var out, errOut bytes.Buffer
	root, _ := newRootCmd(&out, &errOut)
	root.SetArgs(append([]string{"--server", strings.Join(seeds, ","), "--local-dir", t.TempDir()}, args...))
	err := root.Execute()
	return out.String(), exitCodeFor(err)
}

func runJSON(t *testing.T, seeds []string, v any, args ...string) int {
	t.Helper()
	out, code := run(t, seeds, append(args, "--output", "json")...)
	if err := json.Unmarshal([]byte(out), v); err != nil {
		t.Fatalf("kvctl %v: output is not JSON (%v):\n%s", args, err, out)
	}
	return code
}

func TestKvctl_DataCommands(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 2})
	seeds := c.Seeds()

	if out, code := run(t, seeds, "put", "greeting", "hello"); code != 0 || !strings.Contains(out, "OK") {
		t.Fatalf("put: code=%d out=%q", code, out)
	}
	if out, code := run(t, seeds, "get", "greeting"); code != 0 || strings.TrimSpace(out) != "hello" {
		t.Fatalf("get: code=%d out=%q", code, out)
	}
	var got dataResult
	if code := runJSON(t, seeds, &got, "get", "greeting", "--consistency", "follower-ok"); code != 0 || !got.OK || got.Value != "hello" {
		t.Fatalf("get json: code=%d %+v", code, got)
	}
	if _, code := run(t, seeds, "cas", "greeting", "wrong", "x"); code != exitPrecondition {
		t.Fatalf("failed cas exit code = %d, want %d", code, exitPrecondition)
	}
	if _, code := run(t, seeds, "cas", "greeting", "hello", "bye"); code != 0 {
		t.Fatalf("cas exit code = %d", code)
	}
	if _, code := run(t, seeds, "delete", "greeting"); code != 0 {
		t.Fatal("delete failed")
	}
	if out, code := run(t, seeds, "get", "greeting"); code != exitNotFound || !strings.Contains(out, "not found") {
		t.Fatalf("get missing: code=%d out=%q", code, out)
	}
	if _, code := run(t, seeds, "put", "__lock__:x", "v"); code != exitInvalid {
		t.Fatalf("reserved key exit code = %d, want %d", code, exitInvalid)
	}

	var lk dataResult
	if code := runJSON(t, seeds, &lk, "lock", "acquire", "job", "--owner", "a", "--ttl", "5s"); code != 0 || lk.Token == 0 {
		t.Fatalf("lock acquire: code=%d %+v", code, lk)
	}
	if _, code := run(t, seeds, "lock", "acquire", "job", "--owner", "b"); code != exitPrecondition {
		t.Fatalf("contended acquire exit code = %d", code)
	}
	if _, code := run(t, seeds, "lock", "release", "job", "--owner", "a"); code != 0 {
		t.Fatal("release failed")
	}
}

func TestKvctl_Introspection(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 2})
	seeds := c.Seeds()
	run(t, seeds, "put", "k", "v")

	out, code := run(t, seeds, "cluster", "status")
	if code != 0 || !strings.Contains(out, "Shard 0") || !strings.Contains(out, "Leader:") || !strings.Contains(out, "Metadata group") {
		t.Fatalf("cluster status: code=%d\n%s", code, out)
	}
	var st struct {
		Shards []struct {
			ShardID int32  `json:"shard_id"`
			Leader  string `json:"leader"`
		} `json:"shards"`
		Nodes []struct {
			NodeID string `json:"node_id"`
			Up     bool   `json:"up"`
		} `json:"nodes"`
	}
	if code := runJSON(t, seeds, &st, "cluster", "status"); code != 0 || len(st.Shards) != 3 || len(st.Nodes) != 3 {
		t.Fatalf("cluster status json: %+v", st)
	}
	for _, s := range st.Shards {
		if l, _, _ := c.ShardLeader(s.ShardID); s.Leader != l {
			t.Errorf("shard %d: status says leader %q, Raft says %q", s.ShardID, s.Leader, l)
		}
	}
	if out, code := run(t, seeds, "shard", "status", "--shard", "1"); code != 0 || !strings.Contains(out, "Shard 1") {
		t.Fatalf("shard status: %d\n%s", code, out)
	}
	if out, code := run(t, seeds, "node", "status", "--node", "node-2"); code != 0 || !strings.Contains(out, "node-2") {
		t.Fatalf("node status: %d\n%s", code, out)
	}
	if out, code := run(t, seeds, "cluster", "metrics", "--node", "node-1"); code != 0 || !strings.Contains(out, "samanvaydb_raft_term") {
		t.Fatalf("cluster metrics: %d\n%s", code, out)
	}
}

func TestKvctl_FaultCommands(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1})
	seeds := c.Seeds()
	if _, code := run(t, seeds, "partition", "create", "--side-a", "node-1"); code != 0 {
		t.Fatal("partition create failed")
	}
	if !strings.Contains(strings.Join(c.Node("node-2").Faults().Describe().Partitions, ","), "node-1<->node-2") {
		t.Fatalf("partition not applied: %+v", c.Node("node-2").Faults().Describe())
	}
	if _, code := run(t, seeds, "partition", "heal"); code != 0 {
		t.Fatal("heal failed")
	}
	if len(c.Node("node-2").Faults().Describe().Partitions) != 0 {
		t.Fatal("heal did not clear partitions")
	}
	if _, code := run(t, seeds, "node", "slow", "node-3", "--latency", "50ms"); code != 0 {
		t.Fatal("slow failed")
	}
	if c.Node("node-3").Faults().SlowDelay().Milliseconds() != 50 {
		t.Fatal("slow not applied")
	}
	if _, code := run(t, seeds, "node", "kill", "node-1"); code != exitUsage {
		t.Fatalf("kill without a local cluster should be a usage error, got %d", code)
	}
	if _, code := run(t, seeds, "bogus-command"); code == 0 {
		t.Fatal("unknown command succeeded")
	}
}
