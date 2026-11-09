package faultinjector

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"samanvaydb/proto/kvpb"
)

// buildServer compiles samanvaydb-server into a temp dir.
func buildServer(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds and runs real server processes; skipped in -short mode")
	}
	name := "samanvaydb-server"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	out := filepath.Join(t.TempDir(), name)
	cmd := exec.Command("go", "build", "-o", out, "samanvaydb/cmd/samanvaydb-server")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build server: %v", err)
	}
	return out
}

func put(t *testing.T, addr, k, v string) (*kvpb.PutResponse, error) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return kvpb.NewKVClient(cc).Put(ctx, &kvpb.PutRequest{Key: []byte(k), Value: []byte(v)})
}

func get(t *testing.T, addr, k string) (*kvpb.GetResponse, error) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return kvpb.NewKVClient(cc).Get(ctx, &kvpb.GetRequest{Key: []byte(k)})
}

// TestLocalCluster_KillIsRealAndRestartRecovers — Commit 56: kill a running
// node process; confirm it is actually gone (its process exited and its
// port refuses connections, not merely unresponsive); restart it and it
// rejoins and serves the data written before the crash.
func TestLocalCluster_KillIsRealAndRestartRecovers(t *testing.T) {
	bin := buildServer(t)
	lc, err := CreateLocal(LocalOptions{Dir: filepath.Join(t.TempDir(), "cluster"), Binary: bin, Nodes: 1, Shards: 1, RF: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer lc.StopAll(true)
	addr := lc.Addrs()["node-1"]

	deadline := time.Now().Add(10 * time.Second)
	for {
		r, err := put(t, addr, "durable", "yes")
		if err == nil && r.Ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("put never succeeded: %v %v", r, err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err := lc.KillNode("node-1"); err != nil {
		t.Fatal(err)
	}
	if lc.Alive("node-1") {
		t.Fatal("node still answering after SIGKILL")
	}
	if _, err := put(t, addr, "x", "y"); err == nil {
		t.Fatal("killed node still accepted an RPC")
	}

	if err := lc.RestartNode("node-1"); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		r, err := get(t, addr, "durable")
		if err == nil && r.Found && string(r.Value) == "yes" {
			break // recovered from its fsynced Raft log
		}
		if time.Now().After(deadline) {
			t.Fatalf("acknowledged write lost across kill -9: %v %v", r, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
