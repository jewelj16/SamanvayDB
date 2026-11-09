// Package failure holds fault-injection scenarios against REAL multi-process
// clusters (separate OS processes, real SIGKILL/TerminateProcess, network
// faults through the Admin API). The faster in-process scenarios live in
// tests/integration and internal/raft.
package failure

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"samanvaydb/internal/faultinjector"
	"samanvaydb/pkg/client"
)

func buildServer(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("multi-process chaos test skipped in -short mode")
	}
	name := "samanvaydb-server"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	out := filepath.Join(t.TempDir(), name)
	cmd := exec.Command("go", "build", "-o", out, "samanvaydb/cmd/samanvaydb-server")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build: %v", err)
	}
	return out
}

// TestProcessChaos_SeededKillsAndPartitions runs a seeded chaos sequence —
// kill a random node (SIGKILL), restart it, partition a random node away,
// heal — repeatedly, while a client writes. Every acknowledged write must
// be readable afterwards. The seed is logged for exact replay.
func TestProcessChaos_SeededKillsAndPartitions(t *testing.T) {
	bin := buildServer(t)
	lc, err := faultinjector.CreateLocal(faultinjector.LocalOptions{
		Dir: filepath.Join(t.TempDir(), "cluster"), Binary: bin, Nodes: 5, Shards: 2, RF: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer lc.StopAll(true)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cl, err := client.New(ctx, client.Config{Seeds: lc.Seeds()})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	seed := int64(31337)
	if s := os.Getenv("CHAOS_SEED"); s != "" {
		fmt.Sscan(s, &seed)
	}
	sc := faultinjector.NewScenario(seed)
	t.Logf("chaos seed %d (replay with CHAOS_SEED=%d)", sc.Seed, sc.Seed)
	rec := &faultinjector.Recorder{Inner: lc}

	var mu sync.Mutex
	acked := map[string]string{}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			k, v := fmt.Sprintf("chaos-%05d", i), fmt.Sprintf("v%d", i)
			wctx, wcancel := context.WithTimeout(ctx, 10*time.Second)
			err := cl.Put(wctx, []byte(k), []byte(v))
			wcancel()
			if err == nil {
				mu.Lock()
				acked[k] = v
				mu.Unlock()
			}
		}
	}()

	for round := 0; round < 3; round++ {
		if err := faultinjector.ChaosRound(rec, sc, lc.NodeIDs(), func() { time.Sleep(1500 * time.Millisecond) }); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
	}
	time.Sleep(time.Second)
	close(stop)
	<-done
	t.Logf("faults injected: %v", rec.Log())

	mu.Lock()
	defer mu.Unlock()
	if len(acked) < 50 {
		t.Fatalf("only %d writes acknowledged during chaos", len(acked))
	}
	for k, v := range acked {
		got, ok, err := cl.Get(ctx, []byte(k))
		if err != nil || !ok || string(got) != v {
			t.Fatalf("acknowledged write %s lost: %q found=%v err=%v", k, got, ok, err)
		}
	}
	t.Logf("all %d acknowledged writes survived", len(acked))
}
