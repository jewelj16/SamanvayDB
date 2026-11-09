package benchmarks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"samanvaydb/internal/testcluster"
)

// TestBenchmarkMatrix runs the two comparative matrices from the plan and
// writes benchmarks/results/latest.md:
//
//	replica-count scaling: 1 shard, RF = 1/3/6/9 (cluster of that many nodes)
//	shard-count scaling:   6 nodes, RF=3, 1/3/6 shards
//
// Each cell runs a write-heavy and a read-heavy workload 3 times and keeps
// the median run. Skipped unless SAMANVAY_BENCH=1 (it takes minutes).
func TestBenchmarkMatrix(t *testing.T) {
	if os.Getenv("SAMANVAY_BENCH") == "" {
		t.Skip("set SAMANVAY_BENCH=1 to run the benchmark matrix")
	}
	runs := 3
	dur := 3 * time.Second
	if v := os.Getenv("SAMANVAY_BENCH_SECONDS"); v != "" {
		var s int
		fmt.Sscan(v, &s)
		dur = time.Duration(s) * time.Second
	}
	workloads := []Workload{
		{Name: "write-heavy", Clients: 32, ReadRatio: 0.1, KeySpace: 10000, ValueSize: 128, Warmup: time.Second, Duration: dur},
		{Name: "read-heavy", Clients: 32, ReadRatio: 0.9, KeySpace: 10000, ValueSize: 128, Warmup: time.Second, Duration: dur},
	}
	type cell struct {
		label     string
		nodes, rf int
		shards    int32
		sync      bool
	}
	matrices := []struct {
		title string
		cells []cell
	}{
		{"Replica-count scaling (1 shard, fsync on)", []cell{
			{"1 node, RF=1", 1, 1, 1, true}, {"3 nodes, RF=3", 3, 3, 1, true},
			{"6 nodes, RF=6", 6, 6, 1, true}, {"9 nodes, RF=9", 9, 9, 1, true}}},
		{"Shard-count scaling (6 nodes, RF=3, fsync on)", []cell{
			{"1 shard", 6, 3, 1, true}, {"3 shards", 6, 3, 3, true}, {"6 shards", 6, 3, 6, true}}},
	}

	var md strings.Builder
	fmt.Fprintf(&md, "# SamanvayDB benchmark results\n\n")
	fmt.Fprintf(&md, "Generated %s by `make bench` (TestBenchmarkMatrix).\n\n", time.Now().Format("2006-01-02 15:04 MST"))
	fmt.Fprintf(&md, "Environment: %s/%s, %d logical CPUs, %s. Whole cluster runs in ONE process on ONE machine over localhost gRPC, so absolute numbers are bounded by that machine (and by its fsync latency); compare cells relative to each other.\n\n",
		runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Fprintf(&md, "Each cell: median of %d runs. Reads use LEADER consistency (ReadIndex).\n\n", runs)
	for _, w := range workloads {
		fmt.Fprintf(&md, "- %s\n", w)
	}
	for _, m := range matrices {
		fmt.Fprintf(&md, "\n## %s\n\n| Topology | Workload | Throughput (ops/s) | p50 | p95 | p99 | max | errors |\n|---|---|---:|---:|---:|---:|---:|---:|\n", m.title)
		for _, c := range m.cells {
			cl := testcluster.Start(t, testcluster.Options{Nodes: c.nodes, Shards: c.shards, RF: c.rf, Sync: c.sync,
				Raft: testRaft()})
			client := cl.Client()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			if err := Preload(ctx, client, workloads[0]); err != nil {
				t.Errorf("%s: preload failed: %v", c.label, err)
				fmt.Fprintf(&md, "| %s | (preload failed: %v) | | | | | | |\n", c.label, err)
				cancel()
				cl.Stop()
				continue
			}
			for _, w := range workloads {
				var rs []Result
				for i := 0; i < runs; i++ {
					rs = append(rs, Run(ctx, client, w))
				}
				r := Median(rs)
				t.Logf("%-16s %-12s %s", c.label, w.Name, r)
				fmt.Fprintf(&md, "| %s | %s | %.0f | %v | %v | %v | %v | %d |\n", c.label, w.Name, r.Throughput,
					r.P50.Round(10*time.Microsecond), r.P95.Round(10*time.Microsecond),
					r.P99.Round(10*time.Microsecond), r.Max.Round(100*time.Microsecond), r.Errors)
			}
			cancel()
			cl.Stop()
		}
	}
	dir := "results"
	os.MkdirAll(dir, 0o755)
	if err := os.WriteFile(filepath.Join(dir, "latest.md"), []byte(md.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", filepath.Join(dir, "latest.md"))
}
