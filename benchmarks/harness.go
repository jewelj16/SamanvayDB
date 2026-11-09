// Package benchmarks measures SamanvayDB throughput and latency.
//
// Methodology (for every recorded number):
//   - a fixed, named workload definition (op mix, clients, key space, value size);
//   - a warm-up window whose samples are discarded;
//   - throughput plus p50/p95/p99/max latency — never just an average;
//   - each configuration run several times; the median run is reported;
//   - the exact topology/config and the machine are recorded alongside.
package benchmarks

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"samanvaydb/pkg/client"
)

// Workload is a fixed benchmark workload definition.
type Workload struct {
	Name      string
	Clients   int
	ReadRatio float64 // fraction of GETs (the rest are PUTs)
	KeySpace  int
	ValueSize int
	Warmup    time.Duration
	Duration  time.Duration
	// FollowerReads serves GETs with FOLLOWER_OK instead of LEADER.
	FollowerReads bool
}

func (w Workload) String() string {
	return fmt.Sprintf("%s: %d clients, %.0f%% GET / %.0f%% PUT, %d keys, %dB values, %v warm-up + %v measured",
		w.Name, w.Clients, w.ReadRatio*100, (1-w.ReadRatio)*100, w.KeySpace, w.ValueSize, w.Warmup, w.Duration)
}

// Result summarizes one run.
type Result struct {
	Ops        int
	Errors     int
	Throughput float64 // ops/sec
	P50, P95   time.Duration
	P99, Max   time.Duration
}

func (r Result) String() string {
	return fmt.Sprintf("%8.0f ops/s  p50=%-9v p95=%-9v p99=%-9v max=%-9v errors=%d",
		r.Throughput, r.P50.Round(10*time.Microsecond), r.P95.Round(10*time.Microsecond),
		r.P99.Round(10*time.Microsecond), r.Max.Round(10*time.Microsecond), r.Errors)
}

// Run executes w against cl.
func Run(ctx context.Context, cl *client.Client, w Workload) Result {
	value := make([]byte, w.ValueSize)
	rand.New(rand.NewSource(1)).Read(value)
	var (
		measuring atomic.Bool
		mu        sync.Mutex
		lat       []time.Duration
		errs      atomic.Int64
		wg        sync.WaitGroup
	)
	stop := make(chan struct{})
	for c := 0; c < w.Clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(c) + 7))
			local := make([]time.Duration, 0, 4096)
			for {
				select {
				case <-stop:
					mu.Lock()
					lat = append(lat, local...)
					mu.Unlock()
					return
				default:
				}
				key := []byte(fmt.Sprintf("bench-%d", rng.Intn(w.KeySpace)))
				start := time.Now()
				var err error
				if rng.Float64() < w.ReadRatio {
					if w.FollowerReads {
						_, _, err = cl.Get(ctx, key, client.WithFollowerOK())
					} else {
						_, _, err = cl.Get(ctx, key)
					}
				} else {
					err = cl.Put(ctx, key, value)
				}
				d := time.Since(start)
				if measuring.Load() {
					if err != nil {
						errs.Add(1)
					} else {
						local = append(local, d)
					}
				}
			}
		}(c)
	}
	time.Sleep(w.Warmup)
	measuring.Store(true)
	begin := time.Now()
	time.Sleep(w.Duration)
	measuring.Store(false)
	elapsed := time.Since(begin)
	close(stop)
	wg.Wait()

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	r := Result{Ops: len(lat), Errors: int(errs.Load()), Throughput: float64(len(lat)) / elapsed.Seconds()}
	if n := len(lat); n > 0 {
		pct := func(p float64) time.Duration { return lat[int(float64(n-1)*p)] }
		r.P50, r.P95, r.P99, r.Max = pct(0.50), pct(0.95), pct(0.99), lat[n-1]
	}
	return r
}

// Median returns the run with the median throughput.
func Median(rs []Result) Result {
	sorted := append([]Result(nil), rs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Throughput < sorted[j].Throughput })
	return sorted[len(sorted)/2]
}

// Preload writes every key of the key space once so reads hit real data.
func Preload(ctx context.Context, cl *client.Client, w Workload) error {
	value := make([]byte, w.ValueSize)
	var wg sync.WaitGroup
	errCh := make(chan error, 16)
	per := (w.KeySpace + 15) / 16
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := g * per; i < (g+1)*per && i < w.KeySpace; i++ {
				if err := cl.Put(ctx, []byte(fmt.Sprintf("bench-%d", i)), value); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	return <-errCh
}
