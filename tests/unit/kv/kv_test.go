// Package kv contains the Phase 1 checkpoint test suite for the KV layer.
// All tests here must pass under -race.
package kv_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"log/slog"

	"samanvaydb/internal/kvserver"
	"samanvaydb/internal/logging"
	"samanvaydb/internal/storage"
	"samanvaydb/proto/kvpb"
)

// ---- test helpers ----

func newClient(t *testing.T) (kvpb.KVClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	engine := storage.NewMemEngine()
	logger := logging.NewLogger(logging.Options{NodeID: "test", Level: slog.LevelError})
	srv := grpc.NewServer()
	kvpb.RegisterKVServer(srv, kvserver.New(engine, logger))
	go srv.Serve(lis)

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return kvpb.NewKVClient(conn), func() { conn.Close(); srv.Stop() }
}

// ---- correctness tests ----

func TestKV_PutThenGet(t *testing.T) {
	c, done := newClient(t)
	defer done()
	ctx := context.Background()

	c.Put(ctx, &kvpb.PutRequest{Key: []byte("k"), Value: []byte("v")})
	gr, _ := c.Get(ctx, &kvpb.GetRequest{Key: []byte("k")})
	if !gr.Found || string(gr.Value) != "v" {
		t.Errorf("got found=%v val=%q, want found=true val=%q", gr.Found, gr.Value, "v")
	}
}

func TestKV_GetMissingKey(t *testing.T) {
	c, done := newClient(t)
	defer done()
	gr, _ := c.Get(context.Background(), &kvpb.GetRequest{Key: []byte("nope")})
	if gr.Found {
		t.Error("expected found=false, got true")
	}
}

func TestKV_DeleteThenGet(t *testing.T) {
	c, done := newClient(t)
	defer done()
	ctx := context.Background()

	c.Put(ctx, &kvpb.PutRequest{Key: []byte("x"), Value: []byte("val")})
	c.Delete(ctx, &kvpb.DeleteRequest{Key: []byte("x")})
	gr, _ := c.Get(ctx, &kvpb.GetRequest{Key: []byte("x")})
	if gr.Found {
		t.Error("expected found=false after delete")
	}
}

func TestKV_PutOverwrite(t *testing.T) {
	c, done := newClient(t)
	defer done()
	ctx := context.Background()

	c.Put(ctx, &kvpb.PutRequest{Key: []byte("k"), Value: []byte("old")})
	c.Put(ctx, &kvpb.PutRequest{Key: []byte("k"), Value: []byte("new")})
	gr, _ := c.Get(ctx, &kvpb.GetRequest{Key: []byte("k")})
	if string(gr.Value) != "new" {
		t.Errorf("expected %q, got %q", "new", gr.Value)
	}
}

func TestKV_EmptyKeyRejected(t *testing.T) {
	c, done := newClient(t)
	defer done()
	ctx := context.Background()

	pr, _ := c.Put(ctx, &kvpb.PutRequest{Key: []byte{}, Value: []byte("v")})
	if pr.Ok {
		t.Error("empty key Put should fail")
	}
	dr, _ := c.Delete(ctx, &kvpb.DeleteRequest{Key: []byte{}})
	if dr.Ok {
		t.Error("empty key Delete should fail")
	}
}

// ---- concurrency stress test ----

// TestKV_ConcurrencyStress hammers the gRPC KV server from many goroutines
// simultaneously with overlapping and disjoint keys. Must pass under -race.
func TestKV_ConcurrencyStress(t *testing.T) {
	c, done := newClient(t)
	defer done()

	const workers = 50
	const opsPerWorker = 100

	var wg sync.WaitGroup

	// Writers on disjoint keys.
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			for j := 0; j < opsPerWorker; j++ {
				key := []byte(fmt.Sprintf("w%d-k%d", id, j))
				val := []byte(fmt.Sprintf("val-%d-%d", id, j))
				resp, err := c.Put(ctx, &kvpb.PutRequest{Key: key, Value: val})
				if err != nil || !resp.Ok {
					t.Errorf("Put w%d k%d failed: err=%v ok=%v", id, j, err, resp.GetOk())
				}
			}
		}(i)
	}

	// Readers + writers on overlapping keys.
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			for j := 0; j < opsPerWorker; j++ {
				key := []byte(fmt.Sprintf("shared-%d", j%10))
				switch j % 3 {
				case 0:
					c.Put(ctx, &kvpb.PutRequest{Key: key, Value: []byte(fmt.Sprintf("v%d", id))})
				case 1:
					c.Get(ctx, &kvpb.GetRequest{Key: key})
				case 2:
					c.Delete(ctx, &kvpb.DeleteRequest{Key: key})
				}
			}
		}(i)
	}

	wg.Wait()
}
