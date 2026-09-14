package kvserver_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"log/slog"

	"samanvaydb/internal/kvserver"
	"samanvaydb/internal/logging"
	"samanvaydb/internal/storage/memkv"
	"samanvaydb/proto/kvpb"
)

func newTestKVClient(t *testing.T) (kvpb.KVClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	store := memkv.New()
	logger := logging.NewLogger(logging.Options{NodeID: "test", Level: slog.LevelError})
	srv := grpc.NewServer()
	kvpb.RegisterKVServer(srv, kvserver.New(store, logger))
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

func TestKVServer_PutGetDelete(t *testing.T) {
	client, cleanup := newTestKVClient(t)
	defer cleanup()
	ctx := context.Background()

	// Put
	resp, err := client.Put(ctx, &kvpb.PutRequest{Key: []byte("name"), Value: []byte("samanvay")})
	if err != nil || !resp.Ok {
		t.Fatalf("Put failed: err=%v ok=%v", err, resp.GetOk())
	}

	// Get — present
	gr, err := client.Get(ctx, &kvpb.GetRequest{Key: []byte("name")})
	if err != nil || !gr.Found || string(gr.Value) != "samanvay" {
		t.Fatalf("Get failed: err=%v found=%v val=%q", err, gr.GetFound(), gr.GetValue())
	}

	// Overwrite
	client.Put(ctx, &kvpb.PutRequest{Key: []byte("name"), Value: []byte("db")})
	gr2, _ := client.Get(ctx, &kvpb.GetRequest{Key: []byte("name")})
	if string(gr2.Value) != "db" {
		t.Errorf("overwrite: got %q want %q", gr2.Value, "db")
	}

	// Delete
	dr, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: []byte("name")})
	if err != nil || !dr.Ok {
		t.Fatalf("Delete failed: err=%v ok=%v", err, dr.GetOk())
	}

	// Get — absent
	gr3, _ := client.Get(ctx, &kvpb.GetRequest{Key: []byte("name")})
	if gr3.Found {
		t.Error("Get found=true after Delete, want false")
	}
}

func TestKVServer_GetMissingKey(t *testing.T) {
	client, cleanup := newTestKVClient(t)
	defer cleanup()

	gr, err := client.Get(context.Background(), &kvpb.GetRequest{Key: []byte("missing")})
	if err != nil {
		t.Fatalf("Get RPC error: %v", err)
	}
	if gr.Found {
		t.Error("expected found=false for missing key")
	}
}

func TestKVServer_EmptyKeyReturnsError(t *testing.T) {
	client, cleanup := newTestKVClient(t)
	defer cleanup()
	ctx := context.Background()

	pr, _ := client.Put(ctx, &kvpb.PutRequest{Key: []byte{}, Value: []byte("v")})
	if pr.Ok {
		t.Error("Put with empty key should return ok=false")
	}

	dr, _ := client.Delete(ctx, &kvpb.DeleteRequest{Key: []byte{}})
	if dr.Ok {
		t.Error("Delete with empty key should return ok=false")
	}
}
