package kvpb_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"samanvaydb/proto/kvpb"
)

const bufSize = 1024 * 1024

// testKVServer is a trivial in-process KV server for the round-trip test.
type testKVServer struct {
	kvpb.UnimplementedKVServer
	data map[string][]byte
}

func (s *testKVServer) Put(_ context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	s.data[string(req.Key)] = req.Value
	return &kvpb.PutResponse{Ok: true}, nil
}

func (s *testKVServer) Get(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	v, ok := s.data[string(req.Key)]
	if !ok {
		return &kvpb.GetResponse{Found: false}, nil
	}
	return &kvpb.GetResponse{Found: true, Value: v}, nil
}

func (s *testKVServer) Delete(_ context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	delete(s.data, string(req.Key))
	return &kvpb.DeleteResponse{Ok: true}, nil
}

func newTestServer(t *testing.T) (*grpc.ClientConn, func()) {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	kvpb.RegisterKVServer(srv, &testKVServer{data: make(map[string][]byte)})

	go srv.Serve(lis)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	conn, err := grpc.DialContext(
		context.Background(), "bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.DialContext: %v", err)
	}

	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return conn, cleanup
}

// TestKVStubs_RoundTrip verifies that generated stubs compile and a trivial
// Put→Get→Delete round-trip works over an in-process bufconn.
func TestKVStubs_RoundTrip(t *testing.T) {
	conn, cleanup := newTestServer(t)
	defer cleanup()

	client := kvpb.NewKVClient(conn)
	ctx := context.Background()

	// Put
	putResp, err := client.Put(ctx, &kvpb.PutRequest{
		Key:   []byte("hello"),
		Value: []byte("world"),
	})
	if err != nil {
		t.Fatalf("Put RPC failed: %v", err)
	}
	if !putResp.Ok {
		t.Errorf("Put ok=false, want true")
	}

	// Get — key present
	getResp, err := client.Get(ctx, &kvpb.GetRequest{Key: []byte("hello")})
	if err != nil {
		t.Fatalf("Get RPC failed: %v", err)
	}
	if !getResp.Found {
		t.Fatal("Get found=false, want true")
	}
	if string(getResp.Value) != "world" {
		t.Errorf("Get value=%q, want %q", getResp.Value, "world")
	}

	// Delete
	delResp, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: []byte("hello")})
	if err != nil {
		t.Fatalf("Delete RPC failed: %v", err)
	}
	if !delResp.Ok {
		t.Errorf("Delete ok=false, want true")
	}

	// Get — key absent
	getResp2, err := client.Get(ctx, &kvpb.GetRequest{Key: []byte("hello")})
	if err != nil {
		t.Fatalf("Get after Delete RPC failed: %v", err)
	}
	if getResp2.Found {
		t.Error("Get found=true after Delete, want false")
	}
}

// TestKVStubs_BinaryKeysAndValues verifies bytes keys/values survive the wire.
func TestKVStubs_BinaryKeysAndValues(t *testing.T) {
	conn, cleanup := newTestServer(t)
	defer cleanup()

	client := kvpb.NewKVClient(conn)
	ctx := context.Background()

	key := []byte{0x00, 0xFF, 0xAB}
	value := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	client.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})

	resp, err := client.Get(ctx, &kvpb.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get RPC failed: %v", err)
	}
	if !resp.Found {
		t.Fatal("Get found=false for binary key")
	}
	if string(resp.Value) != string(value) {
		t.Errorf("Get value=%x, want %x", resp.Value, value)
	}
}
