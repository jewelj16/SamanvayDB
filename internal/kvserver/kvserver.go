// Package kvserver implements the gRPC KV service backed by the in-memory
// store. In Phase 2 this will be swapped for the LSM engine without changing
// the gRPC interface.
package kvserver

import (
	"context"
	"fmt"

	"samanvaydb/internal/logging"
	"samanvaydb/internal/storage/memkv"
	"samanvaydb/proto/kvpb"
)

// KVServer implements kvpb.KVServer backed by a memkv.Store.
type KVServer struct {
	kvpb.UnimplementedKVServer
	store  *memkv.Store
	logger *logging.Logger
}

// New creates a KVServer wrapping the given store.
func New(store *memkv.Store, logger *logging.Logger) *KVServer {
	return &KVServer{
		store:  store,
		logger: logger.WithComponent("kv-server"),
	}
}

// Put handles a PUT RPC, translating it to a memkv Put.
func (s *KVServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if err := s.store.Put(req.Key, req.Value); err != nil {
		s.logger.Error("Put failed", "key", fmt.Sprintf("%x", req.Key), "error", err)
		return &kvpb.PutResponse{Ok: false, Error: err.Error()}, nil
	}
	s.logger.Info("Put", "key", string(req.Key))
	return &kvpb.PutResponse{Ok: true}, nil
}

// Get handles a GET RPC, translating it to a memkv Get.
func (s *KVServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	value, found, err := s.store.Get(req.Key)
	if err != nil {
		return &kvpb.GetResponse{Error: err.Error()}, nil
	}
	if !found {
		return &kvpb.GetResponse{Found: false}, nil
	}
	return &kvpb.GetResponse{Found: true, Value: value}, nil
}

// Delete handles a DELETE RPC, translating it to a memkv Delete.
func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if err := s.store.Delete(req.Key); err != nil {
		s.logger.Error("Delete failed", "key", fmt.Sprintf("%x", req.Key), "error", err)
		return &kvpb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	s.logger.Info("Delete", "key", string(req.Key))
	return &kvpb.DeleteResponse{Ok: true}, nil
}
