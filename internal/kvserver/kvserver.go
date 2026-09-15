// Package kvserver implements the gRPC KV service.
// It depends on the storage.Engine interface — never on a concrete engine type.
// Swapping MemEngine for LSMEngine requires no changes here.
package kvserver

import (
	"context"
	"fmt"

	"samanvaydb/internal/logging"
	"samanvaydb/internal/storage"
	"samanvaydb/proto/kvpb"
)

// KVServer implements kvpb.KVServer backed by a storage.Engine.
type KVServer struct {
	kvpb.UnimplementedKVServer
	engine storage.Engine
	logger *logging.Logger
}

// New creates a KVServer using the given storage engine.
func New(engine storage.Engine, logger *logging.Logger) *KVServer {
	return &KVServer{
		engine: engine,
		logger: logger.WithComponent("kv-server"),
	}
}

func (s *KVServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if err := s.engine.Put(req.Key, req.Value); err != nil {
		s.logger.Error("Put failed", "key", fmt.Sprintf("%x", req.Key), "error", err)
		return &kvpb.PutResponse{Ok: false, Error: err.Error()}, nil
	}
	return &kvpb.PutResponse{Ok: true}, nil
}

func (s *KVServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	value, found, err := s.engine.Get(req.Key)
	if err != nil {
		return &kvpb.GetResponse{Error: err.Error()}, nil
	}
	if !found {
		return &kvpb.GetResponse{Found: false}, nil
	}
	return &kvpb.GetResponse{Found: true, Value: value}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if err := s.engine.Delete(req.Key); err != nil {
		s.logger.Error("Delete failed", "key", fmt.Sprintf("%x", req.Key), "error", err)
		return &kvpb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &kvpb.DeleteResponse{Ok: true}, nil
}
