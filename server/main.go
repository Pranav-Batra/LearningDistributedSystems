package main

import (
	"context"
	"gocurrencylearning/kvstore"
	pb "gocurrencylearning/protostuff"
	"net"
	"log"
	"fmt"
	"sync"

	"google.golang.org/grpc"
)

var mu sync.Mutex

type kvStoreServer struct {
	pb.UnimplementedKVStoreServer
}

func (kv *kvStoreServer) Get(_ context.Context, k *pb.Key) (*pb.Value, error) {
	ret_val := kvstore.Get(int(k.KeyVal))
	return &pb.Value{Val: ret_val}, nil
}


func (kv *kvStoreServer) Set(_ context.Context, sr *pb.SetRequest) (*pb.Value, error) { 
	input_val := kvstore.Set(int(sr.KeyVal), string(sr.Val))
	return &pb.Value{Val: input_val}, nil
}

func (kv *kvStoreServer) Delete(_ context.Context, k *pb.Key) (*pb.DeleteInfo, error) { 
	exists, del_val := kvstore.Delete(int(k.KeyVal))
	return &pb.DeleteInfo{Existed: exists, Val: del_val}, nil
}

func (kv *kvStoreServer) Watch(k *pb.Key, stream pb.KVStore_WatchServer) (error) {
	key := int(k.KeyVal)
	myChannel := kvstore.RegisterWatcher(key)
	for {
		select {
		case val := <- myChannel:
			if err := stream.Send(&pb.Value{Val: val}); err != nil {
				kvstore.UnregisterWatcher(key, myChannel)
				return err
			} 
			case <- stream.Context().Done():
				kvstore.UnregisterWatcher(key, myChannel)
				return nil
		}
	}
}

func main () { 
	var opts []grpc.ServerOption
	var port = 50051
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// fmt.Println("Server is listening!")
	var kvStore *kvStoreServer = &kvStoreServer{}
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterKVStoreServer(grpcServer, kvStore)
	new_err := grpcServer.Serve(lis)
	if new_err != nil {
		log.Fatalf("failed again: %v", new_err)
	}

}