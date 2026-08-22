package main

import (
	"context"
	"fmt"
	"gocurrencylearning/kvstore"
	pb "gocurrencylearning/protostuff"
	"log"
	"net"
	"os"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var mu sync.Mutex

type kvStoreServer struct {
	pb.UnimplementedKVStoreServer
	peers []pb.KVStoreClient
}

func (kv *kvStoreServer) Get(_ context.Context, k *pb.Key) (*pb.Value, error) {
	ret_val := kvstore.Get(int(k.KeyVal))
	return &pb.Value{Val: ret_val}, nil
}


func (kv *kvStoreServer) Set(_ context.Context, sr *pb.SetRequest) (*pb.Value, error) { 
	input_val := kvstore.Set(int(sr.KeyVal), string(sr.Val))
	if sr.FromClient {
		sr.FromClient = false
		for _, peerCon := range kv.peers {
			ctx := context.Background()
			fmt.Printf("Propagating the value %v for key: %v to server %v\n", sr.Val, sr.KeyVal, peerCon)
			value, err := peerCon.Set(ctx, sr)
			if err != nil || value == nil{
				fmt.Printf("client.Set failed: %v\n", err)
				break
			}
			fmt.Printf("Value set: %v\n", value.Val)
		}
	}
	return &pb.Value{Val: input_val}, nil
}

func (kv *kvStoreServer) Delete(_ context.Context, k *pb.Key) (*pb.DeleteInfo, error) { 
	exists, del_val := kvstore.Delete(int(k.KeyVal))
	if k.FromClient {
		k.FromClient = false
		for _, peerCon := range kv.peers {
			ctx := context.Background()
			fmt.Printf("Propagating the delete of key %v to server %v\n", k.KeyVal, peerCon)
			delInfo, err := peerCon.Delete(ctx, k)
			if err != nil || delInfo == nil{
				fmt.Printf("client.Delete failed: %v\n", err)
				break
			}
			if delInfo.Existed { 
				fmt.Printf("The value %v existed and was deleted\n", delInfo.Val)
			} else {
				fmt.Printf("The key/value pair did NOT exist, nothing was deleted.\n")
			}
		}
	}
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
	var kvStore *kvStoreServer = &kvStoreServer{}
	var allArgs = os.Args
	allPorts := []int{50051, 50052, 50053}
	userPort, _ := strconv.Atoi(allArgs[1])
	for i, port := range allPorts {
		if port == userPort {
			allPorts = append(allPorts[:i], allPorts[i+1:]...)
			break
		}
	}

	for _, otherPort := range allPorts {
		serverAddr := fmt.Sprintf("localhost:%d", otherPort)
		conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to connect to server due to err %v", err)
		}
		defer conn.Close()
		kvStore.peers = append(kvStore.peers, pb.NewKVStoreClient(conn))

	}
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", userPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// fmt.Println("Server is listening!")
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterKVStoreServer(grpcServer, kvStore)
	new_err := grpcServer.Serve(lis)
	if new_err != nil {
		log.Fatalf("failed again: %v", new_err)
	}

}