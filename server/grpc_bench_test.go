package main

import (
	"context"
	"net"
	"testing"

	pb "gocurrencylearning/protostuff"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// startTestServer brings up the real gRPC server over an in-memory listener.
// bufconn keeps the whole gRPC stack in play (codec, framing, interceptors)
// while removing the loopback network, so what you measure is gRPC overhead
// rather than the OS network stack. No ports, no peer forwarding: peers is nil,
// so Set/Delete skip replication.
func startTestServer(tb testing.TB) (pb.KVStoreClient, func()) {
	tb.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pb.RegisterKVStoreServer(srv, &kvStoreServer{raft: &RaftNode{}})

	go func() {
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		tb.Fatalf("dial bufnet: %v", err)
	}

	return pb.NewKVStoreClient(conn), func() {
		conn.Close()
		srv.Stop()
		lis.Close()
	}
}

func BenchmarkGRPCSet(b *testing.B) {
	client, cleanup := startTestServer(b)
	defer cleanup()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := client.Set(ctx, &pb.SetRequest{
			KeyVal:     int32(i % 1000),
			Val:        "value",
			FromClient: false,
		})
		if err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
}

func BenchmarkGRPCGet(b *testing.B) {
	client, cleanup := startTestServer(b)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		if _, err := client.Set(ctx, &pb.SetRequest{KeyVal: int32(i), Val: "value"}); err != nil {
			b.Fatalf("seed Set: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := client.Get(ctx, &pb.Key{KeyVal: int32(i % 1000)}); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

// BenchmarkGRPCGetParallel shows how the server handles concurrent clients.
// gRPC serves each stream on its own goroutine, so this exercises the store's
// RWMutex through the full server path.
func BenchmarkGRPCGetParallel(b *testing.B) {
	client, cleanup := startTestServer(b)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		if _, err := client.Set(ctx, &pb.SetRequest{KeyVal: int32(i), Val: "value"}); err != nil {
			b.Fatalf("seed Set: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb2 *testing.PB) {
		i := 0
		for pb2.Next() {
			if _, err := client.Get(ctx, &pb.Key{KeyVal: int32(i % 1000)}); err != nil {
				b.Fatalf("Get: %v", err)
			}
			i++
		}
	})
}