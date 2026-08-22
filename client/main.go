package main

import (
	"context"
	"fmt"
	pb "gocurrencylearning/protostuff"
	"io"
	"log"
	"time"
	// "os"
	// "strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)


// func Get(client pb.KVStoreClient, k *pb.Key) {
// 	ctx := context.Background()
// 	fmt.Printf("Getting the value for key: %d\n", k.KeyVal)
// 	value, err := client.Get(ctx, k)
// 	if err != nil {
// 		fmt.Printf("client.Get failed: %v\n", err)
// 	}
// 	fmt.Printf("Value returned: %v\n", value.Val)
// }

// func Set(client pb.KVStoreClient, sr *pb.SetRequest) {
// 	sr.FromClient = true
// 	ctx := context.Background()
// 	fmt.Printf("Setting the value %v for key: %v\n", sr.Val, sr.KeyVal)
// 	value, err := client.Set(ctx, sr)
// 	if err != nil {
// 		fmt.Printf("client.Set failed: %v\n", err)
// 	}
// 	fmt.Printf("Value set: %v\n", value.Val)
// }

func Set(client pb.KVStoreClient, sr *pb.SetRequest) {
	ctx := context.Background()
	value, err := client.Set(ctx, sr)
	if err != nil {
		fmt.Printf("  Set failed: %v\n", err)
		return
	}
	fmt.Printf("  Set returned: %q\n", value.Val)
}
 
func Get(client pb.KVStoreClient, k *pb.Key) string {
	ctx := context.Background()
	value, err := client.Get(ctx, k)
	if err != nil {
		fmt.Printf("  Get failed: %v\n", err)
		return ""
	}
	fmt.Printf("  Get returned: %q\n", value.Val)
	return value.Val
}

func Delete(client pb.KVStoreClient, k *pb.Key) { 
	k.FromClient = true
	ctx := context.Background()
	fmt.Printf("Deleting value for key: %d\n", k.KeyVal)
	delInfo, err := client.Delete(ctx, k)
	if err != nil {
		fmt.Printf("client.Delete failed: %v\n", err)
	}
	if delInfo.Existed { 
		fmt.Printf("The value %v existed and was deleted\n", delInfo.Val)
	} else {
		fmt.Printf("The key/value pair did NOT exist, nothing was deleted.\n")
	}
}

func Watch(client pb.KVStoreClient, k *pb.Key) {
	ctx := context.Background()
	stream, err := client.Watch(ctx, k)
	if err != nil {
		fmt.Printf("failed to stream due to %v", err)
		return 
	}
	for {
		value, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("client.Watch failed: %v", err)
		}
		log.Printf("Key: %d was updated with Value: %v", int(k.KeyVal), value.Val)
	}
}

func connectTo(port string) pb.KVStoreClient {
	addr := fmt.Sprintf("localhost:%s", port)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to %s: %v", addr, err)
	}
	return pb.NewKVStoreClient(conn)
}

func main() {
	// One connection per instance, all in this one process.
	clientA := connectTo("50051")
	clientB := connectTo("50052")
	clientC := connectTo("50053")
 
	// =========================================================
	// Test 1: basic replication
	// Set on A, read from B and C, confirm the value propagated.
	// =========================================================
	fmt.Println("=== Test 1: basic replication ===")
	key1 := int32(1)
	Set(clientA, &pb.SetRequest{KeyVal: key1, Val: "hello-from-A", FromClient: true})
 
	fmt.Println("waiting for async forwarding to land...")
	time.Sleep(300 * time.Millisecond)
 
	fmt.Println("Get from B:")
	Get(clientB, &pb.Key{KeyVal: key1})
	fmt.Println("Get from C:")
	Get(clientC, &pb.Key{KeyVal: key1})
	fmt.Println("(expect: both B and C return \"hello-from-A\" if replication worked)")
 
	// =========================================================
	// Test 2: split-brain / concurrent conflicting writes
	// Set the SAME key on A and B at nearly the same instant,
	// with different values, then check if all 3 instances agree.
	// =========================================================
	fmt.Println("\n=== Test 2: concurrent conflicting writes (split-brain) ===")
	key2 := int32(2)
 
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		Set(clientA, &pb.SetRequest{KeyVal: key2, Val: "value-from-A", FromClient: true})
	}()
	go func() {
		defer wg.Done()
		Set(clientB, &pb.SetRequest{KeyVal: key2, Val: "value-from-B", FromClient: true})
	}()
	wg.Wait()
 
	fmt.Println("waiting for async forwarding to land...")
	time.Sleep(300 * time.Millisecond)
 
	fmt.Println("Get from A:")
	valA := Get(clientA, &pb.Key{KeyVal: key2})
	fmt.Println("Get from B:")
	valB := Get(clientB, &pb.Key{KeyVal: key2})
	fmt.Println("Get from C:")
	valC := Get(clientC, &pb.Key{KeyVal: key2})
 
	if valA == valB && valB == valC {
		fmt.Printf("all three instances AGREE on value: %q\n", valA)
	} else {
		fmt.Printf("instances DISAGREE — A=%q B=%q C=%q\n", valA, valB, valC)
		fmt.Println("(this divergence is expected with naive, non-consensus replication)")
	}
 
	// =========================================================
	// Test 3: kill-and-recover (manual step required)
	// =========================================================
	fmt.Println("\n=== Test 3: kill-and-recover ===")
	key3 := int32(3)
	Set(clientA, &pb.SetRequest{KeyVal: key3, Val: "before-kill", FromClient: true})
	fmt.Println("Set key 3 = \"before-kill\" on A.")
	fmt.Println("Now manually: Ctrl+C instance C's terminal, restart it, then press Enter here.")
	fmt.Scanln()
 
	fmt.Println("Get from restarted C:")
	Get(clientC, &pb.Key{KeyVal: key3})
	fmt.Println("(expect: empty/not-found, since C missed this write while down and has no catch-up mechanism)")
}

// func main() {
// 	allArgs := os.Args
// 	chosenPort, _ := strconv.Atoi(allArgs[1])
// 	serverAddr := fmt.Sprintf("localhost:%d", chosenPort)
 
// 	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		log.Fatalf("failed to dial in to server due to %v", err)
// 	}
// 	defer conn.Close()
// 	client := pb.NewKVStoreClient(conn)

// 	Set(client, &pb.SetRequest{KeyVal: 1, Val: "key one", FromClient: true})


 
// 	// --- 1. Get a key that was never set ---
// // 	fmt.Println("=== 1. Get on never-set key ===")
// // 	Get(client, &pb.Key{KeyVal: 100}) // expect: got ""
 
// // 	// --- 2. Set then Get, confirm round trip ---
// // 	fmt.Println("\n=== 2. Set then Get ===")
// // 	Set(client, &pb.SetRequest{KeyVal: 1, Val: "one"})
// // 	Get(client, &pb.Key{KeyVal: 1}) // expect: got "one"
 
// // 	// --- 3. Overwrite the same key, confirm new value wins ---
// // 	fmt.Println("\n=== 3. Overwrite existing key ===")
// // 	Set(client, &pb.SetRequest{KeyVal: 1, Val: "ONE-UPDATED"})
// // 	Get(client, &pb.Key{KeyVal: 1}) // expect: got "ONE-UPDATED"
 
// // 	// --- 4. Multiple distinct keys, check no cross-contamination ---
// // 	fmt.Println("\n=== 4. Multiple distinct keys ===")
// // 	Set(client, &pb.SetRequest{KeyVal: 2, Val: "two"})
// // 	Set(client, &pb.SetRequest{KeyVal: 3, Val: "three"})
// // 	Get(client, &pb.Key{KeyVal: 1}) // expect: "ONE-UPDATED" (unchanged)
// // 	Get(client, &pb.Key{KeyVal: 2}) // expect: "two"
// // 	Get(client, &pb.Key{KeyVal: 3}) // expect: "three"
 
// // 	// --- 5. Delete an existing key ---
// // 	fmt.Println("\n=== 5. Delete existing key ===")
// // 	Delete(client, &pb.Key{KeyVal: 2}) // expect: existed=true, val="two"
// // 	Get(client, &pb.Key{KeyVal: 2})    // expect: got "" (gone now)
 
// // 	// --- 6. Delete an already-deleted / never-existed key ---
// // 	fmt.Println("\n=== 6. Delete non-existent key ===")
// // 	Delete(client, &pb.Key{KeyVal: 2})   // expect: existed=false (already deleted above)
// // 	Delete(client, &pb.Key{KeyVal: 999}) // expect: existed=false (never existed)
 
// // 	// --- 7. Concurrent calls: stress-test server-side locking over real RPCs ---
// // 	fmt.Println("\n=== 7. Concurrent Set/Get/Delete ===")
// // 	var wg sync.WaitGroup
// // 	for i := 0; i < 50; i++ {
// // 		wg.Add(1)
// // 		go func(n int) {
// // 			defer wg.Done()
// // 			key := int32(1000 + n)
// // 			Set(client, &pb.SetRequest{KeyVal: key, Val: fmt.Sprintf("concurrent-%d", n)})
// // 			Get(client, &pb.Key{KeyVal: key})
// // 			if n%2 == 0 {
// // 				Delete(client, &pb.Key{KeyVal: key})
// // 			}
// // 		}(i)
// // 	}
// // 	wg.Wait()
// // 	fmt.Println("concurrent round completed without crashing")
 
// // 	fmt.Println("\nall checks complete")

// // 	// -- Watch function testing -- 
// // 	fmt.Println("\n=== 8. Testing Watch Function ===")
// // 	var wg2 sync.WaitGroup
// // 	for i := 0; i < 15; i++ {
// // 		wg2.Add(1)
// // 		go func(n int) {
// // 			defer wg2.Done()
// // 			key := int32(1000 + n)
// // 			go Watch(client, &pb.Key{KeyVal: key})
// // 			time.Sleep(50 * time.Millisecond)
// // 			for j := 0; j < 10; j++ {
// // 				var rand_string = fmt.Sprintf("randomval_%d", j)
// // 				Set(client, &pb.SetRequest{KeyVal: key, Val: rand_string})
// // 				time.Sleep(100*time.Millisecond)
// // 				if j >= 5 {
// // 					if key % 2 == 0 {
// // 						Delete(client, &pb.Key{KeyVal: key})
// // 					}
// // 				}
// // 			}
// // 		}(i)
// // 	}
// // 	wg2.Wait()
// }



