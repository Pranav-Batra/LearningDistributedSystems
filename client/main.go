package main

import (
	"context"
	"fmt"
	pb "gocurrencylearning/protostuff"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type node struct {
	port   string
	client pb.KVStoreClient
}

func connectTo(port string) pb.KVStoreClient {
	addr := fmt.Sprintf("localhost:%s", port)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to %s: %v", addr, err)
	}
	return pb.NewKVStoreClient(conn)
}

// withLeader tries the given operation against each node in turn and returns the
// node that accepted it (i.e. the leader), or nil if none did. The op returns an
// error when the node is a follower (rejected) or unreachable.
func withLeader(nodes []node, op func(pb.KVStoreClient) error) *node {
	for i := range nodes {
		err := op(nodes[i].client)
		if err == nil {
			return &nodes[i]
		}
		fmt.Printf("  node %s rejected/failed: %v\n", nodes[i].port, err)
	}
	return nil
}

func leaderSet(nodes []node, key int32, val string) *node {
	return withLeader(nodes, func(c pb.KVStoreClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := c.Set(ctx, &pb.SetRequest{KeyVal: key, Val: val})
		return err
	})
}

func leaderDelete(nodes []node, key int32) *node {
	return withLeader(nodes, func(c pb.KVStoreClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := c.Delete(ctx, &pb.Key{KeyVal: key})
		return err
	})
}

func getFrom(n node, key int32) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := n.client.Get(ctx, &pb.Key{KeyVal: key})
	if err != nil {
		fmt.Printf("  Get from %s failed: %v\n", n.port, err)
		return ""
	}
	return resp.Val
}

func printKeyEverywhere(nodes []node, key int32) {
	for _, n := range nodes {
		fmt.Printf("  node %s -> key%d=%q\n", n.port, key, getFrom(n, key))
	}
}

func main() {
	nodes := []node{
		{"50051", connectTo("50051")},
		{"50052", connectTo("50052")},
		{"50053", connectTo("50053")},
	}

	fmt.Println("Waiting for an initial leader election to settle...")
	time.Sleep(2 * time.Second)

	// Test 1: write via leader replicates to all nodes
	fmt.Println("\n=== Test 1: write via leader replicates to all nodes ===")
	leader := leaderSet(nodes, 1, "hello-raft")
	if leader == nil {
		log.Fatalf("no leader accepted the write — cluster may be mid-election, try again")
	}
	fmt.Printf("Leader is node %s\n", leader.port)
	time.Sleep(500 * time.Millisecond)
	fmt.Println("Reading key 1 from every node:")
	printKeyEverywhere(nodes, 1)
	fmt.Println("(expect: all three nodes return \"hello-raft\")")

	// Test 2: followers reject direct writes
	fmt.Println("\n=== Test 2: followers reject direct writes ===")
	for _, n := range nodes {
		if n.port == leader.port {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := n.client.Set(ctx, &pb.SetRequest{KeyVal: 99, Val: "should-be-rejected"})
		cancel()
		if err != nil {
			fmt.Printf("  follower %s correctly rejected: %v\n", n.port, err)
		} else {
			fmt.Printf("  follower %s UNEXPECTEDLY accepted the write (bug!)\n", n.port)
		}
	}

	// Test 3: multiple writes stay consistent
	fmt.Println("\n=== Test 3: multiple writes via leader ===")
	leaderSet(nodes, 2, "two")
	leaderSet(nodes, 3, "three")
	leaderSet(nodes, 1, "one-updated") // overwrite
	time.Sleep(500 * time.Millisecond)
	fmt.Println("Reading keys 1,2,3 from every node:")
	for _, n := range nodes {
		fmt.Printf("  node %s -> key1=%q key2=%q key3=%q\n",
			n.port, getFrom(n, 1), getFrom(n, 2), getFrom(n, 3))
	}
	fmt.Println("(expect: all agree; key1=\"one-updated\", key2=\"two\", key3=\"three\")")

	// Test 4: delete via leader, verify by reading (not by DeleteInfo)
	fmt.Println("\n=== Test 4: delete via leader ===")
	delLeader := leaderDelete(nodes, 2)
	if delLeader == nil {
		log.Fatalf("no leader accepted the delete")
	}
	fmt.Printf("Delete of key 2 accepted by leader %s\n", delLeader.port)
	fmt.Println("(note: DeleteInfo is a placeholder — real result verified below via Get)")
	time.Sleep(500 * time.Millisecond)
	fmt.Println("Reading key 2 from every node after delete:")
	printKeyEverywhere(nodes, 2)
	fmt.Println("(expect: all nodes return \"\" — the delete replicated and applied everywhere)")

	fmt.Println("\nDeleting a never-existed key (500):")
	leaderDelete(nodes, 500)
	time.Sleep(300 * time.Millisecond)
	printKeyEverywhere(nodes, 500)
	fmt.Println("(expect: all nodes return \"\")")

	// Test 5: leader failure recovery (manual step)
	fmt.Println("\n=== Test 5: leader failure recovery ===")
	fmt.Printf("Current leader is node %s.\n", leader.port)
	fmt.Printf("Now MANUALLY Ctrl+C the server on port %s, wait ~2s for re-election, then press Enter.\n", leader.port)
	fmt.Scanln()

	survivors := []node{}
	for _, n := range nodes {
		if n.port != leader.port {
			survivors = append(survivors, n)
		}
	}

	fmt.Println("Writing key 4 to the new leader among survivors...")
	newLeader := leaderSet(survivors, 4, "after-crash")
	if newLeader == nil {
		log.Fatalf("no new leader elected among survivors — check server logs")
	}
	fmt.Printf("New leader is node %s\n", newLeader.port)
	time.Sleep(500 * time.Millisecond)

	fmt.Println("Reading pre-crash data (key 1) and new data (key 4) from survivors:")
	for _, n := range survivors {
		fmt.Printf("  node %s -> key1=%q key4=%q\n", n.port, getFrom(n, 1), getFrom(n, 4))
	}
	fmt.Println("(expect: key1=\"one-updated\" survived the crash, key4=\"after-crash\" is new)")
	fmt.Println("\nCommitted data surviving a leader crash is what naive replication could not do.")
}
// package main

// import (
// 	"context"
// 	"fmt"
// 	pb "gocurrencylearning/protostuff"
// 	"io"
// 	"log"
// 	"time"
// 	// "os"
// 	// "strconv"
// 	"sync"

// 	"google.golang.org/grpc"
// 	"google.golang.org/grpc/credentials/insecure"
// )


// // func Get(client pb.KVStoreClient, k *pb.Key) {
// // 	ctx := context.Background()
// // 	fmt.Printf("Getting the value for key: %d\n", k.KeyVal)
// // 	value, err := client.Get(ctx, k)
// // 	if err != nil {
// // 		fmt.Printf("client.Get failed: %v\n", err)
// // 	}
// // 	fmt.Printf("Value returned: %v\n", value.Val)
// // }

// // func Set(client pb.KVStoreClient, sr *pb.SetRequest) {
// // 	sr.FromClient = true
// // 	ctx := context.Background()
// // 	fmt.Printf("Setting the value %v for key: %v\n", sr.Val, sr.KeyVal)
// // 	value, err := client.Set(ctx, sr)
// // 	if err != nil {
// // 		fmt.Printf("client.Set failed: %v\n", err)
// // 	}
// // 	fmt.Printf("Value set: %v\n", value.Val)
// // }

// func Set(client pb.KVStoreClient, sr *pb.SetRequest) {
// 	ctx := context.Background()
// 	value, err := client.Set(ctx, sr)
// 	if err != nil {
// 		fmt.Printf("  Set failed: %v\n", err)
// 		return
// 	}
// 	fmt.Printf("  Set returned: %q\n", value.Val)
// }
 
// func Get(client pb.KVStoreClient, k *pb.Key) string {
// 	ctx := context.Background()
// 	value, err := client.Get(ctx, k)
// 	if err != nil {
// 		fmt.Printf("  Get failed: %v\n", err)
// 		return ""
// 	}
// 	fmt.Printf("  Get returned: %q\n", value.Val)
// 	return value.Val
// }

// func Delete(client pb.KVStoreClient, k *pb.Key) { 
// 	k.FromClient = true
// 	ctx := context.Background()
// 	fmt.Printf("Deleting value for key: %d\n", k.KeyVal)
// 	delInfo, err := client.Delete(ctx, k)
// 	if err != nil {
// 		fmt.Printf("client.Delete failed: %v\n", err)
// 	}
// 	if delInfo.Existed { 
// 		fmt.Printf("The value %v existed and was deleted\n", delInfo.Val)
// 	} else {
// 		fmt.Printf("The key/value pair did NOT exist, nothing was deleted.\n")
// 	}
// }

// func Watch(client pb.KVStoreClient, k *pb.Key) {
// 	ctx := context.Background()
// 	stream, err := client.Watch(ctx, k)
// 	if err != nil {
// 		fmt.Printf("failed to stream due to %v", err)
// 		return 
// 	}
// 	for {
// 		value, err := stream.Recv()
// 		if err == io.EOF {
// 			break
// 		}
// 		if err != nil {
// 			log.Fatalf("client.Watch failed: %v", err)
// 		}
// 		log.Printf("Key: %d was updated with Value: %v", int(k.KeyVal), value.Val)
// 	}
// }

// func connectTo(port string) pb.KVStoreClient {
// 	addr := fmt.Sprintf("localhost:%s", port)
// 	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		log.Fatalf("failed to connect to %s: %v", addr, err)
// 	}
// 	return pb.NewKVStoreClient(conn)
// }

// func main() {
// 	// One connection per instance, all in this one process.
// 	clientA := connectTo("50051")
// 	clientB := connectTo("50052")
// 	clientC := connectTo("50053")
 
// 	// =========================================================
// 	// Test 1: basic replication
// 	// Set on A, read from B and C, confirm the value propagated.
// 	// =========================================================
// 	fmt.Println("=== Test 1: basic replication ===")
// 	key1 := int32(1)
// 	Set(clientA, &pb.SetRequest{KeyVal: key1, Val: "hello-from-A", FromClient: true})
 
// 	fmt.Println("waiting for async forwarding to land...")
// 	time.Sleep(300 * time.Millisecond)
 
// 	fmt.Println("Get from B:")
// 	Get(clientB, &pb.Key{KeyVal: key1})
// 	fmt.Println("Get from C:")
// 	Get(clientC, &pb.Key{KeyVal: key1})
// 	fmt.Println("(expect: both B and C return \"hello-from-A\" if replication worked)")
 
// 	// =========================================================
// 	// Test 2: split-brain / concurrent conflicting writes
// 	// Set the SAME key on A and B at nearly the same instant,
// 	// with different values, then check if all 3 instances agree.
// 	// =========================================================
// 	fmt.Println("\n=== Test 2: concurrent conflicting writes (split-brain) ===")
// 	key2 := int32(2)
 
// 	var wg sync.WaitGroup
// 	wg.Add(2)
// 	go func() {
// 		defer wg.Done()
// 		Set(clientA, &pb.SetRequest{KeyVal: key2, Val: "value-from-A", FromClient: true})
// 	}()
// 	go func() {
// 		defer wg.Done()
// 		Set(clientB, &pb.SetRequest{KeyVal: key2, Val: "value-from-B", FromClient: true})
// 	}()
// 	wg.Wait()
 
// 	fmt.Println("waiting for async forwarding to land...")
// 	time.Sleep(300 * time.Millisecond)
 
// 	fmt.Println("Get from A:")
// 	valA := Get(clientA, &pb.Key{KeyVal: key2})
// 	fmt.Println("Get from B:")
// 	valB := Get(clientB, &pb.Key{KeyVal: key2})
// 	fmt.Println("Get from C:")
// 	valC := Get(clientC, &pb.Key{KeyVal: key2})
 
// 	if valA == valB && valB == valC {
// 		fmt.Printf("all three instances AGREE on value: %q\n", valA)
// 	} else {
// 		fmt.Printf("instances DISAGREE — A=%q B=%q C=%q\n", valA, valB, valC)
// 		fmt.Println("(this divergence is expected with naive, non-consensus replication)")
// 	}
 
// 	// =========================================================
// 	// Test 3: kill-and-recover (manual step required)
// 	// =========================================================
// 	fmt.Println("\n=== Test 3: kill-and-recover ===")
// 	key3 := int32(3)
// 	Set(clientA, &pb.SetRequest{KeyVal: key3, Val: "before-kill", FromClient: true})
// 	fmt.Println("Set key 3 = \"before-kill\" on A.")
// 	fmt.Println("Now manually: Ctrl+C instance C's terminal, restart it, then press Enter here.")
// 	fmt.Scanln()
 
// 	fmt.Println("Get from restarted C:")
// 	Get(clientC, &pb.Key{KeyVal: key3})
// 	fmt.Println("(expect: empty/not-found, since C missed this write while down and has no catch-up mechanism)")
// }

// // func main() {
// // 	allArgs := os.Args
// // 	chosenPort, _ := strconv.Atoi(allArgs[1])
// // 	serverAddr := fmt.Sprintf("localhost:%d", chosenPort)
 
// // 	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
// // 	if err != nil {
// // 		log.Fatalf("failed to dial in to server due to %v", err)
// // 	}
// // 	defer conn.Close()
// // 	client := pb.NewKVStoreClient(conn)

// // 	Set(client, &pb.SetRequest{KeyVal: 1, Val: "key one", FromClient: true})


 
// // 	// --- 1. Get a key that was never set ---
// // // 	fmt.Println("=== 1. Get on never-set key ===")
// // // 	Get(client, &pb.Key{KeyVal: 100}) // expect: got ""
 
// // // 	// --- 2. Set then Get, confirm round trip ---
// // // 	fmt.Println("\n=== 2. Set then Get ===")
// // // 	Set(client, &pb.SetRequest{KeyVal: 1, Val: "one"})
// // // 	Get(client, &pb.Key{KeyVal: 1}) // expect: got "one"
 
// // // 	// --- 3. Overwrite the same key, confirm new value wins ---
// // // 	fmt.Println("\n=== 3. Overwrite existing key ===")
// // // 	Set(client, &pb.SetRequest{KeyVal: 1, Val: "ONE-UPDATED"})
// // // 	Get(client, &pb.Key{KeyVal: 1}) // expect: got "ONE-UPDATED"
 
// // // 	// --- 4. Multiple distinct keys, check no cross-contamination ---
// // // 	fmt.Println("\n=== 4. Multiple distinct keys ===")
// // // 	Set(client, &pb.SetRequest{KeyVal: 2, Val: "two"})
// // // 	Set(client, &pb.SetRequest{KeyVal: 3, Val: "three"})
// // // 	Get(client, &pb.Key{KeyVal: 1}) // expect: "ONE-UPDATED" (unchanged)
// // // 	Get(client, &pb.Key{KeyVal: 2}) // expect: "two"
// // // 	Get(client, &pb.Key{KeyVal: 3}) // expect: "three"
 
// // // 	// --- 5. Delete an existing key ---
// // // 	fmt.Println("\n=== 5. Delete existing key ===")
// // // 	Delete(client, &pb.Key{KeyVal: 2}) // expect: existed=true, val="two"
// // // 	Get(client, &pb.Key{KeyVal: 2})    // expect: got "" (gone now)
 
// // // 	// --- 6. Delete an already-deleted / never-existed key ---
// // // 	fmt.Println("\n=== 6. Delete non-existent key ===")
// // // 	Delete(client, &pb.Key{KeyVal: 2})   // expect: existed=false (already deleted above)
// // // 	Delete(client, &pb.Key{KeyVal: 999}) // expect: existed=false (never existed)
 
// // // 	// --- 7. Concurrent calls: stress-test server-side locking over real RPCs ---
// // // 	fmt.Println("\n=== 7. Concurrent Set/Get/Delete ===")
// // // 	var wg sync.WaitGroup
// // // 	for i := 0; i < 50; i++ {
// // // 		wg.Add(1)
// // // 		go func(n int) {
// // // 			defer wg.Done()
// // // 			key := int32(1000 + n)
// // // 			Set(client, &pb.SetRequest{KeyVal: key, Val: fmt.Sprintf("concurrent-%d", n)})
// // // 			Get(client, &pb.Key{KeyVal: key})
// // // 			if n%2 == 0 {
// // // 				Delete(client, &pb.Key{KeyVal: key})
// // // 			}
// // // 		}(i)
// // // 	}
// // // 	wg.Wait()
// // // 	fmt.Println("concurrent round completed without crashing")
 
// // // 	fmt.Println("\nall checks complete")

// // // 	// -- Watch function testing -- 
// // // 	fmt.Println("\n=== 8. Testing Watch Function ===")
// // // 	var wg2 sync.WaitGroup
// // // 	for i := 0; i < 15; i++ {
// // // 		wg2.Add(1)
// // // 		go func(n int) {
// // // 			defer wg2.Done()
// // // 			key := int32(1000 + n)
// // // 			go Watch(client, &pb.Key{KeyVal: key})
// // // 			time.Sleep(50 * time.Millisecond)
// // // 			for j := 0; j < 10; j++ {
// // // 				var rand_string = fmt.Sprintf("randomval_%d", j)
// // // 				Set(client, &pb.SetRequest{KeyVal: key, Val: rand_string})
// // // 				time.Sleep(100*time.Millisecond)
// // // 				if j >= 5 {
// // // 					if key % 2 == 0 {
// // // 						Delete(client, &pb.Key{KeyVal: key})
// // // 					}
// // // 				}
// // // 			}
// // // 		}(i)
// // // 	}
// // // 	wg2.Wait()
// // }



