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
	"time"
	"math/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const MAJORITY_VOTES = 2

var mu sync.Mutex

type kvStoreServer struct {
	pb.UnimplementedKVStoreServer
	peers []pb.KVStoreClient
	raft *RaftNode
}

type ServerState int 

const (
	Follower ServerState = iota
	Candidate
	Leader
)

func (s ServerState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return ""
}

type RaftNode struct {
	id int32
	currentTerm int32
	votedFor int32
	state ServerState
	log []pb.LogEntry
	leaderId int32
	peers []pb.KVStoreClient
	resetTimer *time.Timer

	matchIndex []int32
	nextIndex []int32
}

func (node *RaftNode) RunElectionTimer() {
	for {
		select {
		case <- node.resetTimer.C:
			node.StartElection()
			node.resetTimer.Reset(randomElectionTimer())
		}
	}
}

func randomElectionTimer() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

//runs when reset timer fires with no message from leader
func (node *RaftNode) StartElection() {
	mu.Lock()
	node.BecomeCandidate()
	total_votes := 1
	term := node.currentTerm
	id := node.id
	last_log_index := 0
	last_log_term := 0
	if len(node.log) > 0{
		last_log_index = len(node.log) - 1
		last_log_term = int(node.log[last_log_index].Term)
	}
	// last_log_term := node.log[last_log_index].Term
	mu.Unlock()
	votes := make(chan int, 3)
	request_vote := &pb.RequestVote{Term: term, NodeId: id, LastLogIndex: int32(last_log_index), LastLogTerm: int32(last_log_term)}
	for _, peer := range node.peers {
		ctx := context.Background()
		go func(pc pb.KVStoreClient) {
			resp, err := pc.NodeRequestsVote(ctx, request_vote)
			if err != nil {
				fmt.Printf("Failed to get vote response.")
				votes <- 0
				return
			}
			if resp.VotedForCandidate {
				votes <- 1
			} else {
				votes <- 0
			}
		}(peer)
	}

	for i := 0; i < len(node.peers); i++ {
		val := <- votes
		total_votes += val
		if total_votes >= MAJORITY_VOTES {
			mu.Lock()
			if node.state == Candidate {
				node.BecomeLeader()
			}
			mu.Unlock()
			return
		}
	}
}

func (node *RaftNode) BecomeLeader() {
	node.state = Leader
	node.matchIndex = make([]int32, len(node.peers))
	node.nextIndex = make([]int32, len(node.peers))
	for i := range node.nextIndex {
		node.nextIndex[i] = int32(len(node.log))
	}
	node.leaderId = node.id
}

func (node *RaftNode) BecomeFollower(term int32) {
	if term < node.currentTerm {
		return
	}
	if term > node.currentTerm {
		node.votedFor = -1
	}
	node.currentTerm = term
	node.state = Follower
	node.matchIndex = nil
	node.nextIndex = nil
}

func (node *RaftNode) BecomeCandidate() {
	node.currentTerm++
	node.state = Candidate
	node.leaderId = -1
	node.votedFor = node.id
}

func (node *RaftNode) SendAppendEntries() {
	mu.Lock()
	leader_term := node.currentTerm
	leader_id := node.id
	next_index_snapshot := make([]int32, len(node.nextIndex))
	copy(next_index_snapshot, node.nextIndex)
	logSnapshot := make([]pb.LogEntry, len(node.log))
	copy(logSnapshot, node.log)
	mu.Unlock()
	for i, peer := range node.peers {
		go func(peer_idx int, pc pb.KVStoreClient){
			index_to_send := next_index_snapshot[peer_idx]
			log_entries := logSnapshot[index_to_send:]
			ptrEntries := make([]*pb.LogEntry, len(log_entries))
			for i := range log_entries {
				ptrEntries[i] = &log_entries[i]
			}
			prev_index := int32(0)
			prev_term := int32(0)
			if index_to_send > 0 {
				prev_index = index_to_send - 1
				prev_term = logSnapshot[prev_index].Term
			}
			entries_message := &pb.AppendEntries{LeaderTerm: leader_term, Entries: ptrEntries, PrevTerm: prev_term,
				PrevIndex: prev_index, LeaderId: leader_id}
			ctx := context.Background()
			response, err := pc.NodeAppendEntries(ctx, entries_message)
			if err != nil { 
				fmt.Printf("Failed to send append entries message.")
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if response.AppendedEntries {
				node.matchIndex[peer_idx] = int32(len(node.log) - 1)
				node.nextIndex[peer_idx] = int32(len(node.log))
			} else {
				if response.FollowerTerm > node.currentTerm {
					node.BecomeFollower(response.FollowerTerm)
					return
				} else {
					node.nextIndex[peer_idx] -= 1
				}
			}
		}(i, peer)
	}
}

func (node *RaftNode) NodeRequestsVote(rv *pb.RequestVote) (*pb.RequestVoteResponse, error) {
	mu.Lock()
	defer mu.Unlock()
	N := len(node.log)
	if rv.Term > node.currentTerm {
		node.BecomeFollower(rv.Term)
	}
	voteTrue := &pb.RequestVoteResponse{VoterTerm: node.currentTerm, VotedForCandidate: true}
	rejectVote := &pb.RequestVoteResponse{VoterTerm: node.currentTerm, VotedForCandidate: false}
	if node.votedFor != -1 && node.votedFor != rv.NodeId {
		return rejectVote, nil
	}
	if rv.Term < node.currentTerm {
		return rejectVote, nil
	}
	if N == 0 {
		node.votedFor = rv.NodeId
		node.resetTimer.Reset(randomElectionTimer())
		return voteTrue, nil
	
	} else {
		if rv.LastLogTerm > node.log[N-1].Term || (rv.LastLogTerm == node.log[N-1].Term && rv.LastLogIndex > node.log[N-1].Index) {
			node.votedFor = rv.NodeId
			node.resetTimer.Reset(randomElectionTimer())
			return voteTrue, nil
		}
	}
	return rejectVote, nil
}

func (node *RaftNode) NodeAppendEntries(ae *pb.AppendEntries) (*pb.AppendEntriesResponse, error) {
	mu.Lock()
	defer mu.Unlock()
	N := len(node.log)

	if ae.LeaderTerm < node.currentTerm {
		return &pb.AppendEntriesResponse{FollowerTerm: node.currentTerm, AppendedEntries: false}, nil
	}
	if N <= int(ae.PrevIndex) {
		return &pb.AppendEntriesResponse{FollowerTerm: node.currentTerm, AppendedEntries: false}, nil
	}
	if ae.PrevIndex >= 0 {
			if node.log[ae.PrevIndex].Term != ae.PrevTerm {
		return &pb.AppendEntriesResponse{FollowerTerm: node.currentTerm, AppendedEntries: false}, nil
	}
	}
	node.BecomeFollower(ae.LeaderTerm)
	node.resetTimer.Reset(randomElectionTimer())
	node.log = node.log[:ae.PrevIndex+1]
	for i := range len(ae.Entries){
		node.log = append(node.log, *ae.Entries[i])
	}
	return &pb.AppendEntriesResponse{FollowerTerm: node.currentTerm, AppendedEntries: true}, nil
}

func (kv *kvStoreServer) NodeAppendEntries(_ context.Context, ae *pb.AppendEntries) (*pb.AppendEntriesResponse, error) {
	return kv.raft.NodeAppendEntries(ae)
}

func (kv *kvStoreServer) NodeRequestsVote(_ context.Context, rv *pb.RequestVote) (*pb.RequestVoteResponse, error) {
	return kv.raft.NodeRequestsVote(rv)
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
			go func (pc pb.KVStoreClient) {
				ctx := context.Background()
				fmt.Printf("Propagating the value %v for key: %v to server %v\n", sr.Val, sr.KeyVal, peerCon)
				value, err := peerCon.Set(ctx, sr)
				if err != nil || value == nil{
					fmt.Printf("client.Set failed: %v\n", err)
				}
				fmt.Printf("Value set: %v\n", value.Val)
			}(peerCon)
			
		}
	}
	return &pb.Value{Val: input_val}, nil
}

func (kv *kvStoreServer) Delete(_ context.Context, k *pb.Key) (*pb.DeleteInfo, error) { 
	exists, del_val := kvstore.Delete(int(k.KeyVal))
	if k.FromClient {
		k.FromClient = false
		for _, peerCon := range kv.peers {
			go func (pc pb.KVStoreClient) {
				ctx := context.Background()
				fmt.Printf("Propagating the delete of key %v to server %v\n", k.KeyVal, peerCon)
				delInfo, err := peerCon.Delete(ctx, k)
				if err != nil || delInfo == nil{
					fmt.Printf("client.Delete failed: %v\n", err)
				}
				if delInfo.Existed { 
					fmt.Printf("The value %v existed and was deleted\n", delInfo.Val)
				} else {
					fmt.Printf("The key/value pair did NOT exist, nothing was deleted.\n")
				}
			}(peerCon)
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
	kvStore.raft = &RaftNode{}
	var allArgs = os.Args
	allPorts := []int{50051, 50052, 50053}
	userPort, _ := strconv.Atoi(allArgs[1])
	for i, port := range allPorts {
		if port == userPort {
			kvStore.raft.id = int32(i)
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
	kvStore.raft.currentTerm = 0
	kvStore.raft.peers = kvStore.peers
	kvStore.raft.votedFor = -1
	kvStore.raft.resetTimer = time.NewTimer(randomElectionTimer())
	go kvStore.raft.RunElectionTimer()
	go func () {
		ticker := time.NewTicker(50 * time.Millisecond)
		mu.Lock()
		isLeader := kvStore.raft.state == Leader
		mu.Unlock()
		for range ticker.C {
			if isLeader {
				kvStore.raft.SendAppendEntries()
			}
		}
	}()
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