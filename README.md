# Distributed Key-Value Store with Raft Consensus

A concurrent, networked key-value store built in Go, progressing from a single in-memory store to a gRPC service with streaming updates, naive multi-node replication, and (in progress) a hand-rolled Raft consensus implementation for leader election and log replication.

This project was built incrementally, with each stage deliberately exposing the limitations that the next stage solves.

## Project stages

### 1. In-memory KV store
A `Get`/`Set`/`Delete` core backed by a `map[int]string`, protected by a `sync.RWMutex` so concurrent reads don't block each other while writes remain exclusive.

### 2. gRPC service layer
The KV store exposed over gRPC using Protocol Buffers. Unary RPCs (`Get`, `Set`, `Delete`) wrap the core store logic; a `Watch` server-streaming RPC lets clients subscribe to a key and receive updates as they happen. Each key can have multiple independent watchers, implemented as a `map[int][]chan string`, with `Set` taking a locked snapshot of the watcher list before releasing the lock and sending updates (so a slow or disconnected watcher can never block the rest of the store).

### 3. Naive multi-node replication
Three server instances, each holding its own independent copy of the store. A write accepted by one instance is asynchronously forwarded to the other two via gRPC. This stage is deliberately unsafe by design, in order to demonstrate the exact problems that consensus is needed to solve:
- **Split-brain**: two clients writing conflicting values to the same key on two different instances at nearly the same time can leave the cluster in disagreement, with no mechanism to resolve it.
- **False acknowledgment**: an instance reports a write as successful the moment it's applied locally, before confirming the write reached any other node.
- **No recovery**: a node that goes down and restarts has no way to catch up on writes it missed.

### 4. Raft consensus (in progress)
A from-scratch implementation of the Raft algorithm to close the gaps exposed in stage 3: leader election via randomized timeouts and majority voting (`RequestVote`), and (in progress) log replication and commitment via `AppendEntries`, so that a write is only acknowledged once a majority of nodes have durably recorded it.

## Architecture

```
client/       gRPC client + test harness
server/       gRPC server, Raft node, peer-forwarding logic
kvstore/      core Get/Set/Delete logic, watcher registry
protostuff/   .proto definitions and generated code
```

## Running locally

### Prerequisites
- Go (1.21+)
- `protoc` (Protocol Buffers compiler)
- `protoc-gen-go` and `protoc-gen-go-grpc` (installed via `go install`, must be on your `$PATH`)

### Setup
```bash
git clone <repo>
cd GoConcurrencyLearning
go mod tidy
```

### Regenerating protobuf code (only needed after editing the .proto file)
```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       protostuff/kv_store.proto
```

### Running a single instance
```bash
go run ./server 50051
```

### Running the 3-node replicated cluster
Open three terminals:
```bash
go run ./server 50051
go run ./server 50052
go run ./server 50053
```
Each instance dials the other two as peers on startup.

### Running the client
```bash
go run ./client
```
The client test harness connects to one or more instances and exercises `Get`/`Set`/`Delete`/`Watch`, including concurrent-load tests.

## Key challenges and bugs found along the way

A non-exhaustive list of the trickier issues hit during development — kept here because the debugging process was as much the point of this project as the final result.

- **`RLock`/`Unlock` mismatch**: an early version of `Get` acquired a read lock (`RLock`) but released it with the write-lock method (`Unlock`). Mismatched lock/unlock pairs on a `sync.RWMutex` don't fail at compile time and only surface under concurrent load — caught via a dedicated concurrent-read-only test.
- **Snapshot-and-notify race for `Watch`**: broadcasting updates to watchers while holding the store's lock would let one slow or disconnected watcher block every other operation on the store. Fixed by taking a true copy of the watcher list under the lock, releasing the lock, then sending outside of it — combined with non-blocking sends (`select`/`default`) to avoid hanging on a watcher that's disconnected but not yet cleaned up.
- **Goroutine lifetime / orphaned goroutines**: `main()` returning before background goroutines (watchers, replication forwards) had finished caused the process — and their connections — to be torn down mid-operation. Fixed with explicit `sync.WaitGroup` coordination.
- **Raft vote-ordering bug**: an early `RequestVote` handler checked log recency *before* checking whether the node had already voted for a different candidate this term, allowing a double vote in the same term to slip through. Reordered so eligibility (term + vote record) is always checked before recency.
- **Raft `votedFor` reset timing**: resetting a node's vote record needs to happen exactly when its term *strictly increases* — not on every `BecomeFollower` call (which is also invoked when the term is unchanged, e.g., a candidate stepping down after a same-term leader emerges). Resetting unconditionally would have allowed a node to vote twice within a single term.
- **Empty-log edge case in leader election**: the very first election in a fresh cluster involves comparing two empty logs against each other. An empty log is *always* at least as up-to-date as another empty log, so this case needs to unconditionally grant the vote rather than being treated as a comparison that could fail — without this, no election in a brand-new cluster could ever succeed.

## Status

Stages 1–3 are complete and tested, including deliberately reproducing the split-brain and no-recovery failure modes stage 3 is expected to have. Stage 4 (Raft) currently has correct leader-election logic (`RequestVote` handling, term/vote/log-recency rules); `AppendEntries`-based log replication and commitment is in progress.
