# Distributed Key-Value Store with Raft Consensus

A concurrent, networked key-value store built in Go, progressing from a single in-memory store, to a gRPC service with streaming updates, to naive multi-node replication, and finally to a hand-rolled Raft consensus implementation providing leader election, log replication, and strongly-consistent writes.

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

### 4. Raft consensus
A from-scratch implementation of the Raft algorithm that closes the gaps exposed in stage 3. The naive per-node store is replaced by a replicated log: every write becomes a log entry that is agreed upon by a majority of the cluster before it takes effect.

- **Leader election** — nodes use randomized election timeouts to trigger candidacy. A candidate increments its term, votes for itself, and requests votes from peers (`RequestVote`). Votes are granted only if the candidate's term is current, the voter hasn't already voted for someone else this term, and the candidate's log is at least as up-to-date as the voter's (compared by last-log term, then index). A candidate that wins a majority becomes leader and begins sending heartbeats.
- **Log replication** — the leader sends `AppendEntries` (also acting as heartbeats) to each follower, tailored per-follower via `nextIndex`/`matchIndex` bookkeeping. Followers verify log consistency at the entry preceding the new ones (`prevIndex`/`prevTerm`) before appending, truncating any conflicting suffix. A leader that falls behind or a follower that is far behind is reconciled by the leader decrementing `nextIndex` and retrying until the logs agree.
- **Commitment and application** — an entry is *committed* once a majority of nodes have replicated it (computed from the median of the `matchIndex` values, with a current-term safety check). Committed entries are then *applied* to the local KV store, in log order, by a background apply loop that runs identically on every node — so leaders and followers converge to the same state.
- **Client writes go through the log** — a `Set`/`Delete` received by a follower is rejected (the client retries against the leader). On the leader, the operation is appended to the log and the handler blocks until that entry is committed before returning success, guaranteeing the write is durable across a majority.
- **Term-based conflict resolution** — every RPC carries a term; any node that sees a higher term immediately steps down to follower and adopts it, ensuring at most one leader per term.

## Architecture

```
client/       gRPC client + test harness (leader discovery, replication/recovery tests)
server/       gRPC server + Raft node (election, replication, commit/apply)
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

### Running the 3-node cluster
Each server takes a port argument and dials the other two as peers on startup. Open three terminals:
```bash
go run ./server 50051
go run ./server 50052
go run ./server 50053
```
Within a second or two the cluster elects a leader (visible in the server logs).

### Running the client test harness
```bash
go run ./client
```
The harness discovers the current leader, then exercises:
1. A write through the leader, confirming it replicates to all three nodes.
2. Direct writes to followers being rejected (redirected to the leader).
3. Multiple writes (including an overwrite) staying consistent across the cluster.
4. A delete replicating and applying on every node (verified by reading, since the delete result is applied asynchronously).
5. **Leader-failure recovery** (manual step): kill the leader, confirm a new one is elected, and confirm that data committed before the crash survives — the exact failure mode naive replication could not handle.

## Key challenges and bugs found along the way

A non-exhaustive list of the trickier issues hit during development — kept here because the debugging process was as much the point of this project as the final result.

- **`RLock`/`Unlock` mismatch**: an early version of `Get` acquired a read lock (`RLock`) but released it with the write-lock method (`Unlock`). Mismatched lock/unlock pairs on a `sync.RWMutex` don't fail at compile time and only surface under concurrent load — caught via a dedicated concurrent-read-only test.
- **Snapshot-and-notify race for `Watch`**: broadcasting updates to watchers while holding the store's lock would let one slow or disconnected watcher block every other operation on the store. Fixed by taking a true copy of the watcher list under the lock, releasing the lock, then sending outside of it — combined with non-blocking sends (`select`/`default`) to avoid hanging on a watcher that's disconnected but not yet cleaned up.
- **Goroutine lifetime / orphaned goroutines**: `main()` returning before background goroutines had finished caused the process — and their connections — to be torn down mid-operation. Fixed with explicit `sync.WaitGroup` coordination.
- **Raft vote-ordering bug**: an early `RequestVote` handler checked log recency *before* checking whether the node had already voted for a different candidate this term, allowing a double vote in the same term to slip through. Reordered so eligibility (term + vote record) is always checked before recency.
- **Raft `votedFor` reset timing**: resetting a node's vote record needs to happen exactly when its term *strictly increases* — not on every `BecomeFollower` call (which is also invoked when the term is unchanged, e.g., a candidate stepping down after a same-term leader emerges). Resetting unconditionally would have allowed a node to vote twice within a single term.
- **Empty-log edge case in leader election**: the very first election in a fresh cluster compares two empty logs. An empty log is always at least as up-to-date as another empty log, so this case must unconditionally grant the vote — without it, no election in a brand-new cluster could ever succeed.
- **Leader self-deposing / election churn**: a newly-elected leader whose own election timer kept running would time out and start a fresh election against itself, causing constant leadership turnover. Fixed by resetting the leader's timer on becoming leader and on each successful heartbeat round, and by having the election-timer loop skip starting an election while the node is already leader. A too-small gap between the heartbeat interval and the election timeout made this worse, so the election timeout was widened well beyond the heartbeat interval.
- **Stale-goroutine writes after stepping down**: an in-flight `AppendEntries` response goroutine could return *after* its node had already stepped down to follower (which nils out the leader-only `nextIndex`/`matchIndex` slices), causing an index-out-of-range panic. Fixed by re-checking `state == Leader` after re-acquiring the lock in the response handler, since a goroutine that blocked on the network must re-validate its assumptions about current state before acting.
- **`nextIndex` underflow**: repeated `AppendEntries` rejections could decrement a follower's `nextIndex` below zero (there's nowhere earlier than the start of the log to back up to), causing a negative slice index. Guarded so `nextIndex` never drops below zero.
- **Commit-index off-by-ones**: computing the commit index from the sorted `matchIndex` values required distinguishing the *position* in the sorted array from the log *index value* stored there, using the exact-current-term commit rule, and initializing `commitIndex`/`lastApplied` to `-1` (not `0`) so the very first entry (index 0) is actually applied.

## Known limitations / future work

- **No persistence** — the log and Raft state live only in memory, so a restarted node loses its history. Persisting `currentTerm`, `votedFor`, and the log to disk (a true write-ahead log) would make restarts safe and is the natural next step.
- **Delete result reporting** — because entries are applied asynchronously by the commit/apply loop, the `Delete` RPC returns a placeholder result rather than the true `existed`/value from the moment of application. Routing the apply result back to the waiting handler (e.g. via a per-index result channel) would fix this.
- **Reads are not yet linearizable** — `Get` currently reads local applied state on whatever node receives it, which can be stale on a follower. Routing reads through the leader (or a lease/read-index scheme) would make reads strongly consistent.
- **No client-side leader discovery/retry in production form** — the test harness finds the leader by trying each node; a real client library would cache the leader and retry with backoff.
- **Fixed 3-node membership** — cluster size and peers are hardcoded; dynamic membership changes (Raft's joint-consensus protocol) are not implemented.
- **No log compaction / snapshotting** — the log grows without bound.