package kvstore

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Baseline throughput: the store core, no networking.
// ---------------------------------------------------------------------------

func BenchmarkSet(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Set(i%1000, "value")
	}
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i < 1000; i++ {
		Set(i, "value")
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Get(i % 1000)
	}
}

// BenchmarkGetParallel is the number that justifies RWMutex over Mutex:
// concurrent readers should scale with -cpu, since RLock does not serialise them.
func BenchmarkGetParallel(b *testing.B) {
	for i := 0; i < 1000; i++ {
		Set(i, "value")
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			Get(i % 1000)
			i++
		}
	})
}

// BenchmarkMixed is a more honest workload: mostly reads, some writes.
// Writes take the exclusive lock, so this shows how much a small write
// fraction costs the readers.
func BenchmarkMixed(b *testing.B) {
	for i := 0; i < 1000; i++ {
		Set(i, "value")
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				Set(i%1000, "updated")
			} else {
				Get(i % 1000)
			}
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// Watch fan-out: snapshot-and-notify vs. the naive send-under-lock version.
//
// naiveSet is the bug you fixed, reconstructed here so the two can be measured
// against each other. It broadcasts while still holding the write lock, so a
// slow watcher stalls every other operation on the store. The time.After bound
// keeps a stalled send from hanging the benchmark forever.
// ---------------------------------------------------------------------------

func naiveSet(key int, val string) string {
	mu.Lock()
	defer mu.Unlock()
	KVStore[key] = val
	for _, ch := range KVWatchers[key] {
		select {
		case ch <- val:
		case <-time.After(2 * time.Millisecond):
		}
	}
	return val
}

// spawnWatchers registers n watchers on key. One of them is deliberately slow:
// it sleeps between receives, simulating a client on a bad connection.
// Returns a stop func that unregisters everything and halts the drain loops.
func spawnWatchers(key, n int, slowDelay time.Duration) func() {
	done := make(chan struct{})
	var wg sync.WaitGroup
	chans := make([]<-chan string, 0, n)

	for i := 0; i < n; i++ {
		ch := RegisterWatcher(key)
		chans = append(chans, ch)
		slow := i == 0 && slowDelay > 0
		wg.Add(1)
		go func(ch <-chan string, slow bool) {
			defer wg.Done()
			for {
				select {
				case <-ch:
					if slow {
						time.Sleep(slowDelay)
					}
				case <-done:
					return
				}
			}
		}(ch, slow)
	}

	return func() {
		close(done)
		wg.Wait()
		for _, ch := range chans {
			UnregisterWatcher(key, ch)
		}
	}
}

// benchGetDuringBroadcast measures Get latency while writes are broadcasting
// to watchers. With snapshot-and-notify, Get should be unaffected by the slow
// watcher. With send-under-lock, Get waits behind it.
func benchGetDuringBroadcast(b *testing.B, setFn func(int, string) string, watchers int) {
	const key = 42
	stop := spawnWatchers(key, watchers, 5*time.Millisecond)
	defer stop()

	Set(key, "seed")

	writerDone := make(chan struct{})
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		i := 0
		for {
			select {
			case <-writerDone:
				return
			default:
				setFn(key, fmt.Sprintf("v%d", i))
				i++
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Get(key)
	}
	b.StopTimer()

	close(writerDone)
	writerWG.Wait()
}

func BenchmarkGetDuringBroadcast_SnapshotNotify(b *testing.B) {
	benchGetDuringBroadcast(b, Set, 100)
}

func BenchmarkGetDuringBroadcast_LockHeld(b *testing.B) {
	benchGetDuringBroadcast(b, naiveSet, 100)
}

// BenchmarkBroadcastFanout measures the cost of a single Set as the number of
// watchers on that key grows. Run it to find where fan-out starts to hurt.
func BenchmarkBroadcastFanout(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("watchers=%d", n), func(b *testing.B) {
			const key = 7
			stop := spawnWatchers(key, n, 0)
			defer stop()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Set(key, "value")
			}
		})
	}
}