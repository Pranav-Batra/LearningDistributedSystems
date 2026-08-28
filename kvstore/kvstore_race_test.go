package kvstore

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentReadWriteDelete is the race-detector target. It has no
// assertions by design: run it with -race and the detector is the assertion.
func TestConcurrentReadWriteDelete(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			Set(n, fmt.Sprintf("val-%d", n))
			Get(n)
			if n%2 == 0 {
				Delete(n)
			}
		}(i)
	}
	wg.Wait()
}

// TestConcurrentReadOnly targets the RLock/Unlock mismatch specifically.
// A mismatched pair only misbehaves when many readers hold the lock at once,
// so a read-only workload is the case that catches it.
func TestConcurrentReadOnly(t *testing.T) {
	Set(500, "steady-value")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := Get(500); got != "steady-value" {
				t.Errorf("Get(500) = %q, want %q", got, "steady-value")
			}
		}()
	}
	wg.Wait()
}

// TestSlowWatcherDoesNotBlockStore is the regression test for the
// snapshot-and-notify fix. One watcher never reads from its channel; the store
// must stay responsive anyway.
func TestSlowWatcherDoesNotBlockStore(t *testing.T) {
	const key = 99
	stuck := RegisterWatcher(key) // deliberately never drained
	defer UnregisterWatcher(key, stuck)

	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		Set(key, "value")
		Get(key)
		done <- time.Since(start)
	}()

	select {
	case elapsed := <-done:
		if elapsed > 50*time.Millisecond {
			t.Errorf("Set+Get took %v with a stuck watcher; expected it to be unaffected", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Set+Get blocked behind a stuck watcher — broadcast is holding the lock")
	}
}

// TestWatcherReceivesUpdate is a basic delivery check. Note it parks the
// receiver before the Set: RegisterWatcher creates an UNBUFFERED channel and
// Set uses a non-blocking send, so an update is dropped unless a receiver is
// already waiting. See the notes on buffering.
func TestWatcherReceivesUpdate(t *testing.T) {
	const key = 123
	ch := RegisterWatcher(key)
	defer UnregisterWatcher(key, ch)

	got := make(chan string, 1)
	ready := make(chan struct{})
	go func() {
		close(ready)
		got <- <-ch
	}()

	<-ready
	time.Sleep(10 * time.Millisecond) // let the receiver park on the channel
	Set(key, "notified")

	select {
	case v := <-got:
		if v != "notified" {
			t.Errorf("watcher got %q, want %q", v, "notified")
		}
	case <-time.After(time.Second):
		t.Fatal("watcher never received the update")
	}
}