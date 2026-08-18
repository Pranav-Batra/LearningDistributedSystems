package kvstore

import (
	"fmt"
	"sync"
)

var KVStore = make(map[int]string)

var KVWatchers = make(map[int][]chan string)

var mu sync.RWMutex

func Get(key int) string {
	mu.RLock()
	defer mu.RUnlock()
	val := KVStore[key]
	return val
}

func Set(key int, val string) string {
	mu.Lock()
	KVStore[key] = val
	dup := make([]chan string, len(KVWatchers[key]))
	copy(dup, KVWatchers[key])
	mu.Unlock()
	for _, ch := range dup {
		select {
		case ch <- val:
		default:
		}
	}

	return val
}

func removeChannel(chanlist []chan string, target <- chan string) []chan string {
	removalidx := 0
	for idx, ch := range chanlist {
		if ch == target {
			removalidx = idx
			chanlist = append(chanlist[:removalidx], chanlist[removalidx+1:]...)
			break
		}
	}
	return chanlist
}

func UnregisterWatcher(key int, ch <- chan string){
	mu.Lock()
	KVWatchers[key] = removeChannel(KVWatchers[key], ch)
	mu.Unlock()
}


func RegisterWatcher(key int) <- chan string{
	mu.Lock()
	myChannel := make(chan string)
	KVWatchers[key] = append(KVWatchers[key], myChannel)
	mu.Unlock()
	return myChannel
}

func Delete(key int) (bool, string) {
	mu.Lock()
	defer mu.Unlock()
	val, ok := KVStore[key]
	if !ok {
		return false, ""
	} else {
		delete(KVStore, key)
		return true, val
	}
}

func main() {
	// --- Basic single-threaded checks ---
	fmt.Println("=== basic checks ===")
 
	Set(1, "hello")
	fmt.Println("Get(1) after Set:", Get(1)) // expect "hello"
 
	fmt.Println("Get(99) never set:", Get(99)) // expect ""
 
	Delete(1)
	fmt.Println("Get(1) after Delete:", Get(1)) // expect ""
 
	// fmt.Println("Delete(99) not present:", Delete(99)) // expect "", no panic
 
	// --- Concurrent check: many goroutines hitting Get/Set/Delete at once ---
	fmt.Println("\n=== concurrent check ===")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
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
	fmt.Println("concurrent Set/Get/Delete completed without crashing")
 
	// --- Concurrent READ-ONLY check: specifically targets the RLock/RUnlock bug ---
	fmt.Println("\n=== concurrent read-only check ===")
	Set(500, "steady-value")
	var wg2 sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			Get(500)
		}()
	}
	wg2.Wait()
	fmt.Println("concurrent read-only Get completed without deadlock")
}