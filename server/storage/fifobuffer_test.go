package storage

import (
	"sync"
	"testing"

	"github.com/lduseja9/zmq-fifo-buffer/server/interfaces"
)

// startBuffer creates a FifoBuffer, launches its goroutine, and registers Stop
// as a cleanup so every test gets a fresh, isolated instance.
func startBuffer(t *testing.T) *FifoBuffer {
	t.Helper()
	fb := NewFifoBuffer()
	go fb.Start()
	t.Cleanup(fb.Stop)
	return fb
}

// --- Interface compliance ---

// TestFifoBufferImplementsPushPullBuffer verifies at compile time that
// *FifoBuffer satisfies the interfaces.PushPullBuffer interface.
func TestFifoBufferImplementsPushPullBuffer(t *testing.T) {
	var _ interfaces.PushPullBuffer = &FifoBuffer{}
}

// --- Push ---

func TestPushIncreasesSize(t *testing.T) {
	fb := startBuffer(t)
	fb.Push("hello")
	if size := fb.Size(); size != 1 {
		t.Errorf("expected size 1 after one push, got %d", size)
	}
}

func TestPushEmptyStringIsValidItem(t *testing.T) {
	fb := startBuffer(t)
	fb.Push("")
	if size := fb.Size(); size != 1 {
		t.Errorf("expected size 1 after pushing empty string, got %d", size)
	}
}

func TestMultiplePushesAccumulateInOrder(t *testing.T) {
	fb := startBuffer(t)
	fb.Push("a")
	fb.Push("b")
	fb.Push("c")
	if size := fb.Size(); size != 3 {
		t.Errorf("expected size 3 after three pushes, got %d", size)
	}
}

// --- Pull ---

func TestPullReturnsItemWithOkTrue(t *testing.T) {
	fb := startBuffer(t)
	fb.Push("hello")
	result := fb.Pull()
	if !result.Ok {
		t.Fatal("expected Ok=true, got false")
	}
	if result.Item != "hello" {
		t.Errorf("expected %q, got %q", "hello", result.Item)
	}
}

func TestPullOnEmptyBufferReturnsOkFalse(t *testing.T) {
	fb := startBuffer(t)
	result := fb.Pull()
	if result.Ok {
		t.Error("expected Ok=false for empty buffer, got true")
	}
	if result.Item != "" {
		t.Errorf("expected empty item string, got %q", result.Item)
	}
}

func TestPullEmptyStringItemReturnsOkTrue(t *testing.T) {
	fb := startBuffer(t)
	fb.Push("")
	result := fb.Pull()
	if !result.Ok {
		t.Fatal("expected Ok=true for empty string item, got false")
	}
}

func TestPullDecreasesSize(t *testing.T) {
	fb := startBuffer(t)
	fb.Push("x")
	fb.Pull()
	if size := fb.Size(); size != 0 {
		t.Errorf("expected size 0 after pulling last item, got %d", size)
	}
}

func TestPullAfterDrainReturnsOkFalse(t *testing.T) {
	fb := startBuffer(t)
	fb.Push("only")
	fb.Pull()
	result := fb.Pull()
	if result.Ok {
		t.Error("expected Ok=false when pulling from drained buffer, got true")
	}
}

// --- FIFO ordering ---

func TestPullReturnsItemsInFIFOOrder(t *testing.T) {
	fb := startBuffer(t)
	items := []string{"first", "second", "third"}
	for _, item := range items {
		fb.Push(item)
	}
	for _, expected := range items {
		result := fb.Pull()
		if !result.Ok {
			t.Fatalf("expected Ok=true while pulling %q, got false", expected)
		}
		if result.Item != expected {
			t.Errorf("expected %q, got %q", expected, result.Item)
		}
	}
}

// --- Size ---

func TestSizeOnNewBufferIsZero(t *testing.T) {
	fb := startBuffer(t)
	if size := fb.Size(); size != 0 {
		t.Errorf("expected size 0 on empty buffer, got %d", size)
	}
}

func TestSizeTracksInterleavedPushesAndPulls(t *testing.T) {
	fb := startBuffer(t)
	fb.Push("a")
	fb.Push("b")
	fb.Pull()
	fb.Push("c")
	// two items remain: "b" and "c"
	if size := fb.Size(); size != 2 {
		t.Errorf("expected size 2, got %d", size)
	}
}

// --- Concurrency ---

func TestConcurrentPushesAllLand(t *testing.T) {
	fb := startBuffer(t)
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			fb.Push("item")
		}()
	}
	wg.Wait()
	if size := fb.Size(); size != n {
		t.Errorf("expected size %d after %d concurrent pushes, got %d", n, n, size)
	}
}

func TestConcurrentPullsAllSucceed(t *testing.T) {
	fb := startBuffer(t)
	const n = 50
	for i := 0; i < n; i++ {
		fb.Push("item")
	}

	results := make([]interfaces.PullResult, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			results[i] = fb.Pull()
		}()
	}
	wg.Wait()

	for i, r := range results {
		if !r.Ok {
			t.Errorf("pull %d: expected Ok=true, got false", i)
		}
	}
	if size := fb.Size(); size != 0 {
		t.Errorf("expected empty buffer after pulling all items, got size %d", size)
	}
}

func TestConcurrentSizeCallsAreConsistent(t *testing.T) {
	fb := startBuffer(t)
	const n = 20
	for i := 0; i < n; i++ {
		fb.Push("item")
	}

	var wg sync.WaitGroup
	wg.Add(n)
	sizes := make([]int, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			sizes[i] = fb.Size()
		}()
	}
	wg.Wait()

	for i, s := range sizes {
		if s < 0 || s > n {
			t.Errorf("size call %d returned out-of-range value %d", i, s)
		}
	}
}
