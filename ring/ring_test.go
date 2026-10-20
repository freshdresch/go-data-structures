package ring

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRingCreation(t *testing.T) {
	// Counts that are not a power of two are rejected.
	_, err := New[uintptr](13)
	assert.ErrorIs(t, err, ErrInvalidCount)

	_, err = New[uintptr](6)
	assert.ErrorIs(t, err, ErrInvalidCount)

	simple, err := New[uintptr](64)
	assert.NoError(t, err)
	assertNewRing[uintptr](t, simple, 64)
	assert.False(t, simple.multiProd)
	assert.False(t, simple.multiCons)

	mpsc, err := New[uintptr](64, WithMultiProdEnqueue())
	assert.NoError(t, err)
	assertNewRing[uintptr](t, mpsc, 64)
	assert.True(t, mpsc.multiProd)
	assert.False(t, mpsc.multiCons)

	spmc, err := New[uintptr](64, WithMultiConsDequeue())
	assert.NoError(t, err)
	assertNewRing[uintptr](t, spmc, 64)
	assert.False(t, spmc.multiProd)
	assert.True(t, spmc.multiCons)

	mpmc, err := New[uintptr](
		64,
		WithMultiProdEnqueue(),
		WithMultiConsDequeue(),
	)
	assert.NoError(t, err)
	assertNewRing[uintptr](t, mpmc, 64)
	assert.True(t, mpmc.multiProd)
	assert.True(t, mpmc.multiCons)

	large, err := New[uintptr](1 << 16)
	assert.NoError(t, err)
	assertNewRing(t, large, 1<<16)
}

func TestRingCount(t *testing.T) {
	buflen := 32
	buffer, err := New[string](buflen)
	require.NotNil(t, buffer)
	require.NoError(t, err)

	assert.Equal(t, 0, buffer.Count())

	item := "test"
	for i := 0; i < buflen; i++ {
		err = buffer.Enqueue(item)
		require.NoError(t, err)
		assert.Equal(t, i+1, buffer.Count())
	}

	// Single-element calls return their sentinels unwrapped, so they are compared with ==.
	err = buffer.Enqueue(item)
	assert.True(t, err == ErrFull, "got %v", err)

	assert.Equal(t, buflen, buffer.Count())

	for i := 0; i < buflen; i++ {
		_, err = buffer.Dequeue()
		require.NoError(t, err)
		assert.Equal(t, buflen-1-i, buffer.Count())
	}

	_, err = buffer.Dequeue()
	assert.True(t, err == ErrEmpty, "got %v", err)
}

func TestRingReset(t *testing.T) {
	buflen := 32
	buffer, err := New[string](buflen)
	require.NotNil(t, buffer)
	require.NoError(t, err)

	// The other occupancy functions are checked along the way, so they need no tests of their own.
	assert.True(t, buffer.Empty())
	assert.False(t, buffer.Full())
	assert.Equal(t, buflen, buffer.Free())
	assert.Equal(t, 0, buffer.Count())

	item := "test"
	for i := 0; i < buflen; i++ {
		err = buffer.Enqueue(item)
		require.NoError(t, err)

		assert.False(t, buffer.Empty())
		if i == buflen-1 {
			assert.True(t, buffer.Full())
		} else {
			assert.False(t, buffer.Full())
		}

		assert.Equal(t, buflen-1-i, buffer.Free())
		assert.Equal(t, i+1, buffer.Count())
	}

	assert.Equal(t, buflen, buffer.Count())
	assert.Equal(t, 0, buffer.Free())

	buffer.Reset()

	assert.True(t, buffer.Empty())
	assert.False(t, buffer.Full())
	assert.Equal(t, buflen, buffer.Free())
	assert.Equal(t, 0, buffer.Count())

	_, err = buffer.Dequeue()
	assert.ErrorIs(t, err, ErrEmpty)
}

func TestSPSCRingBuffer(t *testing.T) {
	buffer, err := New[string](32)
	require.NotNil(t, buffer)
	require.NoError(t, err)

	item := "test"
	err = buffer.Enqueue(item)
	assert.NoError(t, err)

	dequeuedItem, err := buffer.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, item, dequeuedItem)

	for i := 0; i < 32; i++ {
		err = buffer.Enqueue(item)
		assert.NoError(t, err)
	}
	err = buffer.Enqueue(item)
	assert.ErrorIs(t, err, ErrFull)

	for i := 0; i < 32; i++ {
		_, err = buffer.Dequeue()
		require.NoError(t, err)
	}

	_, err = buffer.Dequeue()
	assert.ErrorIs(t, err, ErrEmpty)
}

func TestSPMCRingBuffer(t *testing.T) {
	dequeuedStrings := map[string]struct{}{}

	buffer, err := New[string](32, WithMultiConsDequeue())
	require.NotNil(t, buffer)
	require.NoError(t, err)

	for i := 0; i < 32; i++ {
		item := fmt.Sprintf("test-%d", i)
		err = buffer.Enqueue(item)
		require.NoError(t, err)
	}

	numDequeues := []int{8, 8, 8, 8}
	// mu guards dequeuedStrings, which every consumer writes.
	var mu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < numDequeues[worker]; j++ {
				dequeuedItem, err := buffer.Dequeue()
				assert.NoError(t, err)

				mu.Lock()
				_, ok := dequeuedStrings[dequeuedItem]
				assert.False(t, ok)
				dequeuedStrings[dequeuedItem] = struct{}{}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	_, err = buffer.Dequeue()
	assert.ErrorIs(t, err, ErrEmpty)
}

func TestMPSCRingBuffer(t *testing.T) {
	buffer, err := New[string](32, WithMultiProdEnqueue())
	require.NotNil(t, buffer)
	require.NoError(t, err)

	numWorkers := 4
	numEnqueues := []int{8, 8, 8, 8}
	testStrings := []string{}
	for i := 0; i < 32; i++ {
		testStrings = append(testStrings, fmt.Sprintf("test-%d", i))
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(worker int) {
			defer wg.Done()

			stride := numEnqueues[worker]
			for j := 0; j < stride; j++ {
				idx := (worker * 8) + j
				err := buffer.Enqueue(testStrings[idx])
				assert.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()

	dequeuedStrings := map[string]struct{}{}
	for i := 0; i < 32; i++ {
		dequeuedItem, err := buffer.Dequeue()
		require.NoError(t, err)

		_, ok := dequeuedStrings[dequeuedItem]
		assert.False(t, ok)
		dequeuedStrings[dequeuedItem] = struct{}{}
	}

	for _, testString := range testStrings {
		_, ok := dequeuedStrings[testString]
		assert.True(t, ok)
	}
}

func TestMPMCRingBuffer(t *testing.T) {
	buffer, err := New[string](
		32,
		WithMultiProdEnqueue(),
		WithMultiConsDequeue(),
	)
	require.NotNil(t, buffer)
	require.NoError(t, err)

	// The producers enqueue far more items than the ring holds, so they hit ErrFull and retry
	// while the consumers drain it.
	const (
		numWorkers  = 4
		perProducer = 500
	)
	testStrings := make([][]string, numWorkers)
	for worker := range numWorkers {
		for j := range perProducer {
			testStrings[worker] = append(testStrings[worker], fmt.Sprintf("test-%d-%d", worker, j))
		}
	}
	const numItems = numWorkers * perProducer

	// Closing stop tells the workers to give up, so a test that times out does not leave them
	// spinning. Deferred calls run last in first out, so stop is closed before the wait.
	stop := make(chan struct{})
	var workers sync.WaitGroup
	defer workers.Wait()
	defer close(stop)

	for worker := range numWorkers {
		workers.Go(func() {
			for _, item := range testStrings[worker] {
				if !enqueueUntilAccepted(t, buffer, item, stop) {
					return
				}
			}
		})
	}

	// The shared counter lets each consumer stop once every item has been dequeued, however the
	// items were split between them.
	var numDequeued atomic.Int64
	results := make(chan string, numItems)
	for range numWorkers {
		workers.Go(func() {
			for numDequeued.Load() < numItems {
				item, err := buffer.Dequeue()
				if errors.Is(err, ErrEmpty) {
					if isClosed(stop) {
						return
					}
					runtime.Gosched()
					continue
				}
				if !assert.NoError(t, err) {
					return
				}
				numDequeued.Add(1)
				results <- item
			}
		})
	}

	timesDequeued := map[string]int{}
	timeout := time.After(5 * time.Second)
	for received := 0; received < numItems; received++ {
		select {
		case item := <-results:
			timesDequeued[item]++
		case <-timeout:
			t.Fatalf("timed out after receiving %d of %d items", received, numItems)
		}
	}

	assert.Len(t, timesDequeued, numItems)
	for _, producerStrings := range testStrings {
		for _, testString := range producerStrings {
			assert.Equal(t, 1, timesDequeued[testString], testString)
		}
	}
}

// enqueueUntilAccepted retries Enqueue while the ring is full, until it succeeds or stop is closed.
// It reports whether the item was enqueued.
func enqueueUntilAccepted(
	t *testing.T,
	buffer *Ring[string],
	item string,
	stop <-chan struct{},
) bool {
	for {
		err := buffer.Enqueue(item)
		if err == nil {
			return true
		}
		if !assert.ErrorIs(t, err, ErrFull) || isClosed(stop) {
			return false
		}
		runtime.Gosched()
	}
}

// isClosed reports whether stop has been closed, without blocking.
func isClosed(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

func TestRingBufferWraparound(t *testing.T) {
	buflen := 32
	buffer, err := New[uint32](buflen)
	require.NoError(t, err)
	require.NotNil(t, buffer)

	for i := 0; i < buflen; i++ {
		err = buffer.Enqueue(uint32(i))
		require.NoError(t, err)
	}

	assert.True(t, buffer.Full())

	err = buffer.Enqueue(uint32(buflen))
	assert.ErrorIs(t, err, ErrFull)

	_, err = buffer.Dequeue()
	require.NoError(t, err)
	assert.False(t, buffer.Full())

	err = buffer.Enqueue(uint32(buflen))
	assert.NoError(t, err)
	assert.True(t, buffer.Full())
}

func TestRingBufferMultipleWraparounds(t *testing.T) {
	buflen := 8
	buffer, err := New[uint32](buflen)
	require.NoError(t, err)
	require.NotNil(t, buffer)

	numIterations := 5
	for i := 0; i < numIterations; i++ {
		for j := 0; j < buflen; j++ {
			err = buffer.Enqueue(uint32(i*buflen + j))
			require.NoError(t, err)
		}

		assert.False(t, buffer.Empty())
		assert.True(t, buffer.Full())
		assert.Equal(t, 0, buffer.Free())
		assert.Equal(t, buflen, buffer.Count())

		for j := 0; j < buflen; j++ {
			item, err := buffer.Dequeue()
			require.NoError(t, err)
			assert.Equal(t, i*buflen+j, int(item))
		}

		assert.True(t, buffer.Empty())
		assert.False(t, buffer.Full())
		assert.Equal(t, buflen, buffer.Free())
		assert.Equal(t, 0, buffer.Count())
	}
}

// TestMPMC_ConsumerSeesOnlyPublishedValues checks that a concurrent multi-consumer receives exactly
// the two values from two multi-producer enqueues and never reads an unpublished slot. It does
// not force out-of-order publication, because producer B starts only after A's enqueue returns.
func TestMPMC_ConsumerSeesOnlyPublishedValues(t *testing.T) {
	const ringSize = 8

	ring, err := New[int](
		ringSize,
		WithMultiProdEnqueue(),
		WithMultiConsDequeue(),
	)
	require.NoError(t, err)

	start := make(chan struct{})
	prodAReady := make(chan struct{})

	// Closing stop tells the consumer to give up, so a test that times out does not leave it
	// spinning. Deferred calls run last in first out, so stop is closed before the wait.
	stop := make(chan struct{})
	var workers sync.WaitGroup
	defer workers.Wait()
	defer close(stop)

	// Producer A enqueues first.
	workers.Go(func() {
		<-start
		assert.NoError(t, ring.Enqueue(1))
		close(prodAReady)
	})

	// Producer B waits until A has gone first.
	workers.Go(func() {
		<-start
		<-prodAReady
		assert.NoError(t, ring.Enqueue(2))
	})

	results := make(chan int, 2)

	// The consumer dequeues both values.
	workers.Go(func() {
		<-start
		for i := 0; i < 2; i++ {
			for {
				value, err := ring.Dequeue()
				if err == nil {
					results <- value
					break
				}
				if isClosed(stop) {
					return
				}
				runtime.Gosched()
			}
		}
	})

	close(start)

	got := make([]int, 0, 2)
	timeout := time.After(2 * time.Second)

	for len(got) < 2 {
		select {
		case value := <-results:
			got = append(got, value)
		case <-timeout:
			t.Fatal("timed out waiting for dequeue results")
		}
	}

	// A zero value means the consumer read a slot before its producer published it.
	for _, value := range got {
		if value == 0 {
			t.Fatalf("observed zero value (unpublished slot): %+v", got)
		}
	}

	set := map[int]struct{}{}
	for _, value := range got {
		set[value] = struct{}{}
	}

	assert.Len(t, set, 2)
	assert.Contains(t, set, 1)
	assert.Contains(t, set, 2)
}

func TestCapacity(t *testing.T) {
	for _, count := range []int{2, 4, 8, 1024, 1 << 16} {
		ring, err := New[int](count)
		require.NoError(t, err)
		assert.Equal(t, count, ring.Capacity(), "count %d", count)
	}
}

// TestNewBounds checks that New accepts the smallest and largest counts and rejects counts below
// 1, above 2^30, or not a power of two. Elements are struct{}, so the largest ring costs no memory.
func TestNewBounds(t *testing.T) {
	for _, count := range []int{1, maxCount} {
		ring, err := New[struct{}](count)
		require.NoError(t, err, "count %d", count)
		assert.Equal(t, count, ring.Capacity(), "count %d", count)
	}

	for _, count := range []int{0, -1, 3, 1000} {
		assertInvalidCount(t, count)
	}

	t.Run("2^31", func(t *testing.T) {
		if math.MaxInt < 1<<31 {
			t.Skip("int cannot hold 2^31")
		}
		// The count is computed at run time, because the constant 2^31 does not compile where
		// int is 32 bits.
		count := maxCount
		count *= 2
		assertInvalidCount(t, count)
	})
}

// assertInvalidCount checks that New rejects count with an error wrapping ErrInvalidCount.
func assertInvalidCount(t *testing.T, count int) {
	t.Helper()
	ring, err := New[struct{}](count)
	assert.ErrorIs(t, err, ErrInvalidCount, "count %d", count)
	assert.True(t, ring == nil, "count %d", count)
}

// TestFullCapacity checks that a ring holds exactly its count, from starting indices on both
// sides of 2^32, so the fill and the drain cross the wrap at different points.
func TestFullCapacity(t *testing.T) {
	for _, capacity := range []int{1, 2, 4, 1024} {
		starts := []uint32{0, math.MaxUint32, math.MaxUint32 - uint32(capacity) + 1}
		if capacity > 2 {
			starts = append(starts, math.MaxUint32-uint32(capacity/2)+1)
		}
		for i, start := range starts {
			// For capacity 1, the third start equals the second, so skip the repeat.
			if slices.Index(starts, start) < i {
				continue
			}
			t.Run(fmt.Sprintf("capacity=%d,start=%d", capacity, start), func(t *testing.T) {
				ring, err := New[int](capacity)
				require.NoError(t, err)
				setIndices(ring, start)

				for i := range capacity {
					require.NoError(t, ring.Enqueue(i))
				}
				assert.True(t, ring.Full())
				assert.Equal(t, 0, ring.Free())
				assert.Equal(t, ring.Capacity(), ring.Count())
				err = ring.Enqueue(capacity)
				assert.True(t, err == ErrFull, "got %v", err)

				for i := range capacity {
					item, err := ring.Dequeue()
					require.NoError(t, err)
					assert.Equal(t, i, item)
				}
			})
		}
	}
}

// TestEmptyAgreesWithCount checks that Empty reports exactly when Count is 0, in every state a
// ring with no concurrent activity passes through while it fills and drains. The start near 2^32
// makes the indices wrap partway through.
func TestEmptyAgreesWithCount(t *testing.T) {
	const capacity = 8
	for _, start := range []uint32{0, math.MaxUint32 - 2} {
		t.Run(fmt.Sprintf("start=%d", start), func(t *testing.T) {
			ring, err := New[int](capacity)
			require.NoError(t, err)
			setIndices(ring, start)

			assertEmptyAgreesWithCount(t, ring)
			for i := range capacity {
				require.NoError(t, ring.Enqueue(i))
				assertEmptyAgreesWithCount(t, ring)
			}
			require.True(t, ring.Full())
			for range capacity {
				_, err := ring.Dequeue()
				require.NoError(t, err)
				assertEmptyAgreesWithCount(t, ring)
			}
			assert.True(t, ring.Empty())
		})
	}
}

// assertEmptyAgreesWithCount checks that Empty reports whether Count is 0.
func assertEmptyAgreesWithCount(t *testing.T, ring *Ring[int]) {
	t.Helper()
	count := ring.Count()
	assert.Equal(t, count == 0, ring.Empty(), "count %d", count)
}

// requireEnqueueBulk enqueues all of items, failing the test if the ring does not accept them.
func requireEnqueueBulk(t *testing.T, ring *Ring[int], items []int) {
	t.Helper()
	_, err := ring.EnqueueBulk(items)
	require.NoError(t, err)
}

func TestEnqueueBulk(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)

	free, err := ring.EnqueueBulk([]int{1, 2, 3, 4, 5, 6})
	require.NoError(t, err)
	assert.Equal(t, 2, free)
	assert.Equal(t, 6, ring.Count())

	free, err = ring.EnqueueBulk([]int{7, 8, 9})
	assert.True(t, err == ErrFull, "got %v", err)
	assert.Equal(t, 2, free)
	assert.Equal(t, 6, ring.Count())

	free, err = ring.EnqueueBulk([]int{7, 8})
	require.NoError(t, err)
	assert.Equal(t, 0, free)

	elems := make([]int, 8)
	_, err = ring.DequeueBulk(elems)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, elems)
}

func TestDequeueBulk(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2, 3, 4, 5})

	available, err := ring.DequeueBulk(make([]int, 6))
	assert.True(t, err == ErrEmpty, "got %v", err)
	assert.Equal(t, 5, available)
	assert.Equal(t, 5, ring.Count())

	elems := make([]int, 3)
	available, err = ring.DequeueBulk(elems)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3}, elems)
	assert.Equal(t, 2, available)

	elems = make([]int, 2)
	available, err = ring.DequeueBulk(elems)
	require.NoError(t, err)
	assert.Equal(t, []int{4, 5}, elems)
	assert.Equal(t, 0, available)

	available, err = ring.DequeueBulk(make([]int, 1))
	assert.True(t, err == ErrEmpty, "got %v", err)
	assert.Equal(t, 0, available)
}

func TestEnqueueBurst(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2, 3, 4, 5})

	enqueued, free := ring.EnqueueBurst([]int{6, 7, 8, 9})
	assert.Equal(t, 3, enqueued)
	assert.Equal(t, 0, free)
	assert.Equal(t, 8, ring.Count())

	enqueued, free = ring.EnqueueBurst([]int{10})
	assert.Equal(t, 0, enqueued)
	assert.Equal(t, 0, free)

	elems := make([]int, 8)
	_, err = ring.DequeueBulk(elems)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, elems)
}

func TestDequeueBurst(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2, 3})

	elems := make([]int, 5)
	dequeued, available := ring.DequeueBurst(elems)
	assert.Equal(t, 3, dequeued)
	assert.Equal(t, []int{1, 2, 3}, elems[:dequeued])
	assert.Equal(t, 0, available)

	dequeued, available = ring.DequeueBurst(make([]int, 2))
	assert.Equal(t, 0, dequeued)
	assert.Equal(t, 0, available)
}

// advanceRing moves the indices of an empty ring forward by steps slots, leaving it empty.
func advanceRing(t *testing.T, ring *Ring[int], steps int) {
	t.Helper()
	for range steps {
		require.NoError(t, ring.Enqueue(0))
		_, err := ring.Dequeue()
		require.NoError(t, err)
	}
}

func TestBatchCopyCrossesEndOfBackingArray(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	batch := []int{1, 2, 3, 4, 5}

	// Starting two slots before the end, a batch of five fills the last two slots of the backing
	// array and then the first three.
	advanceRing(t, ring, 6)
	require.Equal(t, uint32(6), atomic.LoadUint32(ring.prodHead)&ring.mask)
	requireEnqueueBulk(t, ring, batch)
	elems := make([]int, 5)
	_, err = ring.DequeueBulk(elems)
	require.NoError(t, err)
	assert.Equal(t, batch, elems)

	advanceRing(t, ring, 3)
	require.Equal(t, uint32(6), atomic.LoadUint32(ring.prodHead)&ring.mask)
	enqueued, _ := ring.EnqueueBurst(batch)
	assert.Equal(t, 5, enqueued)
	elems = make([]int, 5)
	dequeued, _ := ring.DequeueBurst(elems)
	assert.Equal(t, 5, dequeued)
	assert.Equal(t, batch, elems)
	assert.True(t, ring.Empty())
}

func TestBatchExceedsCapacity(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2})
	batch := []int{3, 4, 5, 6, 7, 8, 9, 10, 11}
	elems := make([]int, ring.Capacity()+1)

	free, err := ring.EnqueueBulk(batch)
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, 6, free)
	assert.Equal(t, 2, ring.Count())

	available, err := ring.DequeueBulk(elems)
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, 2, available)
	assert.Equal(t, 2, ring.Count())

	enqueued, free := ring.EnqueueBurst(batch)
	assert.Equal(t, 6, enqueued)
	assert.Equal(t, 0, free)

	dequeued, available := ring.DequeueBurst(elems)
	assert.Equal(t, 8, dequeued)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, elems[:dequeued])
	assert.Equal(t, 0, available)
}

func TestBulkCheckOrder(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	length := ring.Capacity() + 1

	// An oversized count is reported before an empty ring.
	_, err = ring.DequeueBulk(make([]int, length))
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.NotErrorIs(t, err, ErrEmpty)

	// An oversized count is reported before a full ring.
	requireEnqueueBulk(t, ring, []int{1, 2, 3, 4, 5, 6, 7, 8})
	require.True(t, ring.Full())
	_, err = ring.EnqueueBulk(make([]int, length))
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.NotErrorIs(t, err, ErrFull)
}

// TestEnqueueZeroLength checks that both batch enqueues accept an empty slice, move nothing, and
// return the current free count.
func TestEnqueueZeroLength(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2, 3})

	free, err := ring.EnqueueBulk([]int{})
	assert.NoError(t, err)
	assert.Equal(t, 5, free)
	assert.Equal(t, 3, ring.Count())

	enqueued, free := ring.EnqueueBurst([]int{})
	assert.Equal(t, 0, enqueued)
	assert.Equal(t, 5, free)
	assert.Equal(t, 3, ring.Count())
}

// TestEnqueueErrorCounts checks the free count EnqueueBulk returns with each error on a ring that
// is partly full.
func TestEnqueueErrorCounts(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2, 3, 4, 5})

	free, err := ring.EnqueueBulk(make([]int, 4))
	assert.True(t, err == ErrFull, "got %v", err)
	assert.Equal(t, 3, free)

	free, err = ring.EnqueueBulk(make([]int, ring.Capacity()+1))
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, 3, free)
	assert.Equal(t, 5, ring.Count())
}

// TestEnqueueCapacityBoundary checks that EnqueueBulk accepts exactly Capacity elements on an empty
// ring and rejects one more as exceeding the capacity.
func TestEnqueueCapacityBoundary(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	free, err := ring.EnqueueBulk(make([]int, ring.Capacity()))
	assert.NoError(t, err)
	assert.Equal(t, 0, free)
	assert.True(t, ring.Full())

	ring, err = New[int](8)
	require.NoError(t, err)
	free, err = ring.EnqueueBulk(make([]int, ring.Capacity()+1))
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, ring.Capacity(), free)
	assert.True(t, ring.Empty())
}

// TestEnqueueLongSlice checks that a slice longer than 2^32 does not truncate to a short count.
// Its length, 2^32 + 1, would truncate to 1 as a uint32. Elements are struct{}, so the slice
// costs no memory.
func TestEnqueueLongSlice(t *testing.T) {
	if math.MaxInt < 1<<32 {
		t.Skip("int cannot hold 2^32 + 1")
	}
	// The length is computed at run time, because the constant 2^32 + 1 does not compile where
	// int is 32 bits.
	length := maxCount
	length *= 4
	length++
	items := make([]struct{}, length)

	ring, err := New[struct{}](1024)
	require.NoError(t, err)

	free, err := ring.EnqueueBulk(items)
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, 1024, free)

	enqueued, free := ring.EnqueueBurst(items)
	assert.Equal(t, 1024, enqueued)
	assert.Equal(t, 0, free)
	assert.Equal(t, 1024, ring.Count())
}

// TestDequeueZeroLength checks that both batch dequeues accept an empty slice, move nothing, and
// return the current available count.
func TestDequeueZeroLength(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2})

	available, err := ring.DequeueBulk([]int{})
	assert.NoError(t, err)
	assert.Equal(t, 2, available)
	assert.Equal(t, 2, ring.Count())

	dequeued, available := ring.DequeueBurst([]int{})
	assert.Equal(t, 0, dequeued)
	assert.Equal(t, 2, available)
	assert.Equal(t, 2, ring.Count())
}

// TestDequeueLeavesDst checks that DequeueBurst writes only the part of dst it dequeues into, and
// that a failed DequeueBulk writes none of dst.
func TestDequeueLeavesDst(t *testing.T) {
	// dstSentinel pre-fills dst so the test can tell which slots a call wrote.
	const dstSentinel = -1
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2, 3})

	dst := slices.Repeat([]int{dstSentinel}, 5)
	dequeued, _ := ring.DequeueBurst(dst)
	assert.Equal(t, 3, dequeued)
	assert.Equal(t, []int{1, 2, 3, dstSentinel, dstSentinel}, dst)

	requireEnqueueBulk(t, ring, []int{4, 5})
	dst = slices.Repeat([]int{dstSentinel}, 3)
	_, err = ring.DequeueBulk(dst)
	assert.True(t, err == ErrEmpty, "got %v", err)
	assert.Equal(t, slices.Repeat([]int{dstSentinel}, 3), dst)

	dst = slices.Repeat([]int{dstSentinel}, ring.Capacity()+1)
	_, err = ring.DequeueBulk(dst)
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, slices.Repeat([]int{dstSentinel}, ring.Capacity()+1), dst)
	assert.Equal(t, 2, ring.Count())
}

// TestDequeueErrorCounts checks the available count DequeueBulk returns with each error on a ring
// that is partly full.
func TestDequeueErrorCounts(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2, 3, 4, 5})

	available, err := ring.DequeueBulk(make([]int, 6))
	assert.True(t, err == ErrEmpty, "got %v", err)
	assert.Equal(t, 5, available)

	available, err = ring.DequeueBulk(make([]int, ring.Capacity()+1))
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, 5, available)
	assert.Equal(t, 5, ring.Count())
}

// TestDequeueCapacityBoundary checks that DequeueBulk accepts exactly Capacity elements from a
// full ring and rejects one more as exceeding the capacity.
func TestDequeueCapacityBoundary(t *testing.T) {
	items := []int{1, 2, 3, 4, 5, 6, 7, 8}
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, items)
	dst := make([]int, ring.Capacity())
	available, err := ring.DequeueBulk(dst)
	assert.NoError(t, err)
	assert.Equal(t, 0, available)
	assert.Equal(t, items, dst)
	assert.True(t, ring.Empty())

	ring, err = New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, items)
	available, err = ring.DequeueBulk(make([]int, ring.Capacity()+1))
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, ring.Capacity(), available)
	assert.True(t, ring.Full())
}

// TestDequeueLongSlice checks that a slice longer than 2^32 does not truncate to a short count.
// Its length, 2^32 + 1, would truncate to 1 as a uint32. Elements are struct{}, so the slice
// costs no memory.
func TestDequeueLongSlice(t *testing.T) {
	if math.MaxInt < 1<<32 {
		t.Skip("int cannot hold 2^32 + 1")
	}
	// The length is computed at run time, because the constant 2^32 + 1 does not compile where
	// int is 32 bits.
	length := maxCount
	length *= 4
	length++
	dst := make([]struct{}, length)

	ring, err := New[struct{}](1024)
	require.NoError(t, err)
	_, err = ring.EnqueueBulk(make([]struct{}, 1024))
	require.NoError(t, err)

	available, err := ring.DequeueBulk(dst)
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, 1024, available)

	dequeued, available := ring.DequeueBurst(dst)
	assert.Equal(t, 1024, dequeued)
	assert.Equal(t, 0, available)
	assert.True(t, ring.Empty())
}

// TestIndexWrap places every index just below 2^32, so the uint32 indices overflow partway
// through. A bulk enqueue carries the producer indices past 2^32 and a burst dequeue carries the
// consumer indices past it. Count and Free are checked while only one side has wrapped.
func TestIndexWrap(t *testing.T) {
	modes := map[string][]Option{
		"SPSC": nil,
		"MPMC": {WithMultiProdEnqueue(), WithMultiConsDequeue()},
	}
	for name, options := range modes {
		t.Run(name, func(t *testing.T) {
			ring, err := New[int](8, options...)
			require.NoError(t, err)
			const startIndex = math.MaxUint32 - 2
			setIndices(ring, startIndex)
			assertOccupancy(t, ring, 0)

			require.NoError(t, ring.Enqueue(1))
			require.NoError(t, ring.Enqueue(2))
			assertOccupancy(t, ring, 2)
			item, err := ring.Dequeue()
			require.NoError(t, err)
			assert.Equal(t, 1, item)
			assertOccupancy(t, ring, 1)

			requireEnqueueBulk(t, ring, []int{3, 4, 5})
			require.Less(t, atomic.LoadUint32(ring.prodTail), uint32(startIndex))
			assertOccupancy(t, ring, 4)

			// Only the producer indices have wrapped here, so the occupancy reported by a
			// rejected call comes from indices on opposite sides of 2^32.
			free, err := ring.EnqueueBulk(make([]int, 9))
			assert.ErrorIs(t, err, ErrExceedsCapacity)
			assert.Equal(t, 4, free)
			available, err := ring.DequeueBulk(make([]int, 9))
			assert.ErrorIs(t, err, ErrExceedsCapacity)
			assert.Equal(t, 4, available)
			assertOccupancy(t, ring, 4)
			elems := make([]int, 1)
			_, err = ring.DequeueBulk(elems)
			require.NoError(t, err)
			assert.Equal(t, []int{2}, elems)
			assertOccupancy(t, ring, 3)

			enqueued, _ := ring.EnqueueBurst([]int{6, 7, 8, 9, 10, 11})
			assert.Equal(t, 5, enqueued)
			assertOccupancy(t, ring, 8)
			elems = make([]int, 9)
			dequeued, _ := ring.DequeueBurst(elems)
			assert.Equal(t, 8, dequeued)
			assert.Equal(t, []int{3, 4, 5, 6, 7, 8, 9, 10}, elems[:dequeued])
			require.Less(t, atomic.LoadUint32(ring.consTail), uint32(startIndex))
			assertOccupancy(t, ring, 0)
		})
	}
}

// assertOccupancy checks that Count, Free, and Empty agree with the expected count.
func assertOccupancy(t *testing.T, ring *Ring[int], count int) {
	t.Helper()
	assert.Equal(t, count, ring.Count())
	assert.Equal(t, ring.Capacity()-count, ring.Free())
	assert.Equal(t, count == 0, ring.Empty())
}

func assertNewRing[T any](t *testing.T, ring *Ring[T], count int) {
	t.Helper()
	assert.NotNil(t, ring.entries)
	assert.Equal(t, uint32(count-1), ring.mask)
	assert.Equal(t, count, ring.Capacity())

	assert.Zero(t, *ring.prodHead)
	assert.Zero(t, *ring.prodTail)
	assert.Zero(t, *ring.consHead)
	assert.Zero(t, *ring.consTail)
}

// setIndices places all four indices of an empty ring at start, so a test can run the ring across
// the 2^32 wrap. It is the only test code that writes the indices.
func setIndices[T any](ring *Ring[T], start uint32) {
	for _, index := range []*uint32{ring.prodHead, ring.prodTail, ring.consHead, ring.consTail} {
		atomic.StoreUint32(index, start)
	}
}

// TestMaxBatchOptions checks that both batch limits default to the capacity, that each option
// sets its own side, that WithMaxBatch sets both, that the last option given for a side applies,
// and that New rejects a limit below 1 or above the count.
func TestMaxBatchOptions(t *testing.T) {
	cases := []struct {
		name        string
		options     []Option
		wantEnqueue int
		wantDequeue int
	}{
		{name: "default", wantEnqueue: 16, wantDequeue: 16},
		{name: "both", options: []Option{WithMaxBatch(4)}, wantEnqueue: 4, wantDequeue: 4},
		{name: "enqueueOnly", options: []Option{WithMaxEnqueueBatch(8)}, wantEnqueue: 8,
			wantDequeue: 16},
		{name: "dequeueOnly", options: []Option{WithMaxDequeueBatch(2)}, wantEnqueue: 16,
			wantDequeue: 2},
		{name: "different", options: []Option{WithMaxEnqueueBatch(8), WithMaxDequeueBatch(2)},
			wantEnqueue: 8, wantDequeue: 2},
		{name: "sideAfterBoth", options: []Option{WithMaxBatch(4), WithMaxDequeueBatch(1)},
			wantEnqueue: 4, wantDequeue: 1},
		{name: "bothAfterSide", options: []Option{WithMaxEnqueueBatch(8), WithMaxBatch(4)},
			wantEnqueue: 4, wantDequeue: 4},
		{name: "full", options: []Option{WithMaxBatch(16)}, wantEnqueue: 16, wantDequeue: 16},
		{name: "invalidOverridden", options: []Option{WithMaxBatch(0), WithMaxBatch(4)},
			wantEnqueue: 4, wantDequeue: 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ring, err := New[int](16, tc.options...)
			require.NoError(t, err)
			assert.Equal(t, tc.wantEnqueue, ring.MaxEnqueueBatch())
			assert.Equal(t, tc.wantDequeue, ring.MaxDequeueBatch())
			assert.Equal(t, 16, ring.Capacity())
		})
	}

	for _, bad := range []int{0, -1, 17} {
		for name, option := range map[string]Option{
			"both":    WithMaxBatch(bad),
			"enqueue": WithMaxEnqueueBatch(bad),
			"dequeue": WithMaxDequeueBatch(bad),
		} {
			ring, err := New[int](16, option)
			assert.ErrorIs(t, err, ErrInvalidMaxBatch, "%s %d", name, bad)
			assert.True(t, ring == nil, "%s %d", name, bad)
		}
	}

	// New checks only the limit that applies, so a valid limit followed by an invalid one fails.
	ring, err := New[int](16, WithMaxEnqueueBatch(4), WithMaxBatch(17))
	assert.ErrorIs(t, err, ErrInvalidMaxBatch)
	assert.True(t, ring == nil)
}

// TestMaxEnqueueBatch checks that EnqueueBurst moves at most MaxEnqueueBatch, and that EnqueueBulk
// rejects more than MaxEnqueueBatch with ErrExceedsMaxBatch, leaving the ring unchanged, while a
// request over the capacity still reports ErrExceedsCapacity. The dequeue side has no limit here,
// so a burst drains everything.
func TestMaxEnqueueBatch(t *testing.T) {
	ring, err := New[int](16, WithMaxEnqueueBatch(4))
	require.NoError(t, err)

	enqueued, free := ring.EnqueueBurst([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	assert.Equal(t, 4, enqueued)
	assert.Equal(t, 12, free)

	free, err = ring.EnqueueBulk([]int{5, 6, 7, 8, 9})
	assert.ErrorIs(t, err, ErrExceedsMaxBatch)
	assert.NotErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, 12, free)
	assert.Equal(t, 4, ring.Count())

	free, err = ring.EnqueueBulk([]int{5, 6, 7, 8})
	assert.NoError(t, err)
	assert.Equal(t, 8, free)

	free, err = ring.EnqueueBulk(make([]int, ring.Capacity()+1))
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.NotErrorIs(t, err, ErrExceedsMaxBatch)
	assert.Equal(t, 8, free)

	got := make([]int, 16)
	dequeued, _ := ring.DequeueBurst(got)
	assert.Equal(t, 8, dequeued)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, got[:dequeued])
}

// TestMaxDequeueBatch checks that DequeueBurst moves at most MaxDequeueBatch, and that DequeueBulk
// rejects more than MaxDequeueBatch with ErrExceedsMaxBatch, leaving the ring unchanged. The
// enqueue side has no limit here, so one burst fills the ring.
func TestMaxDequeueBatch(t *testing.T) {
	ring, err := New[int](16, WithMaxDequeueBatch(4))
	require.NoError(t, err)
	items := make([]int, 12)
	for value := range items {
		items[value] = value
	}
	enqueued, _ := ring.EnqueueBurst(items)
	require.Equal(t, 12, enqueued)

	got := make([]int, 10)
	dequeued, available := ring.DequeueBurst(got)
	assert.Equal(t, 4, dequeued)
	assert.Equal(t, []int{0, 1, 2, 3}, got[:dequeued])
	assert.Equal(t, 8, available)

	// dstSentinel pre-fills dst so the test can show that a rejected call writes none of it.
	const dstSentinel = -1
	rejected := slices.Repeat([]int{dstSentinel}, 5)
	available, err = ring.DequeueBulk(rejected)
	assert.ErrorIs(t, err, ErrExceedsMaxBatch)
	assert.NotErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, slices.Repeat([]int{dstSentinel}, 5), rejected)
	assert.Equal(t, 8, available)
	assert.Equal(t, 8, ring.Count())

	available, err = ring.DequeueBulk(make([]int, 17))
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.NotErrorIs(t, err, ErrExceedsMaxBatch)
	assert.Equal(t, 8, available)

	got = make([]int, 4)
	available, err = ring.DequeueBulk(got)
	assert.NoError(t, err)
	assert.Equal(t, []int{4, 5, 6, 7}, got)
	assert.Equal(t, 4, available)
}

// TestMaxBatchUnderContention runs MPMC bursts on a ring with different enqueue and dequeue
// limits. The callers mix requests below and above each side's limit. No burst moves more than its
// side's limit, and every element arrives exactly once.
func TestMaxBatchUnderContention(t *testing.T) {
	const (
		maxEnqueue  = 3
		maxDequeue  = 5
		workers     = 4
		perProducer = 5000
		total       = workers * perProducer
	)
	requests := []int{1, 2, 8}
	ring, err := New[int](
		64,
		WithMultiProdEnqueue(),
		WithMultiConsDequeue(),
		WithMaxEnqueueBatch(maxEnqueue),
		WithMaxDequeueBatch(maxDequeue),
	)
	require.NoError(t, err)

	var producers, consumers sync.WaitGroup
	var received atomic.Int64
	var oversized atomic.Int64
	seen := make([]atomic.Int32, total)
	for producer := range workers {
		producers.Go(func() {
			items := make([]int, 0, slices.Max(requests))
			for call, next := 0, 0; next < perProducer; call++ {
				request := requests[call%len(requests)]
				items = items[:0]
				for k := next; k < min(next+request, perProducer); k++ {
					items = append(items, producer*perProducer+k)
				}
				enqueued, _ := ring.EnqueueBurst(items)
				if enqueued > maxEnqueue {
					oversized.Add(1)
				}
				if enqueued == 0 {
					runtime.Gosched()
				}
				next += enqueued
			}
		})
	}
	for range workers {
		consumers.Go(func() {
			got := make([]int, slices.Max(requests))
			for call := 0; received.Load() < total; call++ {
				request := requests[call%len(requests)]
				dequeued, _ := ring.DequeueBurst(got[:request])
				if dequeued > maxDequeue {
					oversized.Add(1)
				}
				if dequeued == 0 {
					runtime.Gosched()
					continue
				}
				for _, value := range got[:dequeued] {
					seen[value].Add(1)
				}
				received.Add(int64(dequeued))
			}
		})
	}
	producers.Wait()
	consumers.Wait()

	assert.Zero(t, oversized.Load())
	assert.Equal(t, int64(total), received.Load())
	for value := range seen {
		require.Equal(t, int32(1), seen[value].Load(), "value %d", value)
	}
}

// A stressMode is a ring configuration and the number of goroutines on each side of it.
type stressMode struct {
	name      string
	producers int
	consumers int
	options   []Option
}

// stressModes returns the configurations the tagged stress tests run under: one producer and one
// consumer on an SPSC ring, and four of each on an MPMC ring.
func stressModes() []stressMode {
	return []stressMode{
		{name: "SPSC", producers: 1, consumers: 1},
		{
			name:      "MPMC",
			producers: 4,
			consumers: 4,
			options:   []Option{WithMultiProdEnqueue(), WithMultiConsDequeue()},
		},
	}
}

// tagged returns the value a producer sends as its seq-th element. Sequence numbers start at 1,
// so a zero value is never a valid element.
func tagged(producer int, seq uint64) uint64 {
	return uint64(producer)<<32 | seq
}

// A batchCall moves up to len(values) values through a ring and returns how many it moved. A
// producer's call reads values, and a consumer's call writes the moved values into the front of
// values.
type batchCall func(values []uint64) int

// A drainState is what the consumers of one stress run share to decide when to stop. expected is
// the total number of elements the producers send, and the producers' owner sets producersDone
// once they have all finished.
type drainState struct {
	expected      int64
	producersDone atomic.Bool
	received      atomic.Int64
	overReceived  atomic.Bool
}

// consume calls dequeue until a call moves nothing after the producers have finished, and adds
// what each call moves to received. dequeue returns how many elements it moved and how many it
// asked for. A call that asks for none never ends the loop, because it moves nothing even when
// the ring holds elements. consume yields when a call moves nothing.
//
// consume also stops once received exceeds expected, which only a ring that fabricates or
// duplicates elements can cause, and then sets overReceived. Either way a ring that loses,
// fabricates, or duplicates elements ends the run instead of hanging it.
func (drain *drainState) consume(dequeue func() (moved, requested int)) {
	for {
		finished := drain.producersDone.Load()
		moved, requested := dequeue()
		if moved == 0 {
			if finished && requested > 0 {
				return
			}
			runtime.Gosched()
			continue
		}
		if drain.received.Add(int64(moved)) > drain.expected {
			drain.overReceived.Store(true)
			return
		}
	}
}

// stressDeadline calls run on a new goroutine and waits up to 30 seconds for it to return. run
// starts the goroutines of one stress run and waits for them. stressDeadline bounds that run, so
// a hang, including one inside the ring, fails with the test's name instead of waiting for the
// package timeout. On timeout it calls t.Fatalf, so it must be called on the test goroutine. The
// stuck goroutines are then abandoned, because the test is failing anyway.
func stressDeadline(t *testing.T, run func()) {
	t.Helper()
	const deadline = 30 * time.Second
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		run()
	}()
	select {
	case <-finished:
	case <-time.After(deadline):
		t.Fatalf("stress run did not finish within %v", deadline)
	}
}

// sendTagged starts one goroutine per producer that each send perProducer tagged values with
// enqueue, at most enqueueBatch at a time, and one goroutine per consumer that receive with
// dequeue into a buffer of dequeueBatch until drainState.consume stops them. Each producer yields
// when a call moves nothing. sendTagged waits for all of them within stressDeadline, asserts that
// the ring returned no more elements than were sent, and returns the values each consumer
// received, in the order it received them. It must be called on the test goroutine.
func sendTagged(
	t *testing.T,
	producers, consumers, perProducer, enqueueBatch, dequeueBatch int,
	enqueue, dequeue batchCall,
) [][]uint64 {
	t.Helper()
	drain := drainState{expected: int64(producers * perProducer)}
	results := make([][]uint64, consumers)
	stressDeadline(t, func() {
		var producing sync.WaitGroup
		for producer := range producers {
			producing.Go(func() {
				items := make([]uint64, 0, enqueueBatch)
				for next := 1; next <= perProducer; {
					items = items[:0]
					last := min(next+enqueueBatch-1, perProducer)
					for seq := next; seq <= last; seq++ {
						items = append(items, tagged(producer, uint64(seq)))
					}
					moved := enqueue(items)
					if moved == 0 {
						runtime.Gosched()
					}
					next += moved
				}
			})
		}

		var consuming sync.WaitGroup
		for consumer := range consumers {
			consuming.Go(func() {
				dst := make([]uint64, dequeueBatch)
				drain.consume(func() (moved, requested int) {
					moved = dequeue(dst)
					results[consumer] = append(results[consumer], dst[:moved]...)
					return moved, len(dst)
				})
			})
		}
		producing.Wait()
		drain.producersDone.Store(true)
		consuming.Wait()
	})
	assert.False(t, drain.overReceived.Load(), "the ring returned more elements than were sent")
	return results
}

// assertTaggedDelivery checks what sendTagged returned. Every value is a valid tag, within each
// consumer each producer's sequence numbers strictly increase, and across all consumers every
// value arrives exactly once.
func assertTaggedDelivery(t *testing.T, results [][]uint64, producers, perProducer int) {
	t.Helper()
	timesSeen := make([]int, producers*perProducer)
	for consumer, values := range results {
		lastSeq := make([]uint64, producers)
		for _, value := range values {
			seq := value & math.MaxUint32
			validTag := value>>32 < uint64(producers) && seq >= 1 && seq <= uint64(perProducer)
			require.Truef(t, validTag, "consumer %d received invalid value %#x", consumer, value)
			producer := int(value >> 32)
			require.Greaterf(
				t,
				seq,
				lastSeq[producer],
				"consumer %d received producer %d out of order",
				consumer, producer,
			)
			lastSeq[producer] = seq
			timesSeen[producer*perProducer+int(seq)-1]++
		}
	}
	for index, times := range timesSeen {
		producer := index / perProducer
		seq := index%perProducer + 1
		require.Equalf(t, 1, times, "producer %d sequence %d", producer, seq)
	}
}

// TestCapacityOne moves tagged values through a one-slot ring with single-element calls, so every
// element is handed over on its own.
func TestCapacityOne(t *testing.T) {
	const total = 100_000
	for _, mode := range stressModes() {
		t.Run(mode.name, func(t *testing.T) {
			ring, err := New[uint64](1, mode.options...)
			require.NoError(t, err)
			var unexpectedErrors atomic.Int64
			enqueue := func(items []uint64) int {
				err := ring.Enqueue(items[0])
				if err != nil {
					if err != ErrFull {
						unexpectedErrors.Add(1)
					}
					return 0
				}
				return 1
			}
			dequeue := func(dst []uint64) int {
				value, err := ring.Dequeue()
				if err != nil {
					if err != ErrEmpty {
						unexpectedErrors.Add(1)
					}
					return 0
				}
				dst[0] = value
				return 1
			}

			perProducer := total / mode.producers
			results := sendTagged(t, mode.producers, mode.consumers, perProducer, 1, 1, enqueue, dequeue)
			assert.Zero(t, unexpectedErrors.Load(), "unexpected errors")
			assertTaggedDelivery(t, results, mode.producers, perProducer)
		})
	}
}

// TestConcurrentWrap moves tagged values in bursts through a ring whose indices start just below
// 2^32, so they wrap while the producers and consumers contend.
func TestConcurrentWrap(t *testing.T) {
	const (
		total = 100_000
		batch = 8
	)
	for _, mode := range stressModes() {
		t.Run(mode.name, func(t *testing.T) {
			ring, err := New[uint64](64, mode.options...)
			require.NoError(t, err)
			const startIndex = math.MaxUint32 - 999
			setIndices(ring, startIndex)
			enqueue := func(items []uint64) int {
				enqueued, _ := ring.EnqueueBurst(items)
				return enqueued
			}
			dequeue := func(dst []uint64) int {
				dequeued, _ := ring.DequeueBurst(dst)
				return dequeued
			}

			perProducer := total / mode.producers
			results := sendTagged(
				t,
				mode.producers,
				mode.consumers,
				perProducer,
				batch,
				batch,
				enqueue,
				dequeue,
			)
			assertTaggedDelivery(t, results, mode.producers, perProducer)
			assert.Less(t, atomic.LoadUint32(ring.consTail), uint32(startIndex))
		})
	}
}

// TestCountSnapshot checks that Count never reports more entries than have been enqueued and not
// yet dequeued. enqueuedCount counts elements enqueued or in an enqueue call, and drain.received
// counts elements whose dequeue call has returned. A checker reads drain.received, then Count,
// then enqueuedCount. With the consumer tail loaded first, Count is at most their difference.
//
// This test checks correctness. It is not a guaranteed guard for Count's load order, because the
// reversed order breaks the bound only in a narrow window, which the test may miss on some
// machines or schedules.
func TestCountSnapshot(t *testing.T) {
	const (
		workers     = 4
		perProducer = 50_000
		total       = workers * perProducer
		batch       = 8
		checkers    = 2
		yieldEvery  = 64
	)
	ring, err := New[int](64, WithMultiProdEnqueue(), WithMultiConsDequeue())
	require.NoError(t, err)

	var enqueuedCount, overcounts atomic.Int64
	drain := drainState{expected: total}
	stressDeadline(t, func() {
		var producing, consuming, checking sync.WaitGroup
		for range workers {
			producing.Go(func() {
				items := make([]int, batch)
				for sent := 0; sent < perProducer; {
					request := min(batch, perProducer-sent)
					enqueuedCount.Add(int64(request))
					enqueued, _ := ring.EnqueueBurst(items[:request])
					enqueuedCount.Add(int64(enqueued - request))
					if enqueued == 0 {
						runtime.Gosched()
					}
					sent += enqueued
				}
			})
		}
		for range workers {
			consuming.Go(func() {
				dst := make([]int, batch)
				drain.consume(func() (moved, requested int) {
					dequeued, _ := ring.DequeueBurst(dst)
					return dequeued, len(dst)
				})
			})
		}

		// Each checker yields every yieldEvery checks, so the checkers sample densely without
		// starving the producers and consumers when they share one processor.
		var done atomic.Bool
		for range checkers {
			checking.Go(func() {
				for check := 1; !done.Load(); check++ {
					dequeued := drain.received.Load()
					count := ring.Count()
					enqueued := enqueuedCount.Load()
					if int64(count) > enqueued-dequeued {
						overcounts.Add(1)
					}
					if check%yieldEvery == 0 {
						runtime.Gosched()
					}
				}
			})
		}
		producing.Wait()
		drain.producersDone.Store(true)
		consuming.Wait()
		done.Store(true)
		checking.Wait()
	})

	assert.False(t, drain.overReceived.Load(), "the ring returned more elements than were sent")
	assert.Equal(t, int64(total), drain.received.Load())
	assert.Zero(t, overcounts.Load(), "Count exceeded enqueued minus dequeued")
}

// TestBulkUnderContention moves tagged values with bulk calls on an MPMC ring. Every successful
// DequeueBulk fills all of dst with valid values, and a failed one writes nothing.
func TestBulkUnderContention(t *testing.T) {
	const (
		workers      = 4
		perProducer  = 4000
		total        = workers * perProducer
		enqueueBatch = 3
		dequeueBatch = 8
	)
	// A bulk dequeue moves all of dst or nothing, so the last one completes only if total is a
	// multiple of dequeueBatch. This guard fails to build when it is not.
	const _ = uint(-(total % dequeueBatch))
	ring, err := New[uint64](64, WithMultiProdEnqueue(), WithMultiConsDequeue())
	require.NoError(t, err)
	var unexpectedErrors atomic.Int64
	enqueue := func(items []uint64) int {
		_, err := ring.EnqueueBulk(items)
		if err != nil {
			if err != ErrFull {
				unexpectedErrors.Add(1)
			}
			return 0
		}
		return len(items)
	}
	// dst is cleared before each call, so a stale value cannot pass for a new one, and a failed
	// call that wrote anyway leaves non-zero values. Those are returned as received, so the
	// delivery check reports them.
	var failedWrites atomic.Int64
	dequeue := func(dst []uint64) int {
		clear(dst)
		_, err := ring.DequeueBulk(dst)
		if err == nil {
			return len(dst)
		}
		if err != ErrEmpty {
			unexpectedErrors.Add(1)
		}
		written := slices.Index(dst, 0)
		if written == -1 {
			written = len(dst)
		}
		if written > 0 {
			failedWrites.Add(1)
		}
		return written
	}

	results := sendTagged(
		t,
		workers,
		workers,
		perProducer,
		enqueueBatch,
		dequeueBatch,
		enqueue,
		dequeue,
	)
	assert.Zero(t, unexpectedErrors.Load(), "unexpected errors")
	assertTaggedDelivery(t, results, workers, perProducer)
	assert.Zero(t, failedWrites.Load(), "a failed DequeueBulk wrote into dst")
}

// TestCountBounds checks that every free and available count an MPMC burst returns is between 0
// and the capacity. Every fourth call on each side is zero-length, because a call that reserves
// nothing reports a count from a head that may be stale.
//
// A zero-length enqueue overshoots only when consumers pass the producer head it loaded before it
// loads the consumer tail, so it needs other producers and consumers running while the ring is
// nearly empty. A zero-length dequeue overshoots only when producers advance their tail more than
// a capacity past the consumer head it loaded, so it needs other consumers and producers running
// while the ring is nearly full. Single-threaded tests can reach neither.
func TestCountBounds(t *testing.T) {
	const (
		workers      = 4
		perProducer  = 25_000
		total        = workers * perProducer
		enqueueBatch = 8
		dequeueBatch = 4
		zeroEvery    = 4
		probers      = 3
		yieldEvery   = 64
	)
	ring, err := New[int](64, WithMultiProdEnqueue(), WithMultiConsDequeue())
	require.NoError(t, err)
	capacity := ring.Capacity()
	inBounds := func(count int) bool {
		return count >= 0 && count <= capacity
	}

	var freeOutOfBounds, availableOutOfBounds atomic.Int64
	drain := drainState{expected: total}
	stressDeadline(t, func() {
		var producing, consuming, probing sync.WaitGroup
		for range workers {
			producing.Go(func() {
				items := make([]int, enqueueBatch)
				for call, sent := 0, 0; sent < perProducer; call++ {
					request := min(enqueueBatch, perProducer-sent)
					if call%zeroEvery == zeroEvery-1 {
						request = 0
					}
					enqueued, free := ring.EnqueueBurst(items[:request])
					if !inBounds(free) {
						freeOutOfBounds.Add(1)
					}
					if enqueued == 0 {
						runtime.Gosched()
					}
					sent += enqueued
				}
			})
		}
		for range workers {
			consuming.Go(func() {
				dst := make([]int, dequeueBatch)
				call := 0
				drain.consume(func() (moved, requested int) {
					request := dequeueBatch
					if call%zeroEvery == zeroEvery-1 {
						request = 0
					}
					call++
					dequeued, available := ring.DequeueBurst(dst[:request])
					if !inBounds(available) {
						availableOutOfBounds.Add(1)
					}
					return dequeued, request
				})
			})
		}
		var done atomic.Bool
		for range probers {
			probing.Go(func() {
				for call := 1; !done.Load(); call++ {
					_, free := ring.EnqueueBurst(nil)
					if !inBounds(free) {
						freeOutOfBounds.Add(1)
					}
					_, available := ring.DequeueBurst(nil)
					if !inBounds(available) {
						availableOutOfBounds.Add(1)
					}
					if call%yieldEvery == 0 {
						runtime.Gosched()
					}
				}
			})
		}
		producing.Wait()
		drain.producersDone.Store(true)
		consuming.Wait()
		done.Store(true)
		probing.Wait()
	})

	assert.False(t, drain.overReceived.Load(), "the ring returned more elements than were sent")
	assert.Equal(t, int64(total), drain.received.Load())
	assert.Zero(t, freeOutOfBounds.Load(), "free out of bounds")
	assert.Zero(t, availableOutOfBounds.Load(), "available out of bounds")
}

// TestErrorIdentity checks that every call returns ErrFull and ErrEmpty unwrapped, so they compare
// equal with ==, and that each bulk call's ErrExceedsCapacity error matches with errors.Is.
func TestErrorIdentity(t *testing.T) {
	ring, err := New[int](4)
	require.NoError(t, err)

	_, err = ring.Dequeue()
	assert.True(t, err == ErrEmpty, "Dequeue got %v", err)
	_, err = ring.DequeueBulk(make([]int, 1))
	assert.True(t, err == ErrEmpty, "DequeueBulk got %v", err)

	requireEnqueueBulk(t, ring, []int{1, 2, 3, 4})
	err = ring.Enqueue(5)
	assert.True(t, err == ErrFull, "Enqueue got %v", err)
	_, err = ring.EnqueueBulk([]int{5})
	assert.True(t, err == ErrFull, "EnqueueBulk got %v", err)

	_, err = ring.EnqueueBulk(make([]int, 5))
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	_, err = ring.DequeueBulk(make([]int, 5))
	assert.ErrorIs(t, err, ErrExceedsCapacity)
}
