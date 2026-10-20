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

	buffer, err := New[string](
		32, WithMultiConsDequeue(),
	)
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
	buffer, err := New[string](
		32, WithMultiProdEnqueue(),
	)
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

// outSentinel pre-fills an out-parameter so a test can tell that the call overwrote it.
const outSentinel uint32 = 0xDEADBEEF

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

	elems, err := ring.DequeueBulk(8, nil)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, elems)
}

func TestDequeueBulk(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2, 3, 4, 5})

	avail := outSentinel
	elems, err := ring.DequeueBulk(6, &avail)
	assert.True(t, err == ErrEmpty, "got %v", err)
	assert.Nil(t, elems)
	assert.Equal(t, uint32(5), avail)
	assert.Equal(t, 5, ring.Count())

	avail = outSentinel
	elems, err = ring.DequeueBulk(3, &avail)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3}, elems)
	assert.Equal(t, uint32(2), avail)

	avail = outSentinel
	elems, err = ring.DequeueBulk(2, &avail)
	require.NoError(t, err)
	assert.Equal(t, []int{4, 5}, elems)
	assert.Equal(t, uint32(0), avail)

	avail = outSentinel
	elems, err = ring.DequeueBulk(1, &avail)
	assert.True(t, err == ErrEmpty, "got %v", err)
	assert.Nil(t, elems)
	assert.Equal(t, uint32(0), avail)
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

	elems, err := ring.DequeueBulk(8, nil)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, elems)
}

func TestDequeueBurst(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2, 3})

	avail := outSentinel
	elems, numDequeued := ring.DequeueBurst(5, &avail)
	assert.Equal(t, uint32(3), numDequeued)
	assert.Equal(t, []int{1, 2, 3}, elems)
	assert.Equal(t, uint32(0), avail)

	avail = outSentinel
	elems, numDequeued = ring.DequeueBurst(2, &avail)
	assert.Equal(t, uint32(0), numDequeued)
	assert.Nil(t, elems)
	assert.Equal(t, uint32(0), avail)
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
	elems, err := ring.DequeueBulk(5, nil)
	require.NoError(t, err)
	assert.Equal(t, batch, elems)

	advanceRing(t, ring, 3)
	require.Equal(t, uint32(6), atomic.LoadUint32(ring.prodHead)&ring.mask)
	enqueued, _ := ring.EnqueueBurst(batch)
	assert.Equal(t, 5, enqueued)
	elems, numDequeued := ring.DequeueBurst(5, nil)
	assert.Equal(t, uint32(5), numDequeued)
	assert.Equal(t, batch, elems)
	assert.True(t, ring.Empty())
}

func TestBatchExceedsCapacity(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2})
	batch := []int{3, 4, 5, 6, 7, 8, 9, 10, 11}
	numElems := uint32(ring.Capacity()) + 1

	free, err := ring.EnqueueBulk(batch)
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, 6, free)
	assert.Equal(t, 2, ring.Count())

	avail := outSentinel
	elems, err := ring.DequeueBulk(numElems, &avail)
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Nil(t, elems)
	assert.Equal(t, uint32(2), avail)
	assert.Equal(t, 2, ring.Count())

	enqueued, free := ring.EnqueueBurst(batch)
	assert.Equal(t, 6, enqueued)
	assert.Equal(t, 0, free)

	avail = outSentinel
	elems, numDequeued := ring.DequeueBurst(numElems, &avail)
	assert.Equal(t, uint32(8), numDequeued)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, elems)
	assert.Equal(t, uint32(0), avail)
}

func TestBulkCheckOrder(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	numElems := uint32(ring.Capacity()) + 1

	// An oversized count is reported before an empty ring.
	_, err = ring.DequeueBulk(numElems, nil)
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.NotErrorIs(t, err, ErrEmpty)

	// An oversized count is reported before a full ring.
	requireEnqueueBulk(t, ring, []int{1, 2, 3, 4, 5, 6, 7, 8})
	require.True(t, ring.Full())
	_, err = ring.EnqueueBulk(make([]int, numElems))
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

func TestDequeueZeroCount(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	requireEnqueueBulk(t, ring, []int{1, 2})

	avail := outSentinel
	elems, err := ring.DequeueBulk(0, &avail)
	assert.NoError(t, err)
	assert.Nil(t, elems)
	assert.Equal(t, uint32(2), avail)

	avail = outSentinel
	elems, numDequeued := ring.DequeueBurst(0, &avail)
	assert.Equal(t, uint32(0), numDequeued)
	assert.Nil(t, elems)
	assert.Equal(t, uint32(2), avail)
	assert.Equal(t, 2, ring.Count())
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
			avail := outSentinel
			_, err = ring.DequeueBulk(9, &avail)
			assert.ErrorIs(t, err, ErrExceedsCapacity)
			assert.Equal(t, uint32(4), avail)
			assertOccupancy(t, ring, 4)
			elems, err := ring.DequeueBulk(1, nil)
			require.NoError(t, err)
			assert.Equal(t, []int{2}, elems)
			assertOccupancy(t, ring, 3)

			enqueued, _ := ring.EnqueueBurst([]int{6, 7, 8, 9, 10, 11})
			assert.Equal(t, 5, enqueued)
			assertOccupancy(t, ring, 8)
			elems, numDequeued := ring.DequeueBurst(9, nil)
			assert.Equal(t, uint32(8), numDequeued)
			assert.Equal(t, []int{3, 4, 5, 6, 7, 8, 9, 10}, elems)
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
