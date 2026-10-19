package ring

import (
	"errors"
	"fmt"
	"math"
	"runtime"
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
	buffer, err := New[string](uint32(buflen))
	require.NotNil(t, buffer)
	require.NoError(t, err)

	assert.Equal(t, uint32(0), buffer.Count())

	item := "test"
	for i := 0; i < buflen-1; i++ {
		err = buffer.Enqueue(item)
		require.NoError(t, err)
		assert.Equal(t, uint32(i+1), buffer.Count())
	}

	// Single-element calls return their sentinels unwrapped, so they are compared with ==.
	err = buffer.Enqueue(item)
	assert.True(t, err == ErrFull, "got %v", err)

	assert.Equal(t, uint32(buflen-1), buffer.Count())

	for i := 0; i < buflen-1; i++ {
		_, err = buffer.Dequeue()
		require.NoError(t, err)
		assert.Equal(t, uint32(buflen-2-i), buffer.Count())
	}

	_, err = buffer.Dequeue()
	assert.True(t, err == ErrEmpty, "got %v", err)
}

func TestRingReset(t *testing.T) {
	buflen := 32
	buffer, err := New[string](uint32(buflen))
	require.NotNil(t, buffer)
	require.NoError(t, err)

	// The other occupancy functions are checked along the way, so they need no tests of their own.
	assert.True(t, buffer.Empty())
	assert.False(t, buffer.Full())
	assert.Equal(t, uint32(buflen-1), buffer.Free())
	assert.Equal(t, uint32(0), buffer.Count())

	item := "test"
	for i := 0; i < buflen-1; i++ {
		err = buffer.Enqueue(item)
		require.NoError(t, err)

		assert.False(t, buffer.Empty())
		if i == buflen-2 {
			assert.True(t, buffer.Full())
		} else {
			assert.False(t, buffer.Full())
		}

		assert.Equal(t, uint32(buflen-2-i), buffer.Free())
		assert.Equal(t, uint32(i+1), buffer.Count())
	}

	assert.Equal(t, uint32(buflen-1), buffer.Count())
	assert.Equal(t, uint32(0), buffer.Free())

	buffer.Reset()

	assert.True(t, buffer.Empty())
	assert.False(t, buffer.Full())
	assert.Equal(t, uint32(buflen-1), buffer.Free())
	assert.Equal(t, uint32(0), buffer.Count())

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

	for i := 0; i < 31; i++ {
		err = buffer.Enqueue(item)
		assert.NoError(t, err)
	}
	err = buffer.Enqueue(item)
	assert.ErrorIs(t, err, ErrFull)

	for i := 0; i < 31; i++ {
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

	for i := 0; i < 31; i++ {
		item := fmt.Sprintf("test-%d", i)
		err = buffer.Enqueue(item)
		require.NoError(t, err)
	}

	numDequeues := []int{8, 8, 8, 7}
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
	numEnqueues := []int{8, 8, 8, 7}
	testStrings := []string{}
	for i := 0; i < 31; i++ {
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
	for i := 0; i < 31; i++ {
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
	buffer, err := New[uint32](uint32(buflen))
	require.NoError(t, err)
	require.NotNil(t, buffer)

	for i := 0; i < buflen-1; i++ {
		err = buffer.Enqueue(uint32(i))
		require.NoError(t, err)
	}

	assert.True(t, buffer.Full())

	err = buffer.Enqueue(uint32(buflen - 1))
	assert.ErrorIs(t, err, ErrFull)

	_, err = buffer.Dequeue()
	require.NoError(t, err)
	assert.False(t, buffer.Full())

	err = buffer.Enqueue(uint32(buflen - 1))
	assert.NoError(t, err)
	assert.True(t, buffer.Full())
}

func TestRingBufferMultipleWraparounds(t *testing.T) {
	buflen := 8
	buffer, err := New[uint32](uint32(buflen))
	require.NoError(t, err)
	require.NotNil(t, buffer)

	numIterations := 5
	for i := 0; i < numIterations; i++ {
		for j := 0; j < buflen-1; j++ {
			err = buffer.Enqueue(uint32(i*buflen + j))
			require.NoError(t, err)
		}

		assert.False(t, buffer.Empty())
		assert.True(t, buffer.Full())
		assert.Equal(t, uint32(0), buffer.Free())
		assert.Equal(t, uint32(buflen-1), buffer.Count())

		for j := 0; j < buflen-1; j++ {
			item, err := buffer.Dequeue()
			require.NoError(t, err)
			assert.Equal(t, i*buflen+j, int(item))
		}

		assert.True(t, buffer.Empty())
		assert.False(t, buffer.Full())
		assert.Equal(t, uint32(buflen-1), buffer.Free())
		assert.Equal(t, uint32(0), buffer.Count())
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
	for _, count := range []uint32{2, 4, 8, 1024, 1 << 16} {
		ring, err := New[int](count)
		require.NoError(t, err)
		assert.Equal(t, count-1, ring.Capacity(), "count %d", count)
	}
}

func TestNewRejectsInvalidCount(t *testing.T) {
	for _, count := range []uint32{0, 1, 3, 6, 13} {
		ring, err := New[int](count)
		assert.ErrorIs(t, err, ErrInvalidCount, "count %d", count)
		assert.Nil(t, ring, "count %d", count)
	}
}

func TestEnqueueBulk(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)

	freeSpace := outSentinel
	err = ring.EnqueueBulk([]int{1, 2, 3, 4, 5}, 5, &freeSpace)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), freeSpace)
	assert.Equal(t, uint32(5), ring.Count())

	freeSpace = outSentinel
	err = ring.EnqueueBulk([]int{6, 7, 8}, 3, &freeSpace)
	assert.True(t, err == ErrFull, "got %v", err)
	assert.Equal(t, uint32(2), freeSpace)
	assert.Equal(t, uint32(5), ring.Count())

	freeSpace = outSentinel
	err = ring.EnqueueBulk([]int{6, 7}, 2, &freeSpace)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), freeSpace)

	elems, err := ring.DequeueBulk(7, nil)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7}, elems)
}

func TestDequeueBulk(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	require.NoError(t, ring.EnqueueBulk([]int{1, 2, 3, 4, 5}, 5, nil))

	avail := outSentinel
	elems, err := ring.DequeueBulk(6, &avail)
	assert.True(t, err == ErrEmpty, "got %v", err)
	assert.Nil(t, elems)
	assert.Equal(t, uint32(5), avail)
	assert.Equal(t, uint32(5), ring.Count())

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
	require.NoError(t, ring.EnqueueBulk([]int{1, 2, 3, 4, 5}, 5, nil))

	freeSpace := outSentinel
	numEnqueued := ring.EnqueueBurst([]int{6, 7, 8, 9}, 4, &freeSpace)
	assert.Equal(t, uint32(2), numEnqueued)
	assert.Equal(t, uint32(0), freeSpace)
	assert.Equal(t, uint32(7), ring.Count())

	freeSpace = outSentinel
	numEnqueued = ring.EnqueueBurst([]int{10}, 1, &freeSpace)
	assert.Equal(t, uint32(0), numEnqueued)
	assert.Equal(t, uint32(0), freeSpace)

	elems, err := ring.DequeueBulk(7, nil)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7}, elems)
}

func TestDequeueBurst(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	require.NoError(t, ring.EnqueueBulk([]int{1, 2, 3}, 3, nil))

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
	require.NoError(t, ring.EnqueueBulk(batch, 5, nil))
	elems, err := ring.DequeueBulk(5, nil)
	require.NoError(t, err)
	assert.Equal(t, batch, elems)

	advanceRing(t, ring, 3)
	require.Equal(t, uint32(6), atomic.LoadUint32(ring.prodHead)&ring.mask)
	assert.Equal(t, uint32(5), ring.EnqueueBurst(batch, 5, nil))
	elems, numDequeued := ring.DequeueBurst(5, nil)
	assert.Equal(t, uint32(5), numDequeued)
	assert.Equal(t, batch, elems)
	assert.True(t, ring.Empty())
}

func TestEnqueueBulkShortBatch(t *testing.T) {
	batch := []int{1, 2, 3}
	for _, numElems := range []uint32{uint32(len(batch)) + 1, math.MaxUint32} {
		ring, err := New[int](8)
		require.NoError(t, err)
		require.NoError(t, ring.EnqueueBulk([]int{10, 11}, 2, nil))

		freeSpace := outSentinel
		err = ring.EnqueueBulk(batch, numElems, &freeSpace)
		assert.ErrorIs(t, err, ErrShortBatch, "numElems %d", numElems)
		assert.Equal(t, uint32(5), freeSpace, "numElems %d", numElems)
		assert.Equal(t, uint32(2), ring.Count(), "numElems %d", numElems)
		assertNoStrandedReservation(t, ring, numElems)
	}
}

func TestEnqueueBurstShortBatchPanics(t *testing.T) {
	batch := []int{1, 2, 3}
	for _, numElems := range []uint32{uint32(len(batch)) + 1, math.MaxUint32} {
		ring, err := New[int](8)
		require.NoError(t, err)
		require.NoError(t, ring.EnqueueBulk([]int{10, 11}, 2, nil))

		freeSpace := outSentinel
		assert.Panics(t, func() {
			ring.EnqueueBurst(batch, numElems, &freeSpace)
		}, "numElems %d", numElems)
		assert.Equal(t, outSentinel, freeSpace, "numElems %d", numElems)
		assert.Equal(t, uint32(2), ring.Count(), "numElems %d", numElems)
		assertNoStrandedReservation(t, ring, numElems)
	}
}

// assertNoStrandedReservation checks that a rejected enqueue left no producer slots reserved. It
// expects the ring to hold exactly 10 and 11. A stranded reservation would leave prodHead ahead
// of prodTail, and the next enqueue would then publish the unwritten slots along with its own.
func assertNoStrandedReservation(t *testing.T, ring *Ring[int], numElems uint32) {
	t.Helper()
	assert.Equal(t, atomic.LoadUint32(ring.prodTail), atomic.LoadUint32(ring.prodHead),
		"numElems %d", numElems)

	require.NoError(t, ring.Enqueue(12), "numElems %d", numElems)
	elems, err := ring.DequeueBulk(ring.Count(), nil)
	require.NoError(t, err, "numElems %d", numElems)
	assert.Equal(t, []int{10, 11, 12}, elems, "numElems %d", numElems)
}

func TestBatchExceedsCapacity(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	require.NoError(t, ring.EnqueueBulk([]int{1, 2}, 2, nil))
	batch := []int{3, 4, 5, 6, 7, 8, 9, 10}
	numElems := ring.Capacity() + 1

	freeSpace := outSentinel
	err = ring.EnqueueBulk(batch, numElems, &freeSpace)
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Equal(t, uint32(5), freeSpace)
	assert.Equal(t, uint32(2), ring.Count())

	avail := outSentinel
	elems, err := ring.DequeueBulk(numElems, &avail)
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.Nil(t, elems)
	assert.Equal(t, uint32(2), avail)
	assert.Equal(t, uint32(2), ring.Count())

	freeSpace = outSentinel
	numEnqueued := ring.EnqueueBurst(batch, numElems, &freeSpace)
	assert.Equal(t, uint32(5), numEnqueued)
	assert.Equal(t, uint32(0), freeSpace)

	avail = outSentinel
	elems, numDequeued := ring.DequeueBurst(numElems, &avail)
	assert.Equal(t, uint32(7), numDequeued)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7}, elems)
	assert.Equal(t, uint32(0), avail)
}

func TestBulkCheckOrder(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	numElems := ring.Capacity() + 1

	// A short batch is reported before an oversized count.
	err = ring.EnqueueBulk([]int{1, 2, 3}, numElems, nil)
	assert.ErrorIs(t, err, ErrShortBatch)
	assert.NotErrorIs(t, err, ErrExceedsCapacity)

	// An oversized count is reported before an empty ring.
	_, err = ring.DequeueBulk(numElems, nil)
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.NotErrorIs(t, err, ErrEmpty)

	// An oversized count is reported before a full ring.
	require.NoError(t, ring.EnqueueBulk([]int{1, 2, 3, 4, 5, 6, 7}, 7, nil))
	require.True(t, ring.Full())
	err = ring.EnqueueBulk(make([]int, numElems), numElems, nil)
	assert.ErrorIs(t, err, ErrExceedsCapacity)
	assert.NotErrorIs(t, err, ErrFull)
}

func TestDequeueZeroCount(t *testing.T) {
	ring, err := New[int](8)
	require.NoError(t, err)
	require.NoError(t, ring.EnqueueBulk([]int{1, 2}, 2, nil))

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
	assert.Equal(t, uint32(2), ring.Count())
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
			for _, index := range []*uint32{
				ring.prodHead, ring.prodTail, ring.consHead, ring.consTail,
			} {
				atomic.StoreUint32(index, startIndex)
			}
			assertOccupancy(t, ring, 0)

			require.NoError(t, ring.Enqueue(1))
			require.NoError(t, ring.Enqueue(2))
			assertOccupancy(t, ring, 2)
			item, err := ring.Dequeue()
			require.NoError(t, err)
			assert.Equal(t, 1, item)
			assertOccupancy(t, ring, 1)

			require.NoError(t, ring.EnqueueBulk([]int{3, 4, 5}, 3, nil))
			require.Less(t, atomic.LoadUint32(ring.prodTail), uint32(startIndex))
			assertOccupancy(t, ring, 4)

			// Only the producer indices have wrapped here, so the occupancy reported by a
			// rejected call comes from indices on opposite sides of 2^32.
			freeSpace := outSentinel
			err = ring.EnqueueBulk(make([]int, 8), 8, &freeSpace)
			assert.ErrorIs(t, err, ErrExceedsCapacity)
			assert.Equal(t, uint32(3), freeSpace)
			avail := outSentinel
			_, err = ring.DequeueBulk(8, &avail)
			assert.ErrorIs(t, err, ErrExceedsCapacity)
			assert.Equal(t, uint32(4), avail)
			assertOccupancy(t, ring, 4)
			elems, err := ring.DequeueBulk(1, nil)
			require.NoError(t, err)
			assert.Equal(t, []int{2}, elems)
			assertOccupancy(t, ring, 3)

			numEnqueued := ring.EnqueueBurst([]int{6, 7, 8, 9, 10}, 5, nil)
			assert.Equal(t, uint32(4), numEnqueued)
			assertOccupancy(t, ring, 7)
			elems, numDequeued := ring.DequeueBurst(8, nil)
			assert.Equal(t, uint32(7), numDequeued)
			assert.Equal(t, []int{3, 4, 5, 6, 7, 8, 9}, elems)
			require.Less(t, atomic.LoadUint32(ring.consTail), uint32(startIndex))
			assertOccupancy(t, ring, 0)
		})
	}
}

// assertOccupancy checks that Count, Free, and Empty agree with the expected count.
func assertOccupancy(t *testing.T, ring *Ring[int], count uint32) {
	t.Helper()
	assert.Equal(t, count, ring.Count())
	assert.Equal(t, ring.Capacity()-count, ring.Free())
	assert.Equal(t, count == 0, ring.Empty())
}

func BenchmarkSPSCRingBufferSeq(b *testing.B) {
	buflen := 1024
	// The count is a valid power of two, so New cannot fail.
	buffer, _ := New[string](uint32(buflen))

	item := "test"
	b.SetBytes(int64(len(item)))
	b.ResetTimer()

	// The benchmark measures call cost including failed calls, so the error is not checked.
	for i := 0; i < b.N; i++ {
		_ = buffer.Enqueue(item)
		_, _ = buffer.Dequeue()
	}
}

func BenchmarkSPSCRingBufferSeqBatch(b *testing.B) {
	buflen := 1024
	// The count is a valid power of two, so New cannot fail.
	buffer, _ := New[string](uint32(buflen))

	item := "test"
	batchSize := 10
	b.SetBytes(int64(len(item) * batchSize))
	b.ResetTimer()

	// The benchmark measures call cost including failed calls, so the error is not checked.
	for i := 0; i < b.N; i++ {
		for j := 0; j < batchSize; j++ {
			_ = buffer.Enqueue(item)
		}
		for j := 0; j < batchSize; j++ {
			_, _ = buffer.Dequeue()
		}
	}
}

func BenchmarkSPSCRingBufferContention(b *testing.B) {
	buflen := 1024
	// The count is a valid power of two, so New cannot fail.
	buffer, _ := New[string](uint32(buflen))

	item := "test"
	b.SetBytes(int64(len(item) * 2))
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		// The benchmark measures call cost including failed calls, so the error is not checked.
		for i := 0; i < b.N; i++ {
			_ = buffer.Enqueue(item)
		}
	}()
	go func() {
		defer wg.Done()
		// The benchmark measures call cost including failed calls, so the error is not checked.
		for i := 0; i < b.N; i++ {
			_, _ = buffer.Dequeue()
		}
	}()
	wg.Wait()
}

func BenchmarkSPMCRingBufferSeq(b *testing.B) {
	buflen := 1024
	// The count is a valid power of two, so New cannot fail.
	buffer, _ := New[string](
		uint32(buflen),
		WithMultiConsDequeue(),
	)

	item := "test"
	b.SetBytes(int64(len(item)))
	b.ResetTimer()

	// The benchmark measures call cost including failed calls, so the error is not checked.
	for i := 0; i < b.N; i++ {
		_ = buffer.Enqueue(item)
		_, _ = buffer.Dequeue()
	}
}

func BenchmarkSPMCRingBufferParallel(b *testing.B) {
	buflen := 1024
	// The count is a valid power of two, so New cannot fail.
	buffer, _ := New[string](
		uint32(buflen),
		WithMultiConsDequeue(),
	)

	numConsumers := 4
	item := "test"
	b.SetBytes(int64(len(item) * numConsumers))
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	for i := 0; i < numConsumers; i++ {
		go func() {
			defer wg.Done()
			// The benchmark measures call cost including failed calls, so the error is not checked.
			for j := 0; j < b.N; j++ {
				_, _ = buffer.Dequeue()
			}
		}()
	}

	// The benchmark measures call cost including failed calls, so the error is not checked.
	for i := 0; i < b.N*numConsumers; i++ {
		_ = buffer.Enqueue(item)
	}
	wg.Wait()
}

func BenchmarkMPSCRingBufferSeq(b *testing.B) {
	buflen := 1024
	// The count is a valid power of two, so New cannot fail.
	buffer, _ := New[string](
		uint32(buflen),
		WithMultiProdEnqueue(),
	)

	item := "test"
	b.SetBytes(int64(len(item)))
	b.ResetTimer()

	// The benchmark measures call cost including failed calls, so the error is not checked.
	for i := 0; i < b.N; i++ {
		_ = buffer.Enqueue(item)
		_, _ = buffer.Dequeue()
	}
}

func BenchmarkMPSCRingBufferParallel(b *testing.B) {
	buflen := 1024
	// The count is a valid power of two, so New cannot fail.
	buffer, _ := New[string](
		uint32(buflen),
		WithMultiProdEnqueue(),
	)

	numProducers := 4
	item := "test"
	b.SetBytes(int64(len(item) * numProducers))
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(numProducers)

	for i := 0; i < numProducers; i++ {
		go func() {
			defer wg.Done()
			// The benchmark measures call cost including failed calls, so the error is not checked.
			for j := 0; j < b.N; j++ {
				_ = buffer.Enqueue(item)
			}
		}()
	}

	// The benchmark measures call cost including failed calls, so the error is not checked.
	for i := 0; i < b.N*numProducers; i++ {
		_, _ = buffer.Dequeue()
	}
	wg.Wait()
}

func BenchmarkMPMCRingBufferSeq(b *testing.B) {
	buflen := 1024
	// The count is a valid power of two, so New cannot fail.
	buffer, _ := New[string](
		uint32(buflen),
		WithMultiProdEnqueue(),
		WithMultiConsDequeue(),
	)

	item := "test"
	b.SetBytes(int64(len(item)))
	b.ResetTimer()

	// The benchmark measures call cost including failed calls, so the error is not checked.
	for i := 0; i < b.N; i++ {
		_ = buffer.Enqueue(item)
		_, _ = buffer.Dequeue()
	}
}

func BenchmarkMPMCRingBufferParallel(b *testing.B) {
	buflen := 1024
	// The count is a valid power of two, so New cannot fail.
	buffer, _ := New[string](
		uint32(buflen),
		WithMultiProdEnqueue(),
		WithMultiConsDequeue(),
	)

	numProducers := 4
	numConsumers := 4
	item := "test"
	b.SetBytes(int64(len(item) * numProducers))
	b.ResetTimer()

	var producerWG sync.WaitGroup
	producerWG.Add(numProducers)

	for i := 0; i < numProducers; i++ {
		go func() {
			defer producerWG.Done()
			// The benchmark measures call cost including failed calls, so the error is not checked.
			for j := 0; j < b.N; j++ {
				_ = buffer.Enqueue(item)
			}
		}()
	}

	var consumerWG sync.WaitGroup
	consumerWG.Add(numConsumers)

	for i := 0; i < numConsumers; i++ {
		go func() {
			defer consumerWG.Done()
			// The benchmark measures call cost including failed calls, so the error is not checked.
			for j := 0; j < b.N; j++ {
				_, _ = buffer.Dequeue()
			}
		}()
	}

	producerWG.Wait()
	consumerWG.Wait()
}

func assertNewRing[T any](t *testing.T, ring *Ring[T], count uint32) {
	t.Helper()
	assert.NotNil(t, ring.entries)
	assert.Equal(t, count, ring.size)
	assert.Equal(t, count-1, ring.mask)
	assert.Equal(t, count-1, ring.capacity)

	assert.Zero(t, *ring.prodHead)
	assert.Zero(t, *ring.prodTail)
	assert.Zero(t, *ring.consHead)
	assert.Zero(t, *ring.consTail)
}
