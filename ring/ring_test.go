package ring

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

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
	assert.Equal(t, modeSingle, simple.prodMode)
	assert.Equal(t, modeSingle, simple.consMode)
	assert.True(t, simple.clearOnDequeue)

	keeping, err := New[uintptr](64, WithoutDequeueClearing())
	assert.NoError(t, err)
	assertNewRing[uintptr](t, keeping, 64)
	assert.False(t, keeping.clearOnDequeue)

	mpsc, err := New[uintptr](64, WithMultiProdEnqueue())
	assert.NoError(t, err)
	assertNewRing[uintptr](t, mpsc, 64)
	assert.Equal(t, modeMulti, mpsc.prodMode)
	assert.Equal(t, modeSingle, mpsc.consMode)

	spmc, err := New[uintptr](64, WithMultiConsDequeue())
	assert.NoError(t, err)
	assertNewRing[uintptr](t, spmc, 64)
	assert.Equal(t, modeSingle, spmc.prodMode)
	assert.Equal(t, modeMulti, spmc.consMode)

	mpmc, err := New[uintptr](
		64,
		WithMultiProdEnqueue(),
		WithMultiConsDequeue(),
	)
	assert.NoError(t, err)
	assertNewRing[uintptr](t, mpmc, 64)
	assert.Equal(t, modeMulti, mpmc.prodMode)
	assert.Equal(t, modeMulti, mpmc.consMode)

	rtsProd, err := New[uintptr](64, WithRelaxedMultiProdEnqueue())
	assert.NoError(t, err)
	assertNewRing[uintptr](t, rtsProd, 64)
	assert.Equal(t, modeRelaxed, rtsProd.prodMode)
	assert.Equal(t, modeSingle, rtsProd.consMode)
	assert.Equal(t, uint32(8), rtsProd.prodHTDMax)
	assert.Equal(t, uint32(0), rtsProd.consHTDMax)

	rtsCons, err := New[uintptr](64, WithRelaxedMultiConsDequeue())
	assert.NoError(t, err)
	assertNewRing[uintptr](t, rtsCons, 64)
	assert.Equal(t, modeSingle, rtsCons.prodMode)
	assert.Equal(t, modeRelaxed, rtsCons.consMode)
	assert.Equal(t, uint32(0), rtsCons.prodHTDMax)
	assert.Equal(t, uint32(8), rtsCons.consHTDMax)

	// The last option given for a side applies.
	lastMulti, err := New[uintptr](
		64,
		WithRelaxedMultiProdEnqueue(),
		WithMultiProdEnqueue(),
		WithRelaxedMultiConsDequeue(),
		WithMultiConsDequeue(),
	)
	assert.NoError(t, err)
	assert.Equal(t, modeMulti, lastMulti.prodMode)
	assert.Equal(t, modeMulti, lastMulti.consMode)

	lastRelaxed, err := New[uintptr](
		64,
		WithMultiProdEnqueue(),
		WithRelaxedMultiProdEnqueue(),
		WithMultiConsDequeue(),
		WithRelaxedMultiConsDequeue(),
	)
	assert.NoError(t, err)
	assert.Equal(t, modeRelaxed, lastRelaxed.prodMode)
	assert.Equal(t, modeRelaxed, lastRelaxed.consMode)

	rtsSmall, err := New[uintptr](4, relaxedOptions()...)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), rtsSmall.prodHTDMax)
	assert.Equal(t, uint32(0), rtsSmall.consHTDMax)

	rtsEight, err := New[uintptr](8, relaxedOptions()...)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), rtsEight.prodHTDMax)
	assert.Equal(t, uint32(1), rtsEight.consHTDMax)

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
	item := "test"

	t.Run("default", func(t *testing.T) {
		buffer, err := New[string](buflen)
		require.NotNil(t, buffer)
		require.NoError(t, err)

		// The other occupancy functions are checked along the way, so they need no tests of
		// their own.
		assert.True(t, buffer.Empty())
		assert.False(t, buffer.Full())
		assert.Equal(t, buflen, buffer.Free())
		assert.Equal(t, 0, buffer.Count())

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
	})

	t.Run("RTS", func(t *testing.T) {
		ring, err := New[string](buflen, relaxedOptions()...)
		require.NoError(t, err)
		for range buflen {
			require.NoError(t, ring.Enqueue(item))
		}
		require.True(t, ring.Full())

		ring.Reset()

		assert.True(t, ring.Empty())
		assert.False(t, ring.Full())
		assert.Equal(t, buflen, ring.Free())
		assert.Equal(t, 0, ring.Count())
		assert.Zero(t, ring.prodHeadRTS.Load())
		assert.Zero(t, ring.prodTailRTS.Load())
		assert.Zero(t, ring.consHeadRTS.Load())
		assert.Zero(t, ring.consTailRTS.Load())

		_, err = ring.Dequeue()
		assert.ErrorIs(t, err, ErrEmpty)
	})

	// The cache invariant requires Reset to zero both caches. A cachedProdTail left at 1000 would
	// let the Dequeue after Reset admit 1000 cleared slots. Moving 1000 elements, between 1 and the
	// capacity, keeps the admission check from hiding a missing reset.
	t.Run("caches", func(t *testing.T) {
		const moved = 1000
		for _, mode := range cacheModes() {
			t.Run(mode.name, func(t *testing.T) {
				ring, err := New[int](1024, mode.options...)
				require.NoError(t, err)
				advanceRing(t, ring, moved)
				for range ring.Capacity() {
					require.NoError(t, ring.Enqueue(0))
				}
				require.ErrorIs(t, ring.Enqueue(0), ErrFull)
				require.NotZero(t, ring.cachedConsTail.Load())
				require.NotZero(t, ring.cachedProdTail.Load())

				ring.Reset()

				_, err = ring.Dequeue()
				assert.ErrorIs(t, err, ErrEmpty)
				assert.Zero(t, ring.cachedConsTail.Load())
				assert.Zero(t, ring.cachedProdTail.Load())
			})
		}
	})
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
	for _, set := range defaultAndRelaxed() {
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
				name := fmt.Sprintf("%s/capacity=%d,start=%d", set.name, capacity, start)
				t.Run(name, func(t *testing.T) {
					ring, err := New[int](capacity, set.options...)
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
}

// TestEmptyAgreesWithCount checks that Empty reports exactly when Count is 0, in every state a
// ring with no concurrent activity passes through while it fills and drains. The start near 2^32
// makes the indices wrap partway through.
func TestEmptyAgreesWithCount(t *testing.T) {
	const capacity = 8
	for _, set := range defaultAndRelaxed() {
		for _, start := range []uint32{0, math.MaxUint32 - 2} {
			t.Run(fmt.Sprintf("%s/start=%d", set.name, start), func(t *testing.T) {
				ring, err := New[int](capacity, set.options...)
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

// cachedAndExact returns the option sets for a test whose counts hold under both batch-count
// settings: the default cached counts, and WithExactBatchCounts.
func cachedAndExact() []optionSet {
	return []optionSet{{name: "cached"}, {name: "exact", options: []Option{WithExactBatchCounts()}}}
}

// TestEnqueueBulk never lets the consumer move between calls, so both batch-count settings return
// the same counts, and the exact subtest keeps the count assertions running under the option.
func TestEnqueueBulk(t *testing.T) {
	for _, set := range cachedAndExact() {
		t.Run(set.name, func(t *testing.T) {
			ring, err := New[int](8, set.options...)
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
		})
	}
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

// TestDequeueBurst never lets the producer move between calls, so both batch-count settings return
// the same counts, and the exact subtest keeps the count assertions running under the option.
func TestDequeueBurst(t *testing.T) {
	for _, set := range cachedAndExact() {
		t.Run(set.name, func(t *testing.T) {
			ring, err := New[int](8, set.options...)
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
		})
	}
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
	require.Equal(t, uint32(6), ring.prodHead.Load()&ring.mask)
	requireEnqueueBulk(t, ring, batch)
	elems := make([]int, 5)
	_, err = ring.DequeueBulk(elems)
	require.NoError(t, err)
	assert.Equal(t, batch, elems)

	advanceRing(t, ring, 3)
	require.Equal(t, uint32(6), ring.prodHead.Load()&ring.mask)
	enqueued, _ := ring.EnqueueBurst(batch)
	assert.Equal(t, 5, enqueued)
	elems = make([]int, 5)
	dequeued, _ := ring.DequeueBurst(elems)
	assert.Equal(t, 5, dequeued)
	assert.Equal(t, batch, elems)
	assert.True(t, ring.Empty())
}

// TestDequeueClearsSlots checks that every slot a dequeue reads holds nil afterward, while slots it
// did not read keep their elements, and that Reset clears every slot.
func TestDequeueClearsSlots(t *testing.T) {
	allSlots := []int{0, 1, 2, 3, 4, 5, 6, 7}

	t.Run("Dequeue", func(t *testing.T) {
		ring := newPointerRing(t)
		require.NoError(t, ring.Enqueue(new(int)))
		_, err := ring.Dequeue()
		require.NoError(t, err)
		assertSlotsNil(t, ring, 0)
	})

	t.Run("DequeueBulk", func(t *testing.T) {
		ring := newPointerRing(t)
		requireEnqueuePointers(t, ring, 5)
		_, err := ring.DequeueBulk(make([]*int, 3))
		require.NoError(t, err)
		assertSlotsNil(t, ring, 0, 1, 2)
		assert.NotNil(t, ring.entries[3])
		assert.NotNil(t, ring.entries[4])
	})

	t.Run("DequeueBulkMultiConsumer", func(t *testing.T) {
		ring, err := New[*int](8, WithMultiConsDequeue())
		require.NoError(t, err)
		requireEnqueuePointers(t, ring, 5)
		_, err = ring.DequeueBulk(make([]*int, 3))
		require.NoError(t, err)
		assertSlotsNil(t, ring, 0, 1, 2)
		assert.NotNil(t, ring.entries[3])
		assert.NotNil(t, ring.entries[4])
	})

	t.Run("DequeueBurst", func(t *testing.T) {
		ring := newPointerRing(t)
		requireEnqueuePointers(t, ring, 3)
		dequeued, _ := ring.DequeueBurst(make([]*int, 5))
		require.Equal(t, 3, dequeued)
		assertSlotsNil(t, ring, 0, 1, 2)
	})

	// Starting two slots before the end, six enqueues fill slots 6, 7, 0, 1, 2, and 3. Dequeuing
	// five reads slots 6, 7, 0, 1, and 2, so the sixth, unread slot keeps its element.
	t.Run("DequeueBulkAcrossEnd", func(t *testing.T) {
		ring := newPointerRing(t)
		setIndices(ring, 6)
		requireEnqueuePointers(t, ring, 6)
		_, err := ring.DequeueBulk(make([]*int, 5))
		require.NoError(t, err)
		assertSlotsNil(t, ring, 6, 7, 0, 1, 2)
		assert.NotNil(t, ring.entries[3])
	})

	t.Run("DequeueBurstAcrossEnd", func(t *testing.T) {
		ring := newPointerRing(t)
		setIndices(ring, 6)
		requireEnqueuePointers(t, ring, 6)
		dequeued, _ := ring.DequeueBurst(make([]*int, 5))
		require.Equal(t, 5, dequeued)
		assertSlotsNil(t, ring, 6, 7, 0, 1, 2)
		assert.NotNil(t, ring.entries[3])
	})

	t.Run("Reset", func(t *testing.T) {
		ring := newPointerRing(t)
		requireEnqueuePointers(t, ring, 5)
		ring.Reset()
		assertSlotsNil(t, ring, allSlots...)
	})
}

// TestWithoutDequeueClearing checks that a ring created with WithoutDequeueClearing leaves every
// slot a dequeue reads holding its element, and that Reset still clears every slot.
func TestWithoutDequeueClearing(t *testing.T) {
	allSlots := []int{0, 1, 2, 3, 4, 5, 6, 7}

	t.Run("Dequeue", func(t *testing.T) {
		ring := newPointerRing(t, WithoutDequeueClearing())
		require.NoError(t, ring.Enqueue(new(int)))
		_, err := ring.Dequeue()
		require.NoError(t, err)
		assertSlotsNotNil(t, ring, 0)
	})

	t.Run("DequeueBulk", func(t *testing.T) {
		ring := newPointerRing(t, WithoutDequeueClearing())
		requireEnqueuePointers(t, ring, 5)
		_, err := ring.DequeueBulk(make([]*int, 3))
		require.NoError(t, err)
		assertSlotsNotNil(t, ring, 0, 1, 2)
	})

	t.Run("DequeueBulkMultiConsumer", func(t *testing.T) {
		ring := newPointerRing(t, WithMultiConsDequeue(), WithoutDequeueClearing())
		requireEnqueuePointers(t, ring, 5)
		_, err := ring.DequeueBulk(make([]*int, 3))
		require.NoError(t, err)
		assertSlotsNotNil(t, ring, 0, 1, 2)
	})

	// Starting two slots before the end, six enqueues fill slots 6, 7, 0, 1, 2, and 3, and
	// dequeuing five reads slots 6, 7, 0, 1, and 2.
	t.Run("DequeueBulkAcrossEnd", func(t *testing.T) {
		ring := newPointerRing(t, WithoutDequeueClearing())
		setIndices(ring, 6)
		requireEnqueuePointers(t, ring, 6)
		_, err := ring.DequeueBulk(make([]*int, 5))
		require.NoError(t, err)
		assertSlotsNotNil(t, ring, 6, 7, 0, 1, 2)
	})

	t.Run("Reset", func(t *testing.T) {
		ring := newPointerRing(t, WithoutDequeueClearing())
		requireEnqueuePointers(t, ring, 5)
		_, err := ring.DequeueBulk(make([]*int, 3))
		require.NoError(t, err)
		ring.Reset()
		assertSlotsNil(t, ring, allSlots...)
	})
}

// newPointerRing returns an empty 8-slot ring of pointers, created with options, so a cleared slot
// reads nil.
func newPointerRing(t *testing.T, options ...Option) *Ring[*int] {
	t.Helper()
	ring, err := New[*int](8, options...)
	require.NoError(t, err)
	return ring
}

// requireEnqueuePointers enqueues count new non-nil pointers into ring.
func requireEnqueuePointers(t *testing.T, ring *Ring[*int], count int) {
	t.Helper()
	items := make([]*int, count)
	for i := range items {
		items[i] = new(int)
	}
	_, err := ring.EnqueueBulk(items)
	require.NoError(t, err)
}

// assertSlotsNil checks that each listed slot of the ring's backing array is nil.
func assertSlotsNil(t *testing.T, ring *Ring[*int], slots ...int) {
	t.Helper()
	for _, slot := range slots {
		assert.Nil(t, ring.entries[slot], "slot %d", slot)
	}
}

// assertSlotsNotNil checks that each listed slot of the ring's backing array is not nil.
func assertSlotsNotNil(t *testing.T, ring *Ring[*int], slots ...int) {
	t.Helper()
	for _, slot := range slots {
		assert.NotNil(t, ring.entries[slot], "slot %d", slot)
	}
}

// releasable is the object TestDequeueReleasesMemory watches. It is a 64-byte struct that holds a
// pointer because small pointer-free objects can be combined by the tiny allocator, which keeps
// them alive together, so a weak pointer to one could stay set after the ring lets go of it.
type releasable struct {
	_ *releasable
	_ [56]byte
}

// TestDequeueReleasesMemory checks that the ring keeps no reference to an element once it is
// dequeued or the ring is reset, so the garbage collector can free it. The helpers keep the object
// out of the test's own variables, so nothing but the ring can hold it during the collection. Each
// subtest keeps the ring alive past the collection, since an unreachable ring would release the
// element even without clearing its slot.
func TestDequeueReleasesMemory(t *testing.T) {
	t.Run("Dequeue", func(t *testing.T) {
		ring, err := New[*releasable](8)
		require.NoError(t, err)
		watched := enqueueReleasable(t, ring)
		requireDequeuesWatched(t, ring, watched)

		runtime.GC()
		assert.Nil(t, watched.Value())
		runtime.KeepAlive(ring)
	})

	t.Run("Reset", func(t *testing.T) {
		ring, err := New[*releasable](8)
		require.NoError(t, err)
		watched := enqueueReleasable(t, ring)
		ring.Reset()

		runtime.GC()
		assert.Nil(t, watched.Value())
		runtime.KeepAlive(ring)
	})
}

// enqueueReleasable enqueues a new releasable into ring and returns a weak pointer to it.
func enqueueReleasable(t *testing.T, ring *Ring[*releasable]) weak.Pointer[releasable] {
	t.Helper()
	object := &releasable{}
	require.NoError(t, ring.Enqueue(object))
	return weak.Make(object)
}

// requireDequeuesWatched dequeues one element from ring and requires it to be the watched object.
func requireDequeuesWatched(
	t *testing.T,
	ring *Ring[*releasable],
	watched weak.Pointer[releasable],
) {
	t.Helper()
	dequeued, err := ring.Dequeue()
	require.NoError(t, err)
	require.Same(t, watched.Value(), dequeued)
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
	for _, set := range defaultAndRelaxed() {
		t.Run(set.name, func(t *testing.T) {
			ring, err := New[int](8, set.options...)
			require.NoError(t, err)
			requireEnqueueBulk(t, ring, []int{1, 2, 3, 4, 5})

			free, err := ring.EnqueueBulk(make([]int, 4))
			assert.True(t, err == ErrFull, "got %v", err)
			assert.Equal(t, 3, free)

			free, err = ring.EnqueueBulk(make([]int, ring.Capacity()+1))
			assert.ErrorIs(t, err, ErrExceedsCapacity)
			assert.Equal(t, 3, free)
			assert.Equal(t, 5, ring.Count())
		})
	}
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
	for _, set := range defaultAndRelaxed() {
		t.Run(set.name, func(t *testing.T) {
			ring, err := New[int](8, set.options...)
			require.NoError(t, err)
			requireEnqueueBulk(t, ring, []int{1, 2, 3, 4, 5})

			available, err := ring.DequeueBulk(make([]int, 6))
			assert.True(t, err == ErrEmpty, "got %v", err)
			assert.Equal(t, 5, available)

			available, err = ring.DequeueBulk(make([]int, ring.Capacity()+1))
			assert.ErrorIs(t, err, ErrExceedsCapacity)
			assert.Equal(t, 5, available)
			assert.Equal(t, 5, ring.Count())
		})
	}
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
	relaxed := relaxedOptions()
	cases := []struct {
		name    string
		options []Option
		cnt     uint32
	}{
		{name: "SPSC"},
		{name: "MPMC", options: []Option{WithMultiProdEnqueue(), WithMultiConsDequeue()}},
		{name: "RTS", options: relaxed},
		{name: "RTSCounterWrap", options: relaxed, cnt: math.MaxUint32 - 1},
		{name: "RTSProd", options: []Option{WithRelaxedMultiProdEnqueue()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ring, err := New[int](8, tc.options...)
			require.NoError(t, err)
			const startIndex = math.MaxUint32 - 2
			setRelaxedIndices(ring, startIndex, tc.cnt)
			assertOccupancy(t, ring, 0)

			require.NoError(t, ring.Enqueue(1))
			require.NoError(t, ring.Enqueue(2))
			assertOccupancy(t, ring, 2)
			item, err := ring.Dequeue()
			require.NoError(t, err)
			assert.Equal(t, 1, item)
			assertOccupancy(t, ring, 1)

			requireEnqueueBulk(t, ring, []int{3, 4, 5})
			require.Less(t, ring.prodTail.Load(), uint32(startIndex))
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
			require.Less(t, ring.consTail.Load(), uint32(startIndex))
			assertOccupancy(t, ring, 0)
		})
	}
}

// TestRelaxedDispatch runs every enqueue, dequeue, and occupancy call on rings with a relaxed
// producer, a relaxed consumer, or both, so each call site that dispatches on the mode is covered.
// The counters start just below 2^32 and wrap during the subtest, while the positions stay near
// 1000. A site that read a relaxed side's unused 32-bit head, which stays at 0 while the positions
// are at 1000, would report a wrong count.
func TestRelaxedDispatch(t *testing.T) {
	cases := []struct {
		name    string
		options []Option
	}{
		{name: "rtsProd", options: []Option{WithRelaxedMultiProdEnqueue()}},
		{name: "rtsCons", options: []Option{WithRelaxedMultiConsDequeue()}},
		{name: "rts", options: relaxedOptions()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ring, err := New[int](8, tc.options...)
			require.NoError(t, err)
			setRelaxedIndices(ring, 1000, math.MaxUint32-2)

			require.NoError(t, ring.Enqueue(1))
			require.NoError(t, ring.Enqueue(2))
			require.NoError(t, ring.Enqueue(3))
			free, err := ring.EnqueueBulk([]int{4, 5})
			require.NoError(t, err)
			assert.Equal(t, 3, free)
			enqueued, free := ring.EnqueueBurst([]int{6})
			assert.Equal(t, 1, enqueued)
			assert.Equal(t, 2, free)

			assertOccupancy(t, ring, 6)
			assert.False(t, ring.Full())
			assert.Equal(t, uint32(2), ring.freeEntries())
			assert.Equal(t, uint32(6), ring.occupiedEntries())

			free, err = ring.EnqueueBulk(make([]int, 3))
			assert.True(t, err == ErrFull, "got %v", err)
			assert.Equal(t, 2, free)
			free, err = ring.EnqueueBulk(make([]int, 9))
			assert.ErrorIs(t, err, ErrExceedsCapacity)
			assert.Equal(t, 2, free)
			available, err := ring.DequeueBulk(make([]int, 9))
			assert.ErrorIs(t, err, ErrExceedsCapacity)
			assert.Equal(t, 6, available)

			enqueued, free = ring.EnqueueBurst([]int{7, 8})
			assert.Equal(t, 2, enqueued)
			assert.Equal(t, 0, free)
			assert.True(t, ring.Full())

			item, err := ring.Dequeue()
			require.NoError(t, err)
			assert.Equal(t, 1, item)
			elems := make([]int, 2)
			available, err = ring.DequeueBulk(elems)
			require.NoError(t, err)
			assert.Equal(t, []int{2, 3}, elems)
			assert.Equal(t, 5, available)
			elems = make([]int, 4)
			dequeued, available := ring.DequeueBurst(elems)
			assert.Equal(t, 4, dequeued)
			assert.Equal(t, []int{4, 5, 6, 7}, elems)
			assert.Equal(t, 1, available)
			item, err = ring.Dequeue()
			require.NoError(t, err)
			assert.Equal(t, 8, item)

			assertOccupancy(t, ring, 0)
			available, err = ring.DequeueBulk(make([]int, 1))
			assert.True(t, err == ErrEmpty, "got %v", err)
			assert.Equal(t, 0, available)
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

	assert.Zero(t, ring.prodHead.Load())
	assert.Zero(t, ring.prodTail.Load())
	assert.Zero(t, ring.consHead.Load())
	assert.Zero(t, ring.consTail.Load())
}

// setIndices places every index of an empty ring at start, with relaxed counters at 0, so a test
// can run the ring across the 2^32 wrap.
func setIndices[T any](ring *Ring[T], start uint32) {
	setRelaxedIndices(ring, start, 0)
}

// setRelaxedIndices places every index of an empty ring at pos, and the counters of the relaxed
// sides at cnt, so a test can wrap the position and the counter separately. It leaves a relaxed
// side's unused 32-bit head at 0, so a site that wrongly reads it reports a visibly wrong count.
// It also stores pos into both caches, which keeps the cache invariant. Any test that sets indices
// by hand must set the caches to match, or break the invariant on purpose and say so.
func setRelaxedIndices[T any](ring *Ring[T], pos, cnt uint32) {
	ring.prodTail.Store(pos)
	ring.consTail.Store(pos)
	ring.cachedConsTail.Store(pos)
	ring.cachedProdTail.Store(pos)
	if ring.prodMode == modeRelaxed {
		ring.prodHeadRTS.Store(pack(pos, cnt))
		ring.prodTailRTS.Store(pack(pos, cnt))
	} else {
		ring.prodHead.Store(pos)
	}
	if ring.consMode == modeRelaxed {
		ring.consHeadRTS.Store(pack(pos, cnt))
		ring.consTailRTS.Store(pack(pos, cnt))
	} else {
		ring.consHead.Store(pos)
	}
}

// relaxedOptions returns the options that make both sides of a ring relaxed.
func relaxedOptions() []Option {
	return []Option{WithRelaxedMultiProdEnqueue(), WithRelaxedMultiConsDequeue()}
}

// An optionSet is a named set of options a test runs under.
type optionSet struct {
	name    string
	options []Option
}

// defaultAndRelaxed returns the option sets for a test that runs on a ring with no options and on
// one with both sides relaxed.
func defaultAndRelaxed() []optionSet {
	return []optionSet{{name: "default"}, {name: "RTS", options: relaxedOptions()}}
}

// TestMaxBatchOptions checks that both batch limits default to the capacity, that each option
// sets its own side, that WithMaxBatch sets both, that the last option given for a side applies,
// and that New rejects a limit below 1 or above the count. It also checks that the spin bound
// defaults to defaultSpinsBeforeYield, that WithSpinsBeforeYield sets any bound from 0 to
// math.MaxInt32, that the last one given applies, and that New rejects a bound outside that range.
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

	for _, bad := range []int{-1, math.MinInt32} {
		ring, err = New[int](16, WithSpinsBeforeYield(bad))
		assert.ErrorIs(t, err, ErrInvalidSpinsBeforeYield, "spins %d", bad)
		assert.True(t, ring == nil, "spins %d", bad)
	}
	for _, good := range []int{0, math.MaxInt32} {
		ring, err = New[int](16, WithSpinsBeforeYield(good))
		require.NoError(t, err, "spins %d", good)
		assert.Equal(t, uint32(good), ring.spinsBeforeYield, "spins %d", good)
	}

	t.Run("spinsAboveMaxInt32", func(t *testing.T) {
		if math.MaxInt < 1<<31 {
			t.Skip("int cannot hold 2^31")
		}
		// The bound is computed at run time, because the constant 2^31 does not compile where
		// int is 32 bits.
		spins := math.MaxInt32
		spins++
		ring, err := New[int](16, WithSpinsBeforeYield(spins))
		assert.ErrorIs(t, err, ErrInvalidSpinsBeforeYield)
		assert.True(t, ring == nil)
	})

	ring, err = New[int](16)
	require.NoError(t, err)
	assert.Equal(t, uint32(defaultSpinsBeforeYield), ring.spinsBeforeYield)

	ring, err = New[int](16, WithSpinsBeforeYield(-1), WithSpinsBeforeYield(3))
	require.NoError(t, err)
	assert.Equal(t, uint32(3), ring.spinsBeforeYield)
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
// consumer on an SPSC ring, and four of each on an MPMC ring, on a ring with both sides relaxed,
// and on rings with one relaxed side and one multi side.
func stressModes() []stressMode {
	return []stressMode{
		{name: "SPSC", producers: 1, consumers: 1},
		{
			name:      "MPMC",
			producers: 4,
			consumers: 4,
			options:   []Option{WithMultiProdEnqueue(), WithMultiConsDequeue()},
		},
		{
			name:      "RTS",
			producers: 4,
			consumers: 4,
			options:   relaxedOptions(),
		},
		{
			name:      "RTSProdMultiCons",
			producers: 4,
			consumers: 4,
			options:   []Option{WithRelaxedMultiProdEnqueue(), WithMultiConsDequeue()},
		},
		{
			name:      "MultiProdRTSCons",
			producers: 4,
			consumers: 4,
			options:   []Option{WithMultiProdEnqueue(), WithRelaxedMultiConsDequeue()},
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

// burstCalls returns batchCalls that move values through ring with EnqueueBurst and DequeueBurst.
func burstCalls(ring *Ring[uint64]) (enqueue, dequeue batchCall) {
	enqueue = func(items []uint64) int {
		enqueued, _ := ring.EnqueueBurst(items)
		return enqueued
	}
	dequeue = func(dst []uint64) int {
		dequeued, _ := ring.DequeueBurst(dst)
		return dequeued
	}
	return enqueue, dequeue
}

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
// 2^32, so they wrap while the producers and consumers contend. Relaxed sides start their
// counters near 2^32 as well, at a different point from the positions, so both wrap under
// contention.
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
			setRelaxedIndices(ring, startIndex, math.MaxUint32-99)
			enqueue, dequeue := burstCalls(ring)

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
			assert.Less(t, ring.consTail.Load(), uint32(startIndex))
		})
	}
}

// TestGOMAXPROCSOne checks that every element arrives exactly once and in order in every stress
// mode with only one CPU, at the default spin bound and at bounds of 0 and 1. Whether a waiter
// actually meets an unpublished predecessor depends on preemption timing, so TestWaitForTail tests
// the wait itself. It does not call t.Parallel, because GOMAXPROCS is process-wide.
func TestGOMAXPROCSOne(t *testing.T) {
	const (
		total = 100_000
		batch = 8
	)
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, mode := range stressModes() {
		t.Run(mode.name, func(t *testing.T) {
			spinBounds := []optionSet{
				{name: "spins=default", options: mode.options},
				{
					name:    "spins=0",
					options: slices.Concat(mode.options, []Option{WithSpinsBeforeYield(0)}),
				},
				{
					name:    "spins=1",
					options: slices.Concat(mode.options, []Option{WithSpinsBeforeYield(1)}),
				},
			}
			for _, bound := range spinBounds {
				t.Run(bound.name, func(t *testing.T) {
					ring, err := New[uint64](64, bound.options...)
					require.NoError(t, err)
					enqueue, dequeue := burstCalls(ring)

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
				})
			}
		})
	}
}

// TestWaitForTail checks that waitForTail returns only once the tail equals want, not before and
// not on some other change. With one CPU the waiter can run only when the test goroutine yields,
// so each runtime.Gosched hands control to it, and a waiter that returns early is caught
// reliably. The wrap case catches a wait that compares with < or > instead of equality. Every case
// runs at spin bounds of 0, 1, and defaultSpinsBeforeYield. It does not call t.Parallel, because
// GOMAXPROCS is process-wide.
func TestWaitForTail(t *testing.T) {
	const yields = 1000
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })

	cases := []struct {
		name         string
		start        uint32
		want         uint32
		storesBefore []uint32
	}{
		{name: "noWrap", start: 0, want: 5, storesBefore: []uint32{4}},
		{
			name:         "wrap",
			start:        math.MaxUint32 - 1,
			want:         2,
			storesBefore: []uint32{math.MaxUint32, 0, 1},
		},
		{
			name:         "wrapToZero",
			start:        math.MaxUint32 - 1,
			want:         0,
			storesBefore: []uint32{math.MaxUint32},
		},
	}
	for _, spins := range []int{0, 1, defaultSpinsBeforeYield} {
		t.Run(fmt.Sprintf("spins=%d", spins), func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					var tail atomic.Uint32
					tail.Store(tc.start)
					var returned atomic.Bool
					var waiting sync.WaitGroup
					waiting.Go(func() {
						waitForTail(&tail, tc.want, spins)
						returned.Store(true)
					})

					for range yields {
						runtime.Gosched()
					}
					require.False(
						t,
						returned.Load(),
						"waitForTail returned with the tail at %d",
						tc.start,
					)

					for _, value := range tc.storesBefore {
						tail.Store(value)
						for range yields {
							runtime.Gosched()
						}
						require.False(
							t,
							returned.Load(),
							"waitForTail returned with the tail at %d",
							value,
						)
					}

					tail.Store(tc.want)
					stressDeadline(t, waiting.Wait)
				})
			}
		})
	}
}

// TestWaitHeadTailDistance checks that waitHeadTailDistance returns only once the head position
// is at most htdMax past the tail position, and then returns the head word. The head changes
// before the release, so a wait that returned a head loaded before it waited would fail. It runs
// with one CPU for the reason TestWaitForTail gives, and does not call t.Parallel, because
// GOMAXPROCS is process-wide. The wrap case catches a distance computed without wrapping
// arithmetic. The ready check and every case run at spin bounds of 0, 1, and
// defaultSpinsBeforeYield.
func TestWaitHeadTailDistance(t *testing.T) {
	const yields = 1000
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })

	cases := []struct {
		name        string
		htdMax      uint32
		head        uint64
		newHead     uint64
		tail        uint64
		tailsBefore []uint64
		release     uint64
	}{
		{
			name:        "bound2",
			htdMax:      2,
			head:        pack(10, 5),
			newHead:     pack(10, 6),
			tail:        pack(6, 3),
			tailsBefore: []uint64{pack(7, 4)},
			release:     pack(8, 5),
		},
		{
			name:    "bound0",
			htdMax:  0,
			head:    pack(5, 3),
			newHead: pack(5, 4),
			tail:    pack(4, 2),
			release: pack(5, 3),
		},
		{
			name:        "wrap",
			htdMax:      2,
			head:        pack(1, 9),
			newHead:     pack(1, 10),
			tail:        pack(math.MaxUint32-2, 6),
			tailsBefore: []uint64{pack(math.MaxUint32-1, 7)},
			release:     pack(math.MaxUint32, 8),
		},
	}
	for _, spins := range []int{0, 1, defaultSpinsBeforeYield} {
		t.Run(fmt.Sprintf("spins=%d", spins), func(t *testing.T) {
			var readyHead, readyTail atomic.Uint64
			readyHead.Store(pack(10, 5))
			readyTail.Store(pack(8, 5))
			var got uint64
			stressDeadline(t, func() {
				got = waitHeadTailDistance(&readyHead, &readyTail, 2, spins)
			})
			require.Equal(t, pack(10, 5), got)

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					var head, tail atomic.Uint64
					head.Store(tc.head)
					tail.Store(tc.tail)
					var returnedWord atomic.Uint64
					var returned atomic.Bool
					var waiting sync.WaitGroup
					waiting.Go(func() {
						returnedWord.Store(waitHeadTailDistance(&head, &tail, tc.htdMax, spins))
						returned.Store(true)
					})

					for range yields {
						runtime.Gosched()
					}
					require.False(
						t,
						returned.Load(),
						"waitHeadTailDistance returned with the tail at %#x",
						tc.tail,
					)

					for _, value := range tc.tailsBefore {
						tail.Store(value)
						for range yields {
							runtime.Gosched()
						}
						require.False(
							t,
							returned.Load(),
							"waitHeadTailDistance returned with the tail at %#x",
							value,
						)
					}

					head.Store(tc.newHead)
					tail.Store(tc.release)
					stressDeadline(t, waiting.Wait)
					assert.Equal(t, tc.newHead, returnedWord.Load())
				})
			}
		})
	}
}

// TestSpinForTail checks that spinForTail reports a tail it sees within its bound, and reports
// false when the bound runs out or is zero.
func TestSpinForTail(t *testing.T) {
	var tail atomic.Uint32
	tail.Store(7)

	require.True(t, spinForTail(&tail, 7, 1))
	require.False(t, spinForTail(&tail, 8, 16))
	require.False(t, spinForTail(&tail, 7, 0))
	require.False(t, spinForTail(&tail, 8, defaultSpinsBeforeYield))
}

// TestWaitsYieldWithoutPreemption re-runs TestWaitForTail and TestWaitHeadTailDistance in a child
// process with GOMAXPROCS=1 and asynchronous preemption off. In those tests the waiter runs in its
// own goroutine, and the test goroutine stores the tail only after the waiter yields, so a wait
// that never yields hangs the child every time. Without the child, the runtime's asynchronous
// preemption would let such a wait finish. The parent checks that both tests passed in the child,
// so a filter that selects neither cannot make it pass vacuously.
func TestWaitsYieldWithoutPreemption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping the child process in short mode")
	}
	if os.Getenv("RING_WAIT_CHILD") != "" {
		t.Skip("already running in the child process")
	}
	switch runtime.GOOS {
	case "js", "wasip1", "ios":
		t.Skip("cannot start a child process on " + runtime.GOOS)
	}

	exe, err := os.Executable()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(
		ctx,
		exe,
		"-test.run=^(TestWaitForTail|TestWaitHeadTailDistance)$",
		"-test.count=1",
		"-test.v",
	)
	cmd.Env = append(
		childTestEnv(),
		"RING_WAIT_CHILD=1",
		"GOMAXPROCS=1",
		"GODEBUG=asyncpreemptoff=1",
	)

	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("the child did not finish within 30 seconds, so a wait never yielded:\n%s", output)
	}
	require.NoError(t, err, "child output:\n%s", output)
	require.Contains(
		t,
		string(output),
		"\n--- PASS: TestWaitForTail (",
		"child output:\n%s",
		output,
	)
	require.Contains(
		t,
		string(output),
		"\n--- PASS: TestWaitHeadTailDistance (",
		"child output:\n%s",
		output,
	)
}

// childTestEnv returns this process's environment without Bazel's test filter and sharding
// variables. rules_go would turn TESTBRIDGE_TEST_ONLY, the parent's own test filter, into the
// child's filter in place of its -test.run. TEST_SHARD_INDEX and TEST_TOTAL_SHARDS would make the
// child run only its shard's share of the tests, which could leave out either wait test, and
// TEST_SHARD_STATUS_FILE goes with them because it tells Bazel the test supports sharding.
func childTestEnv() []string {
	removed := []string{
		"TESTBRIDGE_TEST_ONLY",
		"TEST_SHARD_INDEX",
		"TEST_TOTAL_SHARDS",
		"TEST_SHARD_STATUS_FILE",
	}
	return slices.DeleteFunc(os.Environ(), func(entry string) bool {
		return slices.ContainsFunc(removed, func(name string) bool {
			return strings.HasPrefix(entry, name+"=")
		})
	})
}

// TestPack checks that unpackPos and unpackCnt return the fields pack put in, at the edges of
// both fields.
func TestPack(t *testing.T) {
	cases := []struct {
		pos uint32
		cnt uint32
	}{
		{pos: 0, cnt: 0},
		{pos: 7, cnt: math.MaxUint32},
		{pos: math.MaxUint32, cnt: 0},
		{pos: math.MaxUint32, cnt: math.MaxUint32},
	}
	for _, tc := range cases {
		word := pack(tc.pos, tc.cnt)
		assert.Equal(t, tc.pos, unpackPos(word), "pos of pack(%d, %d)", tc.pos, tc.cnt)
		assert.Equal(t, tc.cnt, unpackCnt(word), "cnt of pack(%d, %d)", tc.pos, tc.cnt)
	}
}

// TestNextTail checks the tail word nextTail returns, with and without a counter match, and with
// the counter and the position wrapping.
func TestNextTail(t *testing.T) {
	cases := []struct {
		name string
		tail uint64
		head uint64
		want uint64
	}{
		{name: "noMatch", tail: pack(4, 1), head: pack(10, 3), want: pack(4, 2)},
		{name: "match", tail: pack(4, 2), head: pack(10, 3), want: pack(10, 3)},
		{
			name: "counterReachesMax",
			tail: pack(4, math.MaxUint32-1),
			head: pack(10, 0),
			want: pack(4, math.MaxUint32),
		},
		{
			name: "counterWrapsToMatch",
			tail: pack(4, math.MaxUint32),
			head: pack(10, 0),
			want: pack(10, 0),
		},
		// This case catches a counter that carries into the position, which would give
		// pack(5, 0).
		{
			name: "counterWrapsNoMatch",
			tail: pack(4, math.MaxUint32),
			head: pack(10, 1),
			want: pack(4, 0),
		},
		{
			name: "positionWraps",
			tail: pack(math.MaxUint32-1, 6),
			head: pack(3, 7),
			want: pack(3, 7),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, nextTail(tc.tail, tc.head))
		})
	}
}

// TestRelaxedPublishOrder checks that a relaxed producer tail, and its 32-bit mirror, move only
// when every reservation made so far has been published, whatever order the publishes come in.
// The ring has 64 slots, so the head-to-tail bound is 8 and no step waits. The publishes do not
// say which reservation they finish, because publishRelaxed only counts, so the subtests are named
// by the order of events. No slots are copied, since only the indices are checked.
func TestRelaxedPublishOrder(t *testing.T) {
	newRing := func(t *testing.T) *Ring[int] {
		t.Helper()
		ring, err := New[int](64, WithRelaxedMultiProdEnqueue())
		require.NoError(t, err)
		return ring
	}
	reserve := func(t *testing.T, ring *Ring[int], n uint32) uint32 {
		t.Helper()
		oldPos, count, _ := ring.moveProdHeadRelaxed(n, true, admitByLoad)
		require.Equal(t, n, count)
		return oldPos
	}
	publish := func(ring *Ring[int]) bool {
		moved := publishRelaxed(&ring.prodHeadRTS, &ring.prodTailRTS)
		if moved {
			raiseMirror(&ring.prodTail, &ring.prodTailRTS)
		}
		return moved
	}
	requireTail := func(t *testing.T, ring *Ring[int], want uint32) {
		t.Helper()
		require.Equal(t, want, unpackPos(ring.prodTailRTS.Load()), "tail position")
		require.Equal(t, want, ring.prodTail.Load(), "mirror")
	}

	t.Run("reverse", func(t *testing.T) {
		ring := newRing(t)
		reserve(t, ring, 2)
		reserve(t, ring, 3)
		require.False(t, publish(ring))
		requireTail(t, ring, 0)
		require.True(t, publish(ring))
		requireTail(t, ring, 5)
	})

	t.Run("inFlight", func(t *testing.T) {
		ring := newRing(t)
		reserve(t, ring, 2)
		reserve(t, ring, 3)
		require.False(t, publish(ring))
		requireTail(t, ring, 0)
		reserve(t, ring, 1)
		require.False(t, publish(ring))
		requireTail(t, ring, 0)
		require.True(t, publish(ring))
		requireTail(t, ring, 6)
	})

	t.Run("sequential", func(t *testing.T) {
		ring := newRing(t)
		reserve(t, ring, 2)
		require.True(t, publish(ring))
		requireTail(t, ring, 2)
		reserve(t, ring, 3)
		require.True(t, publish(ring))
		requireTail(t, ring, 5)
		reserve(t, ring, 1)
		require.True(t, publish(ring))
		requireTail(t, ring, 6)
	})

	t.Run("allThenPublish", func(t *testing.T) {
		ring := newRing(t)
		reserve(t, ring, 2)
		reserve(t, ring, 3)
		reserve(t, ring, 1)
		require.False(t, publish(ring))
		requireTail(t, ring, 0)
		require.False(t, publish(ring))
		requireTail(t, ring, 0)
		require.True(t, publish(ring))
		requireTail(t, ring, 6)
	})
}

// TestRaiseMirror checks that raiseMirror raises the mirror to the tail position, across the wrap,
// and never lowers it. Two raisers racing between the load and the compare-and-swap cannot be
// staged deterministically, so the relaxed stress modes are what catch a raise that stores
// instead of comparing.
func TestRaiseMirror(t *testing.T) {
	cases := []struct {
		name   string
		mirror uint32
		tail   uint64
		want   uint32
	}{
		{name: "equal", mirror: 5, tail: pack(5, 1), want: 5},
		{name: "mirrorAhead", mirror: 5, tail: pack(4, 1), want: 5},
		// A raiser that saw an older tail position reduces to this case, because raiseMirror
		// takes no position from its caller.
		{name: "tailAhead", mirror: 5, tail: pack(9, 1), want: 9},
		{name: "acrossWrap", mirror: math.MaxUint32 - 1, tail: pack(2, 0), want: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mirror atomic.Uint32
			var tail atomic.Uint64
			mirror.Store(tc.mirror)
			tail.Store(tc.tail)
			raiseMirror(&mirror, &tail)
			assert.Equal(t, tc.want, mirror.Load())
		})
	}
}

// TestMirrorLag checks that the other side reads a relaxed side's 32-bit mirror, not its 64-bit
// tail. Each subtest moves the 64-bit head and tail three slots past the mirror, standing for
// published operations whose mirror raise has not happened yet.
func TestMirrorLag(t *testing.T) {
	t.Run("producer", func(t *testing.T) {
		ring, err := New[int](8, WithRelaxedMultiProdEnqueue())
		require.NoError(t, err)
		require.NoError(t, ring.Enqueue(1))
		require.NoError(t, ring.Enqueue(2))
		ring.prodHeadRTS.Store(pack(5, 3))
		ring.prodTailRTS.Store(pack(5, 3))

		assert.Equal(t, 2, ring.Count())
		assert.Equal(t, uint32(2), ring.occupiedEntries())
		available, err := ring.DequeueBulk(make([]int, 3))
		assert.ErrorIs(t, err, ErrEmpty)
		assert.Equal(t, 2, available)
	})

	t.Run("consumer", func(t *testing.T) {
		ring, err := New[int](8, WithRelaxedMultiConsDequeue())
		require.NoError(t, err)
		_, err = ring.EnqueueBulk([]int{1, 2, 3, 4, 5, 6, 7, 8})
		require.NoError(t, err)
		_, err = ring.DequeueBulk(make([]int, 2))
		require.NoError(t, err)
		ring.consHeadRTS.Store(pack(5, 3))
		ring.consTailRTS.Store(pack(5, 3))

		assert.Equal(t, uint32(2), ring.freeEntries())
		assert.Equal(t, 2, ring.Free())
		free, err := ring.EnqueueBulk(make([]int, 3))
		assert.ErrorIs(t, err, ErrFull)
		assert.Equal(t, 2, free)
	})
}

// TestCountSnapshot checks that Count never reports more entries than have been enqueued and not
// yet dequeued. enqueuedCount counts elements enqueued or in an enqueue call, and drain.received
// counts elements whose dequeue call has returned. A checker reads drain.received, then Count,
// then enqueuedCount. With the consumer tail loaded first, Count is at most their difference.
//
// This test checks correctness. It is not a guaranteed guard for Count's load order, because the
// reversed order breaks the bound only in a narrow window, which the test may miss on some
// machines or schedules. It has no case with relaxed consumers, because a relaxed dequeue can
// return before the consumer tail moves past its elements, so the bound does not hold there.
func TestCountSnapshot(t *testing.T) {
	const (
		workers     = 4
		perProducer = 50_000
		total       = workers * perProducer
		batch       = 8
		checkers    = 2
		yieldEvery  = 64
	)
	cases := []struct {
		name    string
		options []Option
	}{
		{name: "MPMC", options: []Option{WithMultiProdEnqueue(), WithMultiConsDequeue()}},
		{
			name:    "RTSProd",
			options: []Option{WithRelaxedMultiProdEnqueue(), WithMultiConsDequeue()},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ring, err := New[int](64, tc.options...)
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

				// Each checker yields every yieldEvery checks, so the checkers sample densely
				// without starving the producers and consumers when they share one processor.
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

			assert.False(
				t,
				drain.overReceived.Load(),
				"the ring returned more elements than were sent",
			)
			assert.Equal(t, int64(total), drain.received.Load())
			assert.Zero(t, overcounts.Load(), "Count exceeded enqueued minus dequeued")
		})
	}
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
	cases := []struct {
		name    string
		options []Option
	}{
		{name: "MPMC", options: []Option{WithMultiProdEnqueue(), WithMultiConsDequeue()}},
		{name: "RTS", options: relaxedOptions()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ring, err := New[int](64, tc.options...)
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

			assert.False(
				t,
				drain.overReceived.Load(),
				"the ring returned more elements than were sent",
			)
			assert.Equal(t, int64(total), drain.received.Load())
			assert.Zero(t, freeOutOfBounds.Load(), "free out of bounds")
			assert.Zero(t, availableOutOfBounds.Load(), "available out of bounds")
		})
	}
}

// TestErrorIdentity checks that every call returns ErrFull and ErrEmpty unwrapped, so they compare
// equal with ==, and that each bulk call's ErrExceedsCapacity error and New's
// ErrInvalidSpinsBeforeYield error match with errors.Is.
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

	_, err = New[int](4, WithSpinsBeforeYield(-1))
	assert.ErrorIs(t, err, ErrInvalidSpinsBeforeYield)
}

// cacheModes returns the option sets the cache tests run under: one goroutine per side, multi on
// both sides, and relaxed on both sides.
func cacheModes() []optionSet {
	return []optionSet{
		{name: "default"},
		{name: "multi", options: []Option{WithMultiProdEnqueue(), WithMultiConsDequeue()}},
		{name: "relaxed", options: relaxedOptions()},
	}
}

// cacheTestCapacity is the capacity of the rings the cache tests build with newCacheTestRing.
const cacheTestCapacity = 8

// newCacheTestRing returns an empty ring of cacheTestCapacity ints built with options.
func newCacheTestRing(t *testing.T, options []Option) *Ring[int] {
	t.Helper()
	ring, err := New[int](cacheTestCapacity, options...)
	require.NoError(t, err)
	return ring
}

// setCacheState places the producer head and tail at prod and the consumer head and tail at cons,
// on whichever indices each side's mode uses, so the ring holds prod - cons elements. It sets the
// caches to cachedConsTail and cachedProdTail without checking the cache invariant, so each caller
// says when its values break it.
func setCacheState[T any](
	ring *Ring[T],
	prod, cons, cachedConsTail, cachedProdTail uint32,
) {
	ring.prodTail.Store(prod)
	ring.consTail.Store(cons)
	if ring.prodMode == modeRelaxed {
		ring.prodHeadRTS.Store(pack(prod, 0))
		ring.prodTailRTS.Store(pack(prod, 0))
	} else {
		ring.prodHead.Store(prod)
	}
	if ring.consMode == modeRelaxed {
		ring.consHeadRTS.Store(pack(cons, 0))
		ring.consTailRTS.Store(pack(cons, 0))
	} else {
		ring.consHead.Store(cons)
	}
	ring.cachedConsTail.Store(cachedConsTail)
	ring.cachedProdTail.Store(cachedProdTail)
}

// enqueueOne enqueues one element with Enqueue.
func enqueueOne(ring *Ring[int]) error {
	return ring.Enqueue(1)
}

// enqueueThree enqueues three elements with EnqueueBulk.
func enqueueThree(ring *Ring[int]) error {
	_, err := ring.EnqueueBulk([]int{1, 2, 3})
	return err
}

// dequeueOne dequeues one element with Dequeue.
func dequeueOne(ring *Ring[int]) error {
	_, err := ring.Dequeue()
	return err
}

// dequeueThree dequeues three elements with DequeueBulk.
func dequeueThree(ring *Ring[int]) error {
	_, err := ring.DequeueBulk(make([]int, 3))
	return err
}

// consCache returns the producer side's cached copy of the consumer tail.
func consCache(ring *Ring[int]) uint32 {
	return ring.cachedConsTail.Load()
}

// prodCache returns the consumer side's cached copy of the producer tail.
func prodCache(ring *Ring[int]) uint32 {
	return ring.cachedProdTail.Load()
}

// A cacheCase is a ring state set with setCacheState, a call made on it, the error the call must
// return, and the value the cache the case checks must hold afterward.
type cacheCase struct {
	name           string
	prod, cons     uint32
	cachedConsTail uint32
	cachedProdTail uint32
	call           func(ring *Ring[int]) error
	wantErr        error
	cache          func(ring *Ring[int]) uint32
	wantCache      uint32
}

// runCacheCases runs each case on a new ring in each of cacheModes, as subtests named for the mode
// and the case.
func runCacheCases(t *testing.T, cases []cacheCase) {
	t.Helper()
	for _, mode := range cacheModes() {
		t.Run(mode.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					ring := newCacheTestRing(t, mode.options)
					setCacheState(ring, tc.prod, tc.cons, tc.cachedConsTail, tc.cachedProdTail)
					err := tc.call(ring)
					if tc.wantErr == nil {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, tc.wantErr)
					}
					assert.Equal(t, tc.wantCache, tc.cache(ring))
				})
			}
		})
	}
}

// TestCachedAdmission checks the design's admission check and store rule. A request the cache
// shows room or data for is admitted without loading the other side's tail, which shows as a cache
// that keeps its old value while the real tail differs from it. That includes the boundaries
// used == capacity - n, avail == n, and avail == capacity. A request the cache refuses reloads, and
// the store rule leaves the cache equal to the loaded tail, across the 2^32 wrap too. On a relaxed
// side, a reload caches the other side's tail, never the side's own unused 32-bit head.
func TestCachedAdmission(t *testing.T) {
	base := uint32(math.MaxUint32 - 2)
	runCacheCases(t, []cacheCase{
		{
			name: "enqueueFitsCache",
			prod: 10, cons: 10, cachedConsTail: 4, cachedProdTail: 10,
			call: enqueueOne, cache: consCache, wantCache: 4,
		},
		{
			name: "dequeueFitsCache",
			prod: 10, cons: 4, cachedConsTail: 4, cachedProdTail: 6,
			call: dequeueOne, cache: prodCache, wantCache: 6,
		},
		{
			name: "enqueueReloads",
			prod: 12, cons: 10, cachedConsTail: 4, cachedProdTail: 12,
			call: enqueueOne, cache: consCache, wantCache: 10,
		},
		{
			name: "dequeueReloads",
			prod: 10, cons: 6, cachedConsTail: 6, cachedProdTail: 6,
			call: dequeueOne, cache: prodCache, wantCache: 10,
		},
		{
			// used is 5, which is capacity - n for n = 3.
			name: "enqueueUsedIsCapacityMinusN",
			prod: 10, cons: 10, cachedConsTail: 5, cachedProdTail: 10,
			call: enqueueThree, cache: consCache, wantCache: 5,
		},
		{
			// avail is 3, which is n.
			name: "dequeueAvailIsN",
			prod: 10, cons: 4, cachedConsTail: 4, cachedProdTail: 7,
			call: dequeueThree, cache: prodCache, wantCache: 7,
		},
		{
			// This breaks the invariant on purpose. With avail == capacity the only valid real
			// tail equals the cache, so the real producer tail is set to the cache minus 1. A
			// reload would store that lower value, so an unchanged cache shows no reload.
			name: "dequeueAvailIsCapacity",
			prod: 11, cons: 4, cachedConsTail: 4, cachedProdTail: 12,
			call: dequeueOne, cache: prodCache, wantCache: 12,
		},
		{
			name: "enqueueReloadAcrossWrap",
			prod: base + 8, cons: base + 4, cachedConsTail: base, cachedProdTail: base + 8,
			call: enqueueOne, cache: consCache, wantCache: base + 4,
		},
		{
			name: "dequeueReloadAcrossWrap",
			prod: base + 4, cons: base, cachedConsTail: base, cachedProdTail: base,
			call: dequeueOne, cache: prodCache, wantCache: base + 4,
		},
	})

	// setRelaxedIndices leaves each relaxed side's unused 32-bit head at 0, so a reload that read
	// it would store 0 instead of the other side's tail at pos.
	t.Run("relaxedReloadCachesOtherTail", func(t *testing.T) {
		const pos = 1000
		ring := newCacheTestRing(t, relaxedOptions())
		setRelaxedIndices(ring, pos, 7)

		require.ErrorIs(t, dequeueOne(ring), ErrEmpty)
		assert.Equal(t, uint32(pos), ring.cachedProdTail.Load())

		for range cacheTestCapacity {
			require.NoError(t, enqueueOne(ring))
		}
		require.ErrorIs(t, enqueueOne(ring), ErrFull)
		assert.Equal(t, uint32(pos), ring.cachedConsTail.Load())
	})
}

// TestCacheGuard checks the design's guard: an admission check that sees used above the capacity,
// or avail below n or above the capacity, reloads instead of admitting. Each case's call fails,
// and the cache ends equal to the real tail, which shows the reload.
//
// Every case breaks the cache invariant on purpose, as a multi side's out-of-order cache store or
// stale head can. A single side never reaches these states, but the check is the same in every
// mode, so every mode runs them.
func TestCacheGuard(t *testing.T) {
	cases := []cacheCase{
		{
			name: "enqueueCacheAheadOfHead",
			prod: 108, cons: 100, cachedConsTail: 109, cachedProdTail: 108,
			call: enqueueOne, wantErr: ErrFull, cache: consCache, wantCache: 100,
		},
		{
			name: "dequeueCacheTooFarAhead",
			prod: 100, cons: 100, cachedConsTail: 100, cachedProdTail: 100 + cacheTestCapacity + 1,
			call: dequeueOne, wantErr: ErrEmpty, cache: prodCache, wantCache: 100,
		},
		{
			name: "dequeueCacheBehindHead",
			prod: 100, cons: 100, cachedConsTail: 100, cachedProdTail: 99,
			call: dequeueOne, wantErr: ErrEmpty, cache: prodCache, wantCache: 100,
		},
	}
	for k := uint32(1); k <= cacheTestCapacity; k++ {
		cases = append(cases, cacheCase{
			name: fmt.Sprintf("enqueueCacheBackward/k=%d", k),
			prod: 108, cons: 100, cachedConsTail: 100 - k, cachedProdTail: 108,
			call: enqueueOne, wantErr: ErrFull, cache: consCache, wantCache: 100,
		})
	}
	runCacheCases(t, cases)
}

// TestFixedRequestReloads checks the design rule that a fixed request reloads before it refuses.
// An Enqueue and an EnqueueBulk that the producer's cache alone would refuse succeed when the ring
// has room, and a Dequeue and a DequeueBulk that the consumer's cache alone would refuse succeed
// when the ring has data.
func TestFixedRequestReloads(t *testing.T) {
	type fixedCall struct {
		name string
		call func(ring *Ring[int]) error
	}
	enqueues := []fixedCall{{"Enqueue", enqueueOne}, {"EnqueueBulk", enqueueThree}}
	dequeues := []fixedCall{{"Dequeue", dequeueOne}, {"DequeueBulk", dequeueThree}}
	for _, mode := range cacheModes() {
		t.Run(mode.name, func(t *testing.T) {
			for _, enqueue := range enqueues {
				t.Run(enqueue.name, func(t *testing.T) {
					ring := newCacheTestRing(t, mode.options)
					for range cacheTestCapacity {
						require.NoError(t, enqueueOne(ring))
					}
					for range 4 {
						require.NoError(t, dequeueOne(ring))
					}
					// The producer's cache still holds the consumer at 0, so it shows no room.
					require.Zero(t, ring.cachedConsTail.Load())
					require.NoError(t, enqueue.call(ring))
				})
			}
			for _, dequeue := range dequeues {
				t.Run(dequeue.name, func(t *testing.T) {
					ring := newCacheTestRing(t, mode.options)
					for range 4 {
						require.NoError(t, enqueueOne(ring))
						require.NoError(t, dequeueOne(ring))
					}
					for range 4 {
						require.NoError(t, enqueueOne(ring))
					}
					// The consumer's cache still holds the producer at 4, so it shows no data.
					require.Equal(t, uint32(4), ring.cachedProdTail.Load())
					require.NoError(t, dequeue.call(ring))
				})
			}
		})
	}
}

// TestBurstReloads checks the design rule that a Burst call whose cache says fewer elements fit
// reloads, and moves as many as really fit.
func TestBurstReloads(t *testing.T) {
	for _, mode := range cacheModes() {
		t.Run(mode.name, func(t *testing.T) {
			t.Run("EnqueueBurst", func(t *testing.T) {
				ring := newCacheTestRing(t, mode.options)
				for range 6 {
					require.NoError(t, enqueueOne(ring))
					require.NoError(t, dequeueOne(ring))
				}
				// Every enqueue fit the cache, so it still holds the consumer at 0 and shows 2
				// free slots, while all 8 are free.
				require.Zero(t, ring.cachedConsTail.Load())
				enqueued, _ := ring.EnqueueBurst(make([]int, 5))
				assert.Equal(t, 5, enqueued)
			})
			t.Run("DequeueBurst", func(t *testing.T) {
				ring := newCacheTestRing(t, mode.options)
				for range 2 {
					require.NoError(t, enqueueOne(ring))
				}
				require.NoError(t, dequeueOne(ring))
				for range 6 {
					require.NoError(t, enqueueOne(ring))
				}
				// The consumer's cache holds the producer at 2 and shows 1 entry, while the ring
				// holds 7.
				require.Equal(t, uint32(2), ring.cachedProdTail.Load())
				dequeued, _ := ring.DequeueBurst(make([]int, 5))
				assert.Equal(t, 5, dequeued)
			})
		})
	}
}

// TestStaleCacheNeverOverfills checks the design rule that a stale cache never lets a producer
// overwrite live elements: after filling the ring and dequeuing some, the producer's cache is
// stale, and enqueuing until ErrFull adds exactly as many as were dequeued.
func TestStaleCacheNeverOverfills(t *testing.T) {
	const drained = 3
	for _, mode := range cacheModes() {
		t.Run(mode.name, func(t *testing.T) {
			ring := newCacheTestRing(t, mode.options)
			var want []int
			for value := range cacheTestCapacity {
				require.NoError(t, ring.Enqueue(value))
				want = append(want, value)
			}
			require.ErrorIs(t, ring.Enqueue(-1), ErrFull)
			for _, value := range want[:drained] {
				got, err := ring.Dequeue()
				require.NoError(t, err)
				require.Equal(t, value, got)
			}
			want = want[drained:]

			refilled := 0
			for ; refilled <= cacheTestCapacity; refilled++ {
				value := 100 + refilled
				if err := ring.Enqueue(value); err != nil {
					require.ErrorIs(t, err, ErrFull)
					break
				}
				want = append(want, value)
			}
			assert.Equal(t, drained, refilled)

			got := make([]int, cacheTestCapacity)
			_, err := ring.DequeueBulk(got)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

// TestStaleCacheNeverReadsUnpublished checks the design rule that a stale cache never lets a
// consumer return unpublished slots: after the consumer's cache is refreshed and the producer
// enqueues more, dequeuing until ErrEmpty returns exactly the elements enqueued, in order.
func TestStaleCacheNeverReadsUnpublished(t *testing.T) {
	for _, mode := range cacheModes() {
		t.Run(mode.name, func(t *testing.T) {
			ring := newCacheTestRing(t, mode.options)
			for value := range 5 {
				require.NoError(t, ring.Enqueue(value))
			}
			for value := range 2 {
				got, err := ring.Dequeue()
				require.NoError(t, err)
				require.Equal(t, value, got)
			}
			for value := 5; value < 8; value++ {
				require.NoError(t, ring.Enqueue(value))
			}

			var got []int
			for range cacheTestCapacity + 1 {
				value, err := ring.Dequeue()
				if err != nil {
					require.ErrorIs(t, err, ErrEmpty)
					break
				}
				got = append(got, value)
			}
			assert.Equal(t, []int{2, 3, 4, 5, 6, 7}, got)
		})
	}
}

// staleProducerCache leaves ring empty with the producer's cache still holding the consumer at 0,
// so the cache shows no free slots while all of them are free.
func staleProducerCache(t *testing.T, ring *Ring[int]) {
	t.Helper()
	for range cacheTestCapacity {
		require.NoError(t, enqueueOne(ring))
	}
	for range cacheTestCapacity {
		require.NoError(t, dequeueOne(ring))
	}
	require.Zero(t, ring.cachedConsTail.Load())
}

// staleConsumerCache leaves ring holding 5 elements with the consumer's cache still holding the
// producer at 1, so the cache shows no entries.
func staleConsumerCache(t *testing.T, ring *Ring[int]) {
	t.Helper()
	require.NoError(t, enqueueOne(ring))
	require.NoError(t, dequeueOne(ring))
	for range 5 {
		require.NoError(t, enqueueOne(ring))
	}
	require.Equal(t, uint32(1), ring.cachedProdTail.Load())
}

// TestZeroLengthFresh checks the design rule that a zero-length batch call always loads the real
// tail: with a stale cache, each batch call returns the fresh count, by default and under
// WithExactBatchCounts, in each cache mode.
func TestZeroLengthFresh(t *testing.T) {
	calls := []struct {
		name  string
		stale func(t *testing.T, ring *Ring[int])
		count func(t *testing.T, ring *Ring[int]) int
		want  int
	}{
		{
			name:  "EnqueueBulk",
			stale: staleProducerCache,
			count: func(t *testing.T, ring *Ring[int]) int {
				free, err := ring.EnqueueBulk(nil)
				require.NoError(t, err)
				return free
			},
			want: cacheTestCapacity,
		},
		{
			name:  "EnqueueBurst",
			stale: staleProducerCache,
			count: func(t *testing.T, ring *Ring[int]) int {
				enqueued, free := ring.EnqueueBurst(nil)
				require.Zero(t, enqueued)
				return free
			},
			want: cacheTestCapacity,
		},
		{
			name:  "DequeueBulk",
			stale: staleConsumerCache,
			count: func(t *testing.T, ring *Ring[int]) int {
				available, err := ring.DequeueBulk(nil)
				require.NoError(t, err)
				return available
			},
			want: 5,
		},
		{
			name:  "DequeueBurst",
			stale: staleConsumerCache,
			count: func(t *testing.T, ring *Ring[int]) int {
				dequeued, available := ring.DequeueBurst(nil)
				require.Zero(t, dequeued)
				return available
			},
			want: 5,
		},
	}
	for _, mode := range cacheModes() {
		t.Run(mode.name, func(t *testing.T) {
			for _, set := range cachedAndExact() {
				t.Run(set.name, func(t *testing.T) {
					options := slices.Concat(mode.options, set.options)
					for _, call := range calls {
						t.Run(call.name, func(t *testing.T) {
							ring := newCacheTestRing(t, options)
							call.stale(t, ring)
							assert.Equal(t, call.want, call.count(t, ring))
						})
					}
				})
			}
		})
	}
}

// TestCachedBatchCounts checks the design's cached counts. The quiet-ring example returns the
// stale count by default and the fresh one under WithExactBatchCounts, in each cache mode. A call
// that reloads returns a fresh count. On a single own side, every count a sequence of calls returns
// is at most the real value after the call.
func TestCachedBatchCounts(t *testing.T) {
	const (
		quietCapacity = 1024
		quietMoved    = 1000
	)
	// quietRing enqueues and dequeues quietMoved elements in one batch each, so the producer's
	// cache still holds the consumer at 0.
	quietRing := func(t *testing.T, options ...Option) *Ring[int] {
		t.Helper()
		ring, err := New[int](quietCapacity, options...)
		require.NoError(t, err)
		_, err = ring.EnqueueBulk(make([]int, quietMoved))
		require.NoError(t, err)
		_, err = ring.DequeueBulk(make([]int, quietMoved))
		require.NoError(t, err)
		return ring
	}

	t.Run("quietRing", func(t *testing.T) {
		wantFree := map[string]int{
			"cached": quietCapacity - quietMoved - 1,
			"exact":  quietCapacity - 1,
		}
		for _, mode := range cacheModes() {
			t.Run(mode.name, func(t *testing.T) {
				for _, set := range cachedAndExact() {
					t.Run(set.name, func(t *testing.T) {
						ring := quietRing(t, slices.Concat(mode.options, set.options)...)
						enqueued, free := ring.EnqueueBurst([]int{1})
						require.Equal(t, 1, enqueued)
						assert.Equal(t, wantFree[set.name], free)
					})
				}
			})
		}
	})

	t.Run("reloadIsFresh", func(t *testing.T) {
		ring := quietRing(t)
		_, err := ring.EnqueueBulk([]int{1})
		require.NoError(t, err)
		// The cache shows 23 free slots, so a burst of 24 reloads.
		enqueued, free := ring.EnqueueBurst(make([]int, 24))
		require.Equal(t, 24, enqueued)
		assert.Equal(t, ring.Free(), free)
	})

	t.Run("singleSideAtMostReal", func(t *testing.T) {
		ring, err := New[int](16)
		require.NoError(t, err)
		for step := range 200 {
			_, free := ring.EnqueueBurst(make([]int, step%7+1))
			require.LessOrEqual(t, free, ring.Free(), "EnqueueBurst at step %d", step)
			free, _ = ring.EnqueueBulk(make([]int, step%3+1))
			require.LessOrEqual(t, free, ring.Free(), "EnqueueBulk at step %d", step)
			_, available := ring.DequeueBurst(make([]int, step%5+1))
			require.LessOrEqual(t, available, ring.Count(), "DequeueBurst at step %d", step)
			available, _ = ring.DequeueBulk(make([]int, step%4+1))
			require.LessOrEqual(t, available, ring.Count(), "DequeueBulk at step %d", step)
		}
	})
}

// TestCountIgnoresCaches checks the design rule that nothing outside the head moves reads a cache:
// on a ring whose caches are stale, Count, Free, freeEntries, and occupiedEntries return the real
// values.
func TestCountIgnoresCaches(t *testing.T) {
	for _, mode := range cacheModes() {
		t.Run(mode.name, func(t *testing.T) {
			ring := newCacheTestRing(t, mode.options)
			// The ring holds 6 elements. The caches show 0 free slots and 1 entry.
			setCacheState(ring, 10, 4, 2, 5)
			assert.Equal(t, 6, ring.Count())
			assert.Equal(t, 2, ring.Free())
			assert.Equal(t, uint32(2), ring.freeEntries())
			assert.Equal(t, uint32(6), ring.occupiedEntries())
		})
	}
}

// TestHelpersSetCaches checks the design rule that anything that sets the indices sets the caches
// to match: after setIndices and setRelaxedIndices, each cache equals the tail it copies.
func TestHelpersSetCaches(t *testing.T) {
	for _, mode := range cacheModes() {
		t.Run(mode.name, func(t *testing.T) {
			ring := newCacheTestRing(t, mode.options)
			setIndices(ring, 1234)
			assert.Equal(t, ring.consTail.Load(), ring.cachedConsTail.Load(), "setIndices")
			assert.Equal(t, ring.prodTail.Load(), ring.cachedProdTail.Load(), "setIndices")

			setRelaxedIndices(ring, math.MaxUint32-5, 7)
			assert.Equal(t, ring.consTail.Load(), ring.cachedConsTail.Load(), "setRelaxedIndices")
			assert.Equal(t, ring.prodTail.Load(), ring.cachedProdTail.Load(), "setRelaxedIndices")
		})
	}
}

// singleCalls returns batchCalls that move one value at a time through ring with Enqueue and
// Dequeue.
func singleCalls(ring *Ring[uint64]) (enqueue, dequeue batchCall) {
	enqueue = func(items []uint64) int {
		if ring.Enqueue(items[0]) != nil {
			return 0
		}
		return 1
	}
	dequeue = func(dst []uint64) int {
		value, err := ring.Dequeue()
		if err != nil {
			return 0
		}
		dst[0] = value
		return 1
	}
	return enqueue, dequeue
}

// TestCachedStress moves tagged values through small rings in every stress mode, with
// single-element calls and with burst calls, so the cache reloads and the guard run often under
// contention. The 64-slot ring starts its indices and caches just below 2^32, so they wrap while
// the goroutines contend.
func TestCachedStress(t *testing.T) {
	const (
		total = 20_000
		batch = 3
	)
	rings := []struct {
		name  string
		count int
		wrap  bool
	}{
		{name: "slots=4", count: 4},
		{name: "slots=64,wrap", count: 64, wrap: true},
	}
	callKinds := []struct {
		name  string
		batch int
		calls func(ring *Ring[uint64]) (enqueue, dequeue batchCall)
	}{
		{name: "single", batch: 1, calls: singleCalls},
		{name: "burst", batch: batch, calls: burstCalls},
	}
	const startIndex = math.MaxUint32 - 999
	for _, layout := range rings {
		t.Run(layout.name, func(t *testing.T) {
			for _, mode := range stressModes() {
				for _, kind := range callKinds {
					t.Run(mode.name+"/"+kind.name, func(t *testing.T) {
						ring, err := New[uint64](layout.count, mode.options...)
						require.NoError(t, err)
						if layout.wrap {
							setRelaxedIndices(ring, startIndex, math.MaxUint32-99)
						}
						enqueue, dequeue := kind.calls(ring)

						perProducer := total / mode.producers
						results := sendTagged(
							t,
							mode.producers,
							mode.consumers,
							perProducer,
							kind.batch,
							kind.batch,
							enqueue,
							dequeue,
						)
						assertTaggedDelivery(t, results, mode.producers, perProducer)
						if layout.wrap {
							assert.Less(t, ring.consTail.Load(), uint32(startIndex))
						}
					})
				}
			}
		})
	}
}
