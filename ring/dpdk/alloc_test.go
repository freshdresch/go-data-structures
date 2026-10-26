package dpdk_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
)

const (
	// allocRuns is how many runs testing.AllocsPerRun averages over.
	allocRuns = 100
	// allocCalls is how many times testing.AllocsPerRun calls the measured function, one
	// warm-up call and then allocRuns measured ones, so every setup has room for this many.
	allocCalls = allocRuns + 1
	// allocBatch is how many elements each burst and bulk call moves.
	allocBatch = 8
)

// TestAllocs checks how many allocations each ring call makes, on its success path and on its
// bare ErrFull and ErrEmpty paths, none of which allocate. It skips under -race, because the race
// detector changes allocation behavior.
func TestAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	t.Run("elem=int64", func(t *testing.T) {
		testAllocs(t, fillInt64)
	})
	t.Run("elem=ptr", func(t *testing.T) {
		testAllocs(t, fillPtr(newItemPool()))
	})
}

// testAllocs runs the allocation cases for every API with elements fill writes.
func testAllocs[T any](t *testing.T, fill fillFunc[T]) {
	for _, api := range ringAPIs {
		t.Run("api="+api.String(), func(t *testing.T) {
			batch := allocBatch
			if api == apiSingle {
				batch = 1
			}
			elems := make([]T, batch)
			fill(elems, 0, 0)

			t.Run("enqueue", func(t *testing.T) {
				_, enqueue, _ := newAllocRing(t, api, elems)
				allocs, moved := measureAllocs(enqueue)
				assert.Equal(t, allocCalls*batch, moved)
				assert.Zero(t, allocs)
			})

			t.Run("dequeue", func(t *testing.T) {
				created, _, dequeue := newAllocRing(t, api, elems)
				fillRing(t, created, fill, allocCalls*batch)
				allocs, moved := measureAllocs(dequeue)
				assert.Equal(t, allocCalls*batch, moved)
				assert.Zero(t, allocs)
			})

			t.Run("errFull", func(t *testing.T) {
				created, enqueue, _ := newAllocRing(t, api, elems)
				fillRing(t, created, fill, created.Capacity())
				allocs, moved := measureAllocs(enqueue)
				assert.Zero(t, moved)
				assert.Zero(t, allocs)
			})

			t.Run("errEmpty", func(t *testing.T) {
				_, _, dequeue := newAllocRing(t, api, elems)
				allocs, moved := measureAllocs(dequeue)
				assert.Zero(t, moved)
				assert.Zero(t, allocs)
			})
		})
	}
}

// newAllocRing returns an empty SPSC ring with functions that each make one api call on it. The
// enqueue call sends elems, and the dequeue call dequeues into a buffer of len(elems). Each
// returns how many elements it moved, treating ErrFull and ErrEmpty as 0 moved, and panics on any
// other error.
func newAllocRing[T any](
	t *testing.T,
	api ringAPI,
	elems []T,
) (
	created *dpdk.Ring[T],
	enqueue, dequeue func() int,
) {
	t.Helper()
	created, err := newModeRing[T](spscMode)
	require.NoError(t, err)
	dst := make([]T, len(elems))
	switch api {
	case apiSingle:
		enqueue = func() int {
			return movedOrPanic(created.Enqueue(elems[0]), dpdk.ErrFull, 1)
		}
		dequeue = func() int {
			_, err := created.Dequeue()
			return movedOrPanic(err, dpdk.ErrEmpty, 1)
		}
	case apiBurst:
		enqueue = func() int {
			enqueued, _ := created.EnqueueBurst(elems)
			return enqueued
		}
		dequeue = func() int {
			dequeued, _ := created.DequeueBurst(dst)
			return dequeued
		}
	case apiBulk:
		enqueue = func() int {
			_, err := created.EnqueueBulk(elems)
			return movedOrPanic(err, dpdk.ErrFull, len(elems))
		}
		dequeue = func() int {
			_, err := created.DequeueBulk(dst)
			return movedOrPanic(err, dpdk.ErrEmpty, len(dst))
		}
	default:
		t.Fatalf("unknown ring API %v", api)
	}
	return created, enqueue, dequeue
}

// fillRing enqueues n elements that fill writes into target, before a measurement.
func fillRing[T any](t *testing.T, target *dpdk.Ring[T], fill fillFunc[T], n int) {
	t.Helper()
	elems := make([]T, n)
	fill(elems, 0, 0)
	_, err := target.EnqueueBulk(elems)
	require.NoError(t, err)
}

// measureAllocs returns the average allocations per call of call over allocRuns runs, and how
// many elements every call, the warm-up call included, moved in total.
func measureAllocs(call func() int) (allocs float64, moved int) {
	allocs = testing.AllocsPerRun(allocRuns, func() {
		moved += call()
	})
	return allocs, moved
}
