package dpdk_test

import (
	"fmt"
	"testing"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
)

type spscQueue[T any] struct {
	ring *dpdk.SPSCQueue[T]
}

func (q *spscQueue[T]) enqueue(items []T) int {
	moved, _ := q.ring.EnqueueBurst(items)
	return moved
}

func (q *spscQueue[T]) dequeue(dst []T) int {
	moved, _ := q.ring.DequeueBurst(dst)
	return moved
}

func newSPSCAPIQueue[T any](api ringAPI) (queue[T], error) {
	created, err := dpdk.NewSPSCQueue[T](ringCapacity)
	if err != nil {
		return nil, err
	}
	switch api {
	case apiSingle:
		return &spscSingleQueue[T]{ring: created}, nil
	case apiBulk:
		return &spscBulkQueue[T]{ring: created}, nil
	case apiBurst:
		return &spscQueue[T]{ring: created}, nil
	default:
		return nil, fmt.Errorf("unknown SPSC API %v", api)
	}
}

// spscSingleQueue drives the SPSC ring through Enqueue and Dequeue, moving at most one element per
// call. It is rewritten when the SPSC ring API changes.
type spscSingleQueue[T any] struct {
	ring *dpdk.SPSCQueue[T]
}

// enqueue moves only items[0], so it returns 0 or 1.
func (q *spscSingleQueue[T]) enqueue(items []T) int {
	return movedOrPanic(q.ring.Enqueue(items[0]), dpdk.ErrFull, 1)
}

// dequeue fills only dst[0], so it returns 0 or 1.
func (q *spscSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if movedOrPanic(err, dpdk.ErrEmpty, 1) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

// spscBulkQueue drives the SPSC ring through EnqueueBulk and DequeueBulk, which move all of the
// elements or none, so each call returns 0 or the full length.
type spscBulkQueue[T any] struct {
	ring *dpdk.SPSCQueue[T]
}

func (q *spscBulkQueue[T]) enqueue(items []T) int {
	_, err := q.ring.EnqueueBulk(items)
	return movedOrPanic(err, dpdk.ErrFull, len(items))
}

func (q *spscBulkQueue[T]) dequeue(dst []T) int {
	_, err := q.ring.DequeueBulk(dst)
	return movedOrPanic(err, dpdk.ErrEmpty, len(dst))
}

func benchmarkSPSCAPIRun[T any](
	b *testing.B,
	mode benchMode,
	api ringAPI,
	batch int,
	fill fillFunc[T],
) {
	setElemBytes[T](b)
	total := api.total(b.N, batch, mode.producers)
	newQueue := func() (queue[T], error) {
		return newSPSCAPIQueue[T](api)
	}
	result, err := runThroughput(
		b,
		newQueue,
		mode.producers,
		mode.consumers,
		batch,
		total,
		fill,
		noopSinks[T],
	)
	if err != nil {
		b.Fatal(err)
	}
	reportConcurrentAPIMetrics(b, api, result, total)
}

// BenchmarkSPSCAPI reports verified transfer time through the shared queue interface.
func BenchmarkSPSCAPI(b *testing.B) {
	b.Run("mode=SPSC", func(b *testing.B) {
		for _, api := range ringAPIs {
			b.Run("api="+api.String(), func(b *testing.B) {
				for _, batch := range []int{1, 32} {
					if api == apiSingle && batch != 1 {
						continue
					}
					run := func(b *testing.B) {
						b.Run("elem=int64", func(b *testing.B) {
							benchmarkSPSCAPIRun(b, spscMode, api, batch, fillInt64)
						})
						b.Run("elem=ptr", func(b *testing.B) {
							benchmarkSPSCAPIRun(b, spscMode, api, batch, fillPtr(newItemPool()))
						})
					}
					if api == apiSingle {
						run(b)
						continue
					}
					b.Run(fmt.Sprintf("batch=%d", batch), run)
				}
			})
		}
	})
}

func spscComparisonQueues() []adapterImpl[int64] {
	return []adapterImpl[int64]{
		{name: "ring", newQueue: func() (queue[int64], error) {
			return newRingAPIQueue[int64](apiBurst, spscMode)
		}},
		{name: "spsc", newQueue: func() (queue[int64], error) {
			return newSPSCAPIQueue[int64](apiBurst)
		}},
	}
}

// BenchmarkSPSCComparisonHandoff reports the sampled mean full round trip as ns/op over two
// queues of each implementation, from immediately before enqueue to immediately after the reply.
// Samples exclude sender preparation, waiter reset, histogram recording, and counters. Warm-up is
// unrecorded, the outer Go timer calibrates iterations, and handoff percentiles estimate half a
// round trip.
func BenchmarkSPSCComparisonHandoff(b *testing.B) {
	for _, impl := range spscComparisonQueues() {
		b.Run("impl="+impl.name, func(b *testing.B) {
			result, err := runHandoff(b, impl.newQueue, b.N)
			if err != nil {
				b.Fatal(err)
			}
			if result.recorded != b.N {
				b.Fatalf("recorded %d round trips, want %d", result.recorded, b.N)
			}
			reportHandoff(b, result)
		})
	}
}
