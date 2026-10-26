package dpdk_test

import (
	"fmt"
	"testing"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
)

type mpscSingleQueue[T any] struct{ ring *dpdk.MPSCQueue[T] }

func (q *mpscSingleQueue[T]) enqueue(items []T) int {
	return movedOrPanic(q.ring.Enqueue(items[0]), dpdk.ErrFull, 1)
}

func (q *mpscSingleQueue[T]) dequeue(dst []T) int {
	value, err := q.ring.Dequeue()
	if movedOrPanic(err, dpdk.ErrEmpty, 1) == 0 {
		return 0
	}
	dst[0] = value
	return 1
}

type mpscBulkQueue[T any] struct{ ring *dpdk.MPSCQueue[T] }

func (q *mpscBulkQueue[T]) enqueue(items []T) int {
	_, err := q.ring.EnqueueBulk(items)
	return movedOrPanic(err, dpdk.ErrFull, len(items))
}

func (q *mpscBulkQueue[T]) dequeue(dst []T) int {
	_, err := q.ring.DequeueBulk(dst)
	return movedOrPanic(err, dpdk.ErrEmpty, len(dst))
}

type mpscBurstQueue[T any] struct{ ring *dpdk.MPSCQueue[T] }

func (q *mpscBurstQueue[T]) enqueue(items []T) int {
	moved, _ := q.ring.EnqueueBurst(items)
	return moved
}

func (q *mpscBurstQueue[T]) dequeue(dst []T) int {
	moved, _ := q.ring.DequeueBurst(dst)
	return moved
}

func newMPSCAPIQueue[T any](api ringAPI) (queue[T], error) {
	created, err := dpdk.NewMPSCQueue[T](ringCapacity)
	if err != nil {
		return nil, err
	}
	switch api {
	case apiSingle:
		return &mpscSingleQueue[T]{ring: created}, nil
	case apiBulk:
		return &mpscBulkQueue[T]{ring: created}, nil
	case apiBurst:
		return &mpscBurstQueue[T]{ring: created}, nil
	default:
		return nil, fmt.Errorf("unknown MPSC API %v", api)
	}
}

func newMPSCComparisonQueue[T any](implementation string, api ringAPI) (queue[T], error) {
	switch implementation {
	case "ring":
		return newRingAPIQueue[T](api, mpscMode)
	case "mpsc":
		return newMPSCAPIQueue[T](api)
	default:
		return nil, fmt.Errorf("unknown MPSC comparison implementation %q", implementation)
	}
}

// BenchmarkMPSCComparison measures verified transfers with four producers and one consumer.
func BenchmarkMPSCComparison(b *testing.B) {
	benchmarkComparison(
		b,
		mpscComparison,
		newMPSCComparisonQueue[int64],
		newMPSCComparisonQueue[*item],
	)
}
