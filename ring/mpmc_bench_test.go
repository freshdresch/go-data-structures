package ring_test

import (
	"fmt"
	"testing"

	"github.com/freshdresch/go-data-structures/ring"
)

type mpmcSingleQueue[T any] struct{ ring *ring.MPMCQueue[T] }

func (q *mpmcSingleQueue[T]) enqueue(items []T) int {
	return movedOrPanic(q.ring.Enqueue(items[0]), ring.ErrFull, 1)
}

func (q *mpmcSingleQueue[T]) dequeue(dst []T) int {
	value, err := q.ring.Dequeue()
	if movedOrPanic(err, ring.ErrEmpty, 1) == 0 {
		return 0
	}
	dst[0] = value
	return 1
}

type mpmcBulkQueue[T any] struct{ ring *ring.MPMCQueue[T] }

func (q *mpmcBulkQueue[T]) enqueue(items []T) int {
	_, err := q.ring.EnqueueBulk(items)
	return movedOrPanic(err, ring.ErrFull, len(items))
}

func (q *mpmcBulkQueue[T]) dequeue(dst []T) int {
	_, err := q.ring.DequeueBulk(dst)
	return movedOrPanic(err, ring.ErrEmpty, len(dst))
}

type mpmcBurstQueue[T any] struct{ ring *ring.MPMCQueue[T] }

func (q *mpmcBurstQueue[T]) enqueue(items []T) int {
	moved, _ := q.ring.EnqueueBurst(items)
	return moved
}

func (q *mpmcBurstQueue[T]) dequeue(dst []T) int {
	moved, _ := q.ring.DequeueBurst(dst)
	return moved
}

func newMPMCAPIQueue[T any](api ringAPI) (queue[T], error) {
	created, err := ring.NewMPMCQueue[T](ringCapacity)
	if err != nil {
		return nil, err
	}
	switch api {
	case apiSingle:
		return &mpmcSingleQueue[T]{ring: created}, nil
	case apiBulk:
		return &mpmcBulkQueue[T]{ring: created}, nil
	case apiBurst:
		return &mpmcBurstQueue[T]{ring: created}, nil
	default:
		return nil, fmt.Errorf("unknown MPMC API %v", api)
	}
}

func newMPMCComparisonQueue[T any](implementation string, api ringAPI) (queue[T], error) {
	switch implementation {
	case "ring":
		return newRingAPIQueue[T](api, mpmcMode)
	case "mpmc":
		return newMPMCAPIQueue[T](api)
	default:
		return nil, fmt.Errorf("unknown MPMC comparison implementation %q", implementation)
	}
}

// BenchmarkMPMCComparison measures verified transfers with four producers and four consumers.
func BenchmarkMPMCComparison(b *testing.B) {
	benchmarkComparison(
		b,
		mpmcComparison,
		newMPMCComparisonQueue[int64],
		newMPMCComparisonQueue[*item],
	)
}
