package dpdk_test

import (
	"fmt"
	"testing"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
)

type spmcSingleQueue[T any] struct{ ring *dpdk.SPMCQueue[T] }

func (q *spmcSingleQueue[T]) enqueue(items []T) int {
	return movedOrPanic(q.ring.Enqueue(items[0]), dpdk.ErrFull, 1)
}

func (q *spmcSingleQueue[T]) dequeue(dst []T) int {
	value, err := q.ring.Dequeue()
	if movedOrPanic(err, dpdk.ErrEmpty, 1) == 0 {
		return 0
	}
	dst[0] = value
	return 1
}

type spmcBulkQueue[T any] struct{ ring *dpdk.SPMCQueue[T] }

func (q *spmcBulkQueue[T]) enqueue(items []T) int {
	_, err := q.ring.EnqueueBulk(items)
	return movedOrPanic(err, dpdk.ErrFull, len(items))
}

func (q *spmcBulkQueue[T]) dequeue(dst []T) int {
	_, err := q.ring.DequeueBulk(dst)
	return movedOrPanic(err, dpdk.ErrEmpty, len(dst))
}

type spmcBurstQueue[T any] struct{ ring *dpdk.SPMCQueue[T] }

func (q *spmcBurstQueue[T]) enqueue(items []T) int {
	moved, _ := q.ring.EnqueueBurst(items)
	return moved
}

func (q *spmcBurstQueue[T]) dequeue(dst []T) int {
	moved, _ := q.ring.DequeueBurst(dst)
	return moved
}

func newSPMCAPIQueue[T any](api ringAPI) (queue[T], error) {
	created, err := dpdk.NewSPMCQueue[T](ringCapacity)
	if err != nil {
		return nil, err
	}
	switch api {
	case apiSingle:
		return &spmcSingleQueue[T]{ring: created}, nil
	case apiBulk:
		return &spmcBulkQueue[T]{ring: created}, nil
	case apiBurst:
		return &spmcBurstQueue[T]{ring: created}, nil
	default:
		return nil, fmt.Errorf("unknown SPMC API %v", api)
	}
}

func newSPMCComparisonQueue[T any](implementation string, api ringAPI) (queue[T], error) {
	switch implementation {
	case "ring":
		return newRingAPIQueue[T](api, spmcMode)
	case "spmc":
		return newSPMCAPIQueue[T](api)
	default:
		return nil, fmt.Errorf("unknown SPMC comparison implementation %q", implementation)
	}
}

// BenchmarkSPMCComparison measures verified transfers with one producer and four consumers.
func BenchmarkSPMCComparison(b *testing.B) {
	benchmarkComparison(
		b,
		spmcComparison,
		newSPMCComparisonQueue[int64],
		newSPMCComparisonQueue[*item],
	)
}
