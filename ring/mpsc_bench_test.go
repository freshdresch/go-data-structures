package ring_test

import (
	"encoding/json"
	"fmt"
	"runtime"
	"testing"

	"github.com/freshdresch/go-data-structures/ring"
)

type mpscSingleQueue[T any] struct{ ring *ring.MPSCQueue[T] }

func (q *mpscSingleQueue[T]) enqueue(items []T) int {
	return movedOrPanic(q.ring.Enqueue(items[0]), ring.ErrFull, 1)
}

func (q *mpscSingleQueue[T]) dequeue(dst []T) int {
	value, err := q.ring.Dequeue()
	if movedOrPanic(err, ring.ErrEmpty, 1) == 0 {
		return 0
	}
	dst[0] = value
	return 1
}

type mpscBulkQueue[T any] struct{ ring *ring.MPSCQueue[T] }

func (q *mpscBulkQueue[T]) enqueue(items []T) int {
	_, err := q.ring.EnqueueBulk(items)
	return movedOrPanic(err, ring.ErrFull, len(items))
}

func (q *mpscBulkQueue[T]) dequeue(dst []T) int {
	_, err := q.ring.DequeueBulk(dst)
	return movedOrPanic(err, ring.ErrEmpty, len(dst))
}

type mpscBurstQueue[T any] struct{ ring *ring.MPSCQueue[T] }

func (q *mpscBurstQueue[T]) enqueue(items []T) int {
	moved, _ := q.ring.EnqueueBurst(items)
	return moved
}

func (q *mpscBurstQueue[T]) dequeue(dst []T) int {
	moved, _ := q.ring.DequeueBurst(dst)
	return moved
}

func newMPSCAPIQueue[T any](api ringAPI) (queue[T], error) {
	created, err := ring.NewMPSCQueue[T](ringCapacity)
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
	for _, implementation := range []string{"ring", "mpsc"} {
		b.Run("impl="+implementation+"/mode=MPSC", func(b *testing.B) {
			for _, api := range []ringAPI{apiSingle, apiBulk, apiBurst} {
				b.Run("api="+api.String(), func(b *testing.B) {
					for _, batch := range []int{1, 32} {
						if api == apiSingle && batch != 1 {
							continue
						}
						run := func(b *testing.B) {
							b.Run("elem=int64", func(b *testing.B) {
								benchmarkMPSCComparisonRun(b, implementation, api, batch, fillInt64)
							})
							b.Run("elem=ptr", func(b *testing.B) {
								benchmarkMPSCComparisonRun(b, implementation, api, batch, fillPtr(newItemPool()))
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
}

func benchmarkMPSCComparisonRun[T any](
	b *testing.B,
	implementation string,
	api ringAPI,
	batch int,
	fill fillFunc[T],
) {
	setElemBytes[T](b)
	total := api.total(b.N, batch, 4)
	var actual queue[T]
	factory := func() (queue[T], error) {
		created, err := newMPSCComparisonQueue[T](implementation, api)
		actual = created
		return created, err
	}
	result, err := runThroughput(b, factory, 4, 1, batch, total, fill, noopSinks[T])
	if err != nil {
		b.Fatal(err)
	}
	reportThroughput(b, result, total)
	nsPerElem, elemPerOp, err := ringAPIMetrics(b.Elapsed(), b.N, total, result.received)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(nsPerElem, "ns/elem")
	if api == apiBulk {
		b.ReportMetric(elemPerOp, "elem/op")
	}
	// runThroughput has stopped the timer before any layout inspection or serialization.
	evidence := spmcComparisonLayoutRecord{
		Name: b.Name(), Iterations: b.N, VerifiedTotal: result.received,
		Layout: spmcComparisonQueueLayout(actual, "actual benchmark queue"),
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		b.Fatal(err)
	}
	b.Log("MPSC_COMPARISON_LAYOUT " + string(encoded))
	runtime.KeepAlive(actual)
}
