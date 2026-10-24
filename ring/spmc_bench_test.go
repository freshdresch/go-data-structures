package ring_test

import (
	"encoding/json"
	"fmt"
	"runtime"
	"testing"

	"github.com/freshdresch/go-data-structures/ring"
)

type spmcSingleQueue[T any] struct{ ring *ring.SPMC[T] }

func (q *spmcSingleQueue[T]) enqueue(items []T) int {
	return movedOrPanic(q.ring.Enqueue(items[0]), ring.ErrFull, 1)
}

func (q *spmcSingleQueue[T]) dequeue(dst []T) int {
	value, err := q.ring.Dequeue()
	if movedOrPanic(err, ring.ErrEmpty, 1) == 0 {
		return 0
	}
	dst[0] = value
	return 1
}

type spmcBulkQueue[T any] struct{ ring *ring.SPMC[T] }

func (q *spmcBulkQueue[T]) enqueue(items []T) int {
	_, err := q.ring.EnqueueBulk(items)
	return movedOrPanic(err, ring.ErrFull, len(items))
}

func (q *spmcBulkQueue[T]) dequeue(dst []T) int {
	_, err := q.ring.DequeueBulk(dst)
	return movedOrPanic(err, ring.ErrEmpty, len(dst))
}

type spmcBurstQueue[T any] struct{ ring *ring.SPMC[T] }

func (q *spmcBurstQueue[T]) enqueue(items []T) int {
	moved, _ := q.ring.EnqueueBurst(items)
	return moved
}

func (q *spmcBurstQueue[T]) dequeue(dst []T) int {
	moved, _ := q.ring.DequeueBurst(dst)
	return moved
}

func newSPMCAPIQueue[T any](api ringAPI) (queue[T], error) {
	created, err := ring.NewSPMC[T](ringCapacity)
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
	for _, implementation := range []string{"ring", "spmc"} {
		b.Run("impl="+implementation+"/mode=SPMC", func(b *testing.B) {
			for _, api := range []ringAPI{apiSingle, apiBulk, apiBurst} {
				b.Run("api="+api.String(), func(b *testing.B) {
					for _, batch := range []int{1, 32} {
						if api == apiSingle && batch != 1 {
							continue
						}
						run := func(b *testing.B) {
							b.Run("elem=int64", func(b *testing.B) {
								benchmarkSPMCComparisonRun(b, implementation, api, batch, fillInt64)
							})
							b.Run("elem=ptr", func(b *testing.B) {
								benchmarkSPMCComparisonRun(b, implementation, api, batch, fillPtr(newItemPool()))
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

func benchmarkSPMCComparisonRun[T any](
	b *testing.B,
	implementation string,
	api ringAPI,
	batch int,
	fill fillFunc[T],
) {
	setElemBytes[T](b)
	total := api.total(b.N, batch, 1)
	var actual queue[T]
	factory := func() (queue[T], error) {
		created, err := newSPMCComparisonQueue[T](implementation, api)
		actual = created
		return created, err
	}
	result, err := runThroughput(b, factory, 1, 4, batch, total, fill, noopSinks[T])
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
	b.Log("SPMC_COMPARISON_LAYOUT " + string(encoded))
	runtime.KeepAlive(actual)
}
