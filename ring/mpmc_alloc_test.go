package ring

import (
	"fmt"
	"testing"
)

var (
	mpmcIntAllocationSink     *MPMCQueue[int64]
	mpmcPointerAllocationSink *MPMCQueue[*int64]
)

func TestMPMCConstructorAllocations(t *testing.T) {
	configured := []Option{
		WithMaxBatch(8),
		WithMaxEnqueueBatch(4),
		WithMaxDequeueBatch(2),
		WithoutDequeueClearing(),
		WithExactBatchCounts(),
		WithSpinsBeforeYield(0),
	}
	cases := ringAllocationConstructorCases(configured)
	cases = append(cases, ringAllocationConstructorCase{
		name:    "relaxed",
		options: []Option{WithRelaxedMultiProdEnqueue()},
	})
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			integers := testing.AllocsPerRun(100, func() {
				queue, err := NewMPMCQueue[int64](8, test.options...)
				if err != nil {
					t.Fatal(err)
				}
				mpmcIntAllocationSink = queue
			})
			pointers := testing.AllocsPerRun(100, func() {
				queue, err := NewMPMCQueue[*int64](8, test.options...)
				if err != nil {
					t.Fatal(err)
				}
				mpmcPointerAllocationSink = queue
			})
			checkRingConstructorAllocations(t, integers, pointers)
		})
	}
}

func TestMPMCSteadyStateAllocations(t *testing.T) {
	checkMPMCSteadyStateAllocations[int64](t, 42)
	value := int64(42)
	checkMPMCSteadyStateAllocations[*int64](t, &value)
}

func checkMPMCSteadyStateAllocations[T any](t *testing.T, value T) {
	t.Helper()
	matrix := ringMultiAllocationMatrix(true, 8)
	for mode := 0; mode < 4; mode++ {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			newQueue := func(options ...Option) *MPMCQueue[T] {
				return mpmcNewForTest[T](t, 8, mpmcTestOptions(mode, options)...)
			}
			checkRingSteadyStateAllocations(t, value, matrix, newQueue)
		})
	}
}
