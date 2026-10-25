package ring

import (
	"fmt"
	"testing"
)

var (
	spmcIntAllocationSink     *SPMCQueue[int64]
	spmcPointerAllocationSink *SPMCQueue[*int64]
)

func TestSPMCConstructorAllocations(t *testing.T) {
	configured := []Option{
		WithMaxEnqueueBatch(4),
		WithMaxDequeueBatch(2),
		WithoutDequeueClearing(),
		WithExactBatchCounts(),
		WithSpinsBeforeYield(0),
	}
	cases := ringAllocationConstructorCases(configured)
	cases = append(cases, ringAllocationConstructorCase{
		name:    "RTS",
		options: append(append([]Option(nil), configured...), WithRelaxedMultiConsDequeue()),
	})
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			integers := testing.AllocsPerRun(100, func() {
				queue, err := NewSPMCQueue[int64](8, test.options...)
				if err != nil {
					t.Fatal(err)
				}
				spmcIntAllocationSink = queue
			})
			pointers := testing.AllocsPerRun(100, func() {
				queue, err := NewSPMCQueue[*int64](8, test.options...)
				if err != nil {
					t.Fatal(err)
				}
				spmcPointerAllocationSink = queue
			})
			checkRingConstructorAllocations(t, integers, pointers)
		})
	}
}

func TestSPMCSteadyStateAllocations(t *testing.T) {
	checkSPMCSteadyStateAllocations[int64](t, 42)
	value := int64(42)
	checkSPMCSteadyStateAllocations[*int64](t, &value)
}

func checkSPMCSteadyStateAllocations[T any](t *testing.T, value T) {
	t.Helper()
	matrix := ringMultiAllocationMatrix(false, 1)
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
			newQueue := func(options ...Option) *SPMCQueue[T] {
				return newSPMCForTest[T](t, 8, spmcProtocolOptions(relaxed, options)...)
			}
			checkRingSteadyStateAllocations(t, value, matrix, newQueue)
		})
	}
}
