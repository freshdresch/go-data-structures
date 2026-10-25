package ring

import (
	"fmt"
	"testing"
)

var (
	mpscIntAllocationSink     *MPSCQueue[int64]
	mpscPointerAllocationSink *MPSCQueue[*int64]
)

func TestMPSCConstructorAllocations(t *testing.T) {
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
				queue, err := NewMPSCQueue[int64](8, test.options...)
				if err != nil {
					t.Fatal(err)
				}
				mpscIntAllocationSink = queue
			})
			pointers := testing.AllocsPerRun(100, func() {
				queue, err := NewMPSCQueue[*int64](8, test.options...)
				if err != nil {
					t.Fatal(err)
				}
				mpscPointerAllocationSink = queue
			})
			checkRingConstructorAllocations(t, integers, pointers)
		})
	}
}

func TestMPSCSteadyStateAllocations(t *testing.T) {
	checkMPSCSteadyStateAllocations[int64](t, 42)
	value := int64(42)
	checkMPSCSteadyStateAllocations[*int64](t, &value)
}

func checkMPSCSteadyStateAllocations[T any](t *testing.T, value T) {
	t.Helper()
	matrix := ringMultiAllocationMatrix(true, 1)
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
			newQueue := func(options ...Option) *MPSCQueue[T] {
				return newMPSCProtocolForTest[T](t, relaxed, 8, options...)
			}
			checkRingSteadyStateAllocations(t, value, matrix, newQueue)
		})
	}
}
