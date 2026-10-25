package ring

import "testing"

var (
	spscIntAllocationSink     *SPSCQueue[int64]
	spscPointerAllocationSink *SPSCQueue[*int64]
)

func TestSPSCConstructorAllocations(t *testing.T) {
	configured := []Option{
		WithMaxBatch(4), WithoutDequeueClearing(), WithExactBatchCounts(),
	}
	for _, test := range ringAllocationConstructorCases(configured) {
		t.Run(test.name, func(t *testing.T) {
			integers := testing.AllocsPerRun(100, func() {
				queue, err := NewSPSCQueue[int64](8, test.options...)
				if err != nil {
					t.Fatal(err)
				}
				spscIntAllocationSink = queue
			})
			pointers := testing.AllocsPerRun(100, func() {
				queue, err := NewSPSCQueue[*int64](8, test.options...)
				if err != nil {
					t.Fatal(err)
				}
				spscPointerAllocationSink = queue
			})
			checkRingConstructorAllocations(t, integers, pointers)
		})
	}
}

func TestSPSCSteadyStateAllocations(t *testing.T) {
	checkSPSCSteadyStateAllocations[int64](t, 42)
	value := int64(42)
	checkSPSCSteadyStateAllocations[*int64](t, &value)
}

func checkSPSCSteadyStateAllocations[T any](t *testing.T, value T) {
	t.Helper()
	matrix := ringAllocationMatrix{
		methods:  []string{"single", "bulk", "burst"},
		clearing: []bool{true},
		pairs:    1,
	}
	checkRingSteadyStateAllocations(t, value, matrix, func(options ...Option) *SPSCQueue[T] {
		return newSPSCForTest[T](t, 8, options...)
	})
}
