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
	for _, test := range []struct {
		name    string
		options []Option
	}{
		{"default", nil},
		{"configured", configured},
	} {
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
			t.Logf("constructor allocations int64=%g pointer=%g", integers, pointers)
			if integers > 3 || pointers > 3 {
				t.Fatal("constructor exceeded three allocations")
			}
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
	for _, exact := range []bool{false, true} {
		var options []Option
		if exact {
			options = append(options, WithExactBatchCounts())
		}
		queue := newSPSCForTest[T](t, 8, options...)
		items := []T{value, value, value}
		dst := make([]T, len(items))
		for _, method := range []string{"single", "bulk", "burst"} {
			allocations := testing.AllocsPerRun(100, func() {
				switch method {
				case "single":
					if err := queue.Enqueue(value); err != nil {
						t.Fatal(err)
					}
					if _, err := queue.Dequeue(); err != nil {
						t.Fatal(err)
					}
				case "bulk":
					if _, err := queue.EnqueueBulk(items); err != nil {
						t.Fatal(err)
					}
					if _, err := queue.DequeueBulk(dst); err != nil {
						t.Fatal(err)
					}
				case "burst":
					if moved, _ := queue.EnqueueBurst(items); moved != len(items) {
						t.Fatal("short enqueue")
					}
					if moved, _ := queue.DequeueBurst(dst); moved != len(dst) {
						t.Fatal("short dequeue")
					}
				}
			})
			t.Logf(
				"payload=%T exact=%t method=%s allocations=%g",
				value, exact, method, allocations,
			)
			if allocations != 0 {
				t.Fatal("steady-state allocation")
			}
		}
	}
}
