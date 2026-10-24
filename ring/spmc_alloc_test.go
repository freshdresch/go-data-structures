package ring

import "testing"

var (
	spmcIntAllocationSink     *SPMC[int64]
	spmcPointerAllocationSink *SPMC[*int64]
)

func TestSPMCConstructorAllocations(t *testing.T) {
	configured := []SPMCOption{
		WithSPMCMaxEnqueueBatch(4),
		WithSPMCMaxDequeueBatch(2),
		WithoutSPMCDequeueClearing(),
		WithSPMCExactBatchCounts(),
		WithSPMCSpinsBeforeYield(0),
	}
	for _, test := range []struct {
		name    string
		options []SPMCOption
	}{
		{"default", nil},
		{"configured", configured},
	} {
		t.Run(test.name, func(t *testing.T) {
			integers := testing.AllocsPerRun(100, func() {
				queue, err := NewSPMC[int64](8, test.options...)
				if err != nil {
					t.Fatal(err)
				}
				spmcIntAllocationSink = queue
			})
			pointers := testing.AllocsPerRun(100, func() {
				queue, err := NewSPMC[*int64](8, test.options...)
				if err != nil {
					t.Fatal(err)
				}
				spmcPointerAllocationSink = queue
			})
			t.Logf("constructor allocations int64=%g pointer=%g", integers, pointers)
			if integers > 3 || pointers > 3 {
				t.Fatal("constructor exceeded three allocations")
			}
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
	for _, exact := range []bool{false, true} {
		for _, clearing := range []bool{false, true} {
			var options []SPMCOption
			if exact {
				options = append(options, WithSPMCExactBatchCounts())
			}
			if !clearing {
				options = append(options, WithoutSPMCDequeueClearing())
			}
			queue := newSPMCForTest[T](t, 8, options...)
			clampedOptions := append(options, WithSPMCMaxBatch(1))
			clamped := newSPMCForTest[T](t, 8, clampedOptions...)
			items := []T{value, value, value}
			dst := make([]T, 3)
			full := []T{value, value, value, value, value, value, value, value}
			fullDst := make([]T, 8)
			for _, method := range []string{
				"single", "bulk-one", "burst-clamped-one", "bulk-wrapped", "burst-wrapped",
				"admission-and-observers",
			} {
				allocations := testing.AllocsPerRun(100, func() {
					switch method {
					case "single":
						if err := queue.Enqueue(value); err != nil {
							t.Fatal(err)
						}
						if _, err := queue.Dequeue(); err != nil {
							t.Fatal(err)
						}
					case "bulk-one":
						if _, err := queue.EnqueueBulk(items[:1]); err != nil {
							t.Fatal(err)
						}
						if _, err := queue.DequeueBulk(dst[:1]); err != nil {
							t.Fatal(err)
						}
					case "burst-clamped-one":
						if count, _ := clamped.EnqueueBurst(items); count != 1 {
							t.Fatal("clamped enqueue count", count)
						}
						if count, _ := clamped.DequeueBurst(dst); count != 1 {
							t.Fatal("clamped dequeue count", count)
						}
					case "bulk-wrapped":
						if _, err := queue.EnqueueBulk(items); err != nil {
							t.Fatal(err)
						}
						if _, err := queue.DequeueBulk(dst); err != nil {
							t.Fatal(err)
						}
					case "burst-wrapped":
						if count, _ := queue.EnqueueBurst(items); count != 3 {
							t.Fatal("burst enqueue count", count)
						}
						if count, _ := queue.DequeueBurst(dst); count != 3 {
							t.Fatal("burst dequeue count", count)
						}
					case "admission-and-observers":
						checkSPMCAdmissionAllocations(t, queue, full, fullDst)
					}
				})
				t.Logf(
					"payload=%T exact=%t clearing=%t method=%s allocations=%g",
					value, exact, clearing, method, allocations,
				)
				if allocations != 0 {
					t.Fatal("steady-state allocation")
				}
			}
		}
	}
}

func checkSPMCAdmissionAllocations[T any](t *testing.T, queue *SPMC[T], items, dst []T) {
	t.Helper()
	if _, err := queue.Dequeue(); err != ErrEmpty {
		t.Fatal("empty single", err)
	}
	if _, err := queue.DequeueBulk(dst); err != ErrEmpty {
		t.Fatal("empty bulk", err)
	}
	if count, _ := queue.DequeueBurst(dst); count != 0 {
		t.Fatal("empty burst", count)
	}
	if _, err := queue.EnqueueBulk(items); err != nil {
		t.Fatal(err)
	}
	if err := queue.Enqueue(items[0]); err != ErrFull {
		t.Fatal("full single", err)
	}
	if _, err := queue.EnqueueBulk(items[:1]); err != ErrFull {
		t.Fatal("full bulk", err)
	}
	if count, _ := queue.EnqueueBurst(items); count != 0 {
		t.Fatal("full burst", count)
	}
	if _, err := queue.EnqueueBulk(nil); err != nil {
		t.Fatal("zero bulk enqueue", err)
	}
	if _, err := queue.DequeueBulk(nil); err != nil {
		t.Fatal("zero bulk dequeue", err)
	}
	if count, _ := queue.EnqueueBurst(nil); count != 0 {
		t.Fatal("zero burst enqueue", count)
	}
	if count, _ := queue.DequeueBurst(nil); count != 0 {
		t.Fatal("zero burst dequeue", count)
	}
	if queue.Count() != 8 || queue.Free() != 0 || !queue.Full() || queue.Empty() ||
		queue.Capacity() != 8 || queue.MaxEnqueueBatch() != 8 || queue.MaxDequeueBatch() != 8 {
		t.Fatal("full observers")
	}
	if _, err := queue.DequeueBulk(dst[:1]); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.EnqueueBulk(items[:2]); err != ErrFull {
		t.Fatal("incomplete bulk admission", err)
	}
	if count, _ := queue.EnqueueBurst(items[:2]); count != 1 {
		t.Fatal("partial enqueue burst", count)
	}
	if count, _ := queue.DequeueBurst(dst[:7]); count != 7 {
		t.Fatal("dequeue prefix", count)
	}
	if _, err := queue.DequeueBulk(dst[:2]); err != ErrEmpty {
		t.Fatal("incomplete dequeue bulk admission", err)
	}
	if count, _ := queue.DequeueBurst(dst[:2]); count != 1 {
		t.Fatal("partial dequeue burst", count)
	}
	if queue.Count() != 0 || queue.Free() != 8 || queue.Full() || !queue.Empty() {
		t.Fatal("empty observers")
	}
}
