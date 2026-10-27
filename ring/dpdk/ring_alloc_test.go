package dpdk

import "testing"

type ringAllocationQueue[T any] interface {
	*SPSCQueue[T] | *SPMCQueue[T] | *MPSCQueue[T] | *MPMCQueue[T]
	Enqueue(T) error
	Dequeue() (T, error)
	EnqueueBulk([]T) (int, error)
	DequeueBulk([]T) (int, error)
	EnqueueBurst([]T) (int, int)
	DequeueBurst([]T) (int, int)
	Count() int
	Free() int
	Full() bool
	Empty() bool
	Capacity() int
	MaxEnqueueBatch() int
	MaxDequeueBatch() int
}

type ringAllocationConstructorCase struct {
	name    string
	options []Option
}

func ringAllocationConstructorCases(configured []Option) []ringAllocationConstructorCase {
	return []ringAllocationConstructorCase{
		{"default", nil},
		{"configured", configured},
	}
}

func checkRingConstructorAllocations(t *testing.T, integers, pointers float64) {
	t.Helper()
	t.Logf("constructor allocations int64=%g pointer=%g", integers, pointers)
	if integers > 3 || pointers > 3 {
		t.Fatal("constructor exceeded three allocations")
	}
}

type ringAllocationMatrix struct {
	methods  []string
	clearing []bool
	pairs    int
}

func ringMultiAllocationMatrix(burstOne bool, pairs int) ringAllocationMatrix {
	methods := []string{"single", "bulk-one"}
	if burstOne {
		methods = append(methods, "burst-one")
	}
	methods = append(
		methods,
		"burst-clamped-one",
		"bulk-wrapped",
		"burst-wrapped",
		"admission-and-observers",
	)
	return ringAllocationMatrix{
		methods:  methods,
		clearing: []bool{false, true},
		pairs:    pairs,
	}
}

func checkRingSteadyStateAllocations[T any, Q ringAllocationQueue[T]](
	t *testing.T,
	value T,
	matrix ringAllocationMatrix,
	newQueue func(...Option) Q,
) {
	t.Helper()
	for _, exact := range []bool{false, true} {
		for _, clearing := range matrix.clearing {
			var options []Option
			if exact {
				options = append(options, WithExactBatchCounts())
			}
			if !clearing {
				options = append(options, WithoutDequeueClearing())
			}
			queue := newQueue(options...)
			clampedOptions := append([]Option(nil), options...)
			clampedOptions = append(clampedOptions, WithMaxBatch(1))
			clamped := newQueue(clampedOptions...)
			items := []T{value, value, value}
			dst := make([]T, 3)
			full := []T{value, value, value, value, value, value, value, value}
			fullDst := make([]T, 8)
			for _, method := range matrix.methods {
				allocations := testing.AllocsPerRun(100, func() {
					for range matrix.pairs {
						switch method {
						case "single":
							if err := queue.Enqueue(value); err != nil {
								t.Fatal(err)
							}
							if _, err := queue.Dequeue(); err != nil {
								t.Fatal(err)
							}
						case "bulk", "bulk-one", "bulk-wrapped":
							count := len(items)
							if method == "bulk-one" {
								count = 1
							}
							pairs := 1
							if method == "bulk-wrapped" {
								pairs = 8
							}
							for range pairs {
								if _, err := queue.EnqueueBulk(items[:count]); err != nil {
									t.Fatal(err)
								}
								if _, err := queue.DequeueBulk(dst[:count]); err != nil {
									t.Fatal(err)
								}
							}
						case "burst", "burst-one", "burst-wrapped":
							count := len(items)
							if method == "burst-one" {
								count = 1
							}
							pairs := 1
							if method == "burst-wrapped" {
								pairs = 8
							}
							for range pairs {
								if moved, _ := queue.EnqueueBurst(items[:count]); moved != count {
									t.Fatal("burst enqueue count", moved)
								}
								if moved, _ := queue.DequeueBurst(dst[:count]); moved != count {
									t.Fatal("burst dequeue count", moved)
								}
							}
						case "burst-clamped-one":
							if count, _ := clamped.EnqueueBurst(items); count != 1 {
								t.Fatal("clamped enqueue count", count)
							}
							if count, _ := clamped.DequeueBurst(dst); count != 1 {
								t.Fatal("clamped dequeue count", count)
							}
						case "admission-and-observers":
							checkRingAdmissionAllocations(t, queue, full, fullDst)
						}
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

func checkRingAdmissionAllocations[T any, Q ringAllocationQueue[T]](
	t *testing.T,
	queue Q,
	items, dst []T,
) {
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
