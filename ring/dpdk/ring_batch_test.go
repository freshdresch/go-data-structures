package dpdk

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"
)

func TestRingBatchErrorsAndLimits(t *testing.T) {
	for _, test := range ringTestCases[int]() {
		for _, exact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/exact=%t", test.name, exact), func(t *testing.T) {
				options := append(ringBehaviorOptions(exact, false), WithMaxBatch(2))
				queue := test.newFixture(t, 4, options...).queue
				for _, length := range []int{5, 3, 2} {
					want := ErrEmpty
					switch length {
					case 5:
						want = ErrExceedsCapacity
					case 3:
						want = ErrExceedsMaxBatch
					}
					dst := []int{9, 9, 9, 9, 9}[:length]
					before := slices.Clone(dst)
					available, err := queue.DequeueBulk(dst)
					if available != 0 || !errors.Is(err, want) || !slices.Equal(dst, before) {
						t.Fatalf("length=%d available=%d error=%v dst=%v", length, available, err, dst)
					}
					if want == ErrEmpty && err != want {
						t.Fatal("wrapped empty")
					}
				}
				if moved, free := queue.EnqueueBurst([]int{1, 2, 3, 4, 5}); moved != 2 || free != 2 {
					t.Fatalf("clamp moved=%d free=%d", moved, free)
				}
				if _, err := queue.EnqueueBulk([]int{3, 4}); err != nil {
					t.Fatal(err)
				}
				for _, length := range []int{5, 3, 2} {
					want := ErrFull
					switch length {
					case 5:
						want = ErrExceedsCapacity
					case 3:
						want = ErrExceedsMaxBatch
					}
					free, err := queue.EnqueueBulk(make([]int, length))
					if free != 0 || !errors.Is(err, want) {
						t.Fatalf("length=%d free=%d error=%v", length, free, err)
					}
					if want == ErrFull && err != want {
						t.Fatal("wrapped full")
					}
				}
				dst := []int{9, 9, 9, 9, 9}
				if moved, left := queue.DequeueBurst(dst); moved != 2 || left != 2 {
					t.Fatalf("clamp moved=%d available=%d", moved, left)
				}
				if !slices.Equal(dst, []int{1, 2, 9, 9, 9}) {
					t.Fatalf("clamp dst=%v", dst)
				}
				for _, want := range []int{3, 4} {
					value, err := queue.Dequeue()
					if value != want || err != nil {
						t.Fatalf("drain value=%d want=%d error=%v", value, want, err)
					}
				}
				if !queue.Empty() || queue.Full() || queue.Count() != 0 || queue.Free() != 4 {
					t.Fatal("drained queue observers")
				}
				for _, length := range []int{5, 3, 2} {
					want := ErrEmpty
					switch length {
					case 5:
						want = ErrExceedsCapacity
					case 3:
						want = ErrExceedsMaxBatch
					}
					dst := []int{-1, -1, -1, -1, -1}
					before := slices.Clone(dst)
					available, err := queue.DequeueBulk(dst[:length])
					if available != 0 || !errors.Is(err, want) || !slices.Equal(dst, before) {
						t.Fatalf("length=%d available=%d error=%v dst=%v", length, available, err, dst)
					}
					if want == ErrEmpty && err != ErrEmpty {
						t.Fatal("wrapped empty after drain")
					}
				}
			})
		}
	}
}

func TestRingDirectionalLimits(t *testing.T) {
	for _, test := range ringTestCases[int]() {
		for _, exact := range []bool{false, true} {
			for _, enqueueLimit := range []int{1, 2} {
				name := fmt.Sprintf("%s/exact=%t/enqueue=%d", test.name, exact, enqueueLimit)
				t.Run(name, func(t *testing.T) {
					options := append(
						ringBehaviorOptions(exact, false),
						WithMaxEnqueueBatch(enqueueLimit),
						WithMaxDequeueBatch(1),
					)
					queue := test.newFixture(t, 4, options...).queue
					items := []int{10, 11, 12}
					moved, free := queue.EnqueueBurst(items)
					if moved != enqueueLimit || free != 4-enqueueLimit {
						t.Fatal("enqueue clamp", moved, free)
					}
					dst := []int{-1, -1, -1}
					moved, left := queue.DequeueBurst(dst)
					if moved != 1 || left != enqueueLimit-1 || !slices.Equal(dst, []int{10, -1, -1}) {
						t.Fatal("dequeue clamp", moved, left, dst)
					}
					if _, err := queue.EnqueueBulk(items); !errors.Is(err, ErrExceedsMaxBatch) {
						t.Fatal("enqueue limit", err)
					}
					if _, err := queue.DequeueBulk(dst[:2]); !errors.Is(err, ErrExceedsMaxBatch) {
						t.Fatal("dequeue limit", err)
					}
				})
			}
		}
	}
}

func TestRingWideLengths(t *testing.T) {
	if strconv.IntSize <= 32 {
		t.Skip("slice length beyond uint32 requires a 64-bit int")
	}
	length := uint64(math.MaxUint32) + 2
	items := make([]struct{}, int(length))
	for _, test := range ringTestCases[struct{}]() {
		for _, exact := range []bool{false, true} {
			for _, limits := range []struct{ capacity, enqueue, dequeue int }{
				{4, 2, 1}, {4, 2, 2}, {8, 1, 1},
			} {
				name := fmt.Sprintf(
					"%s/exact=%t/capacity=%d/limits=%d,%d",
					test.name, exact, limits.capacity, limits.enqueue, limits.dequeue,
				)
				t.Run(name, func(t *testing.T) {
					options := append(
						ringBehaviorOptions(exact, false),
						WithMaxEnqueueBatch(limits.enqueue),
						WithMaxDequeueBatch(limits.dequeue),
					)
					queue := test.newFixture(t, limits.capacity, options...).queue
					free, err := queue.EnqueueBulk(items)
					if free != limits.capacity || !errors.Is(err, ErrExceedsCapacity) ||
						err == ErrExceedsCapacity {
						t.Fatal("wide enqueue", free, err)
					}
					if left, err := queue.DequeueBulk(items); left != 0 ||
						!errors.Is(err, ErrExceedsCapacity) || err == ErrExceedsCapacity {
						t.Fatal("wide empty dequeue", left, err)
					}
					moved, free := queue.EnqueueBurst(items)
					if moved != limits.enqueue || free != limits.capacity-limits.enqueue {
						t.Fatal("wide enqueue clamp", moved, free)
					}
					left, err := queue.DequeueBulk(items)
					if left != limits.enqueue || !errors.Is(err, ErrExceedsCapacity) ||
						err == ErrExceedsCapacity {
						t.Fatal("wide dequeue", left, err)
					}
					remaining := limits.enqueue
					for remaining > 0 {
						moved, left := queue.DequeueBurst(items)
						expected := min(remaining, limits.dequeue)
						remaining -= expected
						if moved != expected || left != remaining {
							t.Fatal("wide dequeue clamp", moved, left)
						}
					}
				})
			}
		}
	}
}
