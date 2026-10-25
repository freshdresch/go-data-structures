package ring

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestRingFIFO(t *testing.T) {
	for _, test := range ringTestCases[int]() {
		for _, exact := range []bool{false, true} {
			for _, retain := range []bool{false, true} {
				for _, capacity := range []int{1, 2, 4, 8, 64} {
					name := fmt.Sprintf("%s/exact=%t/retain=%t/capacity=%d", test.name, exact, retain, capacity)
					t.Run(name, func(t *testing.T) {
						options := ringBehaviorOptions(exact, retain)
						fixture := test.newFixture(t, capacity, options...)
						fixture.seed(uint32(capacity-1), 0)
						queue := fixture.queue
						referenceOptions := append(slices.Clone(test.protocolOptions), options...)
						reference, err := New[int](capacity, referenceOptions...)
						if err != nil {
							t.Fatal(err)
						}
						items := make([]int, capacity)
						actual, expected := make([]int, capacity), make([]int, capacity)
						for round := 0; round < 12; round++ {
							for index := range items {
								items[index] = 100*round + index + 1
							}
							switch round % 3 {
							case 0:
								for _, item := range items {
									if err := queue.Enqueue(item); err != reference.Enqueue(item) {
										t.Fatal("single enqueue", err)
									}
								}
							case 1:
								got, err := queue.EnqueueBulk(items)
								want, refErr := reference.EnqueueBulk(items)
								if got != want || err != refErr || got != 0 {
									t.Fatal("bulk enqueue", got, want, err, refErr)
								}
							case 2:
								got, free := queue.EnqueueBurst(items)
								want, refFree := reference.EnqueueBurst(items)
								if got != want || free != refFree || got != capacity || free != 0 {
									t.Fatal("burst enqueue", got, free, want, refFree)
								}
							}
							if !queue.Full() || queue.Empty() || queue.Count() != capacity || queue.Free() != 0 {
								t.Fatal("full observers")
							}
							if err := queue.Enqueue(-1); err != ErrFull || !errors.Is(err, ErrFull) {
								t.Fatal("full single")
							}
							if free, err := queue.EnqueueBulk(items); free != 0 || err != ErrFull {
								t.Fatal("full bulk", free, err)
							}
							if moved, free := queue.EnqueueBurst(items); moved != 0 || free != 0 {
								t.Fatal("full burst", moved, free)
							}
							switch round % 3 {
							case 0:
								got, err := queue.DequeueBulk(actual)
								want, refErr := reference.DequeueBulk(expected)
								if got != want || err != refErr || got != 0 {
									t.Fatal("bulk dequeue", got, want, err, refErr)
								}
							case 1:
								got, left := queue.DequeueBurst(actual)
								want, refLeft := reference.DequeueBurst(expected)
								if got != want || left != refLeft || got != capacity || left != 0 {
									t.Fatal("burst dequeue", got, left, want, refLeft)
								}
							case 2:
								for index := range actual {
									got, err := queue.Dequeue()
									want, refErr := reference.Dequeue()
									if got != want || err != refErr {
										t.Fatal("single dequeue", got, want, err, refErr)
									}
									actual[index], expected[index] = got, want
								}
							}
							if !slices.Equal(actual, items) || !slices.Equal(actual, expected) {
								t.Fatal("FIFO", actual, items, expected)
							}
							saved := slices.Clone(actual)
							if left, err := queue.DequeueBulk(actual); left != 0 || err != ErrEmpty {
								t.Fatal("empty bulk", left, err)
							}
							if !slices.Equal(saved, actual) {
								t.Fatal("failed dequeue changed destination")
							}
							if value, err := queue.Dequeue(); value != 0 || err != ErrEmpty {
								t.Fatal("empty single", value, err)
							}
							if moved, left := queue.DequeueBurst(actual); moved != 0 || left != 0 ||
								!slices.Equal(saved, actual) {
								t.Fatal("empty burst", moved, left, actual)
							}
							if !queue.Empty() || queue.Full() || queue.Count() != 0 || queue.Free() != capacity {
								t.Fatal("empty observers")
							}
						}

					})
				}
			}
		}
	}
}

func TestRingPartialAdmission(t *testing.T) {
	for _, test := range ringTestCases[int]() {
		for _, capacity := range []int{2, 4, 8} {
			for _, exact := range []bool{false, true} {
				name := fmt.Sprintf("%s/capacity=%d/exact=%t", test.name, capacity, exact)
				t.Run(name, func(t *testing.T) {
					fixture := test.newFixture(t, capacity, ringBehaviorOptions(exact, false)...)
					fixture.seed(uint32(capacity-1), 0)
					queue := fixture.queue
					if err := queue.Enqueue(10); err != nil {
						t.Fatal(err)
					}
					items := make([]int, capacity)
					for index := range items {
						items[index] = index + 11
					}
					free, err := queue.EnqueueBulk(items)
					if free != capacity-1 || err != ErrFull || queue.Count() != 1 {
						t.Fatalf("failed bulk free=%d error=%v count=%d", free, err, queue.Count())
					}
					moved, free := queue.EnqueueBurst(items)
					if moved != capacity-1 || free != 0 {
						t.Fatalf("partial enqueue moved=%d free=%d", moved, free)
					}
					if value, err := queue.Dequeue(); value != 10 || err != nil {
						t.Fatalf("single value=%d error=%v", value, err)
					}
					dst := make([]int, capacity)
					for index := range dst {
						dst[index] = -1
					}
					before := slices.Clone(dst)
					available, err := queue.DequeueBulk(dst)
					if available != capacity-1 || err != ErrEmpty || !slices.Equal(dst, before) {
						t.Fatalf("failed bulk dst=%v available=%d error=%v", dst, available, err)
					}
					moved, available = queue.DequeueBurst(dst)
					if moved != capacity-1 || available != 0 {
						t.Fatalf("partial dequeue moved=%d available=%d", moved, available)
					}
					want := append(slices.Clone(items[:capacity-1]), -1)
					if !slices.Equal(dst, want) {
						t.Fatalf("partial dst=%v want=%v", dst, want)
					}

				})
			}
		}
	}
}
