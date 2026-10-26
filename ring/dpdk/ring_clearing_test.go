package dpdk

import (
	"fmt"
	"math"
	"slices"
	"testing"
)

func TestRingClearingAndReset(t *testing.T) {
	for _, test := range ringTestCases[*int]() {
		for _, retain := range []bool{false, true} {
			for _, exact := range []bool{false, true} {
				name := fmt.Sprintf("%s/retain=%t/exact=%t", test.name, retain, exact)
				t.Run(name, func(t *testing.T) {
					options := append(ringBehaviorOptions(exact, retain), WithMaxBatch(3))
					fixture := test.newFixture(t, 4, options...)
					fixture.seed(math.MaxUint32-1, 0)
					queue := fixture.queue
					firstValue, secondValue, thirdValue, fourthValue, replacementValue := 10, 11, 12, 13, 14
					if err := queue.Enqueue(&firstValue); err != nil {
						t.Fatal(err)
					}
					items := []*int{&secondValue, &thirdValue, &fourthValue}
					if free, err := queue.EnqueueBulk(items); free != 0 || err != nil {
						t.Fatalf("pointer enqueue free=%d error=%v", free, err)
					}
					if value, err := queue.Dequeue(); value != &firstValue || err != nil {
						t.Fatalf("pointer single value=%p error=%v", value, err)
					}
					wantSingleSlot := (*int)(nil)
					if retain {
						wantSingleSlot = &firstValue
					}
					if fixture.entries[2] != wantSingleSlot {
						t.Fatalf("single backing slot=%p want=%p", fixture.entries[2], wantSingleSlot)
					}
					dst := []*int{nil, nil, nil}
					if left, err := queue.DequeueBulk(dst); left != 0 || err != nil ||
						!slices.Equal(dst, []*int{&secondValue, &thirdValue, &fourthValue}) {
						t.Fatalf("wrapped pointer bulk dst=%v left=%d error=%v", dst, left, err)
					}
					wantSlots := []*int{nil, nil, nil, nil}
					if retain {
						wantSlots = []*int{&thirdValue, &fourthValue, &firstValue, &secondValue}
					}
					if !slices.Equal(fixture.entries, wantSlots) {
						t.Fatalf("consumed backing slots=%v want=%v", fixture.entries, wantSlots)
					}
					if moved, free := queue.EnqueueBurst([]*int{&replacementValue}); moved != 1 || free != 3 {
						t.Fatalf("overwrite moved=%d free=%d", moved, free)
					}
					if fixture.entries[2] != &replacementValue {
						t.Fatal("overwrite did not replace consumed pointer")
					}

					queue.Reset()
					if !slices.Equal(fixture.entries, []*int{nil, nil, nil, nil}) {
						t.Fatal("Reset retained pointers", fixture.entries)
					}
					if !queue.Empty() || queue.Full() || queue.Count() != 0 || queue.Free() != 4 {
						t.Fatal("Reset query results")
					}
					fixture.seed(math.MaxUint32, 0)
					if free, err := queue.EnqueueBulk([]*int{&firstValue, &secondValue}); free != 2 || err != nil {
						t.Fatal("reuse enqueue", free, err)
					}
					dst = []*int{nil, nil, &replacementValue}
					if moved, left := queue.DequeueBurst(dst); moved != 2 || left != 0 ||
						!slices.Equal(dst, []*int{&firstValue, &secondValue, &replacementValue}) {
						t.Fatal("reuse wrapped burst", moved, left, dst)
					}
					expected := []*int{nil, nil, nil, nil}
					if retain {
						expected = []*int{&secondValue, nil, nil, &firstValue}
					}
					if !slices.Equal(fixture.entries, expected) {
						t.Fatal("burst clearing/retention", fixture.entries, expected)
					}
					queue.Reset()
					if !slices.Equal(fixture.entries, []*int{nil, nil, nil, nil}) {
						t.Fatal("Reset retained pointers after burst", fixture.entries)
					}
					if err := queue.Enqueue(&replacementValue); err != nil {
						t.Fatal(err)
					}
					if value, err := queue.Dequeue(); err != nil || value != &replacementValue {
						t.Fatal("reuse scalar", value, err)
					}
				})
			}
		}
	}
}
