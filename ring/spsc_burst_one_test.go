package ring

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
)

type spscBurstOneState[T comparable] struct {
	positions [6]uint32
	entries   []T
}

func snapshotSPSCBurstOne[T comparable](queue *SPSC[T]) spscBurstOneState[T] {
	return spscBurstOneState[T]{
		positions: [6]uint32{
			queue.producer.cursor, queue.producer.cachedConsumer, queue.producer.published.Load(),
			queue.consumer.cursor, queue.consumer.cachedProducer, queue.consumer.published.Load(),
		},
		entries: slices.Clone(queue.producer.entries),
	}
}

func TestSPSCEnqueueBurstOneSnapshots(t *testing.T) {
	for _, test := range []struct {
		name                  string
		exact                 bool
		cursor, cached, fresh uint32
		moved, remaining      int
		wantCache             uint32
	}{
		{"cached", false, 4, 0, 2, 1, 3, 0},
		{"exact", true, 4, 0, 2, 1, 5, 2},
		{"full", false, 8, 0, 0, 0, 0, 0},
		{"exhausted-cache", false, 8, 0, 2, 1, 1, 2},
		{"invalid-cache-recovers", false, 4, 5, 2, 1, 5, 2},
		{"invalid-fresh", false, 4, 5, 5, 0, 8, 5},
		{"exact-invalid-fresh", true, 4, 0, 5, 0, 8, 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			var queues [2]*SPSC[int]
			for index := range queues {
				queue := newSPSCForTest[int](t, 8)
				queue.producer.exactBatchCounts = test.exact
				queue.producer.cursor = test.cursor
				queue.producer.cachedConsumer = test.cached
				queue.producer.published.Store(test.cursor)
				queue.consumer.cursor = test.fresh
				queue.consumer.published.Store(test.fresh)
				for slot := range queue.producer.entries {
					queue.producer.entries[slot] = -10
				}
				queues[index] = queue
			}
			want := snapshotSPSCBurstOne(queues[0])
			want.positions[1] = test.wantCache
			if test.moved == 1 {
				want.positions[0]++
				want.positions[2]++
				want.entries[test.cursor&7] = 77
			}
			moved, remaining := queues[0].EnqueueBurst([]int{77})
			referenceMoved, referenceRemaining := queues[1].enqueueBatch([]int{77}, false)
			if moved != test.moved || remaining != test.remaining {
				t.Fatalf("moved/count=%d/%d want=%d/%d", moved, remaining, test.moved, test.remaining)
			}
			if moved != referenceMoved || remaining != referenceRemaining {
				t.Fatalf("helper moved/count=%d/%d", referenceMoved, referenceRemaining)
			}
			for _, queue := range queues {
				if got := snapshotSPSCBurstOne(queue); !reflect.DeepEqual(got, want) {
					t.Fatalf("state=%+v want=%+v", got, want)
				}
			}
		})
	}
}

func TestSPSCDequeueBurstOneSnapshots(t *testing.T) {
	for _, test := range []struct {
		name                  string
		exact                 bool
		cursor, cached, fresh uint32
		moved, remaining      int
		wantCache             uint32
	}{
		{"cached", false, 2, 4, 6, 1, 1, 4},
		{"exact", true, 2, 4, 6, 1, 3, 6},
		{"empty", false, 2, 2, 2, 0, 0, 2},
		{"exhausted-cache", false, 2, 2, 6, 1, 3, 6},
		{"invalid-cache-recovers", false, 2, 11, 6, 1, 3, 6},
		{"invalid-fresh", false, 2, 11, 11, 0, 8, 11},
		{"exact-invalid-fresh", true, 2, 4, 11, 0, 8, 11},
	} {
		t.Run(test.name, func(t *testing.T) {
			var queues [2]*SPSC[int]
			for index := range queues {
				queue := newSPSCForTest[int](t, 8)
				queue.consumer.exactBatchCounts = test.exact
				queue.consumer.cursor = test.cursor
				queue.consumer.cachedProducer = test.cached
				queue.consumer.published.Store(test.cursor)
				queue.producer.cursor = test.fresh
				queue.producer.published.Store(test.fresh)
				for slot := range queue.producer.entries {
					queue.producer.entries[slot] = 100 + slot
				}
				queues[index] = queue
			}
			want := snapshotSPSCBurstOne(queues[0])
			want.positions[4] = test.wantCache
			wantDst := []int{-1, -2, -3}
			if test.moved == 1 {
				want.positions[3]++
				want.positions[5]++
				want.entries[test.cursor&7] = 0
				wantDst[0] = 102
			}
			dst := []int{-1, -2, -3}
			referenceDst := slices.Clone(dst)
			moved, remaining := queues[0].DequeueBurst(dst[:1])
			referenceMoved, referenceRemaining := queues[1].dequeueBatch(referenceDst[:1], false)
			if moved != test.moved || remaining != test.remaining {
				t.Fatalf("moved/count=%d/%d want=%d/%d", moved, remaining, test.moved, test.remaining)
			}
			if moved != referenceMoved || remaining != referenceRemaining {
				t.Fatalf("helper moved/count=%d/%d", referenceMoved, referenceRemaining)
			}
			if !slices.Equal(dst, wantDst) || !slices.Equal(referenceDst, wantDst) {
				t.Fatalf("destination=%v helper=%v want=%v", dst, referenceDst, wantDst)
			}
			for _, queue := range queues {
				if got := snapshotSPSCBurstOne(queue); !reflect.DeepEqual(got, want) {
					t.Fatalf("state=%+v want=%+v", got, want)
				}
			}
		})
	}
}

func TestSPSCBurstOneClampAndFallback(t *testing.T) {
	for _, test := range []struct {
		name                     string
		limit, length, occupancy int
		enqueued, free           int
		dequeued, available      int
	}{
		{"clamp-one", 1, 3, 2, 1, 5, 1, 1},
		{"nil", 8, -1, 2, 0, 6, 0, 2},
		{"zero", 8, 0, 2, 0, 6, 0, 2},
		{"clamp-two", 2, 3, 3, 2, 3, 2, 1},
		{"enqueue-partial-one", 8, 3, 7, 1, 0, 3, 4},
		{"dequeue-partial-one", 8, 3, 1, 3, 4, 1, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, enqueue := range []bool{false, true} {
				var queues [2]*SPSC[int]
				for index := range queues {
					queue := newSPSCForTest[int](t, 8, WithSPSCMaxBatch(test.limit))
					for value := range test.occupancy {
						if err := queue.Enqueue(10 + value); err != nil {
							t.Fatal(err)
						}
					}
					queues[index] = queue
				}
				items := []int{77, 78, 79}
				if test.length == -1 {
					items = nil
				} else {
					items = items[:test.length]
				}
				length := min(len(items), test.limit)
				var moved, remaining, referenceMoved, referenceRemaining int
				if enqueue {
					moved, remaining = queues[0].EnqueueBurst(items)
					referenceMoved, referenceRemaining = queues[1].enqueueBatch(items[:length], false)
					if moved != test.enqueued || remaining != test.free {
						t.Fatalf("enqueue moved/count=%d/%d", moved, remaining)
					}
				} else {
					dst := []int{-1, -2, -3}
					referenceDst := slices.Clone(dst)
					var target, referenceTarget []int
					if test.length != -1 {
						target = dst[:test.length]
						referenceTarget = referenceDst[:length]
					}
					moved, remaining = queues[0].DequeueBurst(target)
					referenceMoved, referenceRemaining = queues[1].dequeueBatch(referenceTarget, false)
					if moved != test.dequeued || remaining != test.available {
						t.Fatalf("dequeue moved/count=%d/%d", moved, remaining)
					}
					want := []int{-1, -2, -3}
					for index := range test.dequeued {
						want[index] = 10 + index
					}
					if !slices.Equal(dst, want) || !slices.Equal(referenceDst, want) {
						t.Fatalf("destination=%v helper=%v want=%v", dst, referenceDst, want)
					}
				}
				if moved != referenceMoved || remaining != referenceRemaining {
					t.Fatalf("helper moved/count=%d/%d", referenceMoved, referenceRemaining)
				}
				if !reflect.DeepEqual(snapshotSPSCBurstOne(queues[0]), snapshotSPSCBurstOne(queues[1])) {
					t.Fatal("public and helper states differ")
				}
			}
		})
	}
}

func TestSPSCBurstOnePointersAndWrap(t *testing.T) {
	for _, clearSlots := range []bool{false, true} {
		for _, capacity := range []int{1, 8} {
			t.Run(fmt.Sprintf("clear=%t/capacity=%d", clearSlots, capacity), func(t *testing.T) {
				var options []SPSCOption
				if !clearSlots {
					options = append(options, WithoutSPSCDequeueClearing())
				}
				var queues [2]*SPSC[*int]
				for index := range queues {
					queue := newSPSCForTest[*int](t, capacity, options...)
					queue.producer.cursor = math.MaxUint32
					queue.producer.cachedConsumer = math.MaxUint32
					queue.producer.published.Store(math.MaxUint32)
					queue.consumer.cursor = math.MaxUint32
					queue.consumer.cachedProducer = math.MaxUint32
					queue.consumer.published.Store(math.MaxUint32)
					queues[index] = queue
				}
				value, sentinel := 42, -1
				items := []*int{&value}
				moved, remaining := queues[0].EnqueueBurst(items)
				referenceMoved, referenceRemaining := queues[1].enqueueBatch(items, false)
				if moved != 1 || remaining != capacity-1 ||
					referenceMoved != 1 || referenceRemaining != capacity-1 {
					t.Fatal("pointer enqueue counts")
				}
				for _, queue := range queues {
					if queue.producer.cursor != 0 || queue.producer.published.Load() != 0 ||
						queue.producer.entries[capacity-1] != &value {
						t.Fatal("enqueue wrap or physical pointer slot")
					}
				}
				dst, referenceDst := []*int{&sentinel, &sentinel}, []*int{&sentinel, &sentinel}
				moved, remaining = queues[0].DequeueBurst(dst[:1])
				referenceMoved, referenceRemaining = queues[1].dequeueBatch(referenceDst[:1], false)
				if moved != 1 || remaining != 0 || referenceMoved != 1 || referenceRemaining != 0 {
					t.Fatal("pointer dequeue counts")
				}
				wantSlot := &value
				if clearSlots {
					wantSlot = nil
				}
				for _, queue := range queues {
					if queue.consumer.cursor != 0 || queue.consumer.published.Load() != 0 ||
						queue.consumer.entries[capacity-1] != wantSlot {
						t.Fatal("dequeue wrap or physical pointer slot")
					}
				}
				if !slices.Equal(dst, []*int{&value, &sentinel}) || !slices.Equal(dst, referenceDst) {
					t.Fatal("pointer destination or suffix")
				}
				if !reflect.DeepEqual(snapshotSPSCBurstOne(queues[0]), snapshotSPSCBurstOne(queues[1])) {
					t.Fatal("public and helper pointer states differ")
				}
			})
		}
	}
}
