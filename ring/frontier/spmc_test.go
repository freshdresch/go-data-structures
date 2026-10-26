package frontier

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
)

func spmcTestQueue[T any](t *testing.T, capacity int, options ...Option) *SPMCQueue[T] {
	t.Helper()
	queue, err := NewSPMCQueue[T](capacity, options...)
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

// spmcTestSeed moves an empty queue's positions to position, as if that many values had passed.
func spmcTestSeed[T any](queue *SPMCQueue[T], position uint64) {
	queue.producer.cursor = position
	queue.producer.released = position
	queue.producerPublished.Store(position)
	queue.consumerReservation.head.Store(position)
	queue.consumerReservation.cachedProducer.Store(position)
}

func TestSPMCCapacityReuseFIFO(t *testing.T) {
	for _, capacity := range []int{1, 8, 1024} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			queue := spmcTestQueue[int](t, capacity)
			items := make([]int, capacity)
			for lap := range 3 {
				for index := range items {
					items[index] = lap*capacity + index + 1
				}
				if !queue.Empty() || queue.Free() != capacity {
					t.Fatal("initial observation")
				}
				if free, err := queue.EnqueueBulk(items); free != 0 || err != nil {
					t.Fatalf("full reservation free=%d err=%v", free, err)
				}
				if !queue.Full() || queue.Count() != capacity || queue.Enqueue(0) != ErrFull {
					t.Fatal("full observation")
				}
				for _, want := range items {
					got, err := queue.Dequeue()
					if got != want || err != nil {
						t.Fatalf("got=%d want=%d err=%v", got, want, err)
					}
				}
				if _, err := queue.Dequeue(); err != ErrEmpty {
					t.Fatalf("empty=%v", err)
				}
			}
		})
	}
}

func TestSPMCMixedBoundaries(t *testing.T) {
	queue := spmcTestQueue[int](t, 8)
	if err := queue.Enqueue(1); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.EnqueueBulk([]int{2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if moved, free := queue.EnqueueBurst([]int{5, 6, 7, 8, 9}); moved != 4 || free != 0 {
		t.Fatalf("moved=%d free=%d", moved, free)
	}
	dst := []int{-1, -1, -1, -1, -1}
	if left, err := queue.DequeueBulk(dst); left != 3 || err != nil {
		t.Fatalf("left=%d err=%v", left, err)
	}
	if !slices.Equal(dst, []int{1, 2, 3, 4, 5}) {
		t.Fatal(dst)
	}
	if _, err := queue.DequeueBulk(dst[:4]); err != ErrEmpty {
		t.Fatal(err)
	}
	if !slices.Equal(dst, []int{1, 2, 3, 4, 5}) {
		t.Fatal("failed Bulk touched destination")
	}
	if moved, left := queue.DequeueBurst(dst); moved != 3 || left != 0 {
		t.Fatalf("moved=%d left=%d", moved, left)
	}
	if !slices.Equal(dst, []int{6, 7, 8, 4, 5}) {
		t.Fatal(dst)
	}
}

func TestSPMCOutOfOrderRelease(t *testing.T) {
	queue := spmcTestQueue[int](t, 8)
	if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}
	// A reserves 3 and stalls. B reserves 5 and finishes first.
	first, firstCount, _ := queue.spmcReserveConsumer(3, true, false)
	second, secondCount, _ := queue.spmcReserveConsumer(5, true, false)
	if first != 0 || firstCount != 3 || second != 3 || secondCount != 5 {
		t.Fatal("reservations", first, firstCount, second, secondCount)
	}
	got := make([]int, 5)
	queue.spmcReadConsumer(second, got)
	queue.spmcReleaseConsumer(second, secondCount)
	if !slices.Equal(got, []int{4, 5, 6, 7, 8}) {
		t.Fatal(got)
	}
	if queue.Count() != 0 {
		t.Fatal("Count includes reserved values", queue.Count())
	}
	if err := queue.Enqueue(9); err != ErrFull {
		t.Fatal("reused slots behind an unreleased reservation", err)
	}
	if free, _ := queue.EnqueueBurst([]int{9}); free != 0 {
		t.Fatal("free behind hole", free)
	}
	got = got[:3]
	queue.spmcReadConsumer(first, got)
	queue.spmcReleaseConsumer(first, firstCount)
	if !slices.Equal(got, []int{1, 2, 3}) {
		t.Fatal(got)
	}
	if moved, free := queue.EnqueueBurst([]int{9, 10, 11, 12, 13, 14, 15, 16}); moved != 8 ||
		free != 0 {
		t.Fatalf("moved=%d free=%d after both releases", moved, free)
	}
	if queue.producer.released != 8 {
		t.Fatal("released frontier", queue.producer.released)
	}
}

func TestSPMCStaleReleaseRecords(t *testing.T) {
	queue := spmcTestQueue[int](t, 8)
	// Lap 0 releases one record per two values, leaving records at even indexes.
	for range 4 {
		if _, err := queue.EnqueueBulk([]int{1, 2}); err != nil {
			t.Fatal(err)
		}
		if _, err := queue.DequeueBulk(make([]int, 2)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}
	// In lap 1, a hole at 8 must hide the stale lap-0 records ahead of it, and the release of
	// 12..16 must not be counted until 8..12 is released.
	hole, holeCount, _ := queue.spmcReserveConsumer(4, true, false)
	later, laterCount, _ := queue.spmcReserveConsumer(4, true, false)
	queue.spmcReleaseConsumer(later, laterCount)
	queue.spmcDiscoverReleases()
	if queue.producer.released != 8 {
		t.Fatal("discovered past a hole", queue.producer.released)
	}
	queue.spmcReleaseConsumer(hole, holeCount)
	queue.spmcDiscoverReleases()
	if queue.producer.released != 16 {
		t.Fatal("released frontier after hole", queue.producer.released)
	}
}

func TestSPMCAbsoluteWrap(t *testing.T) {
	for _, start := range []uint64{math.MaxUint32 - 5, 1<<40 - 3} {
		queue := spmcTestQueue[int](t, 8)
		spmcTestSeed(queue, start)
		for lap := range 3 {
			items := []int{lap, lap + 1, lap + 2, lap + 3, lap + 4, lap + 5}
			if _, err := queue.EnqueueBulk(items); err != nil {
				t.Fatal(lap, err)
			}
			if queue.Count() != len(items) {
				t.Fatal("count across wrap", queue.Count())
			}
			dst := make([]int, len(items))
			if moved, _ := queue.DequeueBurst(dst[:2]); moved != 2 {
				t.Fatal("first burst", moved)
			}
			if _, err := queue.DequeueBulk(dst[2:]); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(dst, items) {
				t.Fatal(dst, items)
			}
		}
	}
}

func TestSPMCCountsInvalidAndZero(t *testing.T) {
	for _, exact := range []bool{false, true} {
		t.Run(fmt.Sprint(exact), func(t *testing.T) {
			var options []Option
			if exact {
				options = append(options, WithExactBatchCounts())
			}
			queue := spmcTestQueue[int](t, 8, append(options, WithMaxBatch(4))...)
			if free, err := queue.EnqueueBulk(make([]int, 9)); free != 8 ||
				!errors.Is(err, ErrExceedsCapacity) {
				t.Fatal("capacity", free, err)
			}
			if free, err := queue.EnqueueBulk(make([]int, 5)); free != 8 ||
				!errors.Is(err, ErrExceedsMaxBatch) {
				t.Fatal("max batch", free, err)
			}
			if free, err := queue.EnqueueBulk(nil); free != 8 || err != nil {
				t.Fatal("zero enqueue", free, err)
			}
			if moved, free := queue.EnqueueBurst([]int{1, 2, 3, 4, 5}); moved != 4 || free != 4 {
				t.Fatal("clamped burst", moved, free)
			}
			if available, err := queue.DequeueBulk(make([]int, 9)); available != 4 ||
				!errors.Is(err, ErrExceedsCapacity) {
				t.Fatal("dequeue capacity", available, err)
			}
			if available, err := queue.DequeueBulk(nil); available != 4 || err != nil {
				t.Fatal("zero dequeue", available, err)
			}
			if moved, available := queue.DequeueBurst(nil); moved != 0 || available != 4 {
				t.Fatal("zero burst", moved, available)
			}
			if available, err := queue.DequeueBulk(make([]int, 2)); available != 2 || err != nil {
				t.Fatal("bulk", available, err)
			}
			if _, err := queue.DequeueBulk(make([]int, 3)); err != ErrEmpty {
				t.Fatal("short bulk", err)
			}
			if free, err := queue.EnqueueBulk([]int{5, 6}); err != nil || (exact && free != 4) {
				t.Fatal("refill", free, err)
			}
		})
	}
}

func TestSPMCOptions(t *testing.T) {
	for _, test := range []struct {
		name     string
		capacity int
		options  []Option
		want     error
	}{
		{"capacity-first", 3, []Option{WithMaxBatch(0)}, ErrInvalidCount},
		{"zero-count", 0, nil, ErrInvalidCount},
		{"enqueue", 8, []Option{WithMaxEnqueueBatch(9)}, ErrInvalidMaxBatch},
		{"dequeue", 8, []Option{WithMaxDequeueBatch(0)}, ErrInvalidMaxBatch},
		{"last-limit", 8, []Option{WithMaxBatch(0), WithMaxBatch(4)}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue, err := NewSPMCQueue[int](test.capacity, test.options...)
			if !errors.Is(err, test.want) {
				t.Fatalf("err=%v want=%v", err, test.want)
			}
			if test.want != nil && (err == test.want || queue != nil) {
				t.Fatal("constructor error must wrap and return nil")
			}
		})
	}
	queue := spmcTestQueue[int](t, 8, WithMaxBatch(4), WithMaxEnqueueBatch(2),
		WithExactBatchCounts(), WithoutDequeueClearing())
	if queue.Capacity() != 8 || queue.MaxEnqueueBatch() != 2 || queue.MaxDequeueBatch() != 4 ||
		!queue.producer.exactBatchCounts || !queue.consumerConfig.exactBatchCounts ||
		queue.consumerConfig.clearOnDequeue {
		t.Fatal("configuration")
	}
}

func TestSPMCClearingAndReset(t *testing.T) {
	for _, keep := range []bool{false, true} {
		var options []Option
		if keep {
			options = append(options, WithoutDequeueClearing())
		}
		queue := spmcTestQueue[*int](t, 4, options...)
		value := 7
		if err := queue.Enqueue(&value); err != nil {
			t.Fatal(err)
		}
		if _, err := queue.EnqueueBulk([]*int{&value, &value}); err != nil {
			t.Fatal(err)
		}
		if got, err := queue.Dequeue(); err != nil || got != &value {
			t.Fatal(got, err)
		}
		if _, err := queue.DequeueBulk(make([]*int, 2)); err != nil {
			t.Fatal(err)
		}
		for index := range 3 {
			if (queue.producer.entries[index] != nil) != keep {
				t.Fatal("clearing", keep, index)
			}
		}
		queue.Reset()
		if queue.Count() != 0 || queue.producer.cursor != 0 || queue.producer.released != 0 ||
			queue.producerPublished.Load() != 0 || queue.consumerReservation.head.Load() != 0 ||
			queue.consumerReservation.cachedProducer.Load() != 0 {
			t.Fatal("reset positions")
		}
		for index, entry := range queue.producer.entries {
			if entry != nil || queue.releases.records[index].Load() != 0 {
				t.Fatal("reset storage", index)
			}
		}
		if _, err := queue.EnqueueBulk([]*int{&value, &value, &value, &value}); err != nil {
			t.Fatal("reuse after Reset", err)
		}
	}
}

func TestSPMCAllocations(t *testing.T) {
	queue := spmcTestQueue[*int](t, 8)
	value := 1
	source := []*int{&value, &value}
	destination := make([]*int, 2)
	operation := func() {
		if err := queue.Enqueue(&value); err != nil {
			panic(err)
		}
		if _, err := queue.Dequeue(); err != nil {
			panic(err)
		}
		if _, err := queue.EnqueueBulk(source); err != nil {
			panic(err)
		}
		if moved, _ := queue.DequeueBurst(destination); moved != 2 {
			panic("burst")
		}
		_ = queue.Count() + queue.Free()
	}
	if allocations := testing.AllocsPerRun(100, operation); allocations != 0 {
		t.Fatalf("allocations=%v", allocations)
	}
}

// TestSPMCConcurrentAccounting checks that four consumers receive every value exactly once and
// each sees values in increasing order, across batch sizes that exercise release lookahead.
func TestSPMCConcurrentAccounting(t *testing.T) {
	const consumers = 4
	for _, capacity := range []int{8, 64, 1024} {
		for _, batch := range []int{1, 3, 8, 32} {
			if batch > capacity {
				continue
			}
			t.Run(fmt.Sprintf("capacity=%d/batch=%d", capacity, batch), func(t *testing.T) {
				total := 20000
				if testing.Short() {
					total = 2000
				}
				queue := spmcTestQueue[int](t, capacity)
				seen := make([]bool, total)
				var mu sync.Mutex
				var workers sync.WaitGroup
				var remaining sync.WaitGroup
				remaining.Add(total)
				done := make(chan struct{})
				for c := range consumers {
					workers.Go(func() {
						dst := make([]int, batch)
						last := -1
						for {
							select {
							case <-done:
								return
							default:
							}
							var moved int
							if c%2 == 0 {
								moved, _ = queue.DequeueBurst(dst)
							} else if value, err := queue.Dequeue(); err == nil {
								dst[0], moved = value, 1
							}
							for _, value := range dst[:moved] {
								if value <= last {
									t.Errorf("consumer %d saw %d after %d", c, value, last)
								}
								last = value
								mu.Lock()
								if seen[value] {
									t.Errorf("value %d received twice", value)
								}
								seen[value] = true
								mu.Unlock()
								remaining.Done()
							}
						}
					})
				}
				items := make([]int, batch)
				for sent := 0; sent < total; {
					length := min(batch, total-sent)
					for index := range items[:length] {
						items[index] = sent + index
					}
					moved, _ := queue.EnqueueBurst(items[:length])
					sent += moved
				}
				remaining.Wait()
				close(done)
				workers.Wait()
				if !queue.Empty() {
					t.Fatal("not drained")
				}
			})
		}
	}
}
