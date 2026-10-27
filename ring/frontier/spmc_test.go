package frontier

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
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

// TestSPMCCacheBelowHead checks that a shared cache that a delayed consumer moved below the head is
// refreshed instead of being read as more than a capacity of published values.
func TestSPMCCacheBelowHead(t *testing.T) {
	queue := spmcTestQueue[int](t, 8)
	for value := range 5 {
		if err := queue.Enqueue(value); err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		if _, err := queue.Dequeue(); err != nil {
			t.Fatal(err)
		}
	}
	// A delayed consumer stores an older published position after the head reached 4.
	queue.consumerReservation.cachedProducer.Store(2)
	start, count, remaining, retry := queue.spmcAttemptConsumer(4, 1, true, false)
	if start != 4 || count != 1 || remaining != 0 || retry {
		t.Fatalf("start=%d count=%d remaining=%d retry=%t", start, count, remaining, retry)
	}
	if cached := queue.consumerReservation.cachedProducer.Load(); cached != 5 {
		t.Fatalf("admitted attempt stored cache %d, want 5", cached)
	}
}

// TestSPMCStaleHead checks that a head loaded a lap behind the published position reloads, even
// for a zero-length request.
func TestSPMCStaleHead(t *testing.T) {
	queue := spmcTestQueue[int](t, 8)
	spmcTestSeed(queue, 16)
	if _, err := queue.EnqueueBulk([]int{1, 2}); err != nil {
		t.Fatal(err)
	}
	_, count, _, retry := queue.spmcAttemptConsumer(7, 1, true, false)
	if count != 0 || !retry {
		t.Fatalf("count=%d retry=%t", count, retry)
	}
	_, count, _, retry = queue.spmcAttemptConsumer(7, 0, true, false)
	if count != 0 || !retry {
		t.Fatalf("zero-length count=%d retry=%t", count, retry)
	}
}

// TestSPMCFailedAttemptLeavesCache checks that only an attempt that goes on to reserve stores the
// refreshed published position in the shared cache.
func TestSPMCFailedAttemptLeavesCache(t *testing.T) {
	queue := spmcTestQueue[int](t, 8)
	if err := queue.Enqueue(1); err != nil {
		t.Fatal(err)
	}
	_, count, remaining, retry := queue.spmcAttemptConsumer(0, 2, true, false)
	if count != 0 || remaining != 1 || retry {
		t.Fatalf("count=%d remaining=%d retry=%t", count, remaining, retry)
	}
	if cached := queue.consumerReservation.cachedProducer.Load(); cached != 0 {
		t.Fatalf("failed attempt stored cache %d", cached)
	}
	if _, count, _, _ := queue.spmcAttemptConsumer(0, 1, true, false); count != 1 {
		t.Fatalf("admitted count=%d", count)
	}
	if cached := queue.consumerReservation.cachedProducer.Load(); cached != 1 {
		t.Fatalf("admitted attempt stored cache %d, want 1", cached)
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

// TestSPMCFreshCounts checks that failed, invalid-length, and zero-length calls return counts
// that include releases and publications the caller had not discovered yet.
func TestSPMCFreshCounts(t *testing.T) {
	for _, exact := range []bool{false, true} {
		t.Run(fmt.Sprint(exact), func(t *testing.T) {
			options := []Option{WithMaxBatch(4)}
			if exact {
				options = append(options, WithExactBatchCounts())
			}
			queue := spmcTestQueue[int](t, 8, options...)
			for range 2 {
				if _, err := queue.EnqueueBulk(make([]int, 4)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := queue.DequeueBulk(make([]int, 4)); err != nil {
				t.Fatal(err)
			}
			if free, err := queue.EnqueueBulk(make([]int, 5)); free != 4 ||
				!errors.Is(err, ErrExceedsMaxBatch) {
				t.Fatal("invalid enqueue", free, err)
			}
			if _, err := queue.EnqueueBulk(make([]int, 4)); err != nil {
				t.Fatal(err)
			}
			if _, err := queue.DequeueBulk(make([]int, 2)); err != nil {
				t.Fatal(err)
			}
			if free, err := queue.EnqueueBulk(make([]int, 3)); free != 2 || err != ErrFull {
				t.Fatal("full enqueue", free, err)
			}
			if _, err := queue.DequeueBulk(make([]int, 1)); err != nil {
				t.Fatal(err)
			}
			if moved, free := queue.EnqueueBurst(nil); moved != 0 || free != 3 {
				t.Fatal("zero burst", moved, free)
			}
			if available, err := queue.DequeueBulk(make([]int, 5)); available != 5 ||
				!errors.Is(err, ErrExceedsMaxBatch) {
				t.Fatal("invalid dequeue", available, err)
			}
			if available, err := queue.DequeueBulk(make([]int, 9)); available != 5 ||
				!errors.Is(err, ErrExceedsCapacity) {
				t.Fatal("dequeue capacity", available, err)
			}
			for _, length := range []int{4, 1} {
				if _, err := queue.DequeueBulk(make([]int, length)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := queue.EnqueueBulk(make([]int, 2)); err != nil {
				t.Fatal(err)
			}
			if available, err := queue.DequeueBulk(make([]int, 3)); available != 2 ||
				err != ErrEmpty {
				t.Fatal("empty dequeue", available, err)
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
		if _, err := queue.EnqueueBulk([]*int{&value, &value, &value}); err != nil {
			t.Fatal(err)
		}
		if _, err := queue.DequeueBulk(make([]*int, 3)); err != nil {
			t.Fatal(err)
		}
		for _, index := range []int{3, 0, 1} {
			if (queue.producer.entries[index] != nil) != keep {
				t.Fatal("wrapped clearing", keep, index)
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
				for consumer := range consumers {
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
							if consumer%2 == 0 {
								moved, _ = queue.DequeueBurst(dst)
							} else if value, err := queue.Dequeue(); err == nil {
								dst[0], moved = value, 1
							}
							if moved == 0 {
								runtime.Gosched()
								continue
							}
							for _, value := range dst[:moved] {
								if value <= last {
									t.Errorf("consumer %d saw %d after %d", consumer, value, last)
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
				deadline := time.Now().Add(time.Minute)
				items := make([]int, batch)
				for sent := 0; sent < total; {
					if time.Now().After(deadline) {
						close(done)
						workers.Wait()
						t.Fatalf("producer sent %d of %d values within a minute", sent, total)
					}
					length := min(batch, total-sent)
					for index := range items[:length] {
						items[index] = sent + index
					}
					moved, _ := queue.EnqueueBurst(items[:length])
					if moved == 0 {
						runtime.Gosched()
					}
					sent += moved
				}
				received := make(chan struct{})
				go func() {
					remaining.Wait()
					close(received)
				}()
				select {
				case <-received:
				case <-time.After(time.Until(deadline)):
					close(done)
					workers.Wait()
					t.Fatal("consumers did not receive every value within a minute")
				}
				close(done)
				workers.Wait()
				if !queue.Empty() {
					t.Fatal("not drained")
				}
			})
		}
	}
}

// TestSPMCConcurrentMixedAPIs checks exactly-once delivery in increasing order per consumer when
// consumers mix scalar, Burst, and Bulk calls, the producer mixes scalar, Burst, and Bulk calls,
// and an observer calls Count throughout, for each combination of exact counts and clearing.
func TestSPMCConcurrentMixedAPIs(t *testing.T) {
	for _, exact := range []bool{false, true} {
		for _, keep := range []bool{false, true} {
			t.Run(fmt.Sprintf("exact=%t/keep=%t", exact, keep), func(t *testing.T) {
				var options []Option
				if exact {
					options = append(options, WithExactBatchCounts())
				}
				if keep {
					options = append(options, WithoutDequeueClearing())
				}
				spmcTestMixedAPIs(t, spmcTestQueue[int](t, 64, options...))
			})
		}
	}
}

func spmcTestMixedAPIs(t *testing.T, queue *SPMCQueue[int]) {
	t.Helper()
	total := 50000
	if testing.Short() {
		total = 5000
	}
	deadline := time.Now().Add(time.Minute)
	seen := make([]atomic.Bool, total)
	var received atomic.Int64
	done := make(chan struct{})
	var workers sync.WaitGroup
	for consumer, length := range []int{1, 3, 7, 16} {
		workers.Go(func() {
			dst := make([]int, length)
			last := -1
			for {
				select {
				case <-done:
					return
				default:
				}
				moved := spmcTestDequeue(t, queue, consumer, dst)
				if moved == 0 {
					runtime.Gosched()
					continue
				}
				for _, value := range dst[:moved] {
					if value <= last {
						t.Errorf("consumer %d saw %d after %d", consumer, value, last)
					}
					last = value
					if seen[value].Swap(true) {
						t.Errorf("value %d received twice", value)
					}
				}
				received.Add(int64(moved))
			}
		})
	}
	// The observer exercises Count against both sides under the race detector.
	workers.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if count := queue.Count(); count > queue.Capacity() {
				t.Errorf("Count %d exceeds capacity %d", count, queue.Capacity())
			}
			runtime.Gosched()
		}
	})
	batch := make([]int, 5)
	for sent := 0; sent < total; {
		if time.Now().After(deadline) {
			close(done)
			workers.Wait()
			t.Fatalf("producer sent %d of %d values within a minute", sent, total)
		}
		moved := 0
		style := sent % 3
		if total-sent < len(batch) {
			style = 0
		}
		switch style {
		case 0:
			err := queue.Enqueue(sent)
			if err == nil {
				moved = 1
			} else if err != ErrFull {
				t.Fatal(err)
			}
		case 1:
			for index := range batch {
				batch[index] = sent + index
			}
			moved, _ = queue.EnqueueBurst(batch)
		default:
			for index := range batch {
				batch[index] = sent + index
			}
			_, err := queue.EnqueueBulk(batch)
			if err == nil {
				moved = len(batch)
			} else if err != ErrFull {
				t.Fatal(err)
			}
		}
		if moved == 0 {
			runtime.Gosched()
		}
		sent += moved
	}
	for received.Load() < int64(total) {
		if time.Now().After(deadline) {
			close(done)
			workers.Wait()
			t.Fatalf("received %d of %d values within a minute", received.Load(), total)
		}
		runtime.Gosched()
	}
	close(done)
	workers.Wait()
	for value := range seen {
		if !seen[value].Load() {
			t.Fatalf("value %d never received", value)
		}
	}
	if !queue.Empty() {
		t.Fatal("not drained")
	}
}

// spmcTestDequeue dequeues into dst with the call style assigned to consumer. Consumer 0 calls
// Dequeue, consumer 1 calls DequeueBurst, and the others call DequeueBulk for a full dst.
func spmcTestDequeue(t *testing.T, queue *SPMCQueue[int], consumer int, dst []int) int {
	switch consumer {
	case 0:
		value, err := queue.Dequeue()
		if err != nil {
			if err != ErrEmpty {
				t.Error(err)
			}
			return 0
		}
		dst[0] = value
		return 1
	case 1:
		moved, _ := queue.DequeueBurst(dst)
		return moved
	default:
		if _, err := queue.DequeueBulk(dst); err != nil {
			if err != ErrEmpty {
				t.Error(err)
			}
			return 0
		}
		return len(dst)
	}
}

// TestSPMCReleaseLookaheadHole checks release discovery with lookahead over records left from the
// previous lap, with a hole among the predicted boundaries.
func TestSPMCReleaseLookaheadHole(t *testing.T) {
	queue := spmcTestQueue[int](t, 64)
	batch := make([]int, 8)
	// Lap 0 leaves a release record at every eighth position.
	for range 8 {
		if _, err := queue.EnqueueBulk(batch); err != nil {
			t.Fatal(err)
		}
		if _, err := queue.DequeueBulk(batch); err != nil {
			t.Fatal(err)
		}
	}
	for range 8 {
		if _, err := queue.EnqueueBulk(batch); err != nil {
			t.Fatal(err)
		}
	}
	// Lap 1 releases every eight-position interval except the one starting at 80.
	var hole uint64
	var holeCount uint32
	for range 8 {
		start, count, _ := queue.spmcReserveConsumer(8, true, false)
		if start == 80 {
			hole, holeCount = start, count
			continue
		}
		queue.spmcReleaseConsumer(start, count)
	}
	queue.spmcDiscoverReleases()
	if queue.producer.released != 80 {
		t.Fatalf("released %d past the hole, want 80", queue.producer.released)
	}
	queue.spmcReleaseConsumer(hole, holeCount)
	queue.spmcDiscoverReleases()
	if queue.producer.released != 128 {
		t.Fatalf("released %d after the hole, want 128", queue.producer.released)
	}
}

// TestSPMCLayout checks that each group keeps its fields, that 128-byte gaps separate the groups,
// and that the atomic fields are 8-byte aligned.
func TestSPMCLayout(t *testing.T) {
	queue := spmcTestQueue[int64](t, 8)
	outer := reflect.TypeOf(queue).Elem()
	groups := []struct {
		name   string
		fields []string
	}{
		{"producer", []string{"entries", "mask", "cursor", "released", "maxBatch", "exactBatchCounts"}},
		{"producerPublished", nil},
		{"consumerConfig", []string{"entries", "mask", "maxBatch", "clearOnDequeue", "exactBatchCounts"}},
		{"consumerReservation", []string{"head", "cachedProducer"}},
		{"releases", []string{"records"}},
	}
	if outer.NumField() != 2*len(groups)-1 {
		t.Fatalf("got %d fields, want %d", outer.NumField(), 2*len(groups)-1)
	}
	for index, group := range groups {
		field := outer.Field(2 * index)
		if field.Name != group.name {
			t.Fatalf("field %d is %s, want %s", 2*index, field.Name, group.name)
		}
		if index > 0 && outer.Field(2*index-1).Type != reflect.TypeOf([128]byte{}) {
			t.Fatalf("field %d is not a 128-byte gap", 2*index-1)
		}
		if group.fields == nil {
			if field.Type != reflect.TypeOf(atomic.Uint64{}) {
				t.Fatalf("%s is %s, want atomic.Uint64", field.Name, field.Type)
			}
			continue
		}
		if field.Type.NumField() != len(group.fields) {
			t.Fatalf("%s has %d fields, want %d", field.Name, field.Type.NumField(), len(group.fields))
		}
		for position, name := range group.fields {
			if field.Type.Field(position).Name != name {
				t.Fatalf("%s field %d is %s, want %s", field.Name, position,
					field.Type.Field(position).Name, name)
			}
		}
	}
	for _, address := range []uintptr{
		uintptr(unsafe.Pointer(&queue.producerPublished)),
		uintptr(unsafe.Pointer(&queue.consumerReservation.head)),
		uintptr(unsafe.Pointer(&queue.consumerReservation.cachedProducer)),
	} {
		if address%8 != 0 {
			t.Fatalf("atomic field at %#x is not 8-byte aligned", address)
		}
	}
}

// TestSPMCSingleProcessorProgress checks that consumers yielding after a lost compare-and-swap
// still let the producer and each other finish when they share one processor.
func TestSPMCSingleProcessorProgress(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	const total, consumers = 5000, 4
	queue := spmcTestQueue[int](t, 8)
	var received atomic.Int64
	var workers sync.WaitGroup
	for range consumers {
		workers.Go(func() {
			for received.Load() < total {
				if _, err := queue.Dequeue(); err == nil {
					received.Add(1)
				} else {
					runtime.Gosched()
				}
			}
		})
	}
	for sent := 0; sent < total; {
		if queue.Enqueue(sent) == nil {
			sent++
		} else {
			runtime.Gosched()
		}
	}
	workers.Wait()
	if received.Load() != total || !queue.Empty() {
		t.Fatal("received", received.Load())
	}
}
