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

func mpmcTestQueue[T any](t *testing.T, capacity int, options ...Option) *MPMCQueue[T] {
	t.Helper()
	queue, err := NewMPMCQueue[T](capacity, options...)
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

// mpmcTestSeed moves an empty queue's positions to position, as if that many values had passed.
func mpmcTestSeed[T any](queue *MPMCQueue[T], position uint64) {
	queue.producerHead.Store(position)
	queue.completed.Store(position)
	queue.consumerHead.Store(position)
	queue.released.Store(position)
}

func TestMPMCCapacityReuseFIFO(t *testing.T) {
	for _, capacity := range []int{1, 8, 1024} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			queue := mpmcTestQueue[int](t, capacity)
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

func TestMPMCMixedBoundaries(t *testing.T) {
	queue := mpmcTestQueue[int](t, 8)
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

// TestMPMCCompletionHole checks that consumers and Count stop at a producer reservation that has
// not completed, even when a later reservation has.
func TestMPMCCompletionHole(t *testing.T) {
	queue := mpmcTestQueue[int](t, 8)
	first, firstCount, _ := queue.mpmcReserveProducer(3, true, false)
	second, secondCount, _ := queue.mpmcReserveProducer(5, true, false)
	if first != 0 || firstCount != 3 || second != 3 || secondCount != 5 {
		t.Fatal("reservations", first, firstCount, second, secondCount)
	}
	queue.mpmcWriteProducer(second, []int{4, 5, 6, 7, 8})
	queue.mpmcPublishCompletion(second, secondCount)
	if queue.Count() != 0 {
		t.Fatal("Count past a hole", queue.Count())
	}
	if _, err := queue.Dequeue(); err != ErrEmpty {
		t.Fatal("dequeued past a hole", err)
	}
	if completed := queue.completed.Load(); completed != 0 {
		t.Fatal("completed frontier past a hole", completed)
	}
	for _, dst := range [][]int{nil, make([]int, 2)} {
		if moved, available := queue.DequeueBurst(dst); moved != 0 || available != 0 {
			t.Fatalf("publication hole: moved=%d available=%d", moved, available)
		}
	}
	if moved, reusable := queue.EnqueueBurst(nil); moved != 0 || reusable != 0 {
		t.Fatalf("reserved capacity: moved=%d reusable=%d", moved, reusable)
	}
	queue.mpmcWriteProducer(first, []int{1, 2, 3})
	queue.mpmcPublishCompletion(first, firstCount)
	if queue.Count() != 8 {
		t.Fatal("Count after the hole", queue.Count())
	}
	if moved, available := queue.DequeueBurst(nil); moved != 0 || available != 8 {
		t.Fatalf("completed publication: moved=%d available=%d", moved, available)
	}
	for want := 1; want <= 8; want++ {
		got, err := queue.Dequeue()
		if got != want || err != nil {
			t.Fatalf("got=%d want=%d err=%v", got, want, err)
		}
	}
}

// TestMPMCReleaseHole checks that producers do not reuse slots past a consumer reservation that
// has not been released, even when a later reservation has.
func TestMPMCReleaseHole(t *testing.T) {
	queue := mpmcTestQueue[int](t, 8)
	if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}
	first, firstCount, _ := queue.mpmcReserveConsumer(3, true, false)
	second, secondCount, _ := queue.mpmcReserveConsumer(5, true, false)
	if first != 0 || firstCount != 3 || second != 3 || secondCount != 5 {
		t.Fatal("reservations", first, firstCount, second, secondCount)
	}
	got := make([]int, 5)
	queue.mpmcReadConsumer(second, got)
	queue.mpmcPublishRelease(second, secondCount)
	if !slices.Equal(got, []int{4, 5, 6, 7, 8}) {
		t.Fatal(got)
	}
	if err := queue.Enqueue(9); err != ErrFull {
		t.Fatal("reused slots behind an unreleased reservation", err)
	}
	for _, items := range [][]int{nil, {9, 10}} {
		if moved, reusable := queue.EnqueueBurst(items); moved != 0 || reusable != 0 {
			t.Fatalf("release hole: moved=%d reusable=%d", moved, reusable)
		}
	}
	if moved, available := queue.DequeueBurst(nil); moved != 0 || available != 0 {
		t.Fatalf("reserved work: moved=%d available=%d", moved, available)
	}
	if released := queue.released.Load(); released != 0 {
		t.Fatal("released frontier past a hole", released)
	}
	got = got[:3]
	queue.mpmcReadConsumer(first, got)
	queue.mpmcPublishRelease(first, firstCount)
	if !slices.Equal(got, []int{1, 2, 3}) {
		t.Fatal(got)
	}
	if moved, reusable := queue.EnqueueBurst(nil); moved != 0 || reusable != 8 {
		t.Fatalf("completed release: moved=%d reusable=%d", moved, reusable)
	}
	if _, err := queue.EnqueueBulk([]int{9, 10, 11, 12, 13, 14, 15, 16}); err != nil {
		t.Fatal("refill after both releases", err)
	}
}

// TestMPMCLookaheadAcrossHoles checks completion and release discovery with lookahead over records
// left from the previous lap, with a hole among the predicted boundaries.
func TestMPMCLookaheadAcrossHoles(t *testing.T) {
	queue := mpmcTestQueue[int](t, 64)
	batch := make([]int, 8)
	// Lap 0 leaves a completion and a release record at every eighth position.
	for range 8 {
		if _, err := queue.EnqueueBulk(batch); err != nil {
			t.Fatal(err)
		}
		if _, err := queue.DequeueBulk(batch); err != nil {
			t.Fatal(err)
		}
	}
	var hole uint64
	var holeCount uint32
	for range 8 {
		start, count, _ := queue.mpmcReserveProducer(8, true, false)
		if start == 80 {
			hole, holeCount = start, count
			continue
		}
		queue.mpmcPublishCompletion(start, count)
	}
	if completed := queue.mpmcAdvanceCompleted(); completed != 80 {
		t.Fatalf("completed %d past the hole, want 80", completed)
	}
	queue.mpmcPublishCompletion(hole, holeCount)
	if completed := queue.mpmcAdvanceCompleted(); completed != 128 {
		t.Fatalf("completed %d after the hole, want 128", completed)
	}
	for range 8 {
		start, count, _ := queue.mpmcReserveConsumer(8, true, false)
		if start == 80 {
			hole, holeCount = start, count
			continue
		}
		queue.mpmcPublishRelease(start, count)
	}
	if released := queue.mpmcAdvanceReleased(); released != 80 {
		t.Fatalf("released %d past the hole, want 80", released)
	}
	queue.mpmcPublishRelease(hole, holeCount)
	if released := queue.mpmcAdvanceReleased(); released != 128 {
		t.Fatalf("released %d after the hole, want 128", released)
	}
}

// TestMPMCCapacityOne checks holes on both sides when every record shares index 0 and holds the
// end of the previous lap's reservation.
func TestMPMCCapacityOne(t *testing.T) {
	queue := mpmcTestQueue[int](t, 1)
	for lap := range 3 {
		if err := queue.Enqueue(lap); err != nil {
			t.Fatal(lap, err)
		}
		if got, err := queue.Dequeue(); got != lap || err != nil {
			t.Fatalf("got=%d want=%d err=%v", got, lap, err)
		}
	}
	if queue.producerHead.Load() != 3 || queue.completed.Load() != 3 ||
		queue.consumerHead.Load() != 3 || queue.released.Load() != 2 ||
		queue.config.completions[0].Load() != 3 || queue.config.releases[0].Load() != 3 {
		t.Fatal("state after three laps")
	}

	start, count, _ := queue.mpmcReserveProducer(1, true, false)
	if start != 3 || count != 1 {
		t.Fatal("producer reservation", start, count)
	}
	if released := queue.released.Load(); released != 3 {
		t.Fatal("released before the completion hole", released)
	}
	if queue.Count() != 0 {
		t.Fatal("Count past a completion hole", queue.Count())
	}
	if _, err := queue.Dequeue(); err != ErrEmpty {
		t.Fatal("dequeued past a completion hole", err)
	}
	if completed := queue.completed.Load(); completed != 3 {
		t.Fatal("previous lap's completion record accepted", completed)
	}

	queue.mpmcWriteProducer(start, []int{3})
	queue.mpmcPublishCompletion(start, count)
	start, count, _ = queue.mpmcReserveConsumer(1, true, false)
	if start != 3 || count != 1 {
		t.Fatal("consumer reservation", start, count)
	}
	if err := queue.Enqueue(4); err != ErrFull {
		t.Fatal("reused a slot behind an unreleased reservation", err)
	}
	if released := queue.released.Load(); released != 3 {
		t.Fatal("previous lap's release record accepted", released)
	}
	queue.mpmcPublishRelease(start, count)
	if err := queue.Enqueue(4); err != nil {
		t.Fatal("enqueue after release", err)
	}
}

func TestMPMCCountsInvalidAndZero(t *testing.T) {
	for _, exact := range []bool{false, true} {
		t.Run(fmt.Sprint(exact), func(t *testing.T) {
			var options []Option
			if exact {
				options = append(options, WithExactBatchCounts())
			}
			queue := mpmcTestQueue[int](t, 8, append(options, WithMaxBatch(4))...)
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

			// Without exact counts, the second burst trusts the completed frontier of 2 that
			// the first discovered. With them, it discovers up to 4.
			queue = mpmcTestQueue[int](t, 8, options...)
			if _, err := queue.EnqueueBulk([]int{1, 2}); err != nil {
				t.Fatal(err)
			}
			if moved, _ := queue.DequeueBurst(make([]int, 1)); moved != 1 {
				t.Fatal("first burst", moved)
			}
			if _, err := queue.EnqueueBulk([]int{3, 4}); err != nil {
				t.Fatal(err)
			}
			wantAvailable := 0
			if exact {
				wantAvailable = 2
			}
			if moved, available := queue.DequeueBurst(make([]int, 1)); moved != 1 ||
				available != wantAvailable {
				t.Fatalf("second burst moved=%d available=%d want=%d", moved, available,
					wantAvailable)
			}
		})
	}
}

// TestMPMCFreshCounts checks that failed, invalid-length, and zero-length calls return counts
// that include records the shared frontiers have not reached yet.
func TestMPMCFreshCounts(t *testing.T) {
	for _, exact := range []bool{false, true} {
		t.Run(fmt.Sprint(exact), func(t *testing.T) {
			options := []Option{WithMaxBatch(4)}
			if exact {
				options = append(options, WithExactBatchCounts())
			}
			queue := mpmcTestQueue[int](t, 8, options...)
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

// TestMPMCCountDefinition checks that Count counts completed values no consumer has reserved and
// stops at a completion hole.
func TestMPMCCountDefinition(t *testing.T) {
	queue := mpmcTestQueue[int](t, 8)
	if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if _, count, _ := queue.mpmcReserveConsumer(1, true, false); count != 1 {
		t.Fatal("consumer reservation", count)
	}
	hole, holeCount, _ := queue.mpmcReserveProducer(2, true, false)
	later, laterCount, _ := queue.mpmcReserveProducer(1, true, false)
	if hole != 4 || holeCount != 2 || later != 6 || laterCount != 1 {
		t.Fatal("producer reservations", hole, holeCount, later, laterCount)
	}
	queue.mpmcPublishCompletion(later, laterCount)
	if queue.Count() != 3 || queue.Free() != 5 || queue.Empty() {
		t.Fatal("observations before the hole", queue.Count(), queue.Free(), queue.Empty())
	}
	queue.mpmcPublishCompletion(hole, holeCount)
	if queue.Count() != 6 {
		t.Fatal("Count after the hole", queue.Count())
	}
}

// TestMPMCStaleProducerHeadAfterDiscovery checks that a zero-length attempt from a producer head
// that discovery shows to be a lap stale retries instead of reporting a count above capacity.
func TestMPMCStaleProducerHeadAfterDiscovery(t *testing.T) {
	queue := mpmcTestQueue[int](t, 8)
	if _, err := queue.EnqueueBulk(make([]int, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.DequeueBulk(make([]int, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.EnqueueBulk(make([]int, 4)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.DequeueBulk(make([]int, 4)); err != nil {
		t.Fatal(err)
	}
	if queue.producerHead.Load() != 12 || queue.consumerHead.Load() != 12 ||
		queue.released.Load() != 8 {
		t.Fatal("setup", queue.producerHead.Load(), queue.consumerHead.Load(),
			queue.released.Load())
	}
	_, count, remaining, retry := queue.mpmcAttemptProducer(8, 0, true, false)
	if !retry {
		t.Fatalf("count=%d remaining=%d retry=%t", count, remaining, retry)
	}
}

// TestMPMCStaleConsumerHeadAfterDiscovery checks that a zero-length attempt from a consumer head
// that discovery shows to be a lap stale retries instead of reporting a count above capacity.
func TestMPMCStaleConsumerHeadAfterDiscovery(t *testing.T) {
	queue := mpmcTestQueue[int](t, 8)
	if _, err := queue.EnqueueBulk(make([]int, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.DequeueBulk(make([]int, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.EnqueueBulk(make([]int, 8)); err != nil {
		t.Fatal(err)
	}
	if queue.consumerHead.Load() != 8 || queue.completed.Load() != 8 ||
		queue.producerHead.Load() != 16 {
		t.Fatal("setup", queue.consumerHead.Load(), queue.completed.Load(),
			queue.producerHead.Load())
	}
	_, count, remaining, retry := queue.mpmcAttemptConsumer(0, 0, true, false)
	if !retry {
		t.Fatalf("count=%d remaining=%d retry=%t", count, remaining, retry)
	}
}

// TestMPMCRaiseAfterShorterWinner checks that a discoverer still raises a frontier to its own end
// after a caller that proved a shorter range wins the compare-and-swap.
func TestMPMCRaiseAfterShorterWinner(t *testing.T) {
	t.Run("completions", func(t *testing.T) {
		queue := mpmcTestQueue[int](t, 4)
		for value := range 2 {
			if err := queue.Enqueue(value); err != nil {
				t.Fatal(err)
			}
		}
		config := &queue.config
		end := mpmcDiscover(config.completions, config.mask, 0, queue.producerHead.Load())
		if end != 2 {
			t.Fatal("discovered end", end)
		}
		// A Count observer that proved [0, 1) wins.
		if !queue.completed.CompareAndSwap(0, 1) {
			t.Fatal("shorter winner")
		}
		if raised := raiseFrontier(&queue.completed, 0, end); raised != 2 {
			t.Fatal("raised", raised)
		}
		if completed := queue.completed.Load(); completed != 2 {
			t.Fatal("completed", completed)
		}
		if _, err := queue.DequeueBulk(make([]int, 2)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("releases", func(t *testing.T) {
		queue := mpmcTestQueue[int](t, 4)
		for value := range 2 {
			if err := queue.Enqueue(value); err != nil {
				t.Fatal(err)
			}
		}
		for range 2 {
			if _, err := queue.Dequeue(); err != nil {
				t.Fatal(err)
			}
		}
		config := &queue.config
		end := mpmcDiscover(config.releases, config.mask, 0, queue.consumerHead.Load())
		if end != 2 {
			t.Fatal("discovered end", end)
		}
		if !queue.released.CompareAndSwap(0, 1) {
			t.Fatal("shorter winner")
		}
		if raised := raiseFrontier(&queue.released, 0, end); raised != 2 {
			t.Fatal("raised", raised)
		}
		if released := queue.released.Load(); released != 2 {
			t.Fatal("released", released)
		}
		if _, err := queue.EnqueueBulk(make([]int, 4)); err != nil {
			t.Fatal(err)
		}
	})
}

// TestMPMCStaleDiscovererAcrossLap checks that a discoverer that loaded a frontier and paused while
// the queue moved more than a lap rejects later laps' records and leaves the frontier alone.
func TestMPMCStaleDiscovererAcrossLap(t *testing.T) {
	queue := mpmcTestQueue[int](t, 8)
	if _, err := queue.EnqueueBulk(make([]int, 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.DequeueBulk(make([]int, 3)); err != nil {
		t.Fatal(err)
	}
	startCompleted := queue.completed.Load()
	startReleased := queue.released.Load()
	if startCompleted != 3 || startReleased != 0 {
		t.Fatal("starting frontiers", startCompleted, startReleased)
	}
	for range 3 {
		if _, err := queue.EnqueueBulk(make([]int, 8)); err != nil {
			t.Fatal(err)
		}
		if _, err := queue.DequeueBulk(make([]int, 8)); err != nil {
			t.Fatal(err)
		}
	}
	if queue.completed.Load() != 27 || queue.released.Load() != 19 {
		t.Fatal("frontiers after three laps", queue.completed.Load(), queue.released.Load())
	}
	config := &queue.config

	before := queue.completed.Load()
	end := mpmcDiscover(config.completions, config.mask, startCompleted, queue.producerHead.Load())
	if end != startCompleted {
		t.Fatal("accepted a later lap's completion record", end)
	}
	raised := raiseFrontier(&queue.completed, startCompleted, end)
	if queue.completed.Load() != before || raised < before {
		t.Fatal("completed", queue.completed.Load(), raised, before)
	}

	before = queue.released.Load()
	end = mpmcDiscover(config.releases, config.mask, startReleased, queue.consumerHead.Load())
	if end != 3 {
		t.Fatal("release discovery end", end)
	}
	raised = raiseFrontier(&queue.released, startReleased, end)
	if queue.released.Load() != before || raised < before {
		t.Fatal("released", queue.released.Load(), raised, before)
	}
}

func TestMPMCAbsoluteWrap(t *testing.T) {
	for _, start := range []uint64{math.MaxUint32 - 5, 1<<40 - 3} {
		queue := mpmcTestQueue[int](t, 8)
		mpmcTestSeed(queue, start)
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

// TestMPMCConcurrentMixedAPIs checks exactly-once delivery and per-producer order within each
// consumer when producers and consumers mix scalar, Burst, and Bulk calls and an observer calls
// Count throughout, for each combination of exact counts and clearing.
func TestMPMCConcurrentMixedAPIs(t *testing.T) {
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
				mpmcTestMixedAPIs(t, mpmcTestQueue[uint64](t, 64, options...))
			})
		}
	}
}

const (
	mpmcTestProducers    = 4
	mpmcTestSequenceBits = 56
	mpmcTestSequenceMask = 1<<mpmcTestSequenceBits - 1
)

func mpmcTestMixedAPIs(t *testing.T, queue *MPMCQueue[uint64]) {
	t.Helper()
	perProducer := 20000
	if testing.Short() {
		perProducer = 2000
	}
	total := mpmcTestProducers * perProducer
	deadline := time.Now().Add(time.Minute)
	seen := make([][]atomic.Bool, mpmcTestProducers)
	for producer := range seen {
		seen[producer] = make([]atomic.Bool, perProducer)
	}
	var received atomic.Int64
	done := make(chan struct{})
	var workers sync.WaitGroup
	for consumer, length := range []int{1, 5, 3, 7} {
		workers.Go(func() {
			dst := make([]uint64, length)
			var last [mpmcTestProducers]int
			for producer := range last {
				last[producer] = -1
			}
			for {
				select {
				case <-done:
					return
				default:
				}
				moved := mpmcTestDequeue(t, queue, consumer, dst)
				if moved == 0 {
					runtime.Gosched()
					continue
				}
				for _, value := range dst[:moved] {
					producer := int(value >> mpmcTestSequenceBits)
					sequence := int(value & mpmcTestSequenceMask)
					if sequence <= last[producer] {
						t.Errorf(
							"consumer %d saw producer %d's %d after %d",
							consumer, producer, sequence, last[producer],
						)
					}
					last[producer] = sequence
					if seen[producer][sequence].Swap(true) {
						t.Errorf("producer %d's value %d received twice", producer, sequence)
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
			if count := queue.Count(); count < 0 || count > queue.Capacity() {
				t.Errorf("Count %d outside [0, %d]", count, queue.Capacity())
			}
			runtime.Gosched()
		}
	})
	for producer := range mpmcTestProducers {
		workers.Go(func() {
			mpmcTestProduce(t, queue, producer, perProducer, deadline)
		})
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
	for producer := range seen {
		for sequence := range seen[producer] {
			if !seen[producer][sequence].Load() {
				t.Fatalf("producer %d's value %d never received", producer, sequence)
			}
		}
	}
	if !queue.Empty() {
		t.Fatal("not drained")
	}
}

// mpmcTestProduce sends total values for producer, choosing Enqueue, EnqueueBurst, or EnqueueBulk
// by sequence number, and stops with an error at deadline.
func mpmcTestProduce(
	t *testing.T,
	queue *MPMCQueue[uint64],
	producer, total int,
	deadline time.Time,
) {
	tag := uint64(producer) << mpmcTestSequenceBits
	batch := make([]uint64, 4)
	for sent := 0; sent < total; {
		if time.Now().After(deadline) {
			t.Errorf("producer %d sent %d of %d values within a minute", producer, sent, total)
			return
		}
		moved := 0
		style := sent % 3
		if total-sent < len(batch) {
			style = 0
		}
		switch style {
		case 0:
			err := queue.Enqueue(tag | uint64(sent))
			if err == nil {
				moved = 1
			} else if err != ErrFull {
				t.Error(err)
				return
			}
		case 1:
			for index := range batch {
				batch[index] = tag | uint64(sent+index)
			}
			moved, _ = queue.EnqueueBurst(batch)
		default:
			for index := range batch {
				batch[index] = tag | uint64(sent+index)
			}
			_, err := queue.EnqueueBulk(batch)
			if err == nil {
				moved = len(batch)
			} else if err != ErrFull {
				t.Error(err)
				return
			}
		}
		if moved == 0 {
			runtime.Gosched()
		}
		sent += moved
	}
}

// mpmcTestDequeue dequeues into dst with the call style assigned to consumer. Consumer 0 calls
// Dequeue, consumer 1 calls DequeueBurst, and the others call DequeueBulk for a full dst.
func mpmcTestDequeue(t *testing.T, queue *MPMCQueue[uint64], consumer int, dst []uint64) int {
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

// TestMPMCSingleProcessorProgress checks that producers and consumers yielding after a lost
// compare-and-swap still let every caller finish when they share one processor.
func TestMPMCSingleProcessorProgress(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	const producers, perProducer, consumers = 2, 2500, 4
	const total = producers * perProducer
	queue := mpmcTestQueue[int](t, 8)
	deadline := time.Now().Add(time.Minute)
	var received atomic.Int64
	var workers sync.WaitGroup
	for consumer := range consumers {
		workers.Go(func() {
			for received.Load() < total {
				if time.Now().After(deadline) {
					t.Errorf("consumer %d stopped at %d of %d values", consumer,
						received.Load(), total)
					return
				}
				if _, err := queue.Dequeue(); err == nil {
					received.Add(1)
				} else {
					runtime.Gosched()
				}
			}
		})
	}
	for producer := range producers {
		workers.Go(func() {
			for sent := 0; sent < perProducer; {
				if time.Now().After(deadline) {
					t.Errorf("producer %d sent %d of %d values within a minute", producer, sent,
						perProducer)
					return
				}
				if queue.Enqueue(sent) == nil {
					sent++
				} else {
					runtime.Gosched()
				}
			}
		})
	}
	workers.Wait()
	if received.Load() != total || !queue.Empty() {
		t.Fatal("received", received.Load())
	}
}

func TestMPMCClearingAndReset(t *testing.T) {
	for _, keep := range []bool{false, true} {
		var options []Option
		if keep {
			options = append(options, WithoutDequeueClearing())
		}
		queue := mpmcTestQueue[*int](t, 4, options...)
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
			if (queue.config.entries[index] != nil) != keep {
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
			if (queue.config.entries[index] != nil) != keep {
				t.Fatal("wrapped clearing", keep, index)
			}
		}
		queue.Reset()
		if queue.Count() != 0 || queue.producerHead.Load() != 0 || queue.completed.Load() != 0 ||
			queue.consumerHead.Load() != 0 || queue.released.Load() != 0 {
			t.Fatal("reset positions")
		}
		for index, entry := range queue.config.entries {
			if entry != nil || queue.config.completions[index].Load() != 0 ||
				queue.config.releases[index].Load() != 0 {
				t.Fatal("reset storage", index)
			}
		}
		if _, err := queue.EnqueueBulk([]*int{&value, &value, &value, &value}); err != nil {
			t.Fatal("reuse after Reset", err)
		}
	}
}

func TestMPMCAllocations(t *testing.T) {
	queue := mpmcTestQueue[*int](t, 8)
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

// TestMPMCLayout checks that the configuration group keeps its fields, that 128-byte gaps separate
// the groups, and that the atomic fields are 8-byte aligned.
func TestMPMCLayout(t *testing.T) {
	queue := mpmcTestQueue[int64](t, 8)
	outer := reflect.TypeOf(queue).Elem()
	groups := []struct {
		name   string
		fields []string
	}{
		{"config", []string{
			"entries",
			"mask",
			"maxEnqueueBatch",
			"maxDequeueBatch",
			"clearOnDequeue",
			"exactBatchCounts",
			"completions",
			"releases",
		}},
		{"producerHead", nil},
		{"completed", nil},
		{"consumerHead", nil},
		{"released", nil},
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
		uintptr(unsafe.Pointer(&queue.producerHead)),
		uintptr(unsafe.Pointer(&queue.completed)),
		uintptr(unsafe.Pointer(&queue.consumerHead)),
		uintptr(unsafe.Pointer(&queue.released)),
	} {
		if address%8 != 0 {
			t.Fatalf("atomic field at %#x is not 8-byte aligned", address)
		}
	}
}

func TestMPMCOptions(t *testing.T) {
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
			queue, err := NewMPMCQueue[int](test.capacity, test.options...)
			if !errors.Is(err, test.want) {
				t.Fatalf("err=%v want=%v", err, test.want)
			}
			if test.want != nil && (err == test.want || queue != nil) {
				t.Fatal("constructor error must wrap and return nil")
			}
		})
	}
	queue := mpmcTestQueue[int](t, 8, WithMaxBatch(4), WithMaxEnqueueBatch(2),
		WithExactBatchCounts(), WithoutDequeueClearing())
	if queue.Capacity() != 8 || queue.MaxEnqueueBatch() != 2 || queue.MaxDequeueBatch() != 4 ||
		!queue.config.exactBatchCounts || queue.config.clearOnDequeue {
		t.Fatal("configuration")
	}
}
