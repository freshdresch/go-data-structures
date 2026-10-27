package frontier

import (
	"slices"
	"testing"
)

func baseTestComplete[T any](queue *mpscBase[T], start uint64, items []T) {
	queue.mpscWriteProducer(start, items)
	queue.mpscPublishProducer(start, uint32(len(items)))
}

func baseTestObserve(t *testing.T, queue *mpscBase[int], want int) {
	t.Helper()
	records := baseTestRecords(queue)
	payloads := slices.Clone(queue.producerConfig.entries)
	cursor, frontier := queue.consumer.cursor, queue.consumer.frontier
	pair := queue.consumerPublished.Load()
	head := queue.producerReservation.head.Load()
	cache := queue.producerReservation.cachedConsumer.Load()
	if queue.Count() != want || queue.Free() != queue.Capacity()-want ||
		queue.Full() != (want == queue.Capacity()) || queue.Empty() != (want == 0) {
		t.Fatal("observations", want)
	}
	if !slices.Equal(records, baseTestRecords(queue)) ||
		!slices.Equal(payloads, queue.producerConfig.entries) || cursor != queue.consumer.cursor ||
		frontier != queue.consumer.frontier || pair != queue.consumerPublished.Load() ||
		head != queue.producerReservation.head.Load() ||
		cache != queue.producerReservation.cachedConsumer.Load() {
		t.Fatal("observer mutated state")
	}
}

func TestMPSCBaseCountRecordBounds(t *testing.T) {
	for _, capacity := range []int{1, 8} {
		queue := baseTestQueue[int](t, capacity)
		baseTestObserve(t, queue, 0)
		if _, count, _ := queue.mpscReserveProducer(1, true, false); count != 1 {
			t.Fatal(count)
		}
		for _, end := range []uint64{0, uint64(capacity + 1), 2} {
			queue.completion.records[0].Store(end)
			baseTestObserve(t, queue, 0)
		}
		queue.completion.records[0].Store(1)
		baseTestObserve(t, queue, 1)
		baseTestSeed(queue, uint64(capacity))
		if _, count, _ := queue.mpscReserveProducer(1, true, false); count != 1 {
			t.Fatal(count)
		}
		queue.completion.records[0].Store(uint64(capacity))
		baseTestObserve(t, queue, 0)
		if end := queue.mpscDiscover(
			uint64(capacity),
			uint64(capacity),
			false,
			0,
		); end != uint64(capacity) {
			t.Fatal("old full-C equality", end)
		}
		queue.completion.records[0].Store(uint64(2*capacity + 1))
		if end := queue.mpscDiscover(
			uint64(capacity),
			uint64(capacity),
			false,
			0,
		); end != uint64(capacity) {
			t.Fatal("over-window record", end)
		}
	}
}

func TestMPSCBaseScalarPrefixAndHole(t *testing.T) {
	queue := baseTestQueue[int](t, 8)
	for value := 1; value <= 8; value++ {
		if err := queue.Enqueue(value); err != nil {
			t.Fatal(err)
		}
	}
	baseTestObserve(t, queue, 8)
	dst := make([]int, 3)
	if left, err := queue.DequeueBulk(dst); left != 5 || err != nil {
		t.Fatal(left, err)
	}
	if queue.consumer.frontier != 8 {
		t.Fatal("fresh batch did not scan full prefix")
	}
	queue.Reset()
	hole, count, _ := queue.mpscReserveProducer(2, true, false)
	if count != 2 {
		t.Fatal(count)
	}
	later, count, _ := queue.mpscReserveProducer(2, true, false)
	if count != 2 {
		t.Fatal(count)
	}
	baseTestComplete(queue, later, []int{3, 4})
	baseTestObserve(t, queue, 0)
	if _, err := queue.Dequeue(); err != ErrEmpty {
		t.Fatal(err)
	}
	baseTestComplete(queue, hole, []int{1, 2})
	baseTestObserve(t, queue, 4)
}

func TestMPSCBaseHistoricalSnapshotFallback(t *testing.T) {
	queue := baseTestQueue[int](t, 4)
	if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	old := queue.consumerPublished.Load()
	if _, err := queue.DequeueBulk(make([]int, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.EnqueueBulk([]int{5, 6}); err != nil {
		t.Fatal(err)
	}
	if got := queue.mpscCountFromSnapshot(old); got != 2 {
		t.Fatal(got)
	}
	baseTestObserve(t, queue, 4)
	if _, err := queue.DequeueBulk(make([]int, 4)); err != nil {
		t.Fatal(err)
	}
	if err := queue.Enqueue(7); err != nil {
		t.Fatal(err)
	}
	if got := queue.mpscCountFromSnapshot(old); got != 0 {
		t.Fatal("empty fallback", got)
	}
	baseTestObserve(t, queue, 1)
}

// This synthetic pair checks defensive input, not a reachable stable queue lifetime.
func TestMPSCBaseCountUnderflowGuard(t *testing.T) {
	queue := baseTestQueue[int](t, 8)
	before := mpscConsumerWord(1, 1)
	if got := queue.mpscCountFromSnapshot(before); got != 0 {
		t.Fatal(got)
	}
}

func TestMPSCBaseNoMoveAcquisition(t *testing.T) {
	for _, zero := range []bool{false, true} {
		queue := baseTestQueue[int](t, 8)
		if _, err := queue.EnqueueBulk([]int{1, 2}); err != nil {
			t.Fatal(err)
		}
		before := queue.consumerPublished.Load()
		dst := []int{-1, -1, -1}
		if zero {
			if left, err := queue.DequeueBulk(nil); left != 2 || err != nil {
				t.Fatal(left, err)
			}
		} else {
			if left, err := queue.DequeueBulk(dst); left != 2 || err != ErrEmpty {
				t.Fatal(left, err)
			}
		}
		if queue.consumer.cursor != 0 || queue.consumer.frontier != 2 ||
			queue.consumerPublished.Load() != mpscConsumerWord(0, 2) ||
			!slices.Equal(dst, []int{-1, -1, -1}) {
			t.Fatal("no-move acquisition")
		}
		if err := queue.Enqueue(3); err != nil {
			t.Fatal(err)
		}
		if queue.mpscCountFromSnapshot(before) != 2 || queue.Count() != 3 {
			t.Fatal("acquired-prefix fallback")
		}
	}
}

func TestMPSCBasePointerClearing(t *testing.T) {
	for _, retain := range []bool{false, true} {
		var options []Option
		if retain {
			options = append(options, WithoutDequeueClearing())
		}
		queue := baseTestQueue[*int](t, 1, options...)
		value := 1
		if err := queue.Enqueue(&value); err != nil {
			t.Fatal(err)
		}
		if got, err := queue.Dequeue(); got != &value || err != nil {
			t.Fatal(got, err)
		}
		if (queue.producerConfig.entries[0] != nil) != retain {
			t.Fatal("clearing")
		}
		if queue.completion.records[0].Load() != 1 {
			t.Fatal("record retention")
		}
		queue.Reset()
		if queue.producerConfig.entries[0] != nil || queue.completion.records[0].Load() != 0 {
			t.Fatal("Reset clearing")
		}
	}
}

func TestMPSCBaseStableManyRangesAndFreshFailures(t *testing.T) {
	queue := baseTestQueue[int](t, 64)
	for index := 0; index < 64; index++ {
		if err := queue.Enqueue(index); err != nil {
			t.Fatal(err)
		}
	}
	if queue.Count() != 64 {
		t.Fatal(queue.Count())
	}
	if _, err := queue.DequeueBulk(make([]int, 60)); err != nil {
		t.Fatal(err)
	}
	if moved, free := queue.EnqueueBurst(make([]int, 64)); moved != 60 || free != 0 {
		t.Fatal(moved, free)
	}
	if free, err := queue.EnqueueBulk([]int{1}); free != 0 || err != ErrFull {
		t.Fatal(free, err)
	}
	if count, err := queue.DequeueBulk(nil); count != 64 || err != nil {
		t.Fatal(count, err)
	}
	if queue.consumer.cursor != 60 || queue.consumer.frontier != 124 {
		t.Fatal("full fresh prefix")
	}
	queue.Reset()
	if _, err := queue.EnqueueBulk([]int{1}); err != nil {
		t.Fatal(err)
	}
	old := queue.consumerPublished.Load()
	if _, err := queue.Dequeue(); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.EnqueueBulk([]int{2, 3}); err != nil {
		t.Fatal(err)
	}
	if queue.mpscCountFromSnapshot(old) != 0 || queue.Count() != 2 {
		t.Fatal("fallback removed prefix")
	}
}

func TestMPSCBaseBurstCounts(t *testing.T) {
	for _, exact := range []bool{false, true} {
		var options []Option
		if exact {
			options = append(options, WithExactBatchCounts())
		}
		queue := baseTestQueue[int](t, 8, options...)
		if free, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); free != 4 || err != nil {
			t.Fatal(free, err)
		}
		if left, err := queue.DequeueBulk(make([]int, 2)); left != 2 || err != nil {
			t.Fatal(left, err)
		}
		wantFree, wantLeft := 3, 1
		if exact {
			wantFree, wantLeft = 5, 2
		}
		if moved, free := queue.EnqueueBurst([]int{5}); moved != 1 || free != wantFree {
			t.Fatal(moved, free)
		}
		dst := []int{-1}
		if moved, left := queue.DequeueBurst(dst); moved != 1 || left != wantLeft || dst[0] != 3 {
			t.Fatal(moved, left, dst)
		}
		if value, err := queue.Dequeue(); value != 4 || err != nil {
			t.Fatal(value, err)
		}
		if moved, left := queue.DequeueBurst(nil); moved != 0 || left != 1 {
			t.Fatal(moved, left)
		}
		if queue.consumer.cursor != 4 || queue.consumer.frontier != 5 {
			t.Fatal("zero Burst proof")
		}
		if moved, free := queue.EnqueueBurst(nil); moved != 0 || free != 7 {
			t.Fatal(moved, free)
		}
	}
}
