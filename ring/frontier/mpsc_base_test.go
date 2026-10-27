package frontier

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"unsafe"
)

func baseTestQueue[T any](
	t *testing.T,
	capacity int,
	options ...Option,
) *mpscBase[T] {
	t.Helper()
	queue, err := newMPSCBase[T](capacity, options...)
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

func baseTestSeed[T any](queue *mpscBase[T], position uint64) {
	queue.producerReservation.head.Store(position)
	clear(queue.producerConfig.entries)
	for index := range queue.completion.records {
		queue.completion.records[index].Store(0)
	}
	queue.producerReservation.cachedConsumer.Store(uint32(position))
	queue.consumer.cursor = position
	queue.consumer.frontier = position
	queue.consumerPublished.Store(mpscConsumerWord(uint32(position), uint32(position)))
}

func TestMPSCBaseCapacityReuseFIFO(t *testing.T) {
	for _, capacity := range []int{1, 8, 1024, 16384} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			queue := baseTestQueue[int](t, capacity)
			items := make([]int, capacity)
			for lap := 0; lap < 3; lap++ {
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

func TestMPSCBaseMixedBoundaries(t *testing.T) {
	queue := baseTestQueue[int](t, 8)
	if err := queue.Enqueue(1); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.EnqueueBulk([]int{2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if moved, _ := queue.EnqueueBurst([]int{5, 6, 7, 8, 9}); moved != 4 {
		t.Fatalf("moved=%d", moved)
	}
	dst := []int{-1, -1, -1, -1, -1}
	if left, err := queue.DequeueBulk(dst); left != 3 || err != nil {
		t.Fatalf("left=%d err=%v", left, err)
	}
	if !slices.Equal(dst, []int{1, 2, 3, 4, 5}) || queue.consumer.frontier != 8 {
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

func TestMPSCBaseOptions(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		options  []Option
		want     error
	}{
		{"capacity-first", 3, []Option{WithMaxBatch(0)}, ErrInvalidCount},
		{"enqueue-first", 8, []Option{WithMaxBatch(0)}, ErrInvalidMaxBatch},
		{"dequeue", 8, []Option{WithMaxDequeueBatch(0)}, ErrInvalidMaxBatch},
		{"enqueue-above-count", 8, []Option{WithMaxEnqueueBatch(9)}, ErrInvalidMaxBatch},
		{"zero-count", 0, nil, ErrInvalidCount},
		{"last-limit", 8, []Option{WithMaxBatch(0), WithMaxBatch(4)}, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queue, err := newMPSCBase[int](test.capacity, test.options...)
			if !errors.Is(err, test.want) {
				t.Fatalf("err=%v want=%v", err, test.want)
			}
			if test.want != nil && (err == test.want || queue != nil) {
				t.Fatal("constructor error must wrap and return nil")
			}
		})
	}
	queue := baseTestQueue[int](
		t,
		8,
		WithMaxBatch(4),
		WithMaxEnqueueBatch(2),
		WithMaxDequeueBatch(3),
		WithExactBatchCounts(),
		WithoutDequeueClearing(),
	)
	if queue.MaxEnqueueBatch() != 2 || queue.MaxDequeueBatch() != 3 ||
		!queue.consumer.exactBatchCounts || !queue.producerConfig.exactBatchCounts ||
		queue.consumer.clearOnDequeue {
		t.Fatal("configuration")
	}
	if moved, _ := queue.EnqueueBurst([]int{1, 2, 3}); moved != 2 {
		t.Fatal(moved)
	}
	if _, err := queue.EnqueueBulk([]int{1, 2, 3}); !errors.Is(err, ErrExceedsMaxBatch) {
		t.Fatal(err)
	}
}

func baseTestRecords[T any](queue *mpscBase[T]) []uint64 {
	records := make([]uint64, len(queue.completion.records))
	for index := range records {
		records[index] = queue.completion.records[index].Load()
	}
	return records
}

func TestMPSCBaseCountsInvalidAndZero(t *testing.T) {
	for _, exact := range []bool{false, true} {
		t.Run(fmt.Sprint(exact), func(t *testing.T) {
			var options []Option
			if exact {
				options = append(options, WithExactBatchCounts())
			}
			queue := baseTestQueue[int](t, 8, options...)
			if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
				t.Fatal(err)
			}
			if _, err := queue.DequeueBulk(make([]int, 2)); err != nil {
				t.Fatal(err)
			}
			if free, err := queue.EnqueueBulk([]int{5}); err != nil ||
				free != map[bool]int{false: 3, true: 5}[exact] {
				t.Fatalf("cached enqueue free=%d err=%v", free, err)
			}
			dst := []int{-1}
			want := map[bool]int{false: 1, true: 2}[exact]
			if left, err := queue.DequeueBulk(dst); err != nil || left != want {
				t.Fatalf("cached dequeue left=%d err=%v", left, err)
			}
			for _, direction := range []string{"enqueue", "dequeue"} {
				slots := slices.Clone(queue.producerConfig.entries)
				records := baseTestRecords(queue)
				state := queue.consumerPublished.Load()
				cursor, frontier := queue.consumer.cursor, queue.consumer.frontier
				cache := queue.producerReservation.cachedConsumer.Load()
				head := queue.producerReservation.head.Load()
				dst := make([]int, 9)
				var left int
				var err error
				if direction == "enqueue" {
					left, err = queue.EnqueueBulk(dst)
					if left != 6 {
						t.Fatal(left)
					}
				} else {
					left, err = queue.DequeueBulk(dst)
					if left != 2 {
						t.Fatal(left)
					}
				}
				if !errors.Is(err, ErrExceedsCapacity) || err == ErrExceedsCapacity {
					t.Fatal(err)
				}
				if !slices.Equal(slots, queue.producerConfig.entries) ||
					!slices.Equal(records, baseTestRecords(queue)) ||
					state != queue.consumerPublished.Load() ||
					cursor != queue.consumer.cursor || frontier != queue.consumer.frontier ||
					cache != queue.producerReservation.cachedConsumer.Load() ||
					head != queue.producerReservation.head.Load() ||
					!slices.Equal(dst, make([]int, 9)) {
					t.Fatal("invalid request mutated state")
				}
			}
			if left, err := queue.DequeueBulk(nil); left != 2 || err != nil ||
				queue.consumer.frontier != 5 || queue.consumer.cursor != 3 {
				t.Fatalf("zero refresh left=%d err=%v", left, err)
			}
			if free, err := queue.EnqueueBulk(nil); free != 6 || err != nil {
				t.Fatalf("zero producer free=%d err=%v", free, err)
			}
			if left, err := queue.DequeueBulk(make([]int, 3)); left != 2 || err != ErrEmpty {
				t.Fatalf("failed fresh left=%d err=%v", left, err)
			}
			if moved, left := queue.DequeueBurst(make([]int, 3)); moved != 2 || left != 0 {
				t.Fatalf("partial moved=%d left=%d", moved, left)
			}
		})
	}
}

func TestMPSCBaseStaleHeadAndCache(t *testing.T) {
	queue := baseTestQueue[int](t, 8)
	baseTestSeed(queue, 16)
	queue.producerReservation.cachedConsumer.Store(8)
	_, count, remaining, retry := queue.mpscAttemptProducer(15, 1, true, true)
	if count != 0 || remaining != 8 || !retry {
		t.Fatalf("stale count=%d remaining=%d retry=%t", count, remaining, retry)
	}
	_, count, remaining, retry = queue.mpscAttemptProducer(15, 0, true, true)
	if count != 0 || remaining != 8 || retry {
		t.Fatal("zero stale snapshot")
	}
	// A delayed refresh can move the shared cache backward within its documented lifetime.
	queue.producerReservation.cachedConsumer.Store(12)
	if err := queue.Enqueue(1); err != nil || queue.Count() != 1 {
		t.Fatal(err)
	}
}

// baseTestFill enqueues one Bulk reservation per length, numbering payloads from next.
func baseTestFill(t *testing.T, queue *mpscBase[int], next int, lengths ...int) int {
	t.Helper()
	for _, length := range lengths {
		items := make([]int, length)
		for index := range items {
			items[index] = next
			next++
		}
		if _, err := queue.EnqueueBulk(items); err != nil {
			t.Fatalf("length %d: %v", length, err)
		}
	}
	return next
}

func TestMPSCBaseDiscoverLookahead(t *testing.T) {
	discover := func(queue *mpscBase[int], stop bool, requested uint32) uint64 {
		return queue.mpscDiscover(
			queue.consumer.cursor, queue.consumer.frontier, stop, requested,
		) - queue.consumer.cursor
	}
	t.Run("uniform runs fill the window", func(t *testing.T) {
		queue := baseTestQueue[int](t, 64)
		baseTestFill(t, queue, 0, 8, 8, 8, 8, 8, 8, 8, 8)
		if got := discover(queue, false, 0); got != 64 {
			t.Fatalf("discovered %d, want 64", got)
		}
	})
	t.Run("stop once the request fits", func(t *testing.T) {
		queue := baseTestQueue[int](t, 64)
		baseTestFill(t, queue, 0, 8, 8, 8, 8, 8, 8)
		if got := discover(queue, true, 20); got != 24 {
			t.Fatalf("discovered %d, want the first boundary at or past 20, 24", got)
		}
	})
	t.Run("length change mid run", func(t *testing.T) {
		queue := baseTestQueue[int](t, 64)
		baseTestFill(t, queue, 0, 8, 8, 8, 16, 8, 1, 8)
		if got := discover(queue, false, 0); got != 57 {
			t.Fatalf("discovered %d, want 57", got)
		}
	})
	t.Run("hole inside the predicted lanes", func(t *testing.T) {
		queue := baseTestQueue[int](t, 64)
		next := baseTestFill(t, queue, 0, 8, 8)
		start, count, _ := queue.mpscReserveProducer(8, true, false)
		baseTestFill(t, queue, next+8, 8, 8)
		if got := discover(queue, false, 0); got != 16 {
			t.Fatalf("discovered %d past a hole, want 16", got)
		}
		queue.mpscWriteProducer(start, make([]int, count))
		queue.mpscPublishProducer(start, count)
		if got := discover(queue, false, 0); got != 40 {
			t.Fatalf("discovered %d after filling the hole, want 40", got)
		}
	})
	t.Run("records from the previous lap", func(t *testing.T) {
		queue := baseTestQueue[int](t, 64)
		next := baseTestFill(t, queue, 0, 8, 8, 8, 8, 8, 8, 8, 8)
		dst := make([]int, 64)
		if _, err := queue.DequeueBulk(dst); err != nil {
			t.Fatal(err)
		}
		next = baseTestFill(t, queue, next, 8, 8)
		if got := discover(queue, false, 0); got != 16 {
			t.Fatalf("discovered %d through stale records, want 16", got)
		}
		baseTestFill(t, queue, next, 8, 8, 8)
		moved, _ := queue.DequeueBurst(dst)
		if moved != 40 {
			t.Fatalf("moved %d, want 40", moved)
		}
		for index, value := range dst[:moved] {
			if value != 64+index {
				t.Fatalf("dst[%d] = %d, want %d", index, value, 64+index)
			}
		}
	})
	t.Run("absolute wrap", func(t *testing.T) {
		queue := baseTestQueue[int](t, 64)
		baseTestSeed(queue, 1<<32-20)
		baseTestFill(t, queue, 0, 8, 8, 8, 8, 8, 8, 8, 8)
		if got := discover(queue, false, 0); got != 64 {
			t.Fatalf("discovered %d across the wrap, want 64", got)
		}
		dst := make([]int, 64)
		if _, err := queue.DequeueBulk(dst); err != nil {
			t.Fatal(err)
		}
		for index, value := range dst {
			if value != index {
				t.Fatalf("dst[%d] = %d, want %d", index, value, index)
			}
		}
	})
}

func TestMPSCBaseFailedAttemptLeavesCache(t *testing.T) {
	queue := baseTestQueue[int](t, 8)
	baseTestFill(t, queue, 0, 8)
	if _, err := queue.DequeueBulk(make([]int, 4)); err != nil {
		t.Fatal(err)
	}
	// The cache still says the ring is full, and the refreshed release leaves 4 free.
	_, count, remaining, retry := queue.mpscAttemptProducer(8, 8, true, false)
	if count != 0 || remaining != 4 || retry {
		t.Fatalf("count=%d remaining=%d retry=%t", count, remaining, retry)
	}
	if cached := queue.producerReservation.cachedConsumer.Load(); cached != 0 {
		t.Fatalf("failed attempt stored cache %d", cached)
	}
	if _, count, _, _ := queue.mpscAttemptProducer(8, 4, true, false); count != 4 {
		t.Fatalf("admitted count=%d", count)
	}
	if cached := queue.producerReservation.cachedConsumer.Load(); cached != 4 {
		t.Fatalf("admitted attempt stored cache %d, want 4", cached)
	}
}

func TestMPSCBaseLayout(t *testing.T) {
	baseTestLayout[int64](t)
	baseTestLayout[*int](t)
}

func baseTestLayout[T any](t *testing.T) {
	t.Helper()
	queue := baseTestQueue[T](t, 1024)
	outer := reflect.TypeOf(queue).Elem()
	if outer.NumField() != 9 {
		t.Fatal(outer.NumField())
	}
	expected := []struct {
		names []string
		types []reflect.Type
	}{
		{[]string{"entries", "mask", "maxBatch", "exactBatchCounts"},
			[]reflect.Type{reflect.TypeOf([]T{}), reflect.TypeOf(uint32(0)),
				reflect.TypeOf(uint32(0)), reflect.TypeOf(false)}},
		{[]string{"head", "cachedConsumer"},
			[]reflect.Type{reflect.TypeOf(atomic.Uint64{}), reflect.TypeOf(atomic.Uint32{})}},
		{[]string{"records"}, []reflect.Type{reflect.TypeOf([]atomic.Uint64{})}},
		{[]string{"entries", "mask", "cursor", "frontier", "clearOnDequeue",
			"maxBatch", "exactBatchCounts"},
			[]reflect.Type{reflect.TypeOf([]T{}), reflect.TypeOf(uint32(0)), reflect.TypeOf(uint64(0)),
				reflect.TypeOf(uint64(0)), reflect.TypeOf(false),
				reflect.TypeOf(uint32(0)), reflect.TypeOf(false)}},
	}
	for index, block := range expected {
		actual := outer.Field(index * 2).Type
		if actual.NumField() != len(block.names) {
			t.Fatal(actual)
		}
		for field, name := range block.names {
			if actual.Field(field).Name != name || actual.Field(field).Type != block.types[field] {
				t.Fatalf("block=%d field=%d", index, field)
			}
		}
	}
	for _, index := range []int{1, 3, 5, 7} {
		if outer.Field(index).Type != reflect.TypeOf([128]byte{}) {
			t.Fatal("gap")
		}
	}
	if outer.Field(8).Type != reflect.TypeOf(atomic.Uint64{}) {
		t.Fatal("state type")
	}
	if runtime.GOARCH == "amd64" {
		if outer.Size() != 664 {
			t.Fatal(outer.Size())
		}
		for index, offset := range []uintptr{0, 168, 312, 464, 656} {
			if outer.Field(index*2).Offset != offset {
				t.Fatal(outer.Field(index * 2))
			}
		}
	}
	if runtime.GOARCH == "386" && (outer.Size() != 616 || outer.Field(8).Offset != 608) {
		t.Fatalf("386 size=%d publication=%d", outer.Size(), outer.Field(8).Offset)
	}
	for index := 0; index < outer.NumField(); index++ {
		t.Logf("field=%s offset=%d", outer.Field(index).Name, outer.Field(index).Offset)
	}
	if unsafe.SliceData(queue.producerConfig.entries) != unsafe.SliceData(queue.consumer.entries) ||
		len(queue.completion.records) != queue.Capacity() ||
		unsafe.Sizeof(queue.completion.records[0])*uintptr(queue.Capacity()) != 8*1024 {
		t.Fatal("storage")
	}
	base := uintptr(unsafe.Pointer(queue))
	payload := uintptr(unsafe.Pointer(unsafe.SliceData(queue.producerConfig.entries)))
	metadata := uintptr(unsafe.Pointer(unsafe.SliceData(queue.completion.records)))
	state := uintptr(unsafe.Pointer(&queue.consumerPublished))
	head := uintptr(unsafe.Pointer(&queue.producerReservation.head))
	if head%8 != 0 || state%8 != 0 || metadata%8 != 0 {
		t.Fatal("atomic alignment")
	}
	for index := range queue.completion.records {
		if uintptr(unsafe.Pointer(&queue.completion.records[index]))%8 != 0 {
			t.Fatal("record alignment")
		}
	}
	t.Logf("arch=%s size=%d align=%d", runtime.GOARCH, outer.Size(), outer.Align())
	for _, allocation := range []struct {
		name    string
		address uintptr
	}{
		{"queue", base},
		{"payload", payload},
		{"metadata", metadata},
		{"head", head},
		{"state", state},
	} {
		t.Logf("%s=%#x mod64=%d mod128=%d", allocation.name, allocation.address,
			allocation.address%64, allocation.address%128)
	}
}

func TestMPSCBaseAbsoluteWrapAndRetention(t *testing.T) {
	queue := baseTestQueue[int](t, 8)
	start := uint64(math.MaxUint32 - 1)
	baseTestSeed(queue, start)
	if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if queue.producerReservation.head.Load() != start+4 || queue.Count() != 4 {
		t.Fatal("full head")
	}
	if left, err := queue.DequeueBulk(nil); err != nil || left != 4 {
		t.Fatal(left, err)
	}
	if queue.consumer.cursor != start || queue.consumer.frontier != start+4 {
		t.Fatal("cross-era proof")
	}
	if queue.Count() != 4 {
		t.Fatal("lifted cross-era Count")
	}
	dst := make([]int, 4)
	if _, err := queue.DequeueBulk(dst); err != nil || !slices.Equal(dst, []int{1, 2, 3, 4}) {
		t.Fatal(dst, err)
	}
	if queue.consumer.cursor != start+4 || queue.consumer.frontier != start+4 {
		t.Fatal("absolute owner")
	}
	word := queue.consumerPublished.Load()
	if mpscReleased(word) != 2 || mpscFrontier(word) != 2 {
		t.Fatal(word)
	}
	if queue.completion.records[uint32(start)&7].Load() != start+4 {
		t.Fatal("record not retained")
	}
}

func TestMPSCBaseMaximumEndAndReset(t *testing.T) {
	queue := baseTestQueue[int](t, 8)
	baseTestSeed(queue, math.MaxUint64-4)
	if free, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); free != 4 || err != nil {
		t.Fatal(free, err)
	}
	if queue.producerReservation.head.Load() != math.MaxUint64 || queue.Count() != 4 {
		t.Fatal("maximum end")
	}
	dst := make([]int, 4)
	if left, err := queue.DequeueBulk(dst); left != 0 || err != nil {
		t.Fatal(left, err)
	}
	if !slices.Equal(dst, []int{1, 2, 3, 4}) || queue.consumer.cursor != math.MaxUint64 {
		t.Fatal(dst)
	}
	if queue.completion.records[3].Load() != math.MaxUint64 {
		t.Fatal("maximum record")
	}
	queue.Reset()
	if queue.producerReservation.head.Load() != 0 || queue.consumer.cursor != 0 ||
		queue.consumer.frontier != 0 || queue.consumerPublished.Load() != 0 ||
		queue.producerReservation.cachedConsumer.Load() != 0 {
		t.Fatal("reset positions")
	}
	for index, value := range queue.producerConfig.entries {
		if value != 0 || queue.completion.records[index].Load() != 0 {
			t.Fatal("reset storage")
		}
	}
	if err := queue.Enqueue(5); err != nil {
		t.Fatal(err)
	}
	if got, err := queue.Dequeue(); got != 5 || err != nil {
		t.Fatal(got, err)
	}
}
