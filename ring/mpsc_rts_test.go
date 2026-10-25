package ring

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestMPSCSharedOptions(t *testing.T) {
	for _, capacity := range []int{1, 4, 8, 16} {
		queue := newMPSCForTest[int](t, capacity, WithRelaxedMultiProdEnqueue())
		if !queue.producerConfig.relaxed || queue.producerConfig.htdMax != uint32(capacity/8) {
			t.Fatal("RTS defaults")
		}
	}
	largest := int(^uint(0) >> 1)
	if uint64(largest) > math.MaxUint32 {
		maximum := uint64(math.MaxUint32)
		largest = int(maximum)
	}
	for _, distance := range []int{0, 1, 7, 8, 9, largest} {
		for _, before := range []bool{false, true} {
			options := []Option{WithRelaxedMultiProdEnqueue(),
				WithMaxProducerHeadTailDistance(distance)}
			if before {
				slices.Reverse(options)
			}
			queue := newMPSCForTest[int](t, 8, options...)
			if queue.producerConfig.htdMax != uint32(distance) {
				t.Fatalf("distance=%d got=%d", distance, queue.producerConfig.htdMax)
			}
		}
	}
	for _, test := range []struct {
		name     string
		options  []Option
		sentinel error
		context  string
	}{
		{"ordered-distance", []Option{WithMaxProducerHeadTailDistance(0)},
			ErrHeadTailDistanceRequiresRelaxed, "WithRelaxedMultiProdEnqueue"},
		{"negative-producer", []Option{WithMaxProducerHeadTailDistance(-1)},
			ErrInvalidMaxHeadTailDistance, "-1"},
		{"negative-consumer", []Option{WithMaxConsumerHeadTailDistance(-1)},
			ErrInvalidMaxHeadTailDistance, "consumer"},
		{"consumer-distance", []Option{WithMaxConsumerHeadTailDistance(0)},
			ErrUnsupportedOption, "consumer"},
		{"consumer-multi", []Option{WithMultiConsDequeue()}, ErrUnsupportedOption, "consumer"},
		{"consumer-rts", []Option{WithRelaxedMultiConsDequeue()},
			ErrUnsupportedOption, "consumer"},
		{"consumer-overwritten", []Option{WithRelaxedMultiConsDequeue(), WithMultiConsDequeue()},
			ErrUnsupportedOption, "consumer"},
		{"ordered-final", []Option{WithRelaxedMultiProdEnqueue(), WithMultiProdEnqueue(),
			WithMaxProducerHeadTailDistance(1)}, ErrHeadTailDistanceRequiresRelaxed, "producer"},
		{"batch-before-mode", []Option{WithMaxEnqueueBatch(0), WithMultiConsDequeue()},
			ErrInvalidMaxBatch, "enqueue"},
		{"dequeue-before-spin", []Option{WithMaxDequeueBatch(0), WithSpinsBeforeYield(-1)},
			ErrInvalidMaxBatch, "dequeue"},
		{"spin-before-mode", []Option{WithSpinsBeforeYield(-1), WithMultiConsDequeue()},
			ErrInvalidSpinsBeforeYield, "-1"},
		{"mode-before-distance", []Option{WithMultiConsDequeue(),
			WithMaxProducerHeadTailDistance(-1)}, ErrUnsupportedOption, "consumer"},
		{"producer-before-consumer", []Option{WithMaxProducerHeadTailDistance(-1),
			WithMaxConsumerHeadTailDistance(-1)}, ErrInvalidMaxHeadTailDistance, "producer"},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue, err := NewMPSCQueue[int](8, test.options...)
			if queue != nil || !errors.Is(err, test.sentinel) {
				t.Fatalf("queue=%v error=%v", queue, err)
			}
			for _, context := range []string{"NewMPSCQueue", "8", test.context} {
				if !strings.Contains(err.Error(), context) {
					t.Fatalf("missing %q: %v", context, err)
				}
			}
		})
	}
	for _, test := range []struct {
		options []Option
		relaxed bool
	}{
		{[]Option{WithMultiProdEnqueue()}, false},
		{[]Option{WithRelaxedMultiProdEnqueue(), WithMultiProdEnqueue()}, false},
		{[]Option{WithMultiProdEnqueue(), WithRelaxedMultiProdEnqueue()}, true},
		{[]Option{WithMaxProducerHeadTailDistance(-1), WithMaxProducerHeadTailDistance(0),
			WithRelaxedMultiProdEnqueue()}, true},
	} {
		queue := newMPSCForTest[int](t, 8, test.options...)
		if queue.producerConfig.relaxed != test.relaxed {
			t.Fatal("final selector")
		}
	}
	if uint64(^uint(0)>>1) > math.MaxUint32 {
		oversized := uint64(math.MaxUint32) + 1
		queue, err := NewMPSCQueue[int](8, WithMaxProducerHeadTailDistance(int(oversized)))
		if queue != nil || !errors.Is(err, ErrInvalidMaxHeadTailDistance) {
			t.Fatalf("oversized queue=%v error=%v", queue, err)
		}
	}
}

func mpscRTSOperationForTest(
	queue *MPSCQueue[int],
	method string,
	entered, done chan struct{},
	result chan<- error,
) {
	close(entered)
	var err error
	switch method {
	case "single":
		err = queue.Enqueue(99)
	case "fixed":
		_, err = queue.EnqueueBulk([]int{99, 100})
	case "burst":
		queue.EnqueueBurst([]int{99, 100})
	case "zero-fixed":
		_, err = queue.EnqueueBulk(nil)
	case "zero-burst":
		queue.EnqueueBurst(nil)
	}
	result <- err
	close(done)
}

func awaitMPSCDistanceWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(mpscWorkerTimeout)
	stack := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		runtime.Gosched()
		length := runtime.Stack(stack, true)
		for _, worker := range strings.Split(string(stack[:length]), "\n\n") {
			if strings.Contains(worker, "mpscRTSOperationForTest") &&
				strings.Contains(worker, "waitHeadTailDistance") {
				return
			}
		}
		select {
		case <-done:
			t.Fatal("operation returned before distance release")
		default:
		}
	}
	t.Fatal("did not observe test worker in production distance wait")
}

func TestMPSCRTSPreAdmissionWait(t *testing.T) {
	for _, limit := range []int{0, 1, 8, 9} {
		for _, method := range []string{"single", "fixed", "burst", "zero-fixed", "zero-burst"} {
			t.Run(fmt.Sprintf("limit=%d/%s", limit, method), func(t *testing.T) {
				queue := newMPSCForTest[int](
					t,
					8,
					WithRelaxedMultiProdEnqueue(),
					WithMaxProducerHeadTailDistance(limit),
					WithSpinsBeforeYield(0),
				)
				// The second reservation starts exactly at the limit and may cross it.
				held := uint32(8)
				var firstStart, firstCount uint32
				if limit == 1 {
					firstStart, firstCount, _ = queue.mpscReserveProducer(1, true, false)
					queue.mpscWriteProducer(firstStart, []int{1})
					held = 7
				}
				start, count, _ := queue.mpscReserveProducer(held, true, false)
				if count != held {
					t.Fatal("equality admission failed")
				}
				queue.mpscWriteProducer(start, make([]int, count))
				entered, done := make(chan struct{}), make(chan struct{})
				released := false
				t.Cleanup(func() {
					if !released {
						if firstCount != 0 {
							queue.mpscPublishProducer(firstStart, firstCount)
						}
						queue.mpscPublishProducer(start, count)
					}
					awaitMPSCSignal(t, done)
				})
				before := queue.producerReservation.headRTS.Load()
				result := make(chan error, 1)
				go mpscRTSOperationForTest(queue, method, entered, done, result)
				awaitMPSCSignal(t, entered)
				if limit < 8 {
					awaitMPSCDistanceWait(t, done)
					select {
					case <-done:
						t.Fatal("distance wait completed above limit")
					default:
					}
				} else {
					awaitMPSCSignal(t, done)
				}
				if firstCount != 0 {
					queue.mpscPublishProducer(firstStart, firstCount)
				}
				queue.mpscPublishProducer(start, count)
				released = true
				awaitMPSCSignal(t, done)
				wantErr := error(nil)
				if method == "single" || method == "fixed" {
					wantErr = ErrFull
				}
				if err := <-result; err != wantErr {
					t.Fatalf("operation error=%v want=%v", err, wantErr)
				}
				if queue.producerReservation.headRTS.Load() != before {
					t.Fatal("failed or zero operation changed packed position/counter")
				}
			})
		}
	}
}

func TestMPSCRTSLengthErrorsUseActiveHead(t *testing.T) {
	queue := newMPSCForTest[int](
		t,
		8,
		WithRelaxedMultiProdEnqueue(),
		WithMaxProducerHeadTailDistance(0),
		WithMaxEnqueueBatch(3),
	)
	start, count, _ := queue.mpscReserveProducer(3, true, false)
	queue.mpscWriteProducer(start, []int{1, 2, 3})
	t.Cleanup(func() { queue.mpscPublishProducer(start, count) })
	queue.producerReservation.head.Store(12345)
	before := queue.producerReservation.headRTS.Load()
	for _, length := range []int{4, 9} {
		free, err := queue.EnqueueBulk(make([]int, length))
		sentinel := ErrExceedsMaxBatch
		if length > 8 {
			sentinel = ErrExceedsCapacity
		}
		if free != 5 || !errors.Is(err, sentinel) {
			t.Fatalf("free=%d error=%v", free, err)
		}
	}
	if queue.producerReservation.headRTS.Load() != before {
		t.Fatal("length failure reserved")
	}
}

func TestMPSCRTSReorderedCompletion(t *testing.T) {
	for _, keep := range []bool{false, true} {
		for _, position := range []uint32{0, 3, math.MaxUint32 - 1} {
			t.Run(fmt.Sprintf("keep=%t/position=%d", keep, position), func(t *testing.T) {
				options := []Option{WithRelaxedMultiProdEnqueue(), WithMaxProducerHeadTailDistance(4)}
				if keep {
					options = append(options, WithoutDequeueClearing())
				}
				queue := newMPSCForTest[*int](t, 4, options...)
				seedMPSC(queue, position)
				values := []int{1, 2, 3, 4}
				items := []*int{&values[0], &values[1], &values[2], &values[3]}
				startA, countA, _ := queue.mpscReserveProducer(2, true, false)
				startB, countB, _ := queue.mpscReserveProducer(2, true, false)
				queue.mpscWriteProducer(startA, items[:2])
				queue.mpscWriteProducer(startB, items[2:])
				entered, done := make(chan struct{}), make(chan struct{})
				completed := false
				t.Cleanup(func() {
					if !completed {
						queue.mpscPublishProducer(startA, countA)
					}
					awaitMPSCSignal(t, done)
				})
				go mpscPublishForTest(queue, startB, countB, entered, done)
				awaitMPSCSignal(t, entered)
				awaitMPSCSignal(t, done)
				if queue.Count() != 0 || queue.producerPublication.published.Load() != position {
					t.Fatal("later completion exposed either interval")
				}
				if value, err := queue.Dequeue(); value != nil || err != ErrEmpty {
					t.Fatalf("unpublished value=%v error=%v", value, err)
				}
				queue.mpscPublishProducer(startA, countA)
				completed = true
				if queue.Count() != 4 {
					t.Fatal("last completion did not publish entire range")
				}
				destination := make([]*int, 4)
				if _, err := queue.DequeueBulk(destination); err != nil || !slices.Equal(destination, items) {
					t.Fatalf("values=%v error=%v", destination, err)
				}
				for index, item := range items {
					want := item
					if !keep {
						want = nil
					}
					if queue.producerConfig.entries[(position+uint32(index))&3] != want {
						t.Fatal("clearing policy after reordered completion")
					}
				}
			})
		}
	}
}

func TestMPSCRTSRolloverAndReset(t *testing.T) {
	for _, position := range []uint32{0, math.MaxUint32 - 1} {
		for _, counter := range []uint32{0, math.MaxUint32 - 1, math.MaxUint32} {
			queue := newMPSCForTest[*int](
				t,
				4,
				WithRelaxedMultiProdEnqueue(),
				WithMaxProducerHeadTailDistance(4),
				WithSpinsBeforeYield(0),
				WithoutDequeueClearing(),
			)
			seedMPSC(queue, position)
			queue.producerReservation.headRTS.Store(pack(position, counter))
			queue.producerPublication.tailRTS.Store(pack(position, counter))
			value := 42
			for range 3 {
				if err := queue.Enqueue(&value); err != nil {
					t.Fatal(err)
				}
			}
			want := pack(position+3, counter+3)
			if queue.producerReservation.headRTS.Load() != want ||
				queue.producerPublication.tailRTS.Load() != want {
				t.Fatal("position/counter rollover")
			}
			queue.producerReservation.head.Store(99)
			storage := &queue.producerConfig.entries[0]
			queue.Reset()
			if queue.producerReservation.headRTS.Load() != 0 || queue.producerReservation.head.Load() != 0 ||
				queue.producerPublication.tailRTS.Load() != 0 ||
				queue.producerPublication.published.Load() != 0 ||
				queue.producerReservation.cachedConsumer.Load() != 0 || queue.consumer.cursor != 0 ||
				queue.consumer.cachedProducer != 0 || queue.consumerPublished.Load() != 0 {
				t.Fatal("reset retained protocol state")
			}
			if !queue.producerConfig.relaxed || queue.producerConfig.htdMax != 4 ||
				queue.producerConfig.spinsBeforeYield != 0 || queue.consumer.clearOnDequeue ||
				&queue.producerConfig.entries[0] != storage {
				t.Fatal("reset changed configuration/storage")
			}
			for _, entry := range queue.producerConfig.entries {
				if entry != nil {
					t.Fatal("reset retained pointer")
				}
			}
		}
	}
}

func TestMPSCRTSStalePackedCASAndMirror(t *testing.T) {
	queue := newMPSCForTest[int](
		t,
		4,
		WithRelaxedMultiProdEnqueue(),
		WithMaxProducerHeadTailDistance(4),
	)
	seedMPSC(queue, math.MaxUint32-1)
	stale := queue.producerReservation.headRTS.Load()
	start, count, _ := queue.mpscReserveProducer(2, true, false)
	queue.mpscWriteProducer(start, []int{1, 2})
	before := queue.producerReservation.headRTS.Load()
	_, moved, _, retry := queue.mpscAttemptProducer(stale, 1, true, false)
	if !retry || moved != 0 || queue.producerReservation.headRTS.Load() != before {
		t.Fatal("stale packed CAS changed reservation")
	}
	queue.mpscPublishProducer(start, count)
	// Hold the mirror behind its real packed publication, then raise it repeatedly.
	queue.producerPublication.published.Store(start)
	if queue.Count() != 0 {
		t.Fatal("observer read packed tail instead of mirror")
	}
	if _, err := queue.Dequeue(); err != ErrEmpty {
		t.Fatal("consumer ignored publication mirror")
	}
	for range 3 {
		raiseMirror(&queue.producerPublication.published, &queue.producerPublication.tailRTS)
	}
	if queue.Count() != 2 {
		t.Fatal("mirror failed to cross rollover")
	}
	destination := make([]int, 2)
	_, err := queue.DequeueBulk(destination)
	if err != nil || !slices.Equal(destination, []int{1, 2}) {
		t.Fatalf("values=%v error=%v", destination, err)
	}
}

func TestMPSCRTSWaitThenReserve(t *testing.T) {
	for _, limit := range []int{0, 1} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			queue := newMPSCForTest[int](
				t,
				8,
				WithRelaxedMultiProdEnqueue(),
				WithMaxProducerHeadTailDistance(limit),
				WithSpinsBeforeYield(0),
			)
			stale := queue.producerReservation.headRTS.Load()
			start, count, _ := queue.mpscReserveProducer(uint32(limit+1), true, false)
			queue.mpscWriteProducer(start, []int{1, 2}[:count])
			before := queue.producerReservation.headRTS.Load()
			if _, moved, _, retry := queue.mpscAttemptProducer(stale, 1, true, false); !retry || moved != 0 {
				t.Fatal("stale packed attempt did not fail")
			}
			entered, done := make(chan struct{}), make(chan struct{})
			released := false
			t.Cleanup(func() {
				if !released {
					queue.mpscPublishProducer(start, count)
				}
				awaitMPSCSignal(t, done)
			})
			result := make(chan error, 1)
			go mpscRTSOperationForTest(queue, "single", entered, done, result)
			awaitMPSCSignal(t, entered)
			awaitMPSCDistanceWait(t, done)
			if queue.producerReservation.headRTS.Load() != before {
				t.Fatal("retry skipped distance")
			}
			queue.mpscPublishProducer(start, count)
			released = true
			awaitMPSCSignal(t, done)
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			want := pack(start+count+1, unpackCnt(before)+1)
			if queue.producerReservation.headRTS.Load() != want || queue.Count() != int(count+1) {
				t.Fatal("release did not permit the next real reservation")
			}
			destination := make([]int, count+1)
			if _, err := queue.DequeueBulk(destination); err != nil {
				t.Fatal(err)
			}
			if destination[count] != 99 {
				t.Fatalf("next value=%d", destination[count])
			}
		})
	}
}
