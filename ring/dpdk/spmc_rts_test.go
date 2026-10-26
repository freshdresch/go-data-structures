package dpdk

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSPMCRTSConstruction(t *testing.T) {
	for _, capacity := range []int{1, 4, 8, 16} {
		queue := newSPMCForTest[int](t, capacity, WithRelaxedMultiConsDequeue())
		if !queue.consumerConfig.relaxed || queue.consumerConfig.htdMax != uint32(capacity/8) {
			t.Fatal("RTS default", capacity, queue.consumerConfig)
		}
	}
	maximum := int(^uint(0) >> 1)
	largestDistance := uint64(math.MaxUint32)
	if uint64(maximum) > largestDistance {
		maximum = int(largestDistance)
	}
	for _, distance := range []int{0, 1, 7, 8, 9, maximum} {
		for _, before := range []bool{false, true} {
			options := []Option{WithMaxConsumerHeadTailDistance(distance), WithRelaxedMultiConsDequeue()}
			if !before {
				options[0], options[1] = options[1], options[0]
			}
			queue := newSPMCForTest[int](t, 8, options...)
			if queue.consumerConfig.htdMax != uint32(distance) {
				t.Fatal("resolved distance", distance)
			}
		}
	}
	tests := []struct {
		name    string
		options []Option
		want    error
		context string
	}{
		{"ordered-distance", []Option{WithMaxConsumerHeadTailDistance(0)},
			ErrHeadTailDistanceRequiresRelaxed, "WithRelaxedMultiConsDequeue"},
		{"producer-distance", []Option{WithMaxProducerHeadTailDistance(0)},
			ErrUnsupportedOption, "producer"},
		{"negative-producer", []Option{WithMaxProducerHeadTailDistance(-1)},
			ErrInvalidMaxHeadTailDistance, "-1"},
		{"negative-consumer", []Option{WithMaxConsumerHeadTailDistance(-1)},
			ErrInvalidMaxHeadTailDistance, "-1"},
		{"producer-mode", []Option{WithRelaxedMultiProdEnqueue(), WithMultiProdEnqueue()},
			ErrUnsupportedOption, "producer"},
		{"mode-before-distance", []Option{WithMultiProdEnqueue(),
			WithMaxConsumerHeadTailDistance(-1)}, ErrUnsupportedOption, "producer"},
		{"producer-before-consumer", []Option{WithMaxProducerHeadTailDistance(1),
			WithMaxConsumerHeadTailDistance(-1)}, ErrUnsupportedOption, "producer"},
		{"last-ordered", []Option{WithRelaxedMultiConsDequeue(), WithMultiConsDequeue(),
			WithMaxConsumerHeadTailDistance(1)}, ErrHeadTailDistanceRequiresRelaxed, "consumer"},
	}
	if uint64(^uint(0)>>1) > math.MaxUint32 {
		oversized := uint64(math.MaxUint32) + 1
		tests = append(tests, struct {
			name    string
			options []Option
			want    error
			context string
		}{
			"oversized", []Option{WithMaxConsumerHeadTailDistance(int(oversized))},
			ErrInvalidMaxHeadTailDistance, fmt.Sprint(oversized),
		})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queue, err := NewSPMCQueue[int](8, test.options...)
			if queue != nil || !errors.Is(err, test.want) {
				t.Fatalf("queue=%v error=%v", queue, err)
			}
			for _, fragment := range []string{"NewSPMCQueue", "8", test.context} {
				if !strings.Contains(err.Error(), fragment) {
					t.Fatal("missing error context", err, fragment)
				}
			}
		})
	}
	for _, options := range [][]Option{
		{WithMultiConsDequeue()},
		{WithRelaxedMultiConsDequeue(), WithMultiConsDequeue()},
		{WithMultiConsDequeue(), WithRelaxedMultiConsDequeue()},
		{WithRelaxedMultiConsDequeue(), WithMaxConsumerHeadTailDistance(-1),
			WithMaxConsumerHeadTailDistance(0)},
	} {
		newSPMCForTest[int](t, 8, options...)
	}
}

func spmcRTSReserveForTest(
	queue *SPMCQueue[int],
	requested uint32,
	fixed bool,
	entering, done chan struct{},
	result chan [3]uint32,
) {
	close(entering)
	start, count, remaining := queue.spmcReserveConsumer(requested, fixed, true)
	if count != 0 {
		values := make([]int, count)
		queue.spmcReadConsumer(start, values)
		queue.spmcReleaseConsumer(start, count)
	}
	result <- [3]uint32{start, count, remaining}
	close(done)
}

func awaitSPMCDistanceWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(spmcWorkerTimeout)
	stack := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		runtime.Gosched()
		length := runtime.Stack(stack, true)
		for _, worker := range strings.Split(string(stack[:length]), "\n\n") {
			if strings.Contains(worker, "spmcRTSReserveForTest") &&
				strings.Contains(worker, "waitHeadTailDistance") {
				return
			}
		}
		select {
		case <-done:
			t.Fatal("worker returned before distance release")
		default:
		}
	}
	t.Fatal("worker not observed in production distance wait")
}

func TestSPMCRTSDistanceWaitAndCounterStability(t *testing.T) {
	for _, limit := range []int{0, 1} {
		for _, request := range []uint32{0, 1, 2} {
			for _, fixed := range []bool{false, true} {
				t.Run(fmt.Sprintf("limit=%d/request=%d/fixed=%t", limit, request, fixed), func(t *testing.T) {
					queue := newSPMCForTest[int](
						t,
						4,
						WithRelaxedMultiConsDequeue(),
						WithMaxConsumerHeadTailDistance(limit),
						WithSpinsBeforeYield(0),
					)
					enqueueSPMCValues(t, queue, 10, 11)
					start, count, _ := queue.spmcReserveConsumer(2, true, true)
					values := make([]int, count)
					queue.spmcReadConsumer(start, values)
					entering, done := make(chan struct{}), make(chan struct{})
					result := make(chan [3]uint32, 1)
					released := false
					launched := false
					t.Cleanup(func() {
						if !released {
							queue.spmcReleaseConsumer(start, count)
						}
						if launched {
							awaitSPMCSignal(t, done)
						}
					})
					before := queue.consumerReservation.headRTS.Load()
					launched = true
					go spmcRTSReserveForTest(queue, request, fixed, entering, done, result)
					awaitSPMCSignal(t, entering)
					awaitSPMCDistanceWait(t, done)
					if queue.consumerReservation.headRTS.Load() != before {
						t.Fatal("waiting changed head")
					}
					// Invalid lengths bypass the wait and use the packed head despite poisoned ordered state.
					queue.consumerReservation.head.Store(999)
					left, err := queue.DequeueBulk(make([]int, 5))
					if !errors.Is(err, ErrExceedsCapacity) || left != 0 {
						t.Fatal("length snapshot", left, err)
					}
					queue.spmcReleaseConsumer(start, count)
					released = true
					awaitSPMCSignal(t, done)
					got := <-result
					if got[1] != 0 || got[2] != 0 || queue.consumerReservation.headRTS.Load() != before {
						t.Fatal("empty/zero admission changed packed head", got)
					}
				})
			}
		}
	}
}

func TestSPMCRTSDistanceEqualityAndCapacityAdmission(t *testing.T) {
	for _, limit := range []int{0, 1, 4, 5} {
		queue := newSPMCForTest[int](
			t,
			4,
			WithRelaxedMultiConsDequeue(),
			WithMaxConsumerHeadTailDistance(limit),
		)
		enqueueSPMCValues(t, queue, 1, 2, 3, 4)
		first, count, _ := queue.spmcReserveConsumer(uint32(limit%4+1), true, true)
		dst := make([]int, count)
		queue.spmcReadConsumer(first, dst)
		if limit == 1 || limit >= 4 {
			// At equality another whole batch can cross the configured limit.
			if limit == 1 {
				queue.spmcReleaseConsumer(first, count)
				first, count, _ = queue.spmcReserveConsumer(1, true, true)
				queue.spmcReadConsumer(first, dst[:1])
			}
			second, next, _ := queue.spmcReserveConsumer(1, true, true)
			queue.spmcReadConsumer(second, dst[:next])
			queue.spmcReleaseConsumer(second, next)
		}
		queue.spmcReleaseConsumer(first, count)
		for !queue.Empty() {
			if _, err := queue.Dequeue(); err != nil {
				t.Fatal(err)
			}
		}
		before := queue.consumerReservation.headRTS.Load()
		if _, err := queue.Dequeue(); err != ErrEmpty {
			t.Fatal(err)
		}
		if before != queue.consumerReservation.headRTS.Load() {
			t.Fatal("empty changed counter")
		}
	}
}

func TestSPMCRTSReorderedCompletionAndSlotReuse(t *testing.T) {
	for _, keep := range []bool{false, true} {
		for _, position := range []uint32{0, 3, math.MaxUint32 - 1} {
			t.Run(fmt.Sprintf("keep=%t/position=%d", keep, position), func(t *testing.T) {
				options := []Option{WithRelaxedMultiConsDequeue(), WithMaxConsumerHeadTailDistance(4)}
				if keep {
					options = append(options, WithoutDequeueClearing())
				}
				queue := newSPMCForTest[*int](t, 4, options...)
				seedSPMC(queue, position)
				firstValue, secondValue, thirdValue, fourthValue := 1, 2, 3, 4
				items := []*int{&firstValue, &secondValue, &thirdValue, &fourthValue}
				if _, err := queue.EnqueueBulk(items); err != nil {
					t.Fatal(err)
				}
				startA, countA, _ := queue.spmcReserveConsumer(2, true, true)
				startB, countB, _ := queue.spmcReserveConsumer(2, true, true)
				valuesA, valuesB := make([]*int, 2), make([]*int, 2)
				queue.spmcReadConsumer(startA, valuesA)
				queue.spmcReadConsumer(startB, valuesB)
				entering, done := make(chan struct{}), make(chan struct{})
				releasedA := false
				launched := false
				t.Cleanup(func() {
					if !releasedA {
						queue.spmcReleaseConsumer(startA, countA)
					}
					if launched {
						awaitSPMCSignal(t, done)
					}
				})
				launched = true
				go spmcReleaseForTest(queue, startB, countB, entering, done)
				awaitSPMCSignal(t, entering)
				awaitSPMCSignal(t, done)
				if valuesA[0] != &firstValue || valuesA[1] != &secondValue ||
					valuesB[0] != &thirdValue || valuesB[1] != &fourthValue {
					t.Fatal("reserved FIFO values")
				}
				if queue.consumerPublication.published.Load() != position || queue.Count() != 4 {
					t.Fatal("later completion released slots")
				}
				if err := queue.Enqueue(&firstValue); err != ErrFull {
					t.Fatal("premature reuse", err)
				}
				for _, value := range queue.producer.entries {
					if (value != nil) != keep {
						t.Fatal("clearing policy before release")
					}
				}
				queue.spmcReleaseConsumer(startA, countA)
				releasedA = true
				if !queue.Empty() || queue.consumerPublication.published.Load() != position+4 {
					t.Fatal("earliest completion did not release full range")
				}
				for range 4 {
					if err := queue.Enqueue(&firstValue); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestSPMCRTSPackedRolloverAndStaleCAS(t *testing.T) {
	for _, position := range []uint32{0, math.MaxUint32 - 1} {
		for _, counter := range []uint32{0, math.MaxUint32 - 1, math.MaxUint32} {
			for _, exact := range []bool{false, true} {
				for _, fixed := range []bool{false, true} {
					queue := newSPMCForTest[int](
						t,
						4,
						WithRelaxedMultiConsDequeue(),
						WithMaxConsumerHeadTailDistance(4),
					)
					seedSPMC(queue, position)
					queue.consumerReservation.headRTS.Store(pack(position, counter))
					queue.consumerPublication.tailRTS.Store(pack(position, counter))
					enqueueSPMCValues(t, queue, 10, 11, 12, 13)
					word := queue.consumerReservation.headRTS.Load()
					start, count, _, retry := queue.spmcAttemptConsumerWord(word, 2, true, exact)
					if retry || count != 2 {
						t.Fatal("initial reservation")
					}
					dst := make([]int, 2)
					queue.spmcReadConsumer(start, dst)
					queue.spmcReleaseConsumer(start, count)
					before := queue.consumerReservation.headRTS.Load()
					if before != pack(position+2, counter+1) {
						t.Fatal("packed carry", before)
					}
					_, count, _, retry = queue.spmcAttemptConsumerWord(word, 1, fixed, exact)
					if !retry || count != 0 || before != queue.consumerReservation.headRTS.Load() {
						t.Fatal("stale CAS changed packed state")
					}
					start, count, _ = queue.spmcReserveConsumer(2, true, exact)
					queue.spmcReadConsumer(start, dst)
					queue.spmcReleaseConsumer(start, count)
					if queue.consumerPublication.tailRTS.Load() != pack(position+4, counter+2) {
						t.Fatal("completion counter rollover")
					}
					if queue.Count() != 0 {
						t.Fatal("rollover mirror")
					}
				}
			}
		}
	}
}

func TestSPMCRTSMirrorAheadAndDelayedRaisers(t *testing.T) {
	for _, position := range []uint32{0, math.MaxUint32 - 1} {
		queue := newSPMCForTest[int](
			t,
			4,
			WithRelaxedMultiConsDequeue(),
			WithMaxConsumerHeadTailDistance(4),
		)
		seedSPMC(queue, position)
		enqueueSPMCValues(t, queue, 1, 2, 3, 4)
		start, count, _ := queue.spmcReserveConsumer(4, true, true)
		dst := make([]int, 4)
		queue.spmcReadConsumer(start, dst)
		if !publishRelaxed(&queue.consumerReservation.headRTS, &queue.consumerPublication.tailRTS) {
			t.Fatal("packed tail did not advance")
		}
		if queue.Count() != 4 || queue.Free() != 0 || !queue.Full() || queue.Empty() {
			t.Fatal("observer bypassed publication mirror")
		}
		if err := queue.Enqueue(5); err != ErrFull {
			t.Fatal("producer bypassed mirror", err)
		}
		if count != 4 {
			t.Fatal("reservation count")
		}
		entering, done := make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { awaitSPMCSignal(t, done) })
		go spmcRaiseMirrorForTest(queue, entering, done)
		awaitSPMCSignal(t, entering)
		raiseMirror(&queue.consumerPublication.published, &queue.consumerPublication.tailRTS)
		awaitSPMCSignal(t, done)
		if queue.Count() != 0 || queue.consumerPublication.published.Load() != position+4 {
			t.Fatal("mirror raise")
		}
		enqueueSPMCValues(t, queue, 5)
		if _, err := queue.Dequeue(); err != nil {
			t.Fatal(err)
		}
		raiseMirror(&queue.consumerPublication.published, &queue.consumerPublication.tailRTS)
		if queue.consumerPublication.published.Load() != position+5 {
			t.Fatal("mirror moved backward")
		}
	}
}

func spmcRaiseMirrorForTest(queue *SPMCQueue[int], entering, done chan struct{}) {
	close(entering)
	for range 64 {
		raiseMirror(&queue.consumerPublication.published, &queue.consumerPublication.tailRTS)
		runtime.Gosched()
	}
	close(done)
}

func TestSPMCRTSWaitThenReserve(t *testing.T) {
	for _, limit := range []int{0, 1} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			queue := newSPMCForTest[int](
				t,
				4,
				WithRelaxedMultiConsDequeue(),
				WithMaxConsumerHeadTailDistance(limit),
				WithSpinsBeforeYield(0),
			)
			enqueueSPMCValues(t, queue, 10, 11, 12, 13)
			stale := queue.consumerReservation.headRTS.Load()
			start, count, _ := queue.spmcReserveConsumer(uint32(limit+1), true, true)
			values := make([]int, count)
			queue.spmcReadConsumer(start, values)
			before := queue.consumerReservation.headRTS.Load()
			if _, moved, _, retry := queue.spmcAttemptConsumerWord(
				stale,
				1,
				true,
				true,
			); !retry || moved != 0 {
				t.Fatal("stale packed CAS did not fail")
			}
			entering, done := make(chan struct{}), make(chan struct{})
			result := make(chan [3]uint32, 1)
			released := false
			launched := false
			t.Cleanup(func() {
				if !released {
					queue.spmcReleaseConsumer(start, count)
				}
				if launched {
					awaitSPMCSignal(t, done)
				}
			})
			launched = true
			go spmcRTSReserveForTest(queue, 1, true, entering, done, result)
			awaitSPMCSignal(t, entering)
			awaitSPMCDistanceWait(t, done)
			if queue.consumerReservation.headRTS.Load() != before {
				t.Fatal("retry skipped distance")
			}
			queue.spmcReleaseConsumer(start, count)
			released = true
			awaitSPMCSignal(t, done)
			got := <-result
			if got != [3]uint32{start + count, 1, 3 - count} {
				t.Fatal("retry admission", got)
			}
			if queue.consumerReservation.headRTS.Load() != pack(start+count+1, unpackCnt(before)+1) {
				t.Fatal("retry packed head")
			}
			if queue.Count() != int(3-count) {
				t.Fatal("retry publication count")
			}
		})
	}
}

func TestSPMCRTSStaleOutOfRangeHead(t *testing.T) {
	for _, position := range []uint32{0, math.MaxUint32 - 1} {
		for _, exact := range []bool{false, true} {
			for _, fixed := range []bool{false, true} {
				queue := newSPMCForTest[int](
					t,
					2,
					WithRelaxedMultiConsDequeue(),
					WithMaxConsumerHeadTailDistance(0),
				)
				seedSPMC(queue, position)
				enqueueSPMCValues(t, queue, 10, 11)
				stale := queue.consumerReservation.headRTS.Load()
				if value, err := queue.Dequeue(); value != 10 || err != nil {
					t.Fatal(value, err)
				}
				enqueueSPMCValues(t, queue, 12)
				if left, err := queue.DequeueBulk(nil); left != 2 || err != nil {
					t.Fatal(left, err)
				}
				before := queue.consumerReservation.headRTS.Load()
				start, count, left, retry := queue.spmcAttemptConsumerWord(stale, 1, fixed, exact)
				if start != position || count != 0 || left != 2 || !retry {
					t.Fatal("stale out-of-range attempt", start, count, left, retry)
				}
				if queue.consumerReservation.headRTS.Load() != before {
					t.Fatal("stale head changed")
				}
				start, count, left, retry = queue.spmcAttemptConsumerWord(stale, 0, fixed, exact)
				if start != position || count != 0 || left != 2 || retry {
					t.Fatal("stale zero attempt", start, count, left, retry)
				}
				if queue.consumerReservation.headRTS.Load() != before {
					t.Fatal("stale zero head changed")
				}
				dequeueSPMCValues(t, queue, 11, 12)
			}
		}
	}
}
