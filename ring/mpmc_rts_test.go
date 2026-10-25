package ring

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func mpmcRTSHeadWord(queue *MPMCQueue[int], producer bool) uint64 {
	if producer {
		if queue.config.producerRelaxed {
			return queue.producerReservation.headRTS.Load()
		}
		return uint64(queue.producerReservation.head.Load())
	}
	if queue.config.consumerRelaxed {
		return queue.consumerReservation.headRTS.Load()
	}
	return uint64(queue.consumerReservation.head.Load())
}

func mpmcRTSAttempt(
	queue *MPMCQueue[int],
	producer bool,
	word uint64,
	requested uint32,
	fixed, exact bool,
) (uint32, uint32, uint32, bool) {
	if producer {
		return queue.mpmcAttemptProducer(word, requested, fixed, exact)
	}
	return queue.mpmcAttemptConsumer(word, requested, fixed, exact)
}

func mpmcRTSReserve(
	queue *MPMCQueue[int],
	producer bool,
	requested uint32,
	fixed, exact bool,
) (uint32, uint32, uint32) {
	if producer {
		return queue.mpmcReserveProducer(requested, fixed, exact)
	}
	return queue.mpmcReserveConsumer(requested, fixed, exact)
}

func mpmcRTSMoveAndComplete(queue *MPMCQueue[int], producer bool, start, count uint32) {
	if count == 0 {
		return
	}
	values := make([]int, count)
	for index := range values {
		values[index] = int(start + uint32(index))
	}
	if producer {
		queue.mpmcWriteProducer(start, values)
		queue.mpmcPublishProducer(start, count)
		return
	}
	queue.mpmcReadConsumer(start, values)
	queue.mpmcReleaseConsumer(start, count)
}

func mpmcRTSReserveWorker(
	queue *MPMCQueue[int],
	producer bool,
	requested uint32,
	fixed bool,
	result chan<- [3]uint32,
	done chan<- struct{},
) {
	start, count, left := mpmcRTSReserve(queue, producer, requested, fixed, true)
	mpmcRTSMoveAndComplete(queue, producer, start, count)
	result <- [3]uint32{start, count, left}
	close(done)
}

type mpmcRTSPublicResult struct {
	moved, left int
	err         error
}

func mpmcRTSPublicWorker(
	queue *MPMCQueue[int],
	producer bool,
	method string,
	result chan<- mpmcRTSPublicResult,
	done chan<- struct{},
) {
	moved, left := 0, 0
	var err error
	if producer {
		switch method {
		case "single":
			err = queue.Enqueue(7)
			if err == nil {
				moved = 1
			}
		case "bulk":
			left, err = queue.EnqueueBulk([]int{7, 8})
			if err == nil {
				moved = 2
			}
		case "burst":
			moved, left = queue.EnqueueBurst([]int{7, 8})
		case "zero":
			left, err = queue.EnqueueBulk(nil)
		}
	} else {
		destination := make([]int, 2)
		switch method {
		case "single":
			_, err = queue.Dequeue()
			if err == nil {
				moved = 1
			}
		case "bulk":
			left, err = queue.DequeueBulk(destination)
			if err == nil {
				moved = 2
			}
		case "burst":
			moved, left = queue.DequeueBurst(destination)
		case "zero":
			left, err = queue.DequeueBulk(nil)
		}
	}
	result <- mpmcRTSPublicResult{moved: moved, left: left, err: err}
	close(done)
}

func TestMPMCRTSDistanceWaitsBeforeFullEmptyAndZero(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	for _, producer := range []bool{false, true} {
		for _, limit := range []int{0, 1} {
			for _, method := range []string{"single", "bulk", "burst", "zero"} {
				name := fmt.Sprintf("producer=%t/limit=%d/%s", producer, limit, method)
				t.Run(name, func(t *testing.T) {
					mode := mpmcConcurrentMode{true, true}
					options := []Option{WithMaxConsumerHeadTailDistance(limit)}
					if producer {
						options = []Option{WithMaxProducerHeadTailDistance(limit)}
					}
					queue := mpmcConcurrentQueue[int](t, 4, mode, options...)
					mpmcConcurrentSeed(queue, math.MaxUint32-1, math.MaxUint32)
					if !producer {
						if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
							t.Fatal(err)
						}
					}
					start, count, _ := mpmcRTSReserve(queue, producer, 4, true, true)
					if count != 4 {
						t.Fatal("initial interval did not cross distance limit")
					}
					completed := false
					var done chan struct{}
					t.Cleanup(func() {
						if !completed {
							mpmcRTSMoveAndComplete(queue, producer, start, count)
						}
						if done != nil {
							mpmcConcurrentAwait(t, done)
						}
					})
					beforeHead := mpmcRTSHeadWord(queue, producer)
					beforeTail := queue.consumerPublication.tailRTS.Load()
					cache := queue.consumerReservation.cachedProducer.Load()
					if producer {
						beforeTail = queue.producerPublication.tailRTS.Load()
						cache = queue.producerReservation.cachedConsumer.Load()
					}
					result := make(chan mpmcRTSPublicResult, 1)
					done = make(chan struct{})
					go mpmcRTSPublicWorker(queue, producer, method, result, done)
					mpmcConcurrentWaitFrame(t, done, "mpmcRTSPublicWorker", "waitHeadTailDistance")
					if mpmcRTSHeadWord(queue, producer) != beforeHead {
						t.Fatal("blocked public call changed reservation counter")
					}
					if producer {
						queue.producerReservation.head.Store(999)
						left, err := queue.EnqueueBulk(make([]int, 5))
						if !errors.Is(err, ErrExceedsCapacity) || left != 0 {
							t.Fatalf("invalid producer length active-head count=%d error=%v", left, err)
						}
						if queue.producerReservation.cachedConsumer.Load() != cache ||
							queue.producerPublication.tailRTS.Load() != beforeTail {
							t.Fatal("invalid producer length changed cache or completion counter")
						}
					} else {
						queue.consumerReservation.head.Store(999)
						left, err := queue.DequeueBulk(make([]int, 5))
						if !errors.Is(err, ErrExceedsCapacity) || left != 0 {
							t.Fatalf("invalid consumer length active-head count=%d error=%v", left, err)
						}
						if queue.consumerReservation.cachedProducer.Load() != cache ||
							queue.consumerPublication.tailRTS.Load() != beforeTail {
							t.Fatal("invalid consumer length changed cache or completion counter")
						}
					}
					mpmcRTSMoveAndComplete(queue, producer, start, count)
					completed = true
					mpmcConcurrentAwait(t, done)
					got := <-result
					wantErr := error(nil)
					if method == "single" || method == "bulk" {
						wantErr = ErrEmpty
						if producer {
							wantErr = ErrFull
						}
					}
					if got.err != wantErr {
						t.Fatalf("operation error=%v want=%v", got.err, wantErr)
					}
					if got.moved != 0 || got.left != 0 ||
						mpmcRTSHeadWord(queue, producer) != beforeHead {
						t.Fatalf("full/empty/zero changed reservation: result=%v", got)
					}
					tail := queue.consumerPublication.tailRTS.Load()
					if producer {
						tail = queue.producerPublication.tailRTS.Load()
					}
					if unpackCnt(tail) != unpackCnt(beforeTail)+1 {
						t.Fatal("failed or zero call incremented completion counter")
					}
				})
			}
		}
	}
}

func TestMPMCRTSDistanceEqualityCapacityAndDefaults(t *testing.T) {
	for _, producer := range []bool{false, true} {
		for _, limit := range []int{0, 1, 4, 5} {
			t.Run(fmt.Sprintf("producer=%t/limit=%d", producer, limit), func(t *testing.T) {
				options := []Option{WithMaxConsumerHeadTailDistance(limit)}
				if producer {
					options = []Option{WithMaxProducerHeadTailDistance(limit)}
				}
				queue := mpmcConcurrentQueue[int](t, 4, mpmcConcurrentMode{true, true}, options...)
				if !producer {
					if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
						t.Fatal(err)
					}
				}
				first, count, _ := mpmcRTSReserve(queue, producer, 1, true, true)
				firstDone := false
				t.Cleanup(func() {
					if !firstDone {
						mpmcRTSMoveAndComplete(queue, producer, first, count)
					}
				})
				if count != 1 {
					t.Fatal("initial equality-at-zero reservation failed")
				}
				if limit == 0 {
					mpmcRTSMoveAndComplete(queue, producer, first, count)
					firstDone = true
				}
				second, next, _ := mpmcRTSReserve(queue, producer, 3, true, true)
				if next != 3 {
					t.Fatal("equality or unthrottled distance rejected whole crossing batch")
				}
				mpmcRTSMoveAndComplete(queue, producer, second, next)
				if !firstDone {
					mpmcRTSMoveAndComplete(queue, producer, first, count)
					firstDone = true
				}
				before := mpmcRTSHeadWord(queue, producer)
				_, moved, left := mpmcRTSReserve(queue, producer, 1, true, true)
				if moved != 0 || left != 0 || mpmcRTSHeadWord(queue, producer) != before {
					t.Fatal("distance limit bypassed ordinary capacity admission")
				}
			})
		}
	}
	for _, capacity := range []int{1, 2, 4, 8} {
		queue, err := NewMPMCQueue[int](
			capacity,
			WithRelaxedMultiProdEnqueue(),
			WithRelaxedMultiConsDequeue(),
		)
		if err != nil {
			t.Fatal(err)
		}
		want := uint32(capacity / 8)
		if queue.config.prodHTDMax != want || queue.config.consHTDMax != want {
			t.Fatalf(
				"C=%d default distances=%d/%d",
				capacity,
				queue.config.prodHTDMax,
				queue.config.consHTDMax,
			)
		}
	}
}

func TestMPMCRTSIndependentDistanceWaits(t *testing.T) {
	queue := mpmcConcurrentQueue[int](
		t,
		4,
		mpmcConcurrentMode{true, true},
		WithMaxProducerHeadTailDistance(0),
		WithMaxConsumerHeadTailDistance(0),
	)
	if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Dequeue(); err != nil {
		t.Fatal(err)
	}
	producerStart, producerCount, _ := queue.mpmcReserveProducer(1, true, true)
	consumerStart, consumerCount, _ := queue.mpmcReserveConsumer(1, true, true)
	producerDone, consumerDone := false, false
	doneProducer, doneConsumer := make(chan struct{}), make(chan struct{})
	launched := false
	t.Cleanup(func() {
		if !producerDone {
			mpmcRTSMoveAndComplete(queue, true, producerStart, producerCount)
		}
		if !consumerDone {
			mpmcRTSMoveAndComplete(queue, false, consumerStart, consumerCount)
		}
		if launched {
			mpmcConcurrentAwait(t, doneProducer)
			mpmcConcurrentAwait(t, doneConsumer)
		}
	})
	resultProducer, resultConsumer := make(chan [3]uint32, 1), make(chan [3]uint32, 1)
	launched = true
	go mpmcRTSProducerZeroWorker(queue, resultProducer, doneProducer)
	go mpmcRTSConsumerZeroWorker(queue, resultConsumer, doneConsumer)
	mpmcConcurrentWaitFrame(t, doneProducer, "mpmcRTSProducerZeroWorker", "waitHeadTailDistance")
	mpmcConcurrentWaitFrame(t, doneConsumer, "mpmcRTSConsumerZeroWorker", "waitHeadTailDistance")
	mpmcRTSMoveAndComplete(queue, true, producerStart, producerCount)
	producerDone = true
	mpmcConcurrentAwait(t, doneProducer)
	mpmcConcurrentWaitFrame(t, doneConsumer, "mpmcRTSConsumerZeroWorker", "waitHeadTailDistance")
	mpmcRTSMoveAndComplete(queue, false, consumerStart, consumerCount)
	consumerDone = true
	mpmcConcurrentAwait(t, doneConsumer)
	if (<-resultProducer)[1] != 0 || (<-resultConsumer)[1] != 0 {
		t.Fatal("zero requests reserved intervals")
	}
}

func mpmcRTSProducerZeroWorker(
	queue *MPMCQueue[int],
	result chan<- [3]uint32,
	done chan<- struct{},
) {
	mpmcRTSReserveWorker(queue, true, 0, true, result, done)
}

func mpmcRTSConsumerZeroWorker(
	queue *MPMCQueue[int],
	result chan<- [3]uint32,
	done chan<- struct{},
) {
	mpmcRTSReserveWorker(queue, false, 0, true, result, done)
}

func TestMPMCWholeAttemptStaleCASRecomputesAdmission(t *testing.T) {
	for _, mode := range mpmcConcurrentModes() {
		for _, producer := range []bool{false, true} {
			for _, exact := range []bool{false, true} {
				for _, fixed := range []bool{false, true} {
					name := fmt.Sprintf("%s/producer=%t/exact=%t/fixed=%t", mode, producer, exact, fixed)
					t.Run(name, func(t *testing.T) {
						queue := mpmcConcurrentQueue[int](t, 4, mode)
						mpmcConcurrentSeed(queue, math.MaxUint32-1, 0)
						if !producer {
							if _, err := queue.EnqueueBulk([]int{10, 11, 12, 13}); err != nil {
								t.Fatal(err)
							}
						}
						stale := mpmcRTSHeadWord(queue, producer)
						start, count, _, retry := mpmcRTSAttempt(queue, producer, stale, 3, true, exact)
						if retry || count != 3 {
							t.Fatal("first whole attempt did not own three slots")
						}
						mpmcRTSMoveAndComplete(queue, producer, start, count)
						before := mpmcRTSHeadWord(queue, producer)
						_, failed, _, retry := mpmcRTSAttempt(queue, producer, stale, 2, fixed, exact)
						if !retry || failed != 0 || mpmcRTSHeadWord(queue, producer) != before {
							t.Fatal("captured stale CAS acquired ownership")
						}
						next, moved, left := mpmcRTSReserve(queue, producer, 2, fixed, exact)
						wantMoved, wantLeft := uint32(1), uint32(0)
						if fixed {
							wantMoved, wantLeft = 0, 1
						}
						if moved != wantMoved || left != wantLeft || next != start+3 {
							t.Fatalf("renewed admission start=%d moved=%d left=%d", next, moved, left)
						}
						mpmcRTSMoveAndComplete(queue, producer, next, moved)
					})
				}
			}
		}
	}
}

func TestMPMCRTSRetryRepeatsDistanceAfterStaleObservations(t *testing.T) {
	for _, producer := range []bool{false, true} {
		for _, exact := range []bool{false, true} {
			for _, outOfRange := range []bool{false, true} {
				name := fmt.Sprintf("producer=%t/exact=%t/outOfRange=%t", producer, exact, outOfRange)
				t.Run(name, func(t *testing.T) {
					options := []Option{WithMaxConsumerHeadTailDistance(0)}
					if producer {
						options = []Option{WithMaxProducerHeadTailDistance(0)}
					}
					queue := mpmcConcurrentQueue[int](t, 4, mpmcConcurrentMode{true, true}, options...)
					mpmcConcurrentSeed(queue, math.MaxUint32-1, 0)
					stale := mpmcRTSHeadWord(queue, producer)
					if outOfRange {
						if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
							t.Fatal(err)
						}
						if _, err := queue.Dequeue(); err != nil {
							t.Fatal(err)
						}
						if !producer {
							if err := queue.Enqueue(5); err != nil {
								t.Fatal(err)
							}
						} else {
							if _, err := queue.DequeueBulk(make([]int, 3)); err != nil {
								t.Fatal(err)
							}
						}
					} else if !producer {
						if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
							t.Fatal(err)
						}
					}
					start, count, _ := mpmcRTSReserve(queue, producer, 1, true, exact)
					completed := false
					var done chan struct{}
					t.Cleanup(func() {
						if !completed {
							mpmcRTSMoveAndComplete(queue, producer, start, count)
						}
						if done != nil {
							mpmcConcurrentAwait(t, done)
						}
					})
					before := mpmcRTSHeadWord(queue, producer)
					if outOfRange {
						if producer {
							queue.producerReservation.cachedConsumer.Store(queue.consumerPublication.published.Load())
						} else {
							queue.consumerReservation.cachedProducer.Store(queue.producerPublication.published.Load())
						}
					}
					_, moved, _, retry := mpmcRTSAttempt(queue, producer, stale, 1, true, exact)
					if moved != 0 || !retry || mpmcRTSHeadWord(queue, producer) != before {
						t.Fatal("stale observation did not request a fresh head")
					}
					if outOfRange {
						_, moved, left, retry := mpmcRTSAttempt(queue, producer, stale, 0, true, exact)
						if moved != 0 || left != 4 || retry {
							t.Fatal("stale zero observation did not clamp count without ownership")
						}
					}
					result := make(chan [3]uint32, 1)
					done = make(chan struct{})
					go mpmcRTSReserveWorker(queue, producer, 1, false, result, done)
					mpmcConcurrentWaitFrame(t, done, "mpmcRTSReserveWorker", "waitHeadTailDistance")
					if mpmcRTSHeadWord(queue, producer) != before {
						t.Fatal("retry bypassed distance wait")
					}
					mpmcRTSMoveAndComplete(queue, producer, start, count)
					completed = true
					mpmcConcurrentAwait(t, done)
					got := <-result
					if got[0] != start+count || got[1] != 1 || got[2] > 4 {
						t.Fatalf("retry result=%v", got)
					}
				})
			}
		}
	}
}

func TestMPMCRTSIndependentPositionAndCounterRollover(t *testing.T) {
	for _, producer := range []bool{false, true} {
		for _, position := range []uint32{10, math.MaxUint32 - 1} {
			for _, counter := range []uint32{0, math.MaxUint32 - 1, math.MaxUint32} {
				name := fmt.Sprintf("producer=%t/position=%d/counter=%d", producer, position, counter)
				t.Run(name, func(t *testing.T) {
					queue := mpmcConcurrentQueue[int](t, 4, mpmcConcurrentMode{true, true})
					mpmcConcurrentSeed(queue, position, counter)
					if !producer {
						if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
							t.Fatal(err)
						}
					}
					for range 2 {
						start, count, _ := mpmcRTSReserve(queue, producer, 2, true, true)
						if count != 2 {
							t.Fatal("rollover reservation did not own two slots")
						}
						mpmcRTSMoveAndComplete(queue, producer, start, count)
					}
					before := mpmcRTSHeadWord(queue, producer)
					tail := queue.consumerPublication.tailRTS.Load()
					if producer {
						tail = queue.producerPublication.tailRTS.Load()
					}
					if before != pack(position+4, counter+2) || tail != before {
						t.Fatalf("independent rollover head=%#x tail=%#x", before, tail)
					}
					for _, request := range []uint32{0, 1} {
						_, count, left := mpmcRTSReserve(queue, producer, request, true, true)
						if count != 0 || left != 0 || mpmcRTSHeadWord(queue, producer) != before {
							t.Fatal("failed or zero call changed rollover counter")
						}
					}
				})
			}
		}
	}
}

func mpmcRTSDelayedMirrorWorker(
	head, tail *atomic.Uint64,
	mirror *atomic.Uint32,
	published, resume, done chan struct{},
) {
	publishRelaxed(head, tail)
	close(published)
	<-resume
	raiseMirror(mirror, tail)
	close(done)
}

func TestMPMCRTSDelayedAndConcurrentMirrorRaisers(t *testing.T) {
	for _, producer := range []bool{false, true} {
		for _, position := range []uint32{0, math.MaxUint32 - 1} {
			t.Run(fmt.Sprintf("producer=%t/position=%d", producer, position), func(t *testing.T) {
				queue := mpmcConcurrentQueue[int](t, 4, mpmcConcurrentMode{true, true})
				mpmcConcurrentSeed(queue, position, 0)
				if !producer {
					if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
						t.Fatal(err)
					}
				}
				head := &queue.consumerReservation.headRTS
				tail := &queue.consumerPublication.tailRTS
				mirror := &queue.consumerPublication.published
				if producer {
					head = &queue.producerReservation.headRTS
					tail = &queue.producerPublication.tailRTS
					mirror = &queue.producerPublication.published
				}
				start, count, _ := mpmcRTSReserve(queue, producer, 1, true, true)
				if producer {
					queue.mpmcWriteProducer(start, []int{1})
				} else {
					queue.mpmcReadConsumer(start, make([]int, count))
				}
				published, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				resumed := false
				t.Cleanup(func() {
					if !resumed {
						close(resume)
					}
					mpmcConcurrentAwait(t, done)
				})
				go mpmcRTSDelayedMirrorWorker(head, tail, mirror, published, resume, done)
				mpmcConcurrentAwait(t, published)
				if unpackPos(tail.Load()) != position+1 || mirror.Load() != position {
					t.Fatal("delayed caller did not separate packed completion from mirror raise")
				}
				if producer {
					if _, err := queue.Dequeue(); err != ErrEmpty {
						t.Fatal("consumer bypassed delayed producer mirror")
					}
				} else if err := queue.Enqueue(7); err != ErrFull {
					t.Fatal("producer bypassed delayed consumer mirror")
				}
				next, moved, _ := mpmcRTSReserve(queue, producer, 1, true, true)
				mpmcRTSMoveAndComplete(queue, producer, next, moved)
				if mirror.Load() != position+2 {
					t.Fatal("second completion did not advance mirror across position wrap")
				}
				close(resume)
				resumed = true
				mpmcConcurrentAwait(t, done)
				if mirror.Load() != position+2 {
					t.Fatal("delayed mirror raiser moved publication backward")
				}
				stop := make(chan struct{})
				var epoch atomic.Uint32
				var raisers sync.WaitGroup
				acknowledgments := make([]chan struct{}, 4)
				for index := range acknowledgments {
					acknowledgment := make(chan struct{}, 1)
					acknowledgments[index] = acknowledgment
					raisers.Go(func() {
						var acknowledged uint32
						for {
							select {
							case <-stop:
								return
							default:
							}
							cycle := epoch.Load()
							raiseMirror(mirror, tail)
							if cycle != acknowledged {
								acknowledged = cycle
								acknowledgment <- struct{}{}
							}
							runtime.Gosched()
						}
					})
				}
				var backwards atomic.Bool
				observed := make(chan struct{}, 1)
				observerDone := make(chan struct{})
				initial := mirror.Load()
				go func() {
					defer close(observerDone)
					previous, acknowledgedPosition := initial, initial
					var acknowledged uint32
					for {
						cycle := epoch.Load()
						current := mirror.Load()
						if int32(current-previous) < 0 {
							backwards.Store(true)
						}
						previous = current
						if cycle != acknowledged && current != acknowledgedPosition {
							acknowledged = cycle
							acknowledgedPosition = current
							observed <- struct{}{}
						}
						select {
						case <-stop:
							return
						default:
						}
						runtime.Gosched()
					}
				}()
				stopped := false
				join := func() {
					if !stopped {
						close(stop)
						stopped = true
					}
					joined := make(chan struct{})
					go func() {
						raisers.Wait()
						close(joined)
					}()
					mpmcConcurrentAwait(t, joined)
					mpmcConcurrentAwait(t, observerDone)
				}
				t.Cleanup(join)
				// Complete outstanding visible data before cycling the same concrete queue.
				queue.DequeueBurst(make([]int, 4))
				for cycle := range 64 {
					if err := queue.Enqueue(9); err != nil {
						t.Fatal(err)
					}
					if _, err := queue.Dequeue(); err != nil {
						t.Fatal(err)
					}
					// Each worker must run after this publication before the next cycle.
					epoch.Store(uint32(cycle + 1))
					for _, acknowledgment := range acknowledgments {
						mpmcConcurrentAwait(t, acknowledgment)
					}
					mpmcConcurrentAwait(t, observed)
				}
				join()
				if backwards.Load() || mirror.Load() != unpackPos(tail.Load()) {
					t.Fatal("concurrent mirror raisers violated monotonic publication")
				}
			})
		}
	}
}
