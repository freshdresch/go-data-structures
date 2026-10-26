package dpdk

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

const mpscWorkerTimeout = 10 * time.Second

func awaitMPSCSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(mpscWorkerTimeout)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatal("worker did not reach coordinated milestone")
	}
}

func mpscPublishForTest[T any](
	queue *MPSCQueue[T],
	start, count uint32,
	entering, done chan struct{},
) {
	close(entering)
	queue.mpscPublishProducer(start, count)
	close(done)
}

func awaitMPSCPublishWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(mpscWorkerTimeout)
	stack := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		runtime.Gosched()
		length := runtime.Stack(stack, true)
		for _, worker := range strings.Split(string(stack[:length]), "\n\n") {
			isTestWorker := strings.Contains(worker, "mpscPublishForTest")
			isWaiting := strings.Contains(worker, "waitForTail")
			if isTestWorker && isWaiting {
				return
			}
		}
		select {
		case <-done:
			t.Fatal("later publication completed before earlier interval")
		default:
		}
	}
	t.Fatal("did not observe test worker inside production publication wait")
}

func TestMPSCOrderedPublicationAndSlotLifetime(t *testing.T) {
	for _, keep := range []bool{false, true} {
		for _, writeEarly := range []bool{false, true} {
			for _, position := range []uint32{0, 3, math.MaxUint32 - 1} {
				name := fmt.Sprintf("keep=%t/writeEarly=%t/start=%d", keep, writeEarly, position)
				t.Run(name, func(t *testing.T) {
					t.Run("int64", func(t *testing.T) {
						checkMPSCSlotLifetime(t, []int64{11, 12, 13, 14}, keep, writeEarly, position)
					})
					t.Run("pointer", func(t *testing.T) {
						values := []int64{11, 12, 13, 14}
						items := []*int64{&values[0], &values[1], &values[2], &values[3]}
						checkMPSCSlotLifetime(t, items, keep, writeEarly, position)
					})
				})
			}
		}
	}
}

func checkMPSCSlotLifetime[T comparable](
	t *testing.T,
	items []T,
	keep, writeEarly bool,
	position uint32,
) {
	t.Helper()
	options := []Option{WithSpinsBeforeYield(0)}
	if keep {
		options = append(options, WithoutDequeueClearing())
	}
	queue := newMPSCForTest[T](t, 4, options...)
	seedMPSC(queue, position)
	startA, countA, _ := queue.mpscReserveProducer(2, true, false)
	startB, countB, _ := queue.mpscReserveProducer(2, true, false)
	if startA != position || startB != position+2 || countA != 2 || countB != 2 {
		t.Fatal("reservations did not own adjacent two-entry intervals")
	}
	wroteA := false
	var publicationDone chan struct{}
	defer func() {
		if publicationDone == nil {
			return
		}
		select {
		case <-publicationDone:
			return
		default:
		}
		if !wroteA {
			queue.mpscWriteProducer(startA, items[:2])
		}
		// A's writes precede the only cleanup CAS, which cannot reverse B's publication.
		queue.producerPublication.published.CompareAndSwap(startA, startB)
		timer := time.NewTimer(mpscWorkerTimeout)
		defer timer.Stop()
		select {
		case <-publicationDone:
		case <-timer.C:
			t.Errorf("cleanup B join timed out: publication=%d", queue.producerPublication.published.Load())
		}
	}()
	if writeEarly {
		queue.mpscWriteProducer(startA, items[:2])
		wroteA = true
	}
	queue.mpscWriteProducer(startB, items[2:])
	var zero T
	for index := range items {
		want := items[index]
		if index < 2 && !writeEarly {
			want = zero
		}
		if got := queue.producerConfig.entries[(position+uint32(index))&3]; got != want {
			t.Fatalf("slot %d = %v, want %v", index, got, want)
		}
	}
	entering := make(chan struct{})
	publicationDone = make(chan struct{})
	go mpscPublishForTest(queue, startB, countB, entering, publicationDone)
	awaitMPSCSignal(t, entering)
	awaitMPSCPublishWait(t, publicationDone)
	if queue.producerPublication.published.Load() != position || queue.Count() != 0 {
		t.Fatal("later producer exposed unpublished reservations")
	}
	if value, err := queue.Dequeue(); err != ErrEmpty || value != zero {
		t.Fatalf("consumer saw unpublished data: value=%v error=%v", value, err)
	}
	destination := slices.Clone(items)
	before := slices.Clone(destination)
	if _, err := queue.DequeueBulk(destination); err != ErrEmpty {
		t.Fatalf("consumer bulk saw unpublished data: %v", err)
	}
	if !slices.Equal(destination, before) {
		t.Fatal("unpublished bulk changed destination")
	}
	if moved, _ := queue.DequeueBurst(destination); moved != 0 {
		t.Fatalf("consumer burst saw %d unpublished entries", moved)
	}
	if !slices.Equal(destination, before) {
		t.Fatal("unpublished burst changed destination")
	}
	if !wroteA {
		queue.mpscWriteProducer(startA, items[:2])
		wroteA = true
	}
	queue.mpscPublishProducer(startA, countA)
	awaitMPSCSignal(t, publicationDone)
	if _, err := queue.DequeueBulk(destination); err != nil || !slices.Equal(destination, items) {
		t.Fatalf("contiguous FIFO = %v, error=%v", destination, err)
	}
	for index := range items {
		want := zero
		if keep {
			want = items[index]
		}
		if got := queue.producerConfig.entries[(position+uint32(index))&3]; got != want {
			t.Fatalf("consumed slot %d = %v, want %v", index, got, want)
		}
	}
	replacement := slices.Clone(items)
	slices.Reverse(replacement)
	if _, err := queue.EnqueueBulk(replacement); err != nil {
		t.Fatal(err)
	}
	_, err := queue.DequeueBulk(destination)
	if err != nil || !slices.Equal(destination, replacement) {
		t.Fatalf("reused slots = %v, error=%v", destination, err)
	}
}

func TestMPSCPublicationWaitYields(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	queue := newMPSCForTest[int](t, 2, WithSpinsBeforeYield(0))
	startA, countA, _ := queue.mpscReserveProducer(1, true, false)
	startB, countB, _ := queue.mpscReserveProducer(1, true, false)
	queue.mpscWriteProducer(startA, []int{1})
	queue.mpscWriteProducer(startB, []int{2})
	entering := make(chan struct{})
	done := make(chan struct{})
	defer func() {
		select {
		case <-done:
			return
		default:
		}
		queue.producerPublication.published.CompareAndSwap(startA, startB)
		timer := time.NewTimer(mpscWorkerTimeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			t.Errorf("cleanup B join timed out: publication=%d", queue.producerPublication.published.Load())
		}
	}()
	go mpscPublishForTest(queue, startB, countB, entering, done)
	awaitMPSCSignal(t, entering)
	awaitMPSCPublishWait(t, done)
	progress := make(chan struct{})
	go func() { close(progress) }()
	awaitMPSCSignal(t, progress)
	if queue.producerPublication.published.Load() != startA {
		t.Fatal("publication advanced before A")
	}
	select {
	case <-done:
		t.Fatal("B completed before A publication")
	default:
	}
	queue.mpscPublishProducer(startA, countA)
	awaitMPSCSignal(t, done)
}

func TestMPSCFailedAttemptRecomputesAdmission(t *testing.T) {
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
			for _, exact := range []bool{false, true} {
				for _, fixed := range []bool{false, true} {
					for _, produceA := range []uint32{1, 3} {
						name := fmt.Sprintf("exact=%t/fixed=%t/produceA=%d", exact, fixed, produceA)
						t.Run(name, func(t *testing.T) {
							queue := newMPSCProtocolForTest[int](t, relaxed, 4)
							seedMPSC(queue, math.MaxUint32-1)
							word := mpscHeadWordForTest(queue)
							head := queue.mpscProducerHead()
							startA, countA, _, retry := queue.mpscAttemptProducer(word, produceA, true, exact)
							if retry || countA != produceA || startA != head {
								t.Fatal("first whole attempt did not reserve A")
							}
							queue.mpscWriteProducer(startA, []int{10, 11, 12}[:countA])
							queue.mpscPublishProducer(startA, countA)
							_, countB, _, retry := queue.mpscAttemptProducer(word, 2, fixed, exact)
							if !retry || countB != 0 {
								t.Fatalf("contended attempt count=%d retry=%t", countB, retry)
							}
							before := slices.Clone(queue.producerConfig.entries)
							startB, countB, remaining := queue.mpscReserveProducer(2, fixed, exact)
							wantCount, wantRemaining := uint32(2), uint32(1)
							if produceA == 3 {
								wantCount, wantRemaining = 1, 0
								if fixed {
									wantCount, wantRemaining = 0, 1
								}
							}
							if startB != head+countA || countB != wantCount || remaining != wantRemaining {
								t.Fatalf("retry start/count/remaining = %d/%d/%d", startB, countB, remaining)
							}
							if !slices.Equal(before, queue.producerConfig.entries) {
								t.Fatal("reservation changed slots before movement")
							}
							if countB > 0 {
								queue.mpscWriteProducer(startB, []int{20, 21}[:countB])
								queue.mpscPublishProducer(startB, countB)
							}
							want := append([]int{10, 11, 12}[:countA:countA], []int{20, 21}[:countB]...)
							got := make([]int, len(want))
							if _, err := queue.DequeueBulk(got); err != nil || !slices.Equal(got, want) {
								t.Fatalf("FIFO values=%v error=%v, want %v", got, err, want)
							}
						})
					}
				}
			}

		})
	}
}

func TestMPSCConsumerClearsBeforeReuse(t *testing.T) {
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
			for _, keep := range []bool{false, true} {
				for _, position := range []uint32{0, 3, math.MaxUint32 - 1} {
					t.Run(fmt.Sprintf("keep=%t/start=%d", keep, position), func(t *testing.T) {
						values := []int64{11, 12, 13, 14, 21, 22, 23, 24}
						items := []*int64{&values[0], &values[1], &values[2], &values[3]}
						replacement := []*int64{&values[4], &values[5], &values[6], &values[7]}
						options := []Option{WithSpinsBeforeYield(0)}
						if keep {
							options = append(options, WithoutDequeueClearing())
						}
						queue := newMPSCProtocolForTest[*int64](t, relaxed, 4, options...)
						seedMPSC(queue, position)
						if _, err := queue.EnqueueBulk(items); err != nil {
							t.Fatal(err)
						}
						copied := make([]*int64, 4)
						queue.mpscReadConsumer(queue.consumer.cursor, copied)
						if !slices.Equal(copied, items) {
							t.Fatalf("read before release = %v", copied)
						}
						if queue.consumerPublished.Load() != position || queue.consumer.cursor != position {
							t.Fatal("reading released slots before consumer completion")
						}
						if queue.Count() != 4 {
							t.Fatal("observer excluded consumer-owned slots before release")
						}
						attempted := make(chan struct{})
						var admission error
						go func() {
							_, admission = queue.EnqueueBulk(replacement)
							close(attempted)
						}()
						awaitMPSCSignal(t, attempted)
						if admission != ErrFull {
							t.Fatalf("producer reused slots before release: %v", admission)
						}
						for index, item := range items {
							want := item
							if !keep {
								want = nil
							}
							if got := queue.consumer.entries[(position+uint32(index))&3]; got != want {
								t.Fatalf("read/clear slot %d = %v, want %v", index, got, want)
							}
						}
						queue.mpscReleaseConsumer(4)
						if _, err := queue.EnqueueBulk(replacement); err != nil {
							t.Fatal(err)
						}
						if _, err := queue.DequeueBulk(copied); err != nil {
							t.Fatal(err)
						}
						if !slices.Equal(copied, replacement) {
							t.Fatal("old consumer clearing corrupted producer reuse")
						}
					})
				}
			}

		})
	}
}

func TestMPSCConsumerOwnershipHandoffs(t *testing.T) {
	for _, exact := range []bool{false, true} {
		for _, outgoing := range []string{"successful", "failed", "zero"} {
			t.Run(fmt.Sprintf("exact=%t/%s", exact, outgoing), func(t *testing.T) {
				for _, relaxed := range []bool{false, true} {
					t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
						checkMPSCConsumerHandoff(t, exact, outgoing, relaxed)
					})
				}
			})
		}
	}
}

func checkMPSCConsumerHandoff(t *testing.T, exact bool, outgoing string, relaxed bool) {
	t.Helper()
	options := []Option{WithSpinsBeforeYield(0)}
	if exact {
		options = append(options, WithExactBatchCounts())
	}
	queue := newMPSCProtocolForTest[int](t, relaxed, 4, options...)
	seedMPSC(queue, math.MaxUint32-3)
	if err := queue.Enqueue(1); err != nil {
		t.Fatal(err)
	}
	if value, err := queue.Dequeue(); value != 1 || err != nil {
		t.Fatalf("initial dequeue=%d error=%v", value, err)
	}
	before := queue.consumer.cachedProducer
	ctx, cancel := context.WithTimeout(context.Background(), mpscWorkerTimeout)
	secondPublished := make(chan struct{})
	incomingStarted := make(chan struct{})
	producerDone := make(chan struct{})
	consumerDone := make(chan struct{})
	consumerLaunched := false
	workerErrors := make(chan error, 2)
	defer func() {
		cancel()
		awaitMPSCSignal(t, producerDone)
		if consumerLaunched {
			awaitMPSCSignal(t, consumerDone)
		}
	}()
	go func() {
		defer close(producerDone)
		if err := queue.Enqueue(2); err != nil {
			workerErrors <- fmt.Errorf("publish second: %w", err)
			return
		}
		close(secondPublished)
		select {
		case <-incomingStarted:
		case <-ctx.Done():
			return
		}
		for next := 3; next <= 128 && ctx.Err() == nil; {
			err := queue.Enqueue(next)
			if err != nil && err != ErrFull {
				workerErrors <- fmt.Errorf("producer transfer: %w", err)
				return
			}
			if err == nil {
				next++
			}
			runtime.Gosched()
		}
	}()
	awaitMPSCSignal(t, secondPublished)
	firstExpected := 2
	switch outgoing {
	case "successful":
		destination := []int{-1}
		if _, err := queue.DequeueBulk(destination); err != nil || destination[0] != 2 {
			t.Fatalf("outgoing successful read=%v error=%v", destination, err)
		}
		firstExpected = 3
	case "failed":
		destination := []int{-1, -1}
		if _, err := queue.DequeueBulk(destination); err != ErrEmpty {
			t.Fatalf("outgoing failed admission error=%v", err)
		}
		if !slices.Equal(destination, []int{-1, -1}) {
			t.Fatal("outgoing failed read changed destination")
		}
	case "zero":
		if available, err := queue.DequeueBulk(nil); err != nil || available != 1 {
			t.Fatalf("outgoing zero refresh available=%d error=%v", available, err)
		}
	}
	if queue.consumer.cachedProducer == before {
		t.Fatal("outgoing operation did not refresh the private consumer cache")
	}
	ownership := make(chan *MPSCQueue[int], 1)
	ownership <- queue
	consumerLaunched = true
	go func() {
		defer close(consumerDone)
		consumer := <-ownership
		close(incomingStarted)
		for expected := firstExpected; expected <= 128 && ctx.Err() == nil; {
			value, err := consumer.Dequeue()
			if err == ErrEmpty {
				runtime.Gosched()
				continue
			}
			if err != nil || value != expected {
				workerErrors <- fmt.Errorf(
					"incoming dequeue=%d error=%v, want %d", value, err, expected,
				)
				return
			}
			expected++
		}
	}()
	awaitMPSCSignal(t, producerDone)
	awaitMPSCSignal(t, consumerDone)
	if ctx.Err() != nil {
		t.Error("handoff transfers exceeded deadline")
	}
	close(workerErrors)
	for err := range workerErrors {
		t.Error(err)
	}
	if queue.Count() != 0 || queue.consumer.cursor != queue.consumerPublished.Load() {
		t.Error("handoff queue did not finish empty at a consumer method boundary")
	}
}
