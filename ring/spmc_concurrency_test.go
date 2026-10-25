package ring

import (
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

const spmcWorkerTimeout = 10 * time.Second

func awaitSPMCSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(spmcWorkerTimeout)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatal("worker did not reach coordinated milestone")
	}
}

func spmcReleaseForTest[T any](
	queue *SPMCQueue[T],
	start, count uint32,
	entering, done chan struct{},
) {
	close(entering)
	queue.spmcReleaseConsumer(start, count)
	close(done)
}

func awaitSPMCReleaseWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(spmcWorkerTimeout)
	stack := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		runtime.Gosched()
		length := runtime.Stack(stack, true)
		for _, worker := range strings.Split(string(stack[:length]), "\n\n") {
			isTestWorker := strings.Contains(worker, "spmcReleaseForTest")
			isWaiting := strings.Contains(worker, "waitForTail")
			if isTestWorker && isWaiting {
				return
			}
		}
		select {
		case <-done:
			t.Fatal("later release completed before earlier interval")
		default:
		}
	}
	t.Fatal("did not observe test worker inside production release wait")
}

func TestSPMCOrderedReleaseAndSlotLifetime(t *testing.T) {
	for _, keep := range []bool{false, true} {
		for _, readEarly := range []bool{false, true} {
			for _, position := range []uint32{0, 3, math.MaxUint32 - 1} {
				name := fmt.Sprintf("keep=%t/readEarly=%t/start=%d", keep, readEarly, position)
				t.Run(name, func(t *testing.T) {
					t.Run("int", func(t *testing.T) {
						checkSPMCSlotLifetime(t, []int{11, 12, 13, 14}, keep, readEarly, position)
					})
					t.Run("pointer", func(t *testing.T) {
						values := []int{11, 12, 13, 14}
						items := []*int{&values[0], &values[1], &values[2], &values[3]}
						checkSPMCSlotLifetime(t, items, keep, readEarly, position)
					})
				})
			}
		}
	}
}

func checkSPMCSlotLifetime[T comparable](
	t *testing.T,
	items []T,
	keep, readEarly bool,
	position uint32,
) {
	t.Helper()
	options := []Option{WithSpinsBeforeYield(0)}
	if keep {
		options = append(options, WithoutDequeueClearing())
	}
	queue := newSPMCForTest[T](t, 4, options...)
	seedSPMC(queue, position)
	if _, err := queue.EnqueueBulk(items); err != nil {
		t.Fatal(err)
	}
	startA, countA, _ := queue.spmcReserveConsumer(2, true, false)
	startB, countB, _ := queue.spmcReserveConsumer(2, true, false)
	if startA != position || startB != position+2 || countA != 2 || countB != 2 {
		t.Fatal("reservations did not own adjacent two-entry intervals")
	}
	copiedA := make([]T, 2)
	copiedB := make([]T, 2)
	readA := false
	releasedA := false
	var releaseDone chan struct{}
	defer func() {
		if !readA {
			queue.spmcReadConsumer(startA, copiedA)
			readA = true
		}
		if releaseDone == nil {
			return
		}
		select {
		case <-releaseDone:
			return
		default:
		}
		if !releasedA {
			// CAS unblocks B without waiting for A's old start or reversing B's publication.
			queue.consumerPublication.published.CompareAndSwap(startA, startB)
		}
		publication := queue.consumerPublication.published.Load()
		if publication != startB && publication != startB+countB {
			completed := false
			select {
			case <-releaseDone:
				completed = true
			default:
			}
			t.Errorf("cleanup publication=%d, B complete=%t", publication, completed)
		}
		timer := time.NewTimer(spmcWorkerTimeout)
		defer timer.Stop()
		select {
		case <-releaseDone:
		case <-timer.C:
			t.Errorf(
				"cleanup B join timed out: publication=%d, B complete=false",
				queue.consumerPublication.published.Load(),
			)
		}
	}()
	if readEarly {
		queue.spmcReadConsumer(startA, copiedA)
		readA = true
	}
	queue.spmcReadConsumer(startB, copiedB)
	if !slices.Equal(copiedB, items[2:]) {
		t.Fatalf("B copied %v, want %v", copiedB, items[2:])
	}
	var zero T
	for index := range items {
		want := items[index]
		if !keep && (index >= 2 || readEarly) {
			want = zero
		}
		if got := queue.producer.entries[(position+uint32(index))&3]; got != want {
			t.Fatalf("slot %d = %v, want %v", index, got, want)
		}
	}
	entering := make(chan struct{})
	releaseDone = make(chan struct{})
	go spmcReleaseForTest(queue, startB, countB, entering, releaseDone)
	awaitSPMCSignal(t, entering)
	awaitSPMCReleaseWait(t, releaseDone)
	if queue.consumerPublication.published.Load() != position || queue.Count() != 4 {
		t.Fatal("later release changed publication or excluded live intervals from Count")
	}
	if err := queue.Enqueue(items[0]); err != ErrFull {
		t.Fatalf("producer reused unreleased slots: %v", err)
	}
	if !readA {
		queue.spmcReadConsumer(startA, copiedA)
		readA = true
	}
	if !slices.Equal(copiedA, items[:2]) {
		t.Fatalf("A copied %v, want %v", copiedA, items[:2])
	}
	queue.spmcReleaseConsumer(startA, countA)
	releasedA = true
	awaitSPMCSignal(t, releaseDone)
	replacement := slices.Clone(items)
	slices.Reverse(replacement)
	if _, err := queue.EnqueueBulk(replacement); err != nil {
		t.Fatal(err)
	}
	reused := make([]T, 4)
	if _, err := queue.DequeueBulk(reused); err != nil || !slices.Equal(reused, replacement) {
		t.Fatalf("reused slots = %v, error %v", reused, err)
	}
}

func TestSPMCReleaseWaitYields(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	queue := newSPMCForTest[int](t, 2, WithSpinsBeforeYield(0))
	if _, err := queue.EnqueueBulk([]int{1, 2}); err != nil {
		t.Fatal(err)
	}
	startA, countA, _ := queue.spmcReserveConsumer(1, true, false)
	startB, countB, _ := queue.spmcReserveConsumer(1, true, false)
	queue.spmcReadConsumer(startA, make([]int, 1))
	queue.spmcReadConsumer(startB, make([]int, 1))
	entering := make(chan struct{})
	done := make(chan struct{})
	releasedA := false
	defer func() {
		select {
		case <-done:
			return
		default:
		}
		if !releasedA {
			// CAS unblocks B without waiting for A's old start or reversing B's publication.
			queue.consumerPublication.published.CompareAndSwap(startA, startB)
		}
		publication := queue.consumerPublication.published.Load()
		if publication != startB && publication != startB+countB {
			completed := false
			select {
			case <-done:
				completed = true
			default:
			}
			t.Errorf("cleanup publication=%d, B complete=%t", publication, completed)
		}
		timer := time.NewTimer(spmcWorkerTimeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			t.Errorf(
				"cleanup B join timed out: publication=%d, B complete=false",
				queue.consumerPublication.published.Load(),
			)
		}
	}()
	go spmcReleaseForTest(queue, startB, countB, entering, done)
	awaitSPMCSignal(t, entering)
	awaitSPMCReleaseWait(t, done)
	progress := make(chan struct{})
	go func() { close(progress) }()
	awaitSPMCSignal(t, progress)
	if queue.consumerPublication.published.Load() != startA {
		t.Fatal("publication advanced while earlier release remained paused")
	}
	select {
	case <-done:
		t.Fatal("B completed before A release")
	default:
	}
	queue.spmcReleaseConsumer(startA, countA)
	releasedA = true
	awaitSPMCSignal(t, done)
}

func TestSPMCFailedAttemptRecomputesAdmission(t *testing.T) {
	for _, exact := range []bool{false, true} {
		for _, fixed := range []bool{false, true} {
			for _, consumeA := range []uint32{1, 3} {
				name := fmt.Sprintf("exact=%t/fixed=%t/consumeA=%d", exact, fixed, consumeA)
				t.Run(name, func(t *testing.T) {
					queue := newSPMCForTest[int](t, 4)
					seedSPMC(queue, math.MaxUint32-1)
					if _, err := queue.EnqueueBulk([]int{10, 11, 12, 13}); err != nil {
						t.Fatal(err)
					}
					head := queue.spmcConsumerHeadPosition()
					startA, countA, _, retry := queue.spmcAttemptConsumer(head, consumeA, true, exact)
					if retry || countA != consumeA || startA != head {
						t.Fatal("first whole attempt did not reserve A")
					}
					copiedA := make([]int, countA)
					queue.spmcReadConsumer(startA, copiedA)
					queue.spmcReleaseConsumer(startA, countA)
					_, countB, _, retry := queue.spmcAttemptConsumer(head, 2, fixed, exact)
					if !retry || countB != 0 {
						t.Fatalf("contended attempt count=%d retry=%t", countB, retry)
					}
					startB, countB, remaining := queue.spmcReserveConsumer(2, fixed, exact)
					wantCount, wantRemaining := uint32(2), uint32(1)
					if consumeA == 3 {
						wantCount, wantRemaining = 1, 0
						if fixed {
							wantCount, wantRemaining = 0, 1
						}
					}
					if startB != head+countA || countB != wantCount || remaining != wantRemaining {
						t.Fatalf("retry start/count/remaining = %d/%d/%d", startB, countB, remaining)
					}
					copiedB := []int{-1, -1}
					if countB > 0 {
						queue.spmcReadConsumer(startB, copiedB[:countB])
						queue.spmcReleaseConsumer(startB, countB)
					}
					for index := uint32(0); index < countB; index++ {
						if copiedB[index] != 10+int(countA+index) {
							t.Fatalf("B values %v", copiedB)
						}
					}
					for index := countB; index < 2; index++ {
						if copiedB[index] != -1 {
							t.Fatal("failed bulk or short burst changed destination suffix")
						}
					}
					for index, value := range copiedA {
						if value != 10+index {
							t.Fatalf("A values %v", copiedA)
						}
					}
					for queue.Count() > 0 {
						value, err := queue.Dequeue()
						want := 10 + int(countA+countB)
						if err != nil || value != want {
							t.Fatalf("remaining value=%d error=%v, want %d", value, err, want)
						}
						countB++
					}
				})
			}
		}
	}
}

func TestSPMCProducerOwnershipHandoffs(t *testing.T) {
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("RTS=%t", relaxed), func(t *testing.T) {
			for _, exact := range []bool{false, true} {
				for _, outgoing := range []string{"successful", "failed", "zero"} {
					t.Run(fmt.Sprintf("exact=%t/%s", exact, outgoing), func(t *testing.T) {
						options := []Option{WithSpinsBeforeYield(0)}
						if exact {
							options = append(options, WithExactBatchCounts())
						}
						queue := newSPMCForTest[ringConcurrentTag](
							t,
							4,
							spmcProtocolOptions(relaxed, options)...,
						)
						firstProduced, firstExpected := 2, 1
						if outgoing == "successful" {
							if err := queue.Enqueue(ringConcurrentTag{0, 1}); err != nil {
								t.Fatal(err)
							}
						} else {
							items := []ringConcurrentTag{{0, 1}, {0, 2}, {0, 3}, {0, 4}}
							if _, err := queue.EnqueueBulk(items); err != nil {
								t.Fatal(err)
							}
							if value, err := queue.Dequeue(); err != nil || value != (ringConcurrentTag{0, 1}) {
								t.Fatalf("prepare handoff dequeue=%v error=%v", value, err)
							}
							before := queue.producer.cachedConsumer
							if outgoing == "failed" {
								if _, err := queue.EnqueueBulk([]ringConcurrentTag{{0, 5}, {0, 6}}); err != ErrFull {
									t.Fatalf("failed outgoing admission: %v", err)
								}
							} else if _, err := queue.EnqueueBulk(nil); err != nil {
								t.Fatal(err)
							}
							if queue.producer.cachedConsumer == before {
								t.Fatal("outgoing operation did not change producer cache")
							}
							firstProduced, firstExpected = 5, 2
						}
						ownership := make(chan ringTestQueue[ringConcurrentTag], 1)
						ownership <- queue
						checkRingConcurrentTransfers(
							t,
							queue,
							ringConcurrentScenario{
								producerCount: 1, consumerCount: 4, lastSequence: 128,
								firstProduced: firstProduced, firstExpected: firstExpected,
								method: "single", ownership: ownership,
							},
						)
					})
				}
			}

		})
	}
}
