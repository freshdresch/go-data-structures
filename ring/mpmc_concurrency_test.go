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

const mpmcConcurrentTimeout = 10 * time.Second

type mpmcConcurrentMode struct {
	producer, consumer bool
}

func mpmcConcurrentModes() []mpmcConcurrentMode {
	return []mpmcConcurrentMode{{false, false}, {true, false}, {false, true}, {true, true}}
}

func (m mpmcConcurrentMode) String() string {
	return fmt.Sprintf("producerRTS=%t/consumerRTS=%t", m.producer, m.consumer)
}

func mpmcConcurrentQueue[T any](
	t *testing.T,
	capacity int,
	mode mpmcConcurrentMode,
	options ...Option,
) *MPMCQueue[T] {
	t.Helper()
	selected := []Option{WithSpinsBeforeYield(0)}
	if mode.producer {
		selected = append(
			selected,
			WithRelaxedMultiProdEnqueue(),
			WithMaxProducerHeadTailDistance(capacity),
		)
	}
	if mode.consumer {
		selected = append(
			selected,
			WithRelaxedMultiConsDequeue(),
			WithMaxConsumerHeadTailDistance(capacity),
		)
	}
	selected = append(selected, options...)
	queue, err := NewMPMCQueue[T](capacity, selected...)
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

func mpmcConcurrentSeed[T any](queue *MPMCQueue[T], position, counter uint32) {
	queue.producerReservation.head.Store(position)
	queue.producerReservation.headRTS.Store(pack(position, counter))
	queue.producerReservation.cachedConsumer.Store(position)
	queue.producerPublication.tailRTS.Store(pack(position, counter))
	queue.producerPublication.published.Store(position)
	queue.consumerReservation.head.Store(position)
	queue.consumerReservation.headRTS.Store(pack(position, counter))
	queue.consumerReservation.cachedProducer.Store(position)
	queue.consumerPublication.tailRTS.Store(pack(position, counter))
	queue.consumerPublication.published.Store(position)
}

func mpmcConcurrentAwait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(mpmcConcurrentTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		t.Fatal("worker did not finish coordinated operation")
	}
}

func mpmcConcurrentWaitFrame(
	t *testing.T,
	done <-chan struct{},
	workerName, waitName string,
) {
	t.Helper()
	deadline := time.Now().Add(mpmcConcurrentTimeout)
	stack := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		length := runtime.Stack(stack, true)
		for _, worker := range strings.Split(string(stack[:length]), "\n\n") {
			if strings.Contains(worker, workerName) && strings.Contains(worker, waitName) {
				select {
				case <-done:
					t.Fatal("worker completed before blocked interval was closed")
				default:
				}
				return
			}
		}
		select {
		case <-done:
			t.Fatal("worker returned before entering expected production wait")
		default:
		}
		runtime.Gosched()
	}
	t.Fatalf("did not observe %s inside %s", workerName, waitName)
}

func mpmcConcurrentPublishWorker[T any](
	queue *MPMCQueue[T],
	start, count uint32,
	done chan<- struct{},
) {
	queue.mpmcPublishProducer(start, count)
	close(done)
}

func mpmcConcurrentReleaseWorker[T any](
	queue *MPMCQueue[T],
	start, count uint32,
	done chan<- struct{},
) {
	queue.mpmcReleaseConsumer(start, count)
	close(done)
}

func TestMPMCControlledProducerPublication(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	for _, mode := range mpmcConcurrentModes() {
		for _, position := range []uint32{3, math.MaxUint32 - 1} {
			t.Run(fmt.Sprintf("%s/start=%d", mode, position), func(t *testing.T) {
				queue := mpmcConcurrentQueue[int](t, 4, mode)
				mpmcConcurrentSeed(queue, position, 0)
				startA, countA, _ := queue.mpmcReserveProducer(2, true, false)
				startB, countB, _ := queue.mpmcReserveProducer(2, true, false)
				wroteA, publishedA := false, false
				var done chan struct{}
				t.Cleanup(func() {
					if !wroteA {
						queue.mpmcWriteProducer(startA, []int{11, 12}[:countA])
					}
					if !publishedA && countA != 0 {
						queue.mpmcPublishProducer(startA, countA)
					}
					if done != nil {
						mpmcConcurrentAwait(t, done)
					} else if countB != 0 {
						queue.mpmcWriteProducer(startB, []int{21, 22}[:countB])
						queue.mpmcPublishProducer(startB, countB)
					}
				})
				if startA != position || startB != position+2 || countA != 2 || countB != 2 {
					t.Fatal("producer reservations did not own adjacent intervals")
				}
				queue.mpmcWriteProducer(startB, []int{21, 22})
				done = make(chan struct{})
				go mpmcConcurrentPublishWorker(queue, startB, countB, done)
				if mode.producer {
					mpmcConcurrentAwait(t, done)
				} else {
					mpmcConcurrentWaitFrame(t, done, "mpmcConcurrentPublishWorker", "waitForTail")
				}
				if queue.producerPublication.published.Load() != position || queue.Count() != 0 {
					t.Fatal("later producer exposed an unwritten earlier interval")
				}
				if value, err := queue.Dequeue(); value != 0 || err != ErrEmpty {
					t.Fatalf("unpublished single value=%d error=%v", value, err)
				}
				destination := []int{-1, -1, -1, -1}
				if _, err := queue.DequeueBulk(destination); err != ErrEmpty {
					t.Fatalf("unpublished bulk error=%v", err)
				}
				if moved, _ := queue.DequeueBurst(destination); moved != 0 {
					t.Fatalf("unpublished burst moved=%d", moved)
				}
				if !slices.Equal(destination, []int{-1, -1, -1, -1}) {
					t.Fatal("unpublished read changed destination")
				}
				queue.mpmcWriteProducer(startA, []int{11, 12})
				wroteA = true
				queue.mpmcPublishProducer(startA, countA)
				publishedA = true
				mpmcConcurrentAwait(t, done)
				if _, err := queue.DequeueBulk(destination); err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(destination, []int{11, 12, 21, 22}) {
					t.Fatalf("reservation FIFO=%v", destination)
				}
			})
		}
	}
}

func TestMPMCControlledConsumerLifetime(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	for _, mode := range mpmcConcurrentModes() {
		for _, keep := range []bool{false, true} {
			for _, readEarly := range []bool{false, true} {
				for _, position := range []uint32{3, math.MaxUint32 - 1} {
					name := fmt.Sprintf("%s/keep=%t/readEarly=%t/start=%d", mode, keep, readEarly, position)
					t.Run(name, func(t *testing.T) {
						var options []Option
						if keep {
							options = append(options, WithoutDequeueClearing())
						}
						queue := mpmcConcurrentQueue[*int](t, 4, mode, options...)
						mpmcConcurrentSeed(queue, position, 0)
						values := []int{11, 12, 21, 22, 31, 32, 41, 42}
						items := []*int{&values[0], &values[1], &values[2], &values[3]}
						replacement := []*int{&values[4], &values[5], &values[6], &values[7]}
						if _, err := queue.EnqueueBulk(items); err != nil {
							t.Fatal(err)
						}
						startA, countA, _ := queue.mpmcReserveConsumer(2, true, false)
						startB, countB, _ := queue.mpmcReserveConsumer(2, true, false)
						destinationA, destinationB := make([]*int, 2), make([]*int, 2)
						readA, releasedA := false, false
						var done chan struct{}
						t.Cleanup(func() {
							if !readA {
								queue.mpmcReadConsumer(startA, destinationA[:countA])
							}
							if !releasedA && countA != 0 {
								queue.mpmcReleaseConsumer(startA, countA)
							}
							if done != nil {
								mpmcConcurrentAwait(t, done)
							} else if countB != 0 {
								queue.mpmcReadConsumer(startB, destinationB[:countB])
								queue.mpmcReleaseConsumer(startB, countB)
							}
						})
						if startA != position || startB != position+2 || countA != 2 || countB != 2 {
							t.Fatal("consumer reservations did not own adjacent intervals")
						}
						if readEarly {
							queue.mpmcReadConsumer(startA, destinationA)
							readA = true
						}
						queue.mpmcReadConsumer(startB, destinationB)
						done = make(chan struct{})
						go mpmcConcurrentReleaseWorker(queue, startB, countB, done)
						if mode.consumer {
							mpmcConcurrentAwait(t, done)
						} else {
							mpmcConcurrentWaitFrame(t, done, "mpmcConcurrentReleaseWorker", "waitForTail")
						}
						if queue.consumerPublication.published.Load() != position || queue.Count() != 4 {
							t.Fatal("later consumer released earlier live slots")
						}
						if err := queue.Enqueue(replacement[0]); err != ErrFull {
							t.Fatalf("premature single reuse error=%v", err)
						}
						if _, err := queue.EnqueueBulk(replacement); err != ErrFull {
							t.Fatalf("premature bulk reuse error=%v", err)
						}
						if moved, _ := queue.EnqueueBurst(replacement); moved != 0 {
							t.Fatalf("premature burst reuse moved=%d", moved)
						}
						for index, item := range items {
							want := item
							if !keep && (index >= 2 || readEarly) {
								want = nil
							}
							if got := queue.config.entries[(position+uint32(index))&3]; got != want {
								t.Fatalf("live slot %d=%v, want %v", index, got, want)
							}
						}
						if !readA {
							queue.mpmcReadConsumer(startA, destinationA)
							readA = true
						}
						queue.mpmcReleaseConsumer(startA, countA)
						releasedA = true
						mpmcConcurrentAwait(t, done)
						if !slices.Equal(destinationA, items[:2]) || !slices.Equal(destinationB, items[2:]) {
							t.Fatal("consumer interval contents differ from reservation FIFO")
						}
						if _, err := queue.EnqueueBulk(replacement); err != nil {
							t.Fatal(err)
						}
						copied := make([]*int, 4)
						if _, err := queue.DequeueBulk(copied); err != nil {
							t.Fatal(err)
						}
						if !slices.Equal(copied, replacement) {
							t.Fatal("late clearing corrupted reused slots")
						}
					})
				}
			}
		}
	}
}

func TestMPMCOrderedCompletionSpinCallChain(t *testing.T) {
	previous := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(previous)
	for _, producer := range []bool{false, true} {
		t.Run(fmt.Sprintf("producer=%t", producer), func(t *testing.T) {
			queue := mpmcConcurrentQueue[int](
				t,
				2,
				mpmcConcurrentMode{},
				WithSpinsBeforeYield(math.MaxInt32),
			)
			if !producer {
				if _, err := queue.EnqueueBulk([]int{1, 2}); err != nil {
					t.Fatal(err)
				}
			}
			startA, countA, _ := mpmcRTSReserve(queue, producer, 1, true, true)
			startB, countB, _ := mpmcRTSReserve(queue, producer, 1, true, true)
			if producer {
				queue.mpmcWriteProducer(startA, []int{1})
				queue.mpmcWriteProducer(startB, []int{2})
			} else {
				queue.mpmcReadConsumer(startA, make([]int, countA))
				queue.mpmcReadConsumer(startB, make([]int, countB))
			}
			completedA := false
			done := make(chan struct{})
			t.Cleanup(func() {
				if !completedA {
					if producer {
						queue.mpmcPublishProducer(startA, countA)
					} else {
						queue.mpmcReleaseConsumer(startA, countA)
					}
				}
				mpmcConcurrentAwait(t, done)
			})
			if producer {
				go mpmcConcurrentPublishWorker(queue, startB, countB, done)
				mpmcConcurrentWaitFrame(t, done, "mpmcConcurrentPublishWorker", "spinForTail")
				if queue.producerPublication.published.Load() != startA {
					t.Fatal("producer spin exposed pending earlier interval")
				}
				queue.mpmcPublishProducer(startA, countA)
			} else {
				go mpmcConcurrentReleaseWorker(queue, startB, countB, done)
				mpmcConcurrentWaitFrame(t, done, "mpmcConcurrentReleaseWorker", "spinForTail")
				if queue.consumerPublication.published.Load() != startA {
					t.Fatal("consumer spin released pending earlier interval")
				}
				queue.mpmcReleaseConsumer(startA, countA)
			}
			completedA = true
			mpmcConcurrentAwait(t, done)
		})
	}
}
