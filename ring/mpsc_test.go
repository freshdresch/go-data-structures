package ring

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
)

func newMPSCForTest[T any](t *testing.T, capacity int, options ...Option) *MPSCQueue[T] {
	t.Helper()
	queue, err := NewMPSCQueue[T](capacity, options...)
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

// seedMPSC requires exclusive synchronized access to an empty queue, as Reset does.
func seedMPSC[T any](queue *MPSCQueue[T], position uint32) {
	queue.producerReservation.head.Store(position)
	queue.producerReservation.headRTS.Store(pack(position, 0))
	queue.producerPublication.tailRTS.Store(pack(position, 0))
	queue.producerReservation.cachedConsumer.Store(position)
	queue.producerPublication.published.Store(position)
	queue.consumer.cursor = position
	queue.consumer.cachedProducer = position
	queue.consumerPublished.Store(position)
}

func TestMPSCConstruction(t *testing.T) {
	queue := newMPSCForTest[int](t, 8)
	if !queue.consumer.clearOnDequeue {
		t.Fatal("initial flags")
	}
	if queue.producerConfig.exactBatchCounts || queue.consumer.exactBatchCounts {
		t.Fatal("default exact counts")
	}
	if queue.producerConfig.spinsBeforeYield != defaultSpinsBeforeYield {
		t.Fatal("default spins")
	}
	if &queue.producerConfig.entries[0] != &queue.consumer.entries[0] {
		t.Fatal("entries do not share storage")
	}
	configured := newMPSCForTest[int](
		t,
		8,
		WithMaxBatch(4),
		WithMaxEnqueueBatch(2),
		WithMaxDequeueBatch(3),
		WithoutDequeueClearing(),
		WithExactBatchCounts(),
		WithSpinsBeforeYield(0),
	)
	if configured.consumer.clearOnDequeue || configured.producerConfig.spinsBeforeYield != 0 {
		t.Fatal("configured clearing/spins")
	}
	if !configured.producerConfig.exactBatchCounts || !configured.consumer.exactBatchCounts {
		t.Fatal("configured exact counts")
	}
}

func TestMPSCConstructorValidation(t *testing.T) {
	for _, spins := range []int{0, 1, defaultSpinsBeforeYield, math.MaxInt32} {
		queue := newMPSCForTest[int](t, 1, WithSpinsBeforeYield(spins))
		if queue.producerConfig.spinsBeforeYield != uint32(spins) {
			t.Fatalf("spins=%d got=%d", spins, queue.producerConfig.spinsBeforeYield)
		}
	}
	queue := newMPSCForTest[int](
		t,
		1,
		WithSpinsBeforeYield(-1),
		WithSpinsBeforeYield(1),
	)
	if queue.producerConfig.spinsBeforeYield != 1 {
		t.Fatal("later spin option did not override invalid value")
	}
}

func enqueueMPSCValues(t *testing.T, queue *MPSCQueue[int], values ...int) {
	t.Helper()
	for _, value := range values {
		if err := queue.Enqueue(value); err != nil {
			t.Fatal(err)
		}
	}
}

func dequeueMPSCValues(t *testing.T, queue *MPSCQueue[int], values ...int) {
	t.Helper()
	for _, want := range values {
		if value, err := queue.Dequeue(); value != want || err != nil {
			t.Fatalf("dequeue value=%d want=%d error=%v", value, want, err)
		}
	}
}

func TestMPSCLengthOneCounts(t *testing.T) {
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
			for _, exact := range []bool{false, true} {
				for _, retain := range []bool{false, true} {
					for _, method := range []string{"bulk", "burst", "clamped-burst", "single"} {
						name := fmt.Sprintf("exact=%t/retain=%t/%s", exact, retain, method)
						t.Run(name, func(t *testing.T) {
							options := []Option{WithMaxBatch(1)}
							if exact {
								options = append(options, WithExactBatchCounts())
							}
							if retain {
								options = append(options, WithoutDequeueClearing())
							}
							queue := newMPSCProtocolForTest[int](t, relaxed, 8, options...)
							enqueueMPSCValues(t, queue, 10, 11, 12, 13)
							dequeueMPSCValues(t, queue, 10, 11)
							wantFree, wantProducerCache := 3, uint32(0)
							if exact && method != "single" {
								wantFree, wantProducerCache = 5, 2
							}
							items := []int{14}
							if method == "clamped-burst" {
								items = []int{14, 99, 100}
							}
							switch method {
							case "bulk":
								if free, err := queue.EnqueueBulk(items); free != wantFree || err != nil {
									t.Fatalf("enqueue free=%d want=%d error=%v", free, wantFree, err)
								}
							case "single":
								enqueueMPSCValues(t, queue, 14)
							default:
								moved, free := queue.EnqueueBurst(items)
								if moved != 1 || free != wantFree {
									t.Fatalf("enqueue moved=%d free=%d want=%d", moved, free, wantFree)
								}
							}
							if queue.producerReservation.cachedConsumer.Load() != wantProducerCache {
								t.Fatalf(
									"producer cache=%d want=%d",
									queue.producerReservation.cachedConsumer.Load(), wantProducerCache,
								)
							}
							wantLeft, wantConsumerCache := 1, uint32(4)
							if exact && method != "single" {
								wantLeft, wantConsumerCache = 2, 5
							}
							dst := []int{-1}
							if method == "clamped-burst" {
								dst = []int{-1, -1, -1}
							}
							switch method {
							case "bulk":
								if left, err := queue.DequeueBulk(dst); left != wantLeft || err != nil {
									t.Fatalf("dequeue left=%d want=%d error=%v", left, wantLeft, err)
								}
							case "single":
								value, err := queue.Dequeue()
								if value != 12 || err != nil {
									t.Fatalf("single value=%d error=%v", value, err)
								}
								dst[0] = value
							default:
								moved, left := queue.DequeueBurst(dst)
								if moved != 1 || left != wantLeft {
									t.Fatalf("dequeue moved=%d left=%d want=%d", moved, left, wantLeft)
								}
							}
							wantDst := []int{12}
							if method == "clamped-burst" {
								wantDst = []int{12, -1, -1}
							}
							if !slices.Equal(dst, wantDst) ||
								queue.consumer.cachedProducer != wantConsumerCache {
								t.Fatalf(
									"dst=%v consumer cache=%d want=%d",
									dst, queue.consumer.cachedProducer, wantConsumerCache,
								)
							}
							wantSlot := 0
							if retain {
								wantSlot = 12
							}
							if queue.producerConfig.entries[2] != wantSlot {
								t.Fatalf("consumed slot=%d want=%d", queue.producerConfig.entries[2], wantSlot)
							}
							dequeueMPSCValues(t, queue, 13, 14)
						})
					}
				}
			}

		})
	}
}

func TestMPSCAdmissionRefreshCounts(t *testing.T) {
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
			for _, exact := range []bool{false, true} {
				for _, direction := range []string{"enqueue", "dequeue"} {
					for _, test := range []struct {
						name                string
						request             int
						burst               bool
						wantMoved, wantLeft int
						wantError           error
					}{
						{"success", 2, false, 2, 1, nil},
						{"failed-bulk", 4, false, 0, 3, ErrFull},
						{"partial-burst", 4, true, 3, 0, nil},
					} {
						name := fmt.Sprintf("exact=%t/%s/%s", exact, direction, test.name)
						t.Run(name, func(t *testing.T) {
							var options []Option
							if exact {
								options = append(options, WithExactBatchCounts())
							}
							queue := newMPSCProtocolForTest[int](t, relaxed, 4, options...)
							enqueueMPSCValues(t, queue, 10, 11, 12)
							dequeueMPSCValues(t, queue, 10, 11)
							if direction == "enqueue" {
								items := []int{13, 14, 15, 16}
								slots := slices.Clone(queue.producerConfig.entries)
								head := queue.mpscProducerHead()
								published := queue.producerPublication.published.Load()
								if test.burst {
									moved, left := queue.EnqueueBurst(items[:test.request])
									if moved != test.wantMoved || left != test.wantLeft {
										t.Fatalf("moved=%d left=%d", moved, left)
									}
								} else {
									left, err := queue.EnqueueBulk(items[:test.request])
									if left != test.wantLeft || err != test.wantError {
										t.Fatalf("left=%d error=%v", left, err)
									}
								}
								if test.name == "failed-bulk" &&
									(!slices.Equal(slots, queue.producerConfig.entries) ||
										queue.mpscProducerHead() != head ||
										queue.producerPublication.published.Load() != published ||
										queue.consumer.cursor != 2 || queue.consumerPublished.Load() != 2) {
									t.Fatal("failed enqueue changed slots, positions, or publications")
								}
								if queue.producerReservation.cachedConsumer.Load() != 2 {
									t.Fatalf("producer cache=%d want 2", queue.producerReservation.cachedConsumer.Load())
								}
								want := []int{12, 13, 14}
								if test.name == "failed-bulk" {
									want = []int{12}
								}
								if test.burst {
									want = []int{12, 13, 14, 15}
								}
								dequeueMPSCValues(t, queue, want...)
								return
							}
							enqueueMPSCValues(t, queue, 13, 14)
							dst := []int{-1, -1, -1, -1}
							slots := slices.Clone(queue.producerConfig.entries)
							if test.burst {
								moved, left := queue.DequeueBurst(dst[:test.request])
								if moved != test.wantMoved || left != test.wantLeft {
									t.Fatalf("moved=%d left=%d", moved, left)
								}
							} else {
								wantError := test.wantError
								if wantError == ErrFull {
									wantError = ErrEmpty
								}
								left, err := queue.DequeueBulk(dst[:test.request])
								if left != test.wantLeft || err != wantError {
									t.Fatalf("left=%d error=%v", left, err)
								}
							}
							if test.name == "failed-bulk" &&
								(!slices.Equal(slots, queue.producerConfig.entries) ||
									queue.consumer.cursor != 2 || queue.consumerPublished.Load() != 2 ||
									queue.mpscProducerHead() != 5 || queue.producerPublication.published.Load() != 5) {
								t.Fatal("failed dequeue changed slots, positions, or publications")
							}
							want := []int{12, 13, -1, -1}
							if test.name == "failed-bulk" {
								want = []int{-1, -1, -1, -1}
							}
							if test.burst {
								want = []int{12, 13, 14, -1}
							}
							if !slices.Equal(dst, want) || queue.consumer.cachedProducer != 5 {
								t.Fatalf("dst=%v cache=%d", dst, queue.consumer.cachedProducer)
							}
						})
					}
				}
			}

		})
	}
}

func TestMPSCInvalidLengthsAndZeroSnapshots(t *testing.T) {
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
			for _, exact := range []bool{false, true} {
				for _, direction := range []string{"enqueue", "dequeue"} {
					for _, method := range []string{
						"invalid-capacity", "invalid-maximum", "zero-bulk", "zero-burst",
					} {
						name := fmt.Sprintf("exact=%t/%s/%s", exact, direction, method)
						t.Run(name, func(t *testing.T) {
							options := []Option{WithMaxBatch(2)}
							if exact {
								options = append(options, WithExactBatchCounts())
							}
							queue := newMPSCProtocolForTest[int](t, relaxed, 4, options...)
							enqueueMPSCValues(t, queue, 10, 11, 12)
							dequeueMPSCValues(t, queue, 10, 11)
							enqueueMPSCValues(t, queue, 13)
							slots := slices.Clone(queue.producerConfig.entries)
							producerCache := queue.producerReservation.cachedConsumer.Load()
							consumerCache := queue.consumer.cachedProducer
							cursor := queue.consumer.cursor
							head := queue.mpscProducerHead()
							producerPublished := queue.producerPublication.published.Load()
							consumerPublished := queue.consumerPublished.Load()
							length := 0
							var wantError error
							switch method {
							case "invalid-capacity":
								length, wantError = 5, ErrExceedsCapacity
							case "invalid-maximum":
								length, wantError = 3, ErrExceedsMaxBatch
							}
							dst := []int{-1, -1, -1, -1, -1}
							var left int
							var err error
							if direction == "enqueue" {
								if method == "zero-burst" {
									moved, free := queue.EnqueueBurst(dst[:0])
									left = free
									if moved != 0 {
										t.Fatalf("zero enqueue moved=%d", moved)
									}
								} else {
									left, err = queue.EnqueueBulk(dst[:length])
								}
							} else if method == "zero-burst" {
								moved, available := queue.DequeueBurst(dst[:0])
								left = available
								if moved != 0 {
									t.Fatalf("zero dequeue moved=%d", moved)
								}
							} else {
								left, err = queue.DequeueBulk(dst[:length])
							}
							if left != 2 || !errors.Is(err, wantError) {
								t.Fatalf("left=%d error=%v want=%v", left, err, wantError)
							}
							if wantError != nil && err == wantError {
								t.Fatalf("length error was not wrapped: %v", err)
							}
							if !slices.Equal(dst, []int{-1, -1, -1, -1, -1}) ||
								!slices.Equal(queue.producerConfig.entries, slots) {
								t.Fatalf("destination or slots changed: dst=%v slots=%v", dst, queue.producerConfig.entries)
							}
							if method == "zero-bulk" || method == "zero-burst" {
								if direction == "enqueue" {
									producerCache = 2
								} else {
									consumerCache = 4
								}
							}
							if queue.producerReservation.cachedConsumer.Load() != producerCache ||
								queue.consumer.cachedProducer != consumerCache {
								t.Fatalf(
									"caches=(%d,%d) want=(%d,%d)",
									queue.producerReservation.cachedConsumer.Load(),
									queue.consumer.cachedProducer,
									producerCache,
									consumerCache,
								)
							}
							if queue.consumer.cursor != cursor || queue.mpscProducerHead() != head ||
								queue.producerPublication.published.Load() != producerPublished ||
								queue.consumerPublished.Load() != consumerPublished {
								t.Fatal("zero or invalid call changed positions or publications")
							}
						})
					}
				}
			}

		})
	}
}

func TestMPSCPositionRollover(t *testing.T) {
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
			for _, exact := range []bool{false, true} {
				var options []Option
				if exact {
					options = append(options, WithExactBatchCounts())
				}
				t.Run(fmt.Sprintf("exact=%t", exact), func(t *testing.T) {
					queue := newMPSCProtocolForTest[int](t, relaxed, 4, options...)
					seedMPSC(queue, math.MaxUint32-2)
					enqueueMPSCValues(t, queue, 10)
					if free, err := queue.EnqueueBulk([]int{11, 12}); free != 1 || err != nil {
						t.Fatalf("wrapped bulk free=%d error=%v", free, err)
					}
					if moved, free := queue.EnqueueBurst([]int{13, 99}); moved != 1 || free != 0 {
						t.Fatalf("wrapped burst moved=%d free=%d", moved, free)
					}
					if !queue.Full() || queue.Count() != 4 || queue.mpscProducerHead() != 1 {
						t.Fatalf(
							"wrapped full count=%d head=%d",
							queue.Count(), queue.mpscProducerHead(),
						)
					}
					if err := queue.Enqueue(99); err != ErrFull {
						t.Fatalf("wrapped full error=%v", err)
					}
					dequeueMPSCValues(t, queue, 10)
					dst := []int{-1, -1}
					if left, err := queue.DequeueBulk(dst); left != 1 || err != nil ||
						!slices.Equal(dst, []int{11, 12}) {
						t.Fatalf("wrapped bulk dst=%v left=%d error=%v", dst, left, err)
					}
					dst = []int{-1, -1, -1}
					if moved, left := queue.DequeueBurst(dst); moved != 1 || left != 0 ||
						!slices.Equal(dst, []int{13, -1, -1}) {
						t.Fatalf("wrapped burst dst=%v moved=%d left=%d", dst, moved, left)
					}
					if !queue.Empty() || queue.consumerPublished.Load() != 1 {
						t.Fatalf("wrapped empty publication=%d", queue.consumerPublished.Load())
					}
					if free, err := queue.EnqueueBulk([]int{20, 21, 22, 23}); free != 0 || err != nil {
						t.Fatalf("exact-fit free=%d error=%v", free, err)
					}
					dequeueMPSCValues(t, queue, 20, 21, 22, 23)
					one := newMPSCProtocolForTest[int](t, relaxed, 1, options...)
					seedMPSC(one, math.MaxUint32)
					if free, err := one.EnqueueBulk([]int{30}); free != 0 || err != nil {
						t.Fatalf("capacity-one bulk free=%d error=%v", free, err)
					}
					if one.mpscProducerHead() != 0 || !one.Full() {
						t.Fatal("capacity-one enqueue did not cross rollover")
					}
					dequeueMPSCValues(t, one, 30)
					if moved, free := one.EnqueueBurst([]int{31, 99}); moved != 1 || free != 0 {
						t.Fatalf("capacity-one burst moved=%d free=%d", moved, free)
					}
					dst = []int{-1, -1}
					if moved, left := one.DequeueBurst(dst); moved != 1 || left != 0 ||
						!slices.Equal(dst, []int{31, -1}) || !one.Empty() {
						t.Fatalf("capacity-one dequeue dst=%v moved=%d left=%d", dst, moved, left)
					}
				})
			}

		})
	}
}

func TestMPSCPointerClearingRetentionAndReset(t *testing.T) {
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
			for _, retain := range []bool{false, true} {
				for _, exact := range []bool{false, true} {
					t.Run(fmt.Sprintf("retain=%t/exact=%t", retain, exact), func(t *testing.T) {
						options := []Option{WithMaxBatch(3), WithSpinsBeforeYield(1)}
						if retain {
							options = append(options, WithoutDequeueClearing())
						}
						if exact {
							options = append(options, WithExactBatchCounts())
						}
						queue := newMPSCProtocolForTest[*int](t, relaxed, 4, options...)
						seedMPSC(queue, math.MaxUint32-1)
						value := 42
						for index := range queue.producerConfig.entries {
							queue.producerConfig.entries[index] = &value
						}
						producer := queue.producerConfig
						consumer := queue.consumer
						queue.Reset()
						if !slices.Equal(queue.producerConfig.entries, []*int{nil, nil, nil, nil}) {
							t.Fatalf("Reset backing slots=%v", queue.producerConfig.entries)
						}
						if queue.consumer.cursor != 0 || queue.producerReservation.cachedConsumer.Load() != 0 ||
							queue.producerPublication.published.Load() != 0 || queue.mpscProducerHead() != 0 ||
							queue.producerReservation.head.Load() != 0 ||
							queue.producerReservation.headRTS.Load() != 0 ||
							queue.producerPublication.tailRTS.Load() != 0 ||
							queue.consumer.cachedProducer != 0 || queue.consumerPublished.Load() != 0 {
							t.Fatal("Reset left nonzero position or cache")
						}
						if &queue.producerConfig.entries[0] != &producer.entries[0] ||
							&queue.consumer.entries[0] != &consumer.entries[0] ||
							len(queue.producerConfig.entries) != 4 || cap(queue.producerConfig.entries) != 4 ||
							len(queue.consumer.entries) != 4 || cap(queue.consumer.entries) != 4 {
							t.Fatal("Reset changed backing storage or slice headers")
						}
						if queue.producerConfig.mask != producer.mask ||
							queue.producerConfig.maxBatch != producer.maxBatch ||
							queue.producerConfig.exactBatchCounts != producer.exactBatchCounts ||
							queue.consumer.mask != consumer.mask ||
							queue.consumer.maxBatch != consumer.maxBatch ||
							queue.producerConfig.spinsBeforeYield != producer.spinsBeforeYield ||
							queue.consumer.clearOnDequeue != consumer.clearOnDequeue ||
							queue.consumer.exactBatchCounts != consumer.exactBatchCounts {
							t.Fatal("Reset changed configuration")
						}
					})
				}
			}

		})
	}
}

func TestMPSCBackwardSharedCache(t *testing.T) {
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
			for _, exact := range []bool{false, true} {
				for _, burst := range []bool{false, true} {
					for _, test := range []struct {
						name                            string
						olderIndex, request, cachedFree int
					}{
						{"above-capacity", 0, 2, 2},
						{"below-request", 1, 2, 2},
						{"cached-full-admission", 2, 1, 1},
					} {
						name := fmt.Sprintf("exact=%t/burst=%t/%s", exact, burst, test.name)
						t.Run(name, func(t *testing.T) {
							var options []Option
							if exact {
								options = append(options, WithExactBatchCounts())
							}
							queue := newMPSCProtocolForTest[int](t, relaxed, 4, options...)
							first := queue.consumerPublished.Load()
							for value := 10; value < 13; value++ {
								enqueueMPSCValues(t, queue, value)
								dequeueMPSCValues(t, queue, value)
							}
							second := queue.consumerPublished.Load()
							enqueueMPSCValues(t, queue, 13)
							dequeueMPSCValues(t, queue, 13)
							third := queue.consumerPublished.Load()
							for value := 14; value < 16; value++ {
								enqueueMPSCValues(t, queue, value)
								dequeueMPSCValues(t, queue, value)
							}
							if free, err := queue.EnqueueBulk(nil); free != 4 || err != nil {
								t.Fatalf("refresh free=%d error=%v", free, err)
							}
							older := []uint32{first, second, third}[test.olderIndex]
							// Exclusive access models a delayed Store of an actual older publication.
							queue.producerReservation.cachedConsumer.Store(older)
							items := []int{20, 21}
							wantFree := test.cachedFree
							wantCache := uint32(6)
							if test.name == "cached-full-admission" {
								if exact {
									wantFree = 3
								} else {
									wantCache = older
								}
							}
							if burst {
								moved, free := queue.EnqueueBurst(items[:test.request])
								if moved != test.request || free != wantFree {
									t.Fatalf("moved=%d free=%d want=%d", moved, free, wantFree)
								}
							} else {
								free, err := queue.EnqueueBulk(items[:test.request])
								if free != wantFree || err != nil {
									t.Fatalf("free=%d want=%d error=%v", free, wantFree, err)
								}
							}
							if cached := queue.producerReservation.cachedConsumer.Load(); cached != wantCache {
								t.Fatalf("cache=%d want=%d", cached, wantCache)
							}
							dequeueMPSCValues(t, queue, items[:test.request]...)
							if value, err := queue.Dequeue(); value != 0 || err != ErrEmpty {
								t.Fatalf("final value=%d error=%v", value, err)
							}
						})
					}
				}
			}

		})
	}
}

func TestMPSCStaleHeadRetry(t *testing.T) {
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
			type snapshot struct {
				slots                                        [2]int
				head                                         uint64
				cursor, producerPublished, consumerPublished uint32
			}
			capture := func(queue *MPSCQueue[int]) snapshot {
				return snapshot{
					slots:             [2]int{queue.producerConfig.entries[0], queue.producerConfig.entries[1]},
					head:              mpscHeadWordForTest(queue),
					cursor:            queue.consumer.cursor,
					producerPublished: queue.producerPublication.published.Load(),
					consumerPublished: queue.consumerPublished.Load(),
				}
			}
			for _, exact := range []bool{false, true} {
				for _, fixed := range []bool{false, true} {
					for _, zero := range []bool{false, true} {
						name := fmt.Sprintf("exact=%t/fixed=%t/zero=%t", exact, fixed, zero)
						t.Run(name, func(t *testing.T) {
							queue := newMPSCProtocolForTest[int](t, relaxed, 2)
							capturedHead := mpscHeadWordForTest(queue)
							enqueueMPSCValues(t, queue, 10)
							dequeueMPSCValues(t, queue, 10)
							before := capture(queue)
							if free, err := queue.EnqueueBulk(nil); free != 2 || err != nil {
								t.Fatalf("refresh free=%d error=%v", free, err)
							}
							if capture(queue) != before || queue.producerReservation.cachedConsumer.Load() != 1 {
								t.Fatal("zero refresh changed positions, publications, or slots")
							}
							request := uint32(1)
							if zero {
								request = 0
							}
							start, count, free, retry := queue.mpscAttemptProducer(
								capturedHead, request, fixed, exact,
							)
							if start != 0 || count != 0 || free != 2 || retry == zero {
								t.Fatalf(
									"attempt start=%d count=%d free=%d retry=%t",
									start, count, free, retry,
								)
							}
							if capture(queue) != before {
								t.Fatal("stale attempt changed slots or positions")
							}
							if zero {
								if moved, free := queue.EnqueueBurst(nil); moved != 0 || free != 2 {
									t.Fatalf("zero burst moved=%d free=%d", moved, free)
								}
								if capture(queue) != before {
									t.Fatal("zero burst changed slots or positions")
								}
								return
							}
							start, count, free = queue.mpscReserveProducer(1, fixed, exact)
							if start != 1 || count != 1 || free != 1 {
								t.Fatalf("reservation start=%d count=%d free=%d", start, count, free)
							}
							queue.mpscWriteProducer(start, []int{11})
							queue.mpscPublishProducer(start, count)
							dequeueMPSCValues(t, queue, 11)
							if value, err := queue.Dequeue(); value != 0 || err != ErrEmpty {
								t.Fatalf("final value=%d error=%v", value, err)
							}
						})
					}
				}
			}

		})
	}
}

func TestMPSCObserversUsePublications(t *testing.T) {
	queue := newMPSCForTest[int](t, 4)
	start, count, free := queue.mpscReserveProducer(4, true, false)
	if start != 0 || count != 4 || free != 0 {
		t.Fatalf("reservation start=%d count=%d free=%d", start, count, free)
	}
	queue.mpscWriteProducer(start, []int{10, 11, 12, 13})
	if free, err := queue.EnqueueBulk(nil); free != 0 || err != nil {
		t.Fatalf("reservation free=%d error=%v", free, err)
	}
	queue.producerReservation.cachedConsumer.Store(math.MaxUint32)
	queue.consumer.cachedProducer = math.MaxUint32
	if queue.Count() != 0 || queue.Free() != 4 || queue.Full() || !queue.Empty() {
		t.Fatal("observers included copied unpublished producer reservations")
	}
	queue.mpscPublishProducer(start, count)
	if queue.Count() != 4 || queue.Free() != 0 || !queue.Full() || queue.Empty() {
		t.Fatal("observers ignored producer publication")
	}
	dequeueMPSCValues(t, queue, 10, 11, 12, 13)
	if queue.Count() != 0 || queue.Free() != 4 || queue.Full() || !queue.Empty() {
		t.Fatal("observers used stale caches after consumption")
	}
	// An exclusive observer fixture covers the bounded result of skewed publications.
	queue.consumerPublished.Store(5)
	queue.producerPublication.published.Store(4)
	queue.consumer.cursor = 100
	if queue.Count() != 4 || queue.Free() != 0 || !queue.Full() || queue.Empty() {
		t.Fatal("observers did not clamp a skewed publication snapshot")
	}
}

func TestMPSCConsumerRangeRefresh(t *testing.T) {
	for _, relaxed := range []bool{false, true} {
		t.Run(fmt.Sprintf("relaxed=%t", relaxed), func(t *testing.T) {
			for _, exact := range []bool{false, true} {
				for _, method := range []string{"single", "bulk", "burst", "zero-bulk", "zero-burst"} {
					name := fmt.Sprintf("exact=%t/%s", exact, method)
					t.Run(name, func(t *testing.T) {
						var options []Option
						if exact {
							options = append(options, WithExactBatchCounts())
						}
						queue := newMPSCProtocolForTest[int](t, relaxed, 4, options...)
						enqueueMPSCValues(t, queue, 10)
						older := queue.producerPublication.published.Load()
						dequeueMPSCValues(t, queue, 10)
						enqueueMPSCValues(t, queue, 11)
						dequeueMPSCValues(t, queue, 11)
						enqueueMPSCValues(t, queue, 12, 13)
						// This exclusive fixture tests the range guard with an older real publication.
						queue.consumer.cachedProducer = older
						before := slices.Clone(queue.producerConfig.entries)
						dst := []int{-1, -1, -1}
						switch method {
						case "single":
							value, err := queue.Dequeue()
							if value != 12 || err != nil {
								t.Fatalf("single value=%d error=%v", value, err)
							}
						case "bulk":
							if left, err := queue.DequeueBulk(dst[:1]); left != 1 || err != nil {
								t.Fatalf("bulk left=%d error=%v", left, err)
							}
						case "burst":
							if moved, left := queue.DequeueBurst(dst); moved != 2 || left != 0 {
								t.Fatalf("burst moved=%d left=%d", moved, left)
							}
						case "zero-bulk":
							if left, err := queue.DequeueBulk(dst[:0]); left != 2 || err != nil {
								t.Fatalf("zero bulk left=%d error=%v", left, err)
							}
						case "zero-burst":
							if moved, left := queue.DequeueBurst(dst[:0]); moved != 0 || left != 2 {
								t.Fatalf("zero burst moved=%d left=%d", moved, left)
							}
						}
						if queue.consumer.cachedProducer != 4 {
							t.Fatalf("consumer cache=%d want 4", queue.consumer.cachedProducer)
						}
						switch method {
						case "zero-bulk", "zero-burst":
							if !slices.Equal(dst, []int{-1, -1, -1}) ||
								!slices.Equal(before, queue.producerConfig.entries) ||
								queue.consumer.cursor != 2 || queue.consumerPublished.Load() != 2 ||
								queue.mpscProducerHead() != 4 || queue.producerPublication.published.Load() != 4 {
								t.Fatal("zero call changed slots, destination, positions, or publication")
							}
							dequeueMPSCValues(t, queue, 12, 13)
						case "single", "bulk":
							if method == "bulk" && !slices.Equal(dst, []int{12, -1, -1}) {
								t.Fatalf("bulk destination=%v", dst)
							}
							dequeueMPSCValues(t, queue, 13)
						case "burst":
							if !slices.Equal(dst, []int{12, 13, -1}) {
								t.Fatalf("burst destination=%v", dst)
							}
						}
					})
				}
			}

		})
	}
}

func newMPSCProtocolForTest[T any](
	t *testing.T,
	relaxed bool,
	capacity int,
	options ...Option,
) *MPSCQueue[T] {
	t.Helper()
	if relaxed {
		options = append(options, WithRelaxedMultiProdEnqueue())
	}
	return newMPSCForTest[T](t, capacity, options...)
}

func mpscHeadWordForTest[T any](queue *MPSCQueue[T]) uint64 {
	if queue.producerConfig.relaxed {
		return queue.producerReservation.headRTS.Load()
	}
	return uint64(queue.producerReservation.head.Load())
}
