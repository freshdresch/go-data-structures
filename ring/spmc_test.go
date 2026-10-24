package ring

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
)

func newSPMCForTest[T any](t *testing.T, capacity int, options ...SPMCOption) *SPMC[T] {
	t.Helper()
	queue, err := NewSPMC[T](capacity, options...)
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

// seedSPMC requires exclusive synchronized access to an empty queue, as Reset does.
func seedSPMC[T any](queue *SPMC[T], position uint32) {
	queue.producer.cursor = position
	queue.producer.cachedConsumer = position
	queue.producerPublished.Store(position)
	queue.consumerReservation.head.Store(position)
	queue.consumerReservation.cachedProducer.Store(position)
	queue.consumerPublished.Store(position)
}

func TestSPMCConstruction(t *testing.T) {
	queue := newSPMCForTest[int](t, 8)
	if queue.Capacity() != 8 || queue.Count() != 0 || queue.Free() != 8 {
		t.Fatal("initial capacity/count/free")
	}
	if queue.MaxEnqueueBatch() != 8 || queue.MaxDequeueBatch() != 8 {
		t.Fatal("default batch limits")
	}
	if !queue.Empty() || queue.Full() || !queue.consumerConfig.clearOnDequeue {
		t.Fatal("initial flags")
	}
	if queue.producer.exactBatchCounts || queue.consumerConfig.exactBatchCounts {
		t.Fatal("default exact counts")
	}
	if queue.consumerConfig.spinsBeforeYield != defaultSpinsBeforeYield {
		t.Fatal("default spins")
	}
	if &queue.producer.entries[0] != &queue.consumerConfig.entries[0] {
		t.Fatal("entries do not share storage")
	}
	configured := newSPMCForTest[int](
		t,
		8,
		WithSPMCMaxBatch(4),
		WithSPMCMaxEnqueueBatch(2),
		WithSPMCMaxDequeueBatch(3),
		WithoutSPMCDequeueClearing(),
		WithSPMCExactBatchCounts(),
		WithSPMCSpinsBeforeYield(0),
	)
	if configured.MaxEnqueueBatch() != 2 || configured.MaxDequeueBatch() != 3 {
		t.Fatal("configured batch limits")
	}
	if configured.consumerConfig.clearOnDequeue || configured.consumerConfig.spinsBeforeYield != 0 {
		t.Fatal("configured clearing/spins")
	}
	if !configured.producer.exactBatchCounts || !configured.consumerConfig.exactBatchCounts {
		t.Fatal("configured exact counts")
	}
}

func TestSPMCFullCapacityAndFIFO(t *testing.T) {
	for _, capacity := range []int{1, 2, 4, 8} {
		for _, exact := range []bool{false, true} {
			t.Run(fmt.Sprintf("capacity=%d/exact=%t", capacity, exact), func(t *testing.T) {
				var options []SPMCOption
				if exact {
					options = append(options, WithSPMCExactBatchCounts())
				}
				queue := newSPMCForTest[int](t, capacity, options...)
				seedSPMC(queue, uint32(capacity-1))
				for round := 0; round < 6; round++ {
					items := make([]int, capacity)
					for index := range items {
						items[index] = round*capacity + index + 1
					}
					switch round % 3 {
					case 0:
						for _, item := range items {
							if err := queue.Enqueue(item); err != nil {
								t.Fatal(err)
							}
						}
					case 1:
						if free, err := queue.EnqueueBulk(items); err != nil || free != 0 {
							t.Fatalf("bulk free=%d error=%v", free, err)
						}
					case 2:
						moved, free := queue.EnqueueBurst(items)
						if moved != capacity || free != 0 {
							t.Fatalf("burst moved=%d free=%d", moved, free)
						}
					}
					if !queue.Full() || queue.Empty() || queue.Count() != capacity || queue.Free() != 0 {
						t.Fatal("full query results")
					}
					if err := queue.Enqueue(-1); err != ErrFull {
						t.Fatalf("full error=%v", err)
					}
					if free, err := queue.EnqueueBulk(items); free != 0 || err != ErrFull {
						t.Fatalf("full bulk free=%d error=%v", free, err)
					}
					if moved, free := queue.EnqueueBurst(items); moved != 0 || free != 0 {
						t.Fatalf("full burst moved=%d free=%d", moved, free)
					}
					dst := make([]int, capacity)
					switch round % 3 {
					case 0:
						available, err := queue.DequeueBulk(dst)
						if err != nil || available != 0 {
							t.Fatalf("bulk available=%d error=%v", available, err)
						}
					case 1:
						moved, left := queue.DequeueBurst(dst)
						if moved != capacity || left != 0 {
							t.Fatalf("burst moved=%d available=%d", moved, left)
						}
					case 2:
						for index := range dst {
							value, err := queue.Dequeue()
							if err != nil {
								t.Fatal(err)
							}
							dst[index] = value
						}
					}
					if !slices.Equal(dst, items) {
						t.Fatalf("got %v want %v", dst, items)
					}
					if value, err := queue.Dequeue(); value != 0 || err != ErrEmpty {
						t.Fatalf("empty value=%d error=%v", value, err)
					}
					sentinels := slices.Clone(dst)
					available, err := queue.DequeueBulk(dst)
					if err != ErrEmpty || available != 0 || !slices.Equal(dst, sentinels) {
						t.Fatalf("empty bulk dst=%v available=%d error=%v", dst, available, err)
					}
					moved, left := queue.DequeueBurst(dst)
					if moved != 0 || left != 0 || !slices.Equal(dst, sentinels) {
						t.Fatalf("empty burst dst=%v moved=%d left=%d", dst, moved, left)
					}
					if !queue.Empty() || queue.Full() || queue.Count() != 0 || queue.Free() != capacity {
						t.Fatal("empty query results")
					}
				}
			})
		}
	}
}

func TestSPMCPartialAdmissionAndPhysicalWrap(t *testing.T) {
	for _, capacity := range []int{2, 4, 8} {
		for _, exact := range []bool{false, true} {
			t.Run(fmt.Sprintf("capacity=%d/exact=%t", capacity, exact), func(t *testing.T) {
				var options []SPMCOption
				if exact {
					options = append(options, WithSPMCExactBatchCounts())
				}
				queue := newSPMCForTest[int](t, capacity, options...)
				seedSPMC(queue, uint32(capacity-1))
				if err := queue.Enqueue(10); err != nil {
					t.Fatal(err)
				}
				items := make([]int, capacity)
				for index := range items {
					items[index] = index + 11
				}
				free, err := queue.EnqueueBulk(items)
				if free != capacity-1 || err != ErrFull || queue.Count() != 1 {
					t.Fatalf("failed bulk free=%d error=%v count=%d", free, err, queue.Count())
				}
				moved, free := queue.EnqueueBurst(items)
				if moved != capacity-1 || free != 0 {
					t.Fatalf("partial enqueue moved=%d free=%d", moved, free)
				}
				if value, err := queue.Dequeue(); value != 10 || err != nil {
					t.Fatalf("single value=%d error=%v", value, err)
				}
				dst := make([]int, capacity)
				for index := range dst {
					dst[index] = -1
				}
				before := slices.Clone(dst)
				available, err := queue.DequeueBulk(dst)
				if available != capacity-1 || err != ErrEmpty || !slices.Equal(dst, before) {
					t.Fatalf("failed bulk dst=%v available=%d error=%v", dst, available, err)
				}
				moved, available = queue.DequeueBurst(dst)
				if moved != capacity-1 || available != 0 {
					t.Fatalf("partial dequeue moved=%d available=%d", moved, available)
				}
				want := append(slices.Clone(items[:capacity-1]), -1)
				if !slices.Equal(dst, want) {
					t.Fatalf("partial dst=%v want=%v", dst, want)
				}
			})
		}
	}
}

func TestSPMCBurstDirectionalLimits(t *testing.T) {
	for _, exact := range []bool{false, true} {
		var options []SPMCOption
		options = append(options, WithSPMCMaxEnqueueBatch(1), WithSPMCMaxDequeueBatch(1))
		if exact {
			options = append(options, WithSPMCExactBatchCounts())
		}
		queue := newSPMCForTest[int](t, 4, options...)
		if moved, free := queue.EnqueueBurst([]int{10, 11, 12}); moved != 1 || free != 3 {
			t.Fatalf("clamped enqueue moved=%d free=%d", moved, free)
		}
		dst := []int{-1, -1, -1}
		if moved, left := queue.DequeueBurst(dst); moved != 1 || left != 0 {
			t.Fatalf("clamped dequeue moved=%d left=%d", moved, left)
		}
		if !slices.Equal(dst, []int{10, -1, -1}) {
			t.Fatalf("clamped dst=%v", dst)
		}
	}
}

func TestSPMCStaleHeadRetry(t *testing.T) {
	prepare := func(t *testing.T) (*SPMC[int], uint32) {
		t.Helper()
		queue := newSPMCForTest[int](t, 2)
		for _, value := range []int{10, 11} {
			if err := queue.Enqueue(value); err != nil {
				t.Fatal(err)
			}
		}
		capturedHead := queue.consumerReservation.head.Load()
		if capturedHead != 0 {
			t.Fatalf("captured head=%d want 0", capturedHead)
		}
		if value, err := queue.Dequeue(); value != 10 || err != nil {
			t.Fatalf("first dequeue value=%d error=%v", value, err)
		}
		if err := queue.Enqueue(12); err != nil {
			t.Fatal(err)
		}
		if available, err := queue.DequeueBulk(nil); available != 2 || err != nil {
			t.Fatalf("cache refresh available=%d error=%v", available, err)
		}
		if head := queue.consumerReservation.head.Load(); head != 1 {
			t.Fatalf("head after cache refresh=%d want 1", head)
		}
		if published := queue.consumerPublished.Load(); published != 1 {
			t.Fatalf("consumer publication after cache refresh=%d want 1", published)
		}
		if published := queue.producerPublished.Load(); published != 3 {
			t.Fatalf("producer publication after cache refresh=%d want 3", published)
		}
		if cached := queue.consumerReservation.cachedProducer.Load(); cached != 3 {
			t.Fatalf("cache after refresh=%d want 3", cached)
		}
		return queue, capturedHead
	}
	type snapshot struct {
		slots                                      [2]int
		head, producerPublished, consumerPublished uint32
	}
	capture := func(queue *SPMC[int]) snapshot {
		return snapshot{
			slots:             [2]int{queue.producer.entries[0], queue.producer.entries[1]},
			head:              queue.consumerReservation.head.Load(),
			producerPublished: queue.producerPublished.Load(),
			consumerPublished: queue.consumerPublished.Load(),
		}
	}
	cases := []struct {
		name         string
		fixed, exact bool
	}{
		{name: "cached-fixed", fixed: true},
		{name: "exact-fixed", fixed: true, exact: true},
		{name: "cached-burst"},
		{name: "exact-burst", exact: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Run("nonempty", func(t *testing.T) {
				queue, capturedHead := prepare(t)
				before := capture(queue)
				start, count, remaining, retry := queue.spmcAttemptConsumer(
					capturedHead, 1, test.fixed, test.exact,
				)
				if start != capturedHead || count != 0 || remaining > 2 {
					t.Fatalf("stale attempt start=%d count=%d remaining=%d", start, count, remaining)
				}
				if after := capture(queue); after != before {
					t.Fatalf("stale attempt changed state: before=%+v after=%+v", before, after)
				}
				if !retry {
					t.Fatalf("stale head=%d publication=3: retry=false want true", capturedHead)
				}
				start, count, remaining = queue.spmcReserveConsumer(1, test.fixed, test.exact)
				if start != 1 || count != 1 || remaining != 1 {
					t.Fatalf("fresh reservation start=%d count=%d remaining=%d", start, count, remaining)
				}
				dst := []int{-1}
				queue.spmcReadConsumer(start, dst)
				queue.spmcReleaseConsumer(start, count)
				if dst[0] != 11 {
					t.Fatalf("fresh reservation value=%d want 11", dst[0])
				}
				if value, err := queue.Dequeue(); value != 12 || err != nil {
					t.Fatalf("last dequeue value=%d error=%v", value, err)
				}
				if value, err := queue.Dequeue(); value != 0 || err != ErrEmpty {
					t.Fatalf("empty dequeue value=%d error=%v", value, err)
				}
			})
			t.Run("zero", func(t *testing.T) {
				queue, capturedHead := prepare(t)
				before := capture(queue)
				start, count, remaining, retry := queue.spmcAttemptConsumer(
					capturedHead, 0, test.fixed, test.exact,
				)
				if start != capturedHead || count != 0 || remaining != 2 || retry {
					t.Fatalf(
						"zero attempt start=%d count=%d remaining=%d retry=%t",
						start, count, remaining, retry,
					)
				}
				if after := capture(queue); after != before {
					t.Fatalf("zero attempt changed state: before=%+v after=%+v", before, after)
				}
				if available, err := queue.DequeueBulk(nil); available != 2 || err != nil {
					t.Fatalf("zero bulk available=%d error=%v", available, err)
				}
				if moved, available := queue.DequeueBurst(nil); moved != 0 || available != 2 {
					t.Fatalf("zero burst moved=%d available=%d", moved, available)
				}
				if after := capture(queue); after != before {
					t.Fatalf("zero batches changed state: before=%+v after=%+v", before, after)
				}
				for _, want := range []int{11, 12} {
					if value, err := queue.Dequeue(); value != want || err != nil {
						t.Fatalf("dequeue value=%d want=%d error=%v", value, want, err)
					}
				}
			})
		})
	}
}

func TestSPMCConstructorValidation(t *testing.T) {
	for _, count := range []int{-2, -1, 0, 3, 5, maxCount + 1, math.MaxInt} {
		called := false
		option := func(*spmcConfig) { called = true }
		queue, err := NewSPMC[int](count, option)
		if queue != nil || !errors.Is(err, ErrInvalidCount) || err == ErrInvalidCount || called {
			t.Fatalf("count=%d queue=%p error=%v option called=%t", count, queue, err, called)
		}
	}
	for _, option := range []func(int) SPMCOption{
		WithSPMCMaxBatch, WithSPMCMaxEnqueueBatch, WithSPMCMaxDequeueBatch,
	} {
		for _, limit := range []int{-1, 0, 5, math.MaxInt} {
			queue, err := NewSPMC[int](4, option(limit))
			if queue != nil || !errors.Is(err, ErrInvalidMaxBatch) || err == ErrInvalidMaxBatch {
				t.Fatalf("limit=%d queue=%p error=%v", limit, queue, err)
			}
		}
	}
	cases := []struct {
		name             string
		options          []SPMCOption
		enqueue, dequeue int
	}{
		{"directional-last", []SPMCOption{
			WithSPMCMaxBatch(4), WithSPMCMaxEnqueueBatch(2), WithSPMCMaxDequeueBatch(3),
		}, 2, 3},
		{"both-last", []SPMCOption{
			WithSPMCMaxEnqueueBatch(2), WithSPMCMaxDequeueBatch(3), WithSPMCMaxBatch(4),
		}, 4, 4},
		{"invalid-both-overridden", []SPMCOption{
			WithSPMCMaxBatch(0), WithSPMCMaxEnqueueBatch(2), WithSPMCMaxDequeueBatch(3),
		}, 2, 3},
		{"invalid-directions-overridden", []SPMCOption{
			WithSPMCMaxEnqueueBatch(-1), WithSPMCMaxDequeueBatch(9), WithSPMCMaxBatch(4),
		}, 4, 4},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			queue := newSPMCForTest[int](t, 8, test.options...)
			if queue.MaxEnqueueBatch() != test.enqueue || queue.MaxDequeueBatch() != test.dequeue {
				t.Fatalf("batch limits=(%d,%d)", queue.MaxEnqueueBatch(), queue.MaxDequeueBatch())
			}
		})
	}
	for _, test := range []struct {
		name    string
		options []SPMCOption
		want    error
		context string
	}{
		{"enqueue-first", []SPMCOption{
			WithSPMCMaxEnqueueBatch(0), WithSPMCMaxDequeueBatch(0), WithSPMCSpinsBeforeYield(-1),
		}, ErrInvalidMaxBatch, "max enqueue batch 0"},
		{"dequeue-second", []SPMCOption{
			WithSPMCMaxDequeueBatch(0), WithSPMCSpinsBeforeYield(-1),
		}, ErrInvalidMaxBatch, "max dequeue batch 0"},
		{"spins-last", []SPMCOption{WithSPMCSpinsBeforeYield(-1)},
			ErrInvalidSpinsBeforeYield, "spins before yield -1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue, err := NewSPMC[int](4, test.options...)
			if queue != nil || !errors.Is(err, test.want) || err == test.want {
				t.Fatalf("queue=%p error=%v want wrapped %v", queue, err, test.want)
			}
			if !strings.Contains(err.Error(), test.context) {
				t.Fatalf("error=%q want context %q", err, test.context)
			}
		})
	}
	for _, spins := range []int{0, 1, defaultSpinsBeforeYield, math.MaxInt32} {
		queue := newSPMCForTest[int](t, 1, WithSPMCSpinsBeforeYield(spins))
		if queue.consumerConfig.spinsBeforeYield != uint32(spins) {
			t.Fatalf("spins=%d got=%d", spins, queue.consumerConfig.spinsBeforeYield)
		}
	}
	invalidSpins := []int{-1}
	if uint64(^uint(0)) > math.MaxUint32 {
		above := uint64(math.MaxInt32) + 1
		invalidSpins = append(invalidSpins, int(above))
	}
	for _, spins := range invalidSpins {
		queue, err := NewSPMC[int](1, WithSPMCSpinsBeforeYield(spins))
		if queue != nil || !errors.Is(err, ErrInvalidSpinsBeforeYield) ||
			err == ErrInvalidSpinsBeforeYield {
			t.Fatalf("spins=%d queue=%p error=%v", spins, queue, err)
		}
	}
	queue := newSPMCForTest[int](
		t, 1, WithSPMCSpinsBeforeYield(-1), WithSPMCSpinsBeforeYield(1),
	)
	if queue.consumerConfig.spinsBeforeYield != 1 {
		t.Fatal("later spin option did not override invalid value")
	}
}

func enqueueSPMCValues(t *testing.T, queue *SPMC[int], values ...int) {
	t.Helper()
	for _, value := range values {
		if err := queue.Enqueue(value); err != nil {
			t.Fatal(err)
		}
	}
}

func dequeueSPMCValues(t *testing.T, queue *SPMC[int], values ...int) {
	t.Helper()
	for _, want := range values {
		if value, err := queue.Dequeue(); value != want || err != nil {
			t.Fatalf("dequeue value=%d want=%d error=%v", value, want, err)
		}
	}
}

func TestSPMCLengthOneCounts(t *testing.T) {
	for _, exact := range []bool{false, true} {
		for _, retain := range []bool{false, true} {
			for _, method := range []string{"bulk", "burst", "clamped-burst", "single"} {
				name := fmt.Sprintf("exact=%t/retain=%t/%s", exact, retain, method)
				t.Run(name, func(t *testing.T) {
					options := []SPMCOption{WithSPMCMaxBatch(1)}
					if exact {
						options = append(options, WithSPMCExactBatchCounts())
					}
					if retain {
						options = append(options, WithoutSPMCDequeueClearing())
					}
					queue := newSPMCForTest[int](t, 8, options...)
					enqueueSPMCValues(t, queue, 10, 11, 12, 13)
					dequeueSPMCValues(t, queue, 10, 11)
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
						enqueueSPMCValues(t, queue, 14)
					default:
						moved, free := queue.EnqueueBurst(items)
						if moved != 1 || free != wantFree {
							t.Fatalf("enqueue moved=%d free=%d want=%d", moved, free, wantFree)
						}
					}
					if queue.producer.cachedConsumer != wantProducerCache {
						t.Fatalf(
							"producer cache=%d want=%d",
							queue.producer.cachedConsumer, wantProducerCache,
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
						queue.consumerReservation.cachedProducer.Load() != wantConsumerCache {
						t.Fatalf(
							"dst=%v consumer cache=%d want=%d",
							dst, queue.consumerReservation.cachedProducer.Load(), wantConsumerCache,
						)
					}
					wantSlot := 0
					if retain {
						wantSlot = 12
					}
					if queue.producer.entries[2] != wantSlot {
						t.Fatalf("consumed slot=%d want=%d", queue.producer.entries[2], wantSlot)
					}
					dequeueSPMCValues(t, queue, 13, 14)
				})
			}
		}
	}
}

func TestSPMCAdmissionRefreshCounts(t *testing.T) {
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
					var options []SPMCOption
					if exact {
						options = append(options, WithSPMCExactBatchCounts())
					}
					queue := newSPMCForTest[int](t, 4, options...)
					enqueueSPMCValues(t, queue, 10, 11, 12)
					dequeueSPMCValues(t, queue, 10, 11)
					if direction == "enqueue" {
						items := []int{13, 14, 15, 16}
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
						if queue.producer.cachedConsumer != 2 {
							t.Fatalf("producer cache=%d want 2", queue.producer.cachedConsumer)
						}
						want := []int{12, 13, 14}
						if test.name == "failed-bulk" {
							want = []int{12}
						}
						if test.burst {
							want = []int{12, 13, 14, 15}
						}
						dequeueSPMCValues(t, queue, want...)
						return
					}
					enqueueSPMCValues(t, queue, 13, 14)
					dst := []int{-1, -1, -1, -1}
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
					want := []int{12, 13, -1, -1}
					if test.name == "failed-bulk" {
						want = []int{-1, -1, -1, -1}
					}
					if test.burst {
						want = []int{12, 13, 14, -1}
					}
					if !slices.Equal(dst, want) || queue.consumerReservation.cachedProducer.Load() != 5 {
						t.Fatalf("dst=%v cache=%d", dst, queue.consumerReservation.cachedProducer.Load())
					}
				})
			}
		}
	}
}

func TestSPMCInvalidLengthsAndZeroSnapshots(t *testing.T) {
	for _, exact := range []bool{false, true} {
		for _, direction := range []string{"enqueue", "dequeue"} {
			for _, method := range []string{
				"invalid-capacity", "invalid-maximum", "zero-bulk", "zero-burst",
			} {
				name := fmt.Sprintf("exact=%t/%s/%s", exact, direction, method)
				t.Run(name, func(t *testing.T) {
					options := []SPMCOption{WithSPMCMaxBatch(2)}
					if exact {
						options = append(options, WithSPMCExactBatchCounts())
					}
					queue := newSPMCForTest[int](t, 4, options...)
					enqueueSPMCValues(t, queue, 10, 11, 12)
					dequeueSPMCValues(t, queue, 10, 11)
					enqueueSPMCValues(t, queue, 13)
					slots := slices.Clone(queue.producer.entries)
					producerCache := queue.producer.cachedConsumer
					consumerCache := queue.consumerReservation.cachedProducer.Load()
					cursor := queue.producer.cursor
					head := queue.consumerReservation.head.Load()
					producerPublished := queue.producerPublished.Load()
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
						!slices.Equal(queue.producer.entries, slots) {
						t.Fatalf("destination or slots changed: dst=%v slots=%v", dst, queue.producer.entries)
					}
					if method == "zero-bulk" || method == "zero-burst" {
						if direction == "enqueue" {
							producerCache = 2
						} else {
							consumerCache = 4
						}
					}
					if queue.producer.cachedConsumer != producerCache ||
						queue.consumerReservation.cachedProducer.Load() != consumerCache {
						t.Fatalf(
							"caches=(%d,%d) want=(%d,%d)",
							queue.producer.cachedConsumer,
							queue.consumerReservation.cachedProducer.Load(),
							producerCache,
							consumerCache,
						)
					}
					if queue.producer.cursor != cursor || queue.consumerReservation.head.Load() != head ||
						queue.producerPublished.Load() != producerPublished ||
						queue.consumerPublished.Load() != consumerPublished {
						t.Fatal("zero or invalid call changed positions or publications")
					}
				})
			}
		}
	}
}

func TestSPMCLengthErrorAdmissionPrecedence(t *testing.T) {
	for _, exact := range []bool{false, true} {
		options := []SPMCOption{WithSPMCMaxBatch(2)}
		if exact {
			options = append(options, WithSPMCExactBatchCounts())
		}
		queue := newSPMCForTest[int](t, 4, options...)
		enqueueSPMCValues(t, queue, 10, 11, 12, 13)
		for _, test := range []struct {
			length int
			want   error
		}{{5, ErrExceedsCapacity}, {3, ErrExceedsMaxBatch}, {2, ErrFull}} {
			left, err := queue.EnqueueBulk(make([]int, test.length))
			if left != 0 || !errors.Is(err, test.want) {
				t.Fatalf("enqueue length=%d left=%d error=%v", test.length, left, err)
			}
			if test.want == ErrFull && err != ErrFull {
				t.Fatalf("full error=%v want unwrapped sentinel", err)
			}
		}
		dequeueSPMCValues(t, queue, 10, 11, 12, 13)
		for _, test := range []struct {
			length int
			want   error
		}{{5, ErrExceedsCapacity}, {3, ErrExceedsMaxBatch}, {2, ErrEmpty}} {
			dst := []int{-1, -1, -1, -1, -1}
			left, err := queue.DequeueBulk(dst[:test.length])
			if left != 0 || !errors.Is(err, test.want) ||
				!slices.Equal(dst, []int{-1, -1, -1, -1, -1}) {
				t.Fatalf("dequeue length=%d left=%d error=%v dst=%v", test.length, left, err, dst)
			}
			if test.want == ErrEmpty && err != ErrEmpty {
				t.Fatalf("empty error=%v want unwrapped sentinel", err)
			}
		}
	}
}

func TestSPMCBackwardSharedCache(t *testing.T) {
	for _, exact := range []bool{false, true} {
		for _, burst := range []bool{false, true} {
			for _, test := range []struct {
				name                            string
				olderIndex, request, cachedLeft int
				want, remaining                 []int
			}{
				{"above-capacity", 0, 2, 2, []int{12, 13}, []int{14, 15}},
				{"below-request", 1, 2, 2, []int{12, 13}, []int{14, 15}},
				{"ahead-of-head", 2, 1, 1, []int{12}, []int{13, 14, 15}},
			} {
				name := fmt.Sprintf("exact=%t/burst=%t/%s", exact, burst, test.name)
				t.Run(name, func(t *testing.T) {
					var options []SPMCOption
					if exact {
						options = append(options, WithSPMCExactBatchCounts())
					}
					queue := newSPMCForTest[int](t, 4, options...)
					enqueueSPMCValues(t, queue, 10)
					first := queue.producerPublished.Load()
					enqueueSPMCValues(t, queue, 11, 12)
					second := queue.producerPublished.Load()
					enqueueSPMCValues(t, queue, 13)
					third := queue.producerPublished.Load()
					dequeueSPMCValues(t, queue, 10, 11)
					enqueueSPMCValues(t, queue, 14, 15)
					older := []uint32{first, second, third}[test.olderIndex]
					// Exclusive access models a delayed Store of an actual older publication.
					queue.consumerReservation.cachedProducer.Store(older)
					dst := []int{-1, -1, -1}
					wantLeft := test.cachedLeft
					if exact && test.name == "ahead-of-head" {
						wantLeft = 3
					}
					if burst {
						moved, left := queue.DequeueBurst(dst[:test.request])
						if moved != test.request || left != wantLeft {
							t.Fatalf("moved=%d left=%d want=%d", moved, left, wantLeft)
						}
					} else {
						left, err := queue.DequeueBulk(dst[:test.request])
						if left != wantLeft || err != nil {
							t.Fatalf("left=%d want=%d error=%v", left, wantLeft, err)
						}
					}
					wantDst := []int{12, 13, -1}
					if test.request == 1 {
						wantDst = []int{12, -1, -1}
					}
					if !slices.Equal(dst[:test.request], test.want) || !slices.Equal(dst, wantDst) {
						t.Fatalf("dst=%v want=%v", dst, wantDst)
					}
					wantCache := uint32(6)
					if !exact && test.name == "ahead-of-head" {
						wantCache = older
					}
					if cached := queue.consumerReservation.cachedProducer.Load(); cached != wantCache {
						t.Fatalf("cache=%d want=%d", cached, wantCache)
					}
					dequeueSPMCValues(t, queue, test.remaining...)
					if value, err := queue.Dequeue(); value != 0 || err != ErrEmpty {
						t.Fatalf("final value=%d error=%v", value, err)
					}
				})
			}
		}
	}
}

func TestSPMCPositionRollover(t *testing.T) {
	for _, exact := range []bool{false, true} {
		var options []SPMCOption
		if exact {
			options = append(options, WithSPMCExactBatchCounts())
		}
		t.Run(fmt.Sprintf("exact=%t", exact), func(t *testing.T) {
			queue := newSPMCForTest[int](t, 4, options...)
			seedSPMC(queue, math.MaxUint32-2)
			enqueueSPMCValues(t, queue, 10)
			if free, err := queue.EnqueueBulk([]int{11, 12}); free != 1 || err != nil {
				t.Fatalf("wrapped bulk free=%d error=%v", free, err)
			}
			if moved, free := queue.EnqueueBurst([]int{13, 99}); moved != 1 || free != 0 {
				t.Fatalf("wrapped burst moved=%d free=%d", moved, free)
			}
			if !queue.Full() || queue.Count() != 4 || queue.producer.cursor != 1 {
				t.Fatalf("wrapped full count=%d cursor=%d", queue.Count(), queue.producer.cursor)
			}
			if err := queue.Enqueue(99); err != ErrFull {
				t.Fatalf("wrapped full error=%v", err)
			}
			dequeueSPMCValues(t, queue, 10)
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
			dequeueSPMCValues(t, queue, 20, 21, 22, 23)
			one := newSPMCForTest[int](t, 1, options...)
			seedSPMC(one, math.MaxUint32)
			if free, err := one.EnqueueBulk([]int{30}); free != 0 || err != nil {
				t.Fatalf("capacity-one bulk free=%d error=%v", free, err)
			}
			if one.producer.cursor != 0 || !one.Full() {
				t.Fatal("capacity-one enqueue did not cross rollover")
			}
			dequeueSPMCValues(t, one, 30)
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
}

func TestSPMCLengthClampingBeforeNarrowing(t *testing.T) {
	if uint64(^uint(0)) <= math.MaxUint32 {
		t.Skip("slice length beyond uint32 requires a 64-bit int")
	}
	length := uint64(math.MaxUint32) + 2
	items := make([]struct{}, int(length))
	for _, exact := range []bool{false, true} {
		options := []SPMCOption{WithSPMCMaxEnqueueBatch(2), WithSPMCMaxDequeueBatch(1)}
		if exact {
			options = append(options, WithSPMCExactBatchCounts())
		}
		queue := newSPMCForTest[struct{}](t, 4, options...)
		free, err := queue.EnqueueBulk(items)
		if free != 4 || !errors.Is(err, ErrExceedsCapacity) || err == ErrExceedsCapacity {
			t.Fatalf("large enqueue free=%d error=%v", free, err)
		}
		if moved, free := queue.EnqueueBurst(items); moved != 2 || free != 2 {
			t.Fatalf("large enqueue burst moved=%d free=%d", moved, free)
		}
		left, err := queue.DequeueBulk(items)
		if left != 2 || !errors.Is(err, ErrExceedsCapacity) || err == ErrExceedsCapacity {
			t.Fatalf("large dequeue left=%d error=%v", left, err)
		}
		if moved, left := queue.DequeueBurst(items); moved != 1 || left != 1 {
			t.Fatalf("large dequeue burst moved=%d left=%d", moved, left)
		}
		if moved, left := queue.DequeueBurst(items); moved != 1 || left != 0 {
			t.Fatalf("large final dequeue moved=%d left=%d", moved, left)
		}
	}
}

func TestSPMCPointerClearingRetentionAndReset(t *testing.T) {
	for _, retain := range []bool{false, true} {
		for _, exact := range []bool{false, true} {
			t.Run(fmt.Sprintf("retain=%t/exact=%t", retain, exact), func(t *testing.T) {
				options := []SPMCOption{WithSPMCMaxBatch(3), WithSPMCSpinsBeforeYield(1)}
				if retain {
					options = append(options, WithoutSPMCDequeueClearing())
				}
				if exact {
					options = append(options, WithSPMCExactBatchCounts())
				}
				queue := newSPMCForTest[*int](t, 4, options...)
				seedSPMC(queue, math.MaxUint32-1)
				a, b, c, d, e := 10, 11, 12, 13, 14
				if err := queue.Enqueue(&a); err != nil {
					t.Fatal(err)
				}
				if free, err := queue.EnqueueBulk([]*int{&b, &c, &d}); free != 0 || err != nil {
					t.Fatalf("pointer enqueue free=%d error=%v", free, err)
				}
				if value, err := queue.Dequeue(); value != &a || err != nil {
					t.Fatalf("pointer single value=%p error=%v", value, err)
				}
				wantSingleSlot := (*int)(nil)
				if retain {
					wantSingleSlot = &a
				}
				if queue.producer.entries[2] != wantSingleSlot {
					t.Fatalf("single backing slot=%p want=%p", queue.producer.entries[2], wantSingleSlot)
				}
				dst := []*int{nil, nil, nil}
				if left, err := queue.DequeueBulk(dst); left != 0 || err != nil ||
					!slices.Equal(dst, []*int{&b, &c, &d}) {
					t.Fatalf("wrapped pointer bulk dst=%v left=%d error=%v", dst, left, err)
				}
				wantSlots := []*int{nil, nil, nil, nil}
				if retain {
					wantSlots = []*int{&c, &d, &a, &b}
				}
				if !slices.Equal(queue.producer.entries, wantSlots) {
					t.Fatalf("consumed backing slots=%v want=%v", queue.producer.entries, wantSlots)
				}
				if moved, free := queue.EnqueueBurst([]*int{&e}); moved != 1 || free != 3 {
					t.Fatalf("overwrite moved=%d free=%d", moved, free)
				}
				if queue.producer.entries[2] != &e {
					t.Fatal("overwrite did not replace consumed pointer")
				}
				producer := queue.producer
				consumer := queue.consumerConfig
				queue.Reset()
				if !slices.Equal(queue.producer.entries, []*int{nil, nil, nil, nil}) {
					t.Fatalf("Reset backing slots=%v", queue.producer.entries)
				}
				if queue.producer.cursor != 0 || queue.producer.cachedConsumer != 0 ||
					queue.producerPublished.Load() != 0 || queue.consumerReservation.head.Load() != 0 ||
					queue.consumerReservation.cachedProducer.Load() != 0 || queue.consumerPublished.Load() != 0 {
					t.Fatal("Reset left nonzero position or cache")
				}
				if &queue.producer.entries[0] != &producer.entries[0] ||
					&queue.consumerConfig.entries[0] != &consumer.entries[0] ||
					len(queue.producer.entries) != 4 || cap(queue.producer.entries) != 4 ||
					len(queue.consumerConfig.entries) != 4 || cap(queue.consumerConfig.entries) != 4 {
					t.Fatal("Reset changed backing storage or slice headers")
				}
				if queue.producer.mask != producer.mask || queue.producer.maxBatch != producer.maxBatch ||
					queue.producer.exactBatchCounts != producer.exactBatchCounts ||
					queue.consumerConfig.mask != consumer.mask ||
					queue.consumerConfig.maxBatch != consumer.maxBatch ||
					queue.consumerConfig.spinsBeforeYield != consumer.spinsBeforeYield ||
					queue.consumerConfig.clearOnDequeue != consumer.clearOnDequeue ||
					queue.consumerConfig.exactBatchCounts != consumer.exactBatchCounts {
					t.Fatal("Reset changed configuration")
				}
				if !queue.Empty() || queue.Full() || queue.Count() != 0 || queue.Free() != 4 {
					t.Fatal("Reset query results")
				}
				if free, err := queue.EnqueueBulk([]*int{&a, &b}); free != 2 || err != nil {
					t.Fatalf("reuse enqueue free=%d error=%v", free, err)
				}
				dst = []*int{nil, nil, &e}
				if moved, left := queue.DequeueBurst(dst); moved != 2 || left != 0 ||
					!slices.Equal(dst, []*int{&a, &b, &e}) {
					t.Fatalf("reuse dequeue dst=%v moved=%d left=%d", dst, moved, left)
				}
			})
		}
	}
}

func TestSPMCObserversUsePublications(t *testing.T) {
	queue := newSPMCForTest[int](t, 4)
	enqueueSPMCValues(t, queue, 10, 11, 12, 13)
	start, count, left := queue.spmcReserveConsumer(4, true, false)
	if start != 0 || count != 4 || left != 0 {
		t.Fatalf("reservation start=%d count=%d left=%d", start, count, left)
	}
	if available, err := queue.DequeueBulk(nil); available != 0 || err != nil {
		t.Fatalf("reservation availability=%d error=%v", available, err)
	}
	queue.producer.cachedConsumer = math.MaxUint32
	queue.consumerReservation.cachedProducer.Store(math.MaxUint32)
	if queue.Count() != 4 || queue.Free() != 0 || !queue.Full() || queue.Empty() {
		t.Fatal("observers did not include unreleased reservations")
	}
	dst := []int{-1, -1, -1, -1}
	queue.spmcReadConsumer(start, dst)
	queue.spmcReleaseConsumer(start, count)
	if !slices.Equal(dst, []int{10, 11, 12, 13}) {
		t.Fatalf("reserved FIFO values=%v", dst)
	}
	if queue.Count() != 0 || queue.Free() != 4 || queue.Full() || !queue.Empty() {
		t.Fatal("observers used stale caches after release")
	}
	// An exclusive observer fixture covers the bounded result of skewed publications.
	queue.consumerPublished.Store(5)
	queue.producerPublished.Store(4)
	if queue.Count() != 4 || queue.Free() != 0 || !queue.Full() || queue.Empty() {
		t.Fatal("observers did not clamp a skewed publication snapshot")
	}
}
