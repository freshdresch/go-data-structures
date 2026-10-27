package dpdk

import (
	"errors"
	"math"
	"slices"
	"strconv"
	"testing"
)

func mpmcNewForTest[T any](t *testing.T, capacity int, options ...Option) *MPMCQueue[T] {
	t.Helper()
	queue, err := NewMPMCQueue[T](capacity, options...)
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

func mpmcTestOptions(mode int, options []Option) []Option {
	result := []Option{WithMultiProdEnqueue(), WithMultiConsDequeue()}
	if mode&1 != 0 {
		result = append(result, WithRelaxedMultiProdEnqueue())
	}
	if mode&2 != 0 {
		result = append(result, WithRelaxedMultiConsDequeue())
	}
	return append(result, options...)
}

// mpmcSeed requires exclusive synchronized access to an empty queue, as Reset does.
func mpmcSeed[T any](queue *MPMCQueue[T], position, counter uint32) {
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

func TestMPMCConstruction(t *testing.T) {
	for mode := 0; mode < 4; mode++ {
		queue := mpmcNewForTest[int](t, 8, mpmcTestOptions(mode, nil)...)
		cfg := &queue.config
		if cfg.mask != 7 || cfg.maxEnqueueBatch != 8 || cfg.maxDequeueBatch != 8 ||
			!cfg.clearOnDequeue || cfg.exactBatchCounts || cfg.spinsBeforeYield != 8192 ||
			cfg.producerRelaxed != (mode&1 != 0) || cfg.consumerRelaxed != (mode&2 != 0) {
			t.Fatal("defaults", mode)
		}
		if cfg.prodHTDMax != uint32(mode&1) || cfg.consHTDMax != uint32((mode>>1)&1) {
			t.Fatal("default distances", mode, cfg.prodHTDMax, cfg.consHTDMax)
		}
	}
	configured := mpmcNewForTest[int](
		t,
		8,
		WithMaxBatch(-1),
		WithMaxBatch(4),
		WithMaxEnqueueBatch(2),
		WithMaxDequeueBatch(3),
		WithSpinsBeforeYield(-1),
		WithSpinsBeforeYield(0),
		WithoutDequeueClearing(),
		WithExactBatchCounts(),
		WithMaxProducerHeadTailDistance(-1),
		WithMaxProducerHeadTailDistance(0),
		WithRelaxedMultiProdEnqueue(),
		WithMaxConsumerHeadTailDistance(8),
		WithRelaxedMultiConsDequeue(),
	)
	if configured.MaxEnqueueBatch() != 2 || configured.MaxDequeueBatch() != 3 ||
		configured.config.clearOnDequeue || !configured.config.exactBatchCounts ||
		configured.config.spinsBeforeYield != 0 || configured.config.prodHTDMax != 0 ||
		configured.config.consHTDMax != 8 {
		t.Fatal("final option values")
	}
	maximum := int(^uint(0) >> 1)
	if uint64(maximum) > math.MaxUint32 {
		maxDistance := uint64(math.MaxUint32)
		maximum = int(maxDistance)
	}
	for _, distance := range []int{0, 1, 7, 8, 9, maximum} {
		for _, before := range []bool{false, true} {
			options := []Option{WithMaxProducerHeadTailDistance(distance),
				WithMaxConsumerHeadTailDistance(distance)}
			if before {
				options = append(options, WithRelaxedMultiProdEnqueue(), WithRelaxedMultiConsDequeue())
			} else {
				options = append(
					[]Option{WithRelaxedMultiProdEnqueue(), WithRelaxedMultiConsDequeue()},
					options...,
				)
			}
			queue := mpmcNewForTest[int](t, 8, options...)
			if queue.config.prodHTDMax != uint32(distance) || queue.config.consHTDMax != uint32(distance) {
				t.Fatal("distance retained", distance)
			}
		}
	}
	if strconv.IntSize > 32 {
		tooLarge := uint64(math.MaxUint32) + 1
		for _, option := range []Option{WithMaxProducerHeadTailDistance(int(tooLarge)),
			WithMaxConsumerHeadTailDistance(int(tooLarge))} {
			_, err := NewMPMCQueue[int](
				8,
				WithRelaxedMultiProdEnqueue(),
				WithRelaxedMultiConsDequeue(),
				option,
			)
			if !errors.Is(err, ErrInvalidMaxHeadTailDistance) {
				t.Fatal("distance narrowed", err)
			}
		}
	}
}

func TestMPMCCountsAndLengths(t *testing.T) {
	for mode := 0; mode < 4; mode++ {
		for _, exact := range []bool{false, true} {
			var options []Option
			if exact {
				options = append(options, WithExactBatchCounts())
			}
			queue := mpmcNewForTest[int](t, 8, mpmcTestOptions(mode, options)...)
			if err := queue.Enqueue(10); err != nil {
				t.Fatal(err)
			}
			if _, err := queue.Dequeue(); err != nil {
				t.Fatal(err)
			}
			free, err := queue.EnqueueBulk([]int{11})
			expected := 6
			if exact {
				expected = 7
			}
			if err != nil || free != expected {
				t.Fatal("bulk-one cached/fresh free", mode, exact, free, err)
			}
			if _, err := queue.Dequeue(); err != nil {
				t.Fatal(err)
			}
			if free, err := queue.EnqueueBulk(nil); free != 8 || err != nil {
				t.Fatal("zero refresh", free, err)
			}
			if _, err := queue.EnqueueBulk([]int{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			// The first dequeue refreshes three entries, then later production leaves that cache stale.
			if _, err := queue.Dequeue(); err != nil {
				t.Fatal(err)
			}
			if err := queue.Enqueue(4); err != nil {
				t.Fatal(err)
			}
			dst := []int{-1}
			left, err := queue.DequeueBulk(dst)
			expected = 1
			if exact {
				expected = 2
			}
			if err != nil || left != expected || dst[0] != 2 {
				t.Fatal("bulk-one cached/fresh available", mode, exact, left, dst, err)
			}
			if left, err := queue.DequeueBulk(nil); left != 2 || err != nil {
				t.Fatal("zero consumer refresh", left, err)
			}
			dst = make([]int, 9)
			for index := range dst {
				dst[index] = -1
			}
			prodCache := queue.producerReservation.cachedConsumer.Load()
			consCache := queue.consumerReservation.cachedProducer.Load()
			if free, err := queue.EnqueueBulk(dst); free != 6 || !errors.Is(err, ErrExceedsCapacity) {
				t.Fatal("enqueue length capacity", free, err)
			}
			if left, err := queue.DequeueBulk(dst); left != 2 || !errors.Is(err, ErrExceedsCapacity) {
				t.Fatal("dequeue length capacity", left, err)
			}
			if prodCache != queue.producerReservation.cachedConsumer.Load() ||
				consCache != queue.consumerReservation.cachedProducer.Load() {
				t.Fatal("length errors mutated caches")
			}
			if !slices.Equal(dst, []int{-1, -1, -1, -1, -1, -1, -1, -1, -1}) {
				t.Fatal("length error mutated destination")
			}
			if left, err := queue.DequeueBulk(dst[:3]); left != 2 || err != ErrEmpty {
				t.Fatal("all-or-none failure", left, err)
			}
			if moved, left := queue.DequeueBurst(dst); moved != 2 || left != 0 ||
				dst[0] != 3 || dst[1] != 4 || dst[2] != -1 {
				t.Fatal("prefix and suffix", moved, left, dst)
			}
			if _, err := queue.EnqueueBulk(make([]int, 7)); err != nil {
				t.Fatal(err)
			}
			if free, err := queue.EnqueueBulk([]int{8, 9}); free != 1 || err != ErrFull {
				t.Fatal("required reload before failure", free, err)
			}
			if moved, free := queue.EnqueueBurst([]int{8, 9}); moved != 1 || free != 0 {
				t.Fatal("partial enqueue", moved, free)
			}
		}
		clamped := mpmcNewForTest[int](t, 8, mpmcTestOptions(mode, []Option{WithMaxBatch(1)})...)
		if moved, free := clamped.EnqueueBurst([]int{4, 5, 6}); moved != 1 || free != 7 {
			t.Fatal("clamped scalar enqueue", moved, free)
		}
		dst := []int{-1, -1, -1}
		if moved, left := clamped.DequeueBurst(dst); moved != 1 || left != 0 ||
			!slices.Equal(dst, []int{4, -1, -1}) {
			t.Fatal("clamped scalar dequeue", moved, left, dst)
		}
		if _, err := clamped.EnqueueBulk(dst[:2]); !errors.Is(err, ErrExceedsMaxBatch) {
			t.Fatal("enqueue batch limit", err)
		}
		if _, err := clamped.DequeueBulk(dst[:2]); !errors.Is(err, ErrExceedsMaxBatch) {
			t.Fatal("dequeue batch limit", err)
		}
	}
}

func TestMPMCActiveHeadsAndBoundedCounts(t *testing.T) {
	for mode := 0; mode < 4; mode++ {
		queue := mpmcNewForTest[int](t, 8, mpmcTestOptions(mode, []Option{WithMaxBatch(1)})...)
		mpmcSeed(queue, 100, 0)
		queue.producerReservation.head.Store(102)
		queue.producerReservation.headRTS.Store(pack(103, 1))
		queue.consumerReservation.head.Store(101)
		queue.consumerReservation.headRTS.Store(pack(102, 1))
		queue.producerPublication.published.Store(104)
		queue.consumerPublication.published.Store(100)
		prodHead := uint32(102)
		consHead := uint32(101)
		if mode&1 != 0 {
			prodHead = 103
		}
		if mode&2 != 0 {
			consHead = 102
		}
		if free, err := queue.EnqueueBulk([]int{1, 2}); free != int(108-prodHead) ||
			!errors.Is(err, ErrExceedsMaxBatch) {
			t.Fatal("selected producer head", mode, free, err)
		}
		if available, err := queue.DequeueBulk(make([]int, 2)); available != int(104-consHead) ||
			!errors.Is(err, ErrExceedsMaxBatch) {
			t.Fatal("selected consumer head", mode, available, err)
		}
		if queue.Count() != 4 || queue.Free() != 4 || queue.Full() || queue.Empty() {
			t.Fatal("queries must use publications")
		}
		// Captured stale heads model an attempt delayed across opposing publication progress.
		queue.consumerPublication.published.Store(110)
		queue.producerPublication.published.Store(120)
		queue.producerReservation.cachedConsumer.Store(99)
		queue.consumerReservation.cachedProducer.Store(99)
		word := uint64(100)
		if mode&1 != 0 {
			word = pack(100, 0)
		}
		_, count, left, retry := queue.mpmcAttemptProducer(word, 1, true, true)
		if !retry || count != 0 || left != 8 {
			t.Fatal("out-of-range producer retry", mode, count, left, retry)
		}
		_, count, left, retry = queue.mpmcAttemptProducer(word, 0, true, true)
		if retry || count != 0 || left != 8 {
			t.Fatal("out-of-range zero producer", mode, count, left, retry)
		}
		word = 100
		if mode&2 != 0 {
			word = pack(100, 0)
		}
		_, count, left, retry = queue.mpmcAttemptConsumer(word, 1, true, true)
		if !retry || count != 0 || left != 8 {
			t.Fatal("out-of-range consumer retry", mode, count, left, retry)
		}
		_, count, left, retry = queue.mpmcAttemptConsumer(word, 0, true, true)
		if retry || count != 0 || left != 8 {
			t.Fatal("out-of-range zero consumer", mode, count, left, retry)
		}
		if queue.Count() != 8 || queue.Free() != 0 {
			t.Fatal("out-of-range query clamp")
		}
	}
}

func TestMPMCRolloverClearingReset(t *testing.T) {
	for mode := 0; mode < 4; mode++ {
		for _, clearing := range []bool{false, true} {
			options := []Option{WithExactBatchCounts(), WithMaxBatch(4), WithSpinsBeforeYield(0)}
			if !clearing {
				options = append(options, WithoutDequeueClearing())
			}
			queue := mpmcNewForTest[*int](t, 8, mpmcTestOptions(mode, options)...)
			original := &queue.config.entries[0]
			for _, seed := range []struct{ position, counter uint32 }{
				{math.MaxUint32 - 2, 10}, {6, math.MaxUint32 - 1},
			} {
				mpmcSeed(queue, seed.position, seed.counter)
				for round := 0; round < 4; round++ {
					values := []int{11, 12, 13}
					items := []*int{&values[0], &values[1], &values[2]}
					if _, err := queue.EnqueueBulk(items); err != nil {
						t.Fatal(err)
					}
					dst := make([]*int, 3)
					if _, err := queue.DequeueBulk(dst); err != nil || !slices.Equal(dst, items) {
						t.Fatal("wrapped pointer read", dst, err)
					}
					for index, item := range items {
						slot := (seed.position + uint32(3*round+index)) & 7
						expected := item
						if clearing {
							expected = nil
						}
						if queue.config.entries[slot] != expected {
							t.Fatal("wrapped clearing/retention", slot)
						}
					}
				}
			}
			if err := queue.Enqueue(new(int)); err != nil {
				t.Fatal(err)
			}
			if _, err := queue.Dequeue(); err != nil {
				t.Fatal(err)
			}
			// Inactive words may be arbitrary only under exclusive synchronized access.
			queue.producerReservation.head.Store(111)
			queue.consumerReservation.head.Store(112)
			queue.producerReservation.headRTS.Store(pack(113, 114))
			queue.consumerReservation.headRTS.Store(pack(115, 116))
			queue.producerPublication.tailRTS.Store(pack(117, 118))
			queue.consumerPublication.tailRTS.Store(pack(119, 120))
			queue.Reset()
			words := []uint64{queue.producerReservation.headRTS.Load(),
				uint64(queue.producerReservation.head.Load()),
				uint64(queue.producerReservation.cachedConsumer.Load()),
				queue.producerPublication.tailRTS.Load(), uint64(queue.producerPublication.published.Load()),
				queue.consumerReservation.headRTS.Load(), uint64(queue.consumerReservation.head.Load()),
				uint64(queue.consumerReservation.cachedProducer.Load()),
				queue.consumerPublication.tailRTS.Load(), uint64(queue.consumerPublication.published.Load())}
			for index, word := range words {
				if word != 0 {
					t.Fatal("Reset protocol word", index, word)
				}
			}
			for _, entry := range queue.config.entries {
				if entry != nil {
					t.Fatal("Reset slot retained pointer")
				}
			}
			if &queue.config.entries[0] != original || queue.MaxEnqueueBatch() != 4 ||
				queue.MaxDequeueBatch() != 4 || queue.config.clearOnDequeue != clearing ||
				!queue.config.exactBatchCounts || queue.config.spinsBeforeYield != 0 ||
				queue.config.producerRelaxed != (mode&1 != 0) || queue.config.consumerRelaxed != (mode&2 != 0) {
				t.Fatal("Reset changed configuration or storage")
			}
			value := 42
			if err := queue.Enqueue(&value); err != nil {
				t.Fatal(err)
			}
			if got, err := queue.Dequeue(); err != nil || got != &value {
				t.Fatal("reuse after Reset", got, err)
			}
		}
	}
}
