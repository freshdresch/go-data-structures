package ring

import "sync/atomic"

type spmcProducer[T any] struct {
	entries          []T
	mask             uint32
	cursor           uint32
	cachedConsumer   uint32
	maxBatch         uint32
	exactBatchCounts bool
}

type spmcConsumerConfig[T any] struct {
	entries          []T
	mask             uint32
	maxBatch         uint32
	spinsBeforeYield uint32
	htdMax           uint32
	clearOnDequeue   bool
	exactBatchCounts bool
	relaxed          bool
}

type spmcConsumerReservation struct {
	headRTS        atomic.Uint64
	head           atomic.Uint32
	cachedProducer atomic.Uint32
}

type spmcConsumerPublication struct {
	tailRTS   atomic.Uint64
	published atomic.Uint32
}

// SPMCQueue is a fixed-capacity FIFO with one serialized producer and concurrent consumers.
// Construct it with NewSPMCQueue. Its zero value is unusable, and it must not be copied after use.
// Construction must complete before synchronized publication to callers. Every producer call,
// including failed and zero-length calls, belongs to one serialized owner. Owner handoff
// requires caller synchronization, such as a channel or mutex. Consumers, Count observers,
// and immutable configuration queries may run concurrently with both sides. Reset requires
// exclusive synchronized use against all operations and observers.
//
// Reservation order controls consumer release and FIFO within each reserved interval.
// Calls may return in any order. A stalled reservation can block useful progress on both sides.
// Consumer CAS retries, distance waits, and release waits have no fixed bound. uint32 rollover
// is supported, but the shared cache must lag producer publication by less than 2^32 - Capacity,
// and a consumer head/CAS attempt must not span a complete 32-bit position cycle. RTS also
// requires that no packed counter observation span a complete 32-bit operation cycle.
// RTS consumers can return before earlier reservations finish. Their slots remain unavailable
// for producer reuse until publication closes every outstanding interval. Count can include
// values already returned. RTS waits for the configured distance before admission, including
// failed and zero-length calls. Counts describe bounded admission snapshots.
type SPMCQueue[T any] struct {
	producer            spmcProducer[T]
	_                   [cacheLine]byte
	producerPublished   atomic.Uint32
	_                   [cacheLine]byte
	consumerConfig      spmcConsumerConfig[T]
	_                   [cacheLine]byte
	consumerReservation spmcConsumerReservation
	_                   [cacheLine]byte
	consumerPublication spmcConsumerPublication
}

// NewSPMCQueue returns an empty queue using all count slots.
// Clearing is enabled and batch counts use cached admission by default.
// Count must be a power of two in [1, 2^30], otherwise it wraps ErrInvalidCount before applying
// options. Batch limits default to count and spins default to defaultSpinsBeforeYield (8192).
// Options apply in order and only final values are validated. Consumers default to ordered
// publication. WithRelaxedMultiConsDequeue selects RTS, with a default distance of count / 8.
// WithMaxConsumerHeadTailDistance overrides that limit in element positions and requires RTS.
// Producer multi modes and explicit producer distances wrap ErrUnsupportedOption.
// Validation checks enqueue batch, dequeue batch, spin number, producer mode, consumer mode,
// producer distance, then consumer distance. Batch errors wrap ErrInvalidMaxBatch. Spin errors
// wrap ErrInvalidSpinsBeforeYield. Numeric distance errors wrap ErrInvalidMaxHeadTailDistance
// before applicability checks. An ordered-side distance wraps ErrHeadTailDistanceRequiresRelaxed.
// SPMCQueue documents ownership, synchronization, and RTS visibility or slot reuse.
func NewSPMCQueue[T any](count int, options ...Option) (*SPMCQueue[T], error) {
	cfg, err := resolveConfig("NewSPMCQueue", count, modeSingle, modeMulti, true, options)
	if err != nil {
		return nil, err
	}
	entries := make([]T, count)
	queue := &SPMCQueue[T]{
		producer: spmcProducer[T]{
			entries:          entries,
			mask:             uint32(count - 1),
			maxBatch:         uint32(cfg.maxEnqueueBatch),
			exactBatchCounts: cfg.exactBatchCounts,
		},
		consumerConfig: spmcConsumerConfig[T]{
			entries:          entries,
			mask:             uint32(count - 1),
			maxBatch:         uint32(cfg.maxDequeueBatch),
			spinsBeforeYield: uint32(cfg.spinsBeforeYield),
			htdMax: resolveHeadTailDistance(
				cfg.consMode, count, cfg.maxConsumerHeadTailDistance,
				cfg.consumerHeadTailDistanceSet,
			),
			relaxed:          cfg.consMode == modeRelaxed,
			clearOnDequeue:   !cfg.keepDequeued,
			exactBatchCounts: cfg.exactBatchCounts,
		},
	}
	return queue, nil
}

// Capacity returns the number of usable slots.
func (r *SPMCQueue[T]) Capacity() int { return int(r.producer.mask + 1) }

// MaxEnqueueBatch returns the configured enqueue batch limit.
func (r *SPMCQueue[T]) MaxEnqueueBatch() int { return int(r.producer.maxBatch) }

// MaxDequeueBatch returns the configured dequeue batch limit.
func (r *SPMCQueue[T]) MaxDequeueBatch() int { return int(r.consumerConfig.maxBatch) }

// Count returns a bounded two-publication snapshot, including reservations not yet released.
// It is not a linearizable count of immediately reservable entries or a global snapshot at return.
func (r *SPMCQueue[T]) Count() int {
	consumer := r.consumerPublication.published.Load()
	producer := r.producerPublished.Load()
	return int(min(producer-consumer, r.producer.mask+1))
}

// Free returns Capacity minus a Count snapshot.
func (r *SPMCQueue[T]) Free() int { return r.Capacity() - r.Count() }

// Full reports whether a Count snapshot equals Capacity.
func (r *SPMCQueue[T]) Full() bool { return r.Count() == r.Capacity() }

// Empty reports whether a Count snapshot is zero.
func (r *SPMCQueue[T]) Empty() bool { return r.Count() == 0 }

// Reset clears every slot and all positions and caches while preserving configuration and storage.
// Callers must exclude operations and observers, synchronize completion of prior use, and
// synchronize publication to future users, even when dequeue clearing is disabled.
func (r *SPMCQueue[T]) Reset() {
	clear(r.producer.entries)
	r.producer.cursor = 0
	r.producer.cachedConsumer = 0
	r.producerPublished.Store(0)
	r.consumerReservation.head.Store(0)
	r.consumerReservation.headRTS.Store(0)
	r.consumerPublication.tailRTS.Store(0)
	r.consumerReservation.cachedProducer.Store(0)
	r.consumerPublication.published.Store(0)
}

// Enqueue appends item or returns ErrFull unwrapped. It belongs to the serialized producer side.
func (r *SPMCQueue[T]) Enqueue(item T) error {
	producer := &r.producer
	capacity := producer.mask + 1
	free := capacity + producer.cachedConsumer - producer.cursor
	if free == 0 || free > capacity {
		producer.cachedConsumer = r.consumerPublication.published.Load()
		free = capacity + producer.cachedConsumer - producer.cursor
		if free == 0 || free > capacity {
			return ErrFull
		}
	}
	producer.entries[producer.cursor&producer.mask] = item
	producer.cursor++
	r.producerPublished.Store(producer.cursor)
	return nil
}

// Dequeue removes the oldest reservable value or returns the zero value and ErrEmpty unwrapped.
// Concurrent consumers reserve disjoint intervals. A nonempty reservation waits for earlier
// releases in ordered mode after reading and optional clearing. RTS records completion without
// waiting for earlier releases. Callers may return out of reservation order in either mode.
func (r *SPMCQueue[T]) Dequeue() (T, error) {
	start, count, _ := r.spmcReserveConsumer(1, true, false)
	if count == 0 {
		var zero T
		return zero, ErrEmpty
	}
	consumer := &r.consumerConfig
	index := start & consumer.mask
	value := consumer.entries[index]
	if consumer.clearOnDequeue {
		var zero T
		consumer.entries[index] = zero
	}
	r.spmcReleaseConsumer(start, count)
	return value, nil
}

// EnqueueBulk appends all items or none. It checks capacity, batch limit, then room, returning
// wrapped ErrExceedsCapacity, wrapped ErrExceedsMaxBatch, or unwrapped ErrFull respectively.
// Its free count follows WithExactBatchCounts. Zero length refreshes without publication.
// This is a producer operation, including failed and zero-length calls.
func (r *SPMCQueue[T]) EnqueueBulk(items []T) (free int, err error) {
	producer := &r.producer
	capacity := producer.mask + 1
	if len(items) > int(capacity) || len(items) > int(producer.maxBatch) {
		cursor := producer.cursor
		fresh := capacity + r.consumerPublication.published.Load() - cursor
		return int(min(fresh, capacity)), batchLengthError(
			"enqueue", len(items), producer.maxBatch, capacity,
		)
	}
	moved, remaining := r.spmcEnqueueBatch(items, true)
	if moved != len(items) {
		return remaining, ErrFull
	}
	return remaining, nil
}

// EnqueueBurst appends the available prefix, limited to MaxEnqueueBatch, and returns moved and
// free counts following WithExactBatchCounts. Zero length refreshes without publication.
// This is a producer operation, including zero-length calls.
func (r *SPMCQueue[T]) EnqueueBurst(items []T) (enqueued, free int) {
	length := min(len(items), int(r.producer.maxBatch))
	return r.spmcEnqueueBatch(items[:length], false)
}

// DequeueBulk fills dst in FIFO order or leaves it entirely unchanged. It checks capacity,
// batch limit, then data, returning wrapped ErrExceedsCapacity, wrapped ErrExceedsMaxBatch, or
// unwrapped ErrEmpty respectively. Its available count follows WithExactBatchCounts.
// Zero length refreshes without reservation or publication. Consumers may call it concurrently.
func (r *SPMCQueue[T]) DequeueBulk(dst []T) (available int, err error) {
	consumer := &r.consumerConfig
	capacity := consumer.mask + 1
	if len(dst) > int(capacity) || len(dst) > int(consumer.maxBatch) {
		head := r.spmcConsumerHeadPosition()
		fresh := r.producerPublished.Load() - head
		return int(min(fresh, capacity)), batchLengthError(
			"dequeue", len(dst), consumer.maxBatch, capacity,
		)
	}
	start, count, remaining := r.spmcReserveConsumer(
		uint32(len(dst)), true, consumer.exactBatchCounts,
	)
	if count == 0 {
		if len(dst) == 0 {
			return int(remaining), nil
		}
		return int(remaining), ErrEmpty
	}
	r.spmcReadConsumer(start, dst)
	r.spmcReleaseConsumer(start, count)
	return int(remaining), nil
}

// DequeueBurst fills only the available prefix of dst, limited to MaxDequeueBatch, and leaves
// the suffix unchanged. Its moved and available counts follow WithExactBatchCounts.
// Zero length refreshes without reservation or publication. Consumers may call it concurrently.
func (r *SPMCQueue[T]) DequeueBurst(dst []T) (dequeued, available int) {
	consumer := &r.consumerConfig
	length := min(len(dst), int(consumer.maxBatch))
	start, count, remaining := r.spmcReserveConsumer(
		uint32(length), false, consumer.exactBatchCounts,
	)
	if count == 0 {
		return 0, int(remaining)
	}
	r.spmcReadConsumer(start, dst[:count])
	r.spmcReleaseConsumer(start, count)
	return int(count), int(remaining)
}

func (r *SPMCQueue[T]) spmcEnqueueBatch(items []T, fixed bool) (int, int) {
	producer := &r.producer
	capacity := producer.mask + 1
	length := uint32(len(items))
	free := capacity + producer.cachedConsumer - producer.cursor
	if producer.exactBatchCounts || length == 0 || free < length || free > capacity {
		producer.cachedConsumer = r.consumerPublication.published.Load()
		free = capacity + producer.cachedConsumer - producer.cursor
	}
	if free > capacity {
		return 0, int(capacity)
	}
	if fixed && free < length {
		return 0, int(free)
	}
	length = min(length, free)
	if length == 0 {
		return 0, int(free)
	}
	base := producer.cursor & producer.mask
	if length == 1 {
		producer.entries[base] = items[0]
	} else {
		first := min(length, capacity-base)
		copy(producer.entries[base:base+first], items[:first])
		copy(producer.entries[:length-first], items[first:length])
	}
	producer.cursor += length
	r.producerPublished.Store(producer.cursor)
	return int(length), int(free - length)
}

func (r *SPMCQueue[T]) spmcReserveConsumer(
	requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32) {
	for {
		var word uint64
		if r.consumerConfig.relaxed {
			word = waitHeadTailDistance(
				&r.consumerReservation.headRTS,
				&r.consumerPublication.tailRTS,
				r.consumerConfig.htdMax,
				int(r.consumerConfig.spinsBeforeYield),
			)
		} else {
			word = uint64(r.consumerReservation.head.Load())
		}
		start, count, remaining, retry := r.spmcAttemptConsumerWord(word, requested, fixed, exact)
		if !retry {
			return start, count, remaining
		}
	}
}

func (r *SPMCQueue[T]) spmcConsumerHeadPosition() uint32 {
	if r.consumerConfig.relaxed {
		return unpackPos(r.consumerReservation.headRTS.Load())
	}
	return r.consumerReservation.head.Load()
}

// spmcAttemptConsumer admits and attempts one reservation using a previously loaded head.
// The caller must load head before this attempt and reload it after any retry, including
// a stale out-of-range observation.
func (r *SPMCQueue[T]) spmcAttemptConsumer(
	head, requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32, retry bool) {
	return r.spmcAttemptConsumerWord(uint64(head), requested, fixed, exact)
}

// spmcAttemptConsumerWord uses one complete selected head snapshot for admission and CAS.
// A retry must return through the outer distance check before attempting another reservation.
func (r *SPMCQueue[T]) spmcAttemptConsumerWord(
	word uint64,
	requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32, retry bool) {
	head := uint32(word)
	if r.consumerConfig.relaxed {
		head = unpackPos(word)
	}
	capacity := r.consumerConfig.mask + 1
	cached := r.consumerReservation.cachedProducer.Load()
	published := cached
	if exact || requested == 0 || cached-head-requested > capacity-requested {
		published = r.producerPublished.Load()
		if published != cached {
			r.consumerReservation.cachedProducer.Store(published)
		}
	}
	available := published - head
	if available > capacity {
		return head, 0, capacity, requested != 0
	}
	if fixed && available < requested {
		return head, 0, available, false
	}
	count = min(requested, available)
	if count == 0 {
		return head, 0, available, false
	}
	if r.consumerConfig.relaxed {
		next := pack(head+count, unpackCnt(word)+1)
		if !r.consumerReservation.headRTS.CompareAndSwap(word, next) {
			return head, 0, 0, true
		}
	} else if !r.consumerReservation.head.CompareAndSwap(head, head+count) {
		return head, 0, 0, true
	}
	return head, count, available - count, false
}

// spmcReadConsumer modifies dst and its already reserved backing slots when clearing is enabled.
// The caller must own the complete nonempty interval and finish movement before releasing it.
func (r *SPMCQueue[T]) spmcReadConsumer(start uint32, dst []T) {
	consumer := &r.consumerConfig
	length := uint32(len(dst))
	base := start & consumer.mask
	if length == 1 {
		dst[0] = consumer.entries[base]
		if consumer.clearOnDequeue {
			var zero T
			consumer.entries[base] = zero
		}
		return
	}
	first := min(length, consumer.mask+1-base)
	copy(dst[:first], consumer.entries[base:base+first])
	copy(dst[first:], consumer.entries[:length-first])
	if consumer.clearOnDequeue {
		clear(consumer.entries[base : base+first])
		clear(consumer.entries[:length-first])
	}
}

// spmcReleaseConsumer publishes a nonempty interval after all its reading and clearing.
func (r *SPMCQueue[T]) spmcReleaseConsumer(start, count uint32) {
	if r.consumerConfig.relaxed {
		if publishRelaxed(&r.consumerReservation.headRTS, &r.consumerPublication.tailRTS) {
			raiseMirror(&r.consumerPublication.published, &r.consumerPublication.tailRTS)
		}
		return
	}
	waitForTail(&r.consumerPublication.published, start, int(r.consumerConfig.spinsBeforeYield))
	r.consumerPublication.published.Store(start + count)
}
