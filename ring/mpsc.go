package ring

import "sync/atomic"

type mpscProducerConfig[T any] struct {
	entries          []T
	mask             uint32
	maxBatch         uint32
	spinsBeforeYield uint32
	htdMax           uint32
	relaxed          bool
	exactBatchCounts bool
}

type mpscProducerReservation struct {
	headRTS        atomic.Uint64
	head           atomic.Uint32
	cachedConsumer atomic.Uint32
}

type mpscProducerPublication struct {
	tailRTS   atomic.Uint64
	published atomic.Uint32
}

type mpscConsumer[T any] struct {
	entries          []T
	mask             uint32
	cursor           uint32
	cachedProducer   uint32
	clearOnDequeue   bool
	maxBatch         uint32
	exactBatchCounts bool
}

// MPSCQueue is a fixed-capacity FIFO with concurrent producers and one serialized consumer.
// Construct it with NewMPSCQueue. Its zero value is unusable, and it must not be copied after use.
// Construction must complete before synchronized publication to callers. Every consumer call,
// including failed and zero-length calls, belongs to one serialized owner. Owner handoff
// requires caller synchronization, such as a channel or mutex. Producers, Count observers,
// and immutable configuration queries may run concurrently with both sides. Reset requires
// exclusive synchronized use against all operations and observers.
//
// Producer reservation order controls FIFO. Calls may return in any
// order. A stalled reservation can block useful progress on both sides. Producer CAS retries
// and publication waits have no fixed bound. MPSCQueue is neither wait-free nor formally lock-free.
// Ordinary uint32 rollover is supported, but the shared cache must lag consumer publication
// by less than 2^32 - Capacity, and a producer head/CAS attempt must not span a complete
// 32-bit position cycle. RTS also requires no operation to span a full counter cycle.
// WithRelaxedMultiProdEnqueue selects RTS. A later enqueue may return before an earlier
// reservation makes either interval visible. Every RTS attempt waits for head-tail distance
// before admission, including failed and zero-length operations. A batch can cross the limit.
// RTS publication waits and CAS retries are unbounded. Platform 64-bit atomics may use locks.
type MPSCQueue[T any] struct {
	producerConfig      mpscProducerConfig[T]
	_                   [cacheLine]byte
	producerReservation mpscProducerReservation
	_                   [cacheLine]byte
	producerPublication mpscProducerPublication
	_                   [cacheLine]byte
	consumer            mpscConsumer[T]
	_                   [cacheLine]byte
	consumerPublished   atomic.Uint32
}

// NewMPSCQueue returns an empty queue using all count slots.
// Clearing is enabled and batch counts use cached admission by default.
// Count must be a power of two in [1, 2^30], otherwise it wraps ErrInvalidCount before applying
// options. Batch limits default to count and spins default to defaultSpinsBeforeYield (8192).
// Options apply in order and only final values are validated. Producers default to ordered
// publication. WithRelaxedMultiProdEnqueue selects RTS, with a default distance of count / 8.
// WithMaxProducerHeadTailDistance overrides that limit in element positions and requires RTS.
// Consumer multi modes and explicit consumer distances wrap ErrUnsupportedOption.
// Validation checks enqueue batch, dequeue batch, spin number, producer mode, consumer mode,
// producer distance, then consumer distance. Batch errors wrap ErrInvalidMaxBatch. Spin errors
// wrap ErrInvalidSpinsBeforeYield. Numeric distance errors wrap ErrInvalidMaxHeadTailDistance
// before applicability checks. An ordered-side distance wraps ErrHeadTailDistanceRequiresRelaxed.
// MPSCQueue documents ownership, synchronization, and RTS visibility or slot reuse.
func NewMPSCQueue[T any](count int, options ...Option) (*MPSCQueue[T], error) {
	cfg, err := resolveConfig("NewMPSCQueue", count, modeMulti, modeSingle, true, options)
	if err != nil {
		return nil, err
	}
	entries := make([]T, count)
	queue := &MPSCQueue[T]{
		producerConfig: mpscProducerConfig[T]{
			entries:          entries,
			mask:             uint32(count - 1),
			maxBatch:         uint32(cfg.maxEnqueueBatch),
			spinsBeforeYield: uint32(cfg.spinsBeforeYield),
			relaxed:          cfg.prodMode == modeRelaxed,
			htdMax: resolveHeadTailDistance(
				cfg.prodMode,
				count,
				cfg.maxProducerHeadTailDistance,
				cfg.producerHeadTailDistanceSet,
			),
			exactBatchCounts: cfg.exactBatchCounts,
		},
		consumer: mpscConsumer[T]{
			entries:          entries,
			mask:             uint32(count - 1),
			maxBatch:         uint32(cfg.maxDequeueBatch),
			clearOnDequeue:   !cfg.keepDequeued,
			exactBatchCounts: cfg.exactBatchCounts,
		},
	}
	return queue, nil
}

// Capacity returns the number of usable slots.
func (r *MPSCQueue[T]) Capacity() int { return int(r.producerConfig.mask + 1) }

// MaxEnqueueBatch returns the configured enqueue batch limit.
func (r *MPSCQueue[T]) MaxEnqueueBatch() int { return int(r.producerConfig.maxBatch) }

// MaxDequeueBatch returns the configured dequeue batch limit.
func (r *MPSCQueue[T]) MaxDequeueBatch() int { return int(r.consumer.maxBatch) }

// Count returns a bounded two-publication snapshot, excluding unpublished producer reservations.
// It is not a linearizable count of immediately reservable entries or a global snapshot at return.
func (r *MPSCQueue[T]) Count() int {
	consumer := r.consumerPublished.Load()
	producer := r.producerPublication.published.Load()
	return int(min(producer-consumer, r.producerConfig.mask+1))
}

// Free returns Capacity minus a Count snapshot. Unpublished producer reservations can make
// this exceed immediately reservable space.
func (r *MPSCQueue[T]) Free() int { return r.Capacity() - r.Count() }

// Full reports whether a Count snapshot equals Capacity.
func (r *MPSCQueue[T]) Full() bool { return r.Count() == r.Capacity() }

// Empty reports whether a Count snapshot is zero.
func (r *MPSCQueue[T]) Empty() bool { return r.Count() == 0 }

// Reset clears every slot and all positions and caches while preserving configuration and storage.
// Callers must exclude operations and observers, synchronize completion of prior use, and
// synchronize publication to future users, even when dequeue clearing is disabled.
func (r *MPSCQueue[T]) Reset() {
	clear(r.producerConfig.entries)
	r.producerReservation.head.Store(0)
	r.producerReservation.headRTS.Store(0)
	r.producerPublication.tailRTS.Store(0)
	r.producerReservation.cachedConsumer.Store(0)
	r.producerPublication.published.Store(0)
	r.consumer.cursor = 0
	r.consumer.cachedProducer = 0
	r.consumerPublished.Store(0)
}

// Enqueue appends item or returns ErrFull unwrapped. Concurrent producers reserve disjoint
// intervals and copy before completing publication. RTS can return before earlier intervals
// publish.
func (r *MPSCQueue[T]) Enqueue(item T) error {
	start, count, _ := r.mpscReserveProducer(1, true, false)
	if count == 0 {
		return ErrFull
	}
	producer := &r.producerConfig
	producer.entries[start&producer.mask] = item
	r.mpscPublishProducer(start, count)
	return nil
}

// Dequeue removes the oldest published value or returns zero and ErrEmpty unwrapped.
// It belongs to the serialized consumer side, including failed calls.
func (r *MPSCQueue[T]) Dequeue() (T, error) {
	consumer := &r.consumer
	capacity := consumer.mask + 1
	available := consumer.cachedProducer - consumer.cursor
	if available == 0 || available > capacity {
		consumer.cachedProducer = r.producerPublication.published.Load()
		available = consumer.cachedProducer - consumer.cursor
		if available == 0 || available > capacity {
			var zero T
			return zero, ErrEmpty
		}
	}
	index := consumer.cursor & consumer.mask
	value := consumer.entries[index]
	if consumer.clearOnDequeue {
		var zero T
		consumer.entries[index] = zero
	}
	r.mpscReleaseConsumer(1)
	return value, nil
}

// EnqueueBulk appends all items or none. It checks capacity, batch limit, then room, returning
// wrapped ErrExceedsCapacity, wrapped ErrExceedsMaxBatch, or unwrapped ErrFull respectively.
// Its free count follows WithExactBatchCounts. RTS waits before admission. Zero length refreshes
// without reservation
// or publication. Producers may call it concurrently.
func (r *MPSCQueue[T]) EnqueueBulk(items []T) (free int, err error) {
	producer := &r.producerConfig
	capacity := producer.mask + 1
	if len(items) > int(capacity) || len(items) > int(producer.maxBatch) {
		head := r.mpscProducerHead()
		fresh := capacity + r.consumerPublished.Load() - head
		return int(min(fresh, capacity)), batchLengthError(
			"enqueue",
			len(items),
			producer.maxBatch,
			capacity,
		)
	}
	start, count, remaining := r.mpscReserveProducer(
		uint32(len(items)),
		true,
		producer.exactBatchCounts,
	)
	if count == 0 {
		if len(items) == 0 {
			return int(remaining), nil
		}
		return int(remaining), ErrFull
	}
	r.mpscWriteProducer(start, items)
	r.mpscPublishProducer(start, count)
	return int(remaining), nil
}

// EnqueueBurst appends the available prefix, limited to MaxEnqueueBatch. Its moved and free
// counts follow WithExactBatchCounts. Zero length refreshes without reservation or
// publication. Producers may call it concurrently.
func (r *MPSCQueue[T]) EnqueueBurst(items []T) (enqueued, free int) {
	producer := &r.producerConfig
	length := min(len(items), int(producer.maxBatch))
	start, count, remaining := r.mpscReserveProducer(
		uint32(length),
		false,
		producer.exactBatchCounts,
	)
	if count == 0 {
		return 0, int(remaining)
	}
	r.mpscWriteProducer(start, items[:count])
	r.mpscPublishProducer(start, count)
	return int(count), int(remaining)
}

// DequeueBulk fills dst in FIFO order or leaves it entirely unchanged. It checks capacity,
// batch limit, then data, returning wrapped ErrExceedsCapacity, wrapped ErrExceedsMaxBatch,
// or unwrapped ErrEmpty respectively. Its available count follows WithExactBatchCounts.
// This is a serialized consumer operation, including failed and zero-length calls.
func (r *MPSCQueue[T]) DequeueBulk(dst []T) (available int, err error) {
	consumer := &r.consumer
	capacity := consumer.mask + 1
	if len(dst) > int(capacity) || len(dst) > int(consumer.maxBatch) {
		cursor := consumer.cursor
		fresh := r.producerPublication.published.Load() - cursor
		return int(min(fresh, capacity)), batchLengthError(
			"dequeue",
			len(dst),
			consumer.maxBatch,
			capacity,
		)
	}
	moved, remaining := r.mpscDequeueBatch(dst, true)
	if moved != len(dst) {
		return remaining, ErrEmpty
	}
	return remaining, nil
}

// DequeueBurst fills the available prefix, limited to MaxDequeueBatch, and leaves the suffix
// unchanged. Its counts follow WithExactBatchCounts. Zero length refreshes without release.
// This is a serialized consumer operation, including zero-length calls.
func (r *MPSCQueue[T]) DequeueBurst(dst []T) (dequeued, available int) {
	length := min(len(dst), int(r.consumer.maxBatch))
	return r.mpscDequeueBatch(dst[:length], false)
}

func (r *MPSCQueue[T]) mpscReserveProducer(
	requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32) {
	for {
		var head uint64
		if r.producerConfig.relaxed {
			head = waitHeadTailDistance(
				&r.producerReservation.headRTS,
				&r.producerPublication.tailRTS,
				r.producerConfig.htdMax,
				int(r.producerConfig.spinsBeforeYield),
			)
		} else {
			head = uint64(r.producerReservation.head.Load())
		}
		start, count, remaining, retry := r.mpscAttemptProducer(head, requested, fixed, exact)
		if !retry {
			return start, count, remaining
		}
	}
}

// mpscAttemptProducer admits and attempts one reservation using a previously loaded head.
// The caller must load the selected head before this attempt. After any retry, including
// a stale out-of-range observation, RTS must recheck distance before loading another head.
func (r *MPSCQueue[T]) mpscAttemptProducer(
	word uint64,
	requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32, retry bool) {
	head := uint32(word)
	if r.producerConfig.relaxed {
		head = unpackPos(word)
	}
	capacity := r.producerConfig.mask + 1
	cached := r.producerReservation.cachedConsumer.Load()
	published := cached
	if exact || requested == 0 || head-cached > capacity-requested {
		published = r.consumerPublished.Load()
		if published != cached {
			r.producerReservation.cachedConsumer.Store(published)
		}
	}
	free := capacity + published - head
	if free > capacity {
		return head, 0, capacity, requested != 0
	}
	if fixed && free < requested {
		return head, 0, free, false
	}
	count = min(requested, free)
	if count == 0 {
		return head, 0, free, false
	}
	if r.producerConfig.relaxed {
		next := pack(head+count, unpackCnt(word)+1)
		if !r.producerReservation.headRTS.CompareAndSwap(word, next) {
			return head, 0, 0, true
		}
	} else if !r.producerReservation.head.CompareAndSwap(head, head+count) {
		return head, 0, 0, true
	}
	return head, count, free - count, false
}

// mpscWriteProducer writes an already reserved interval. The caller must own the complete
// nonempty interval and finish copying before publishing it.
func (r *MPSCQueue[T]) mpscWriteProducer(start uint32, items []T) {
	producer := &r.producerConfig
	length := uint32(len(items))
	base := start & producer.mask
	if length == 1 {
		producer.entries[base] = items[0]
		return
	}
	first := min(length, producer.mask+1-base)
	copy(producer.entries[base:base+first], items[:first])
	copy(producer.entries[:length-first], items[first:])
}

// mpscPublishProducer publishes a nonempty interval after all its writes.
func (r *MPSCQueue[T]) mpscPublishProducer(start, count uint32) {
	if r.producerConfig.relaxed {
		publication := &r.producerPublication
		if publishRelaxed(&r.producerReservation.headRTS, &publication.tailRTS) {
			raiseMirror(&publication.published, &publication.tailRTS)
		}
		return
	}
	waitForTail(&r.producerPublication.published, start, int(r.producerConfig.spinsBeforeYield))
	r.producerPublication.published.Store(start + count)
}

func (r *MPSCQueue[T]) mpscDequeueBatch(dst []T, fixed bool) (int, int) {
	consumer := &r.consumer
	capacity := consumer.mask + 1
	length := uint32(len(dst))
	available := consumer.cachedProducer - consumer.cursor
	if consumer.exactBatchCounts || length == 0 || available < length || available > capacity {
		consumer.cachedProducer = r.producerPublication.published.Load()
		available = consumer.cachedProducer - consumer.cursor
	}
	if available > capacity {
		return 0, int(capacity)
	}
	if fixed && available < length {
		return 0, int(available)
	}
	length = min(length, available)
	if length == 0 {
		return 0, int(available)
	}
	r.mpscReadConsumer(consumer.cursor, dst[:length])
	r.mpscReleaseConsumer(length)
	return int(length), int(available - length)
}

// mpscReadConsumer reads and optionally clears an owned, published interval. The serialized
// consumer must finish all reading and clearing before releasing its slots for producer reuse.
func (r *MPSCQueue[T]) mpscReadConsumer(start uint32, dst []T) {
	consumer := &r.consumer
	capacity := consumer.mask + 1
	length := uint32(len(dst))
	base := start & consumer.mask
	if length == 1 {
		dst[0] = consumer.entries[base]
		if consumer.clearOnDequeue {
			var zero T
			consumer.entries[base] = zero
		}
	} else {
		first := min(length, capacity-base)
		copy(dst[:first], consumer.entries[base:base+first])
		copy(dst[first:length], consumer.entries[:length-first])
		if consumer.clearOnDequeue {
			clear(consumer.entries[base : base+first])
			clear(consumer.entries[:length-first])
		}
	}
}

// mpscReleaseConsumer advances the serialized cursor and publishes reusable slots together.
// The consumer must finish reading and clearing the complete nonempty interval first.
func (r *MPSCQueue[T]) mpscReleaseConsumer(count uint32) {
	r.consumer.cursor += count
	r.consumerPublished.Store(r.consumer.cursor)
}

func (r *MPSCQueue[T]) mpscProducerHead() uint32 {
	if r.producerConfig.relaxed {
		return unpackPos(r.producerReservation.headRTS.Load())
	}
	return r.producerReservation.head.Load()
}
