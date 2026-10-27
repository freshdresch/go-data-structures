package dpdk

import "sync/atomic"

type mpmcConfig[T any] struct {
	entries          []T
	mask             uint32
	maxEnqueueBatch  uint32
	maxDequeueBatch  uint32
	prodHTDMax       uint32
	consHTDMax       uint32
	spinsBeforeYield uint32
	producerRelaxed  bool
	consumerRelaxed  bool
	clearOnDequeue   bool
	exactBatchCounts bool
}
type mpmcProducerReservation struct {
	headRTS        atomic.Uint64
	head           atomic.Uint32
	cachedConsumer atomic.Uint32
}
type mpmcProducerPublication struct {
	tailRTS   atomic.Uint64
	published atomic.Uint32
}
type mpmcConsumerReservation struct {
	headRTS        atomic.Uint64
	head           atomic.Uint32
	cachedProducer atomic.Uint32
}
type mpmcConsumerPublication struct {
	tailRTS   atomic.Uint64
	published atomic.Uint32
}

// MPMCQueue is a fixed-capacity FIFO for concurrent producers, consumers and observers.
// Construct it with NewMPMCQueue before synchronized publication. Its zero value is unusable.
// Do not copy a queue after use. Reset requires exclusive synchronized access against every
// operation and observer. Caller slices must remain valid for the call. Concurrent dequeue
// destinations must not overlap without caller synchronization.
//
// FIFO follows reservation order. Calls may return in any order. RTS producers can return
// before earlier reservations permit visibility. RTS consumers can return before earlier
// reservations permit reuse, so Count can include values already returned. Counts are bounded
// snapshots, not linearizable counts of immediately reservable entries or slots.
//
// CAS retries, publication waits and RTS distance waits are unbounded. A stalled reservation
// can block useful progress on both sides. MPMCQueue is neither wait-free nor formally lock-free.
// Platform 64-bit atomics may use locks. Ordinary uint32 rollover is supported. Each shared
// cache must lag publication by less than 2^32 - Capacity. No head/CAS observation may span a
// complete position cycle. RTS packed head, completion and mirror observations also require
// that no observation span a complete position or operation-counter cycle.
type MPMCQueue[T any] struct {
	config              mpmcConfig[T]
	_                   [128]byte
	producerReservation mpmcProducerReservation
	_                   [128]byte
	producerPublication mpmcProducerPublication
	_                   [128]byte
	consumerReservation mpmcConsumerReservation
	_                   [128]byte
	consumerPublication mpmcConsumerPublication
}

// NewMPMCQueue returns an empty queue using all count slots, with ordered producers and
// consumers, clearing enabled and cached batch counts. Count must be a power of two in
// [1, 2^30]. Invalid count wraps ErrInvalidCount before options execute. Batch limits default
// to count and spins default to 8192. Options apply in order and only final values matter.
// Both sides independently accept ordered and RTS selectors. Explicit distances require RTS
// on their corresponding side. Omitted RTS distances default to count / 8.
// Validation checks enqueue batch, dequeue batch, spin number, producer mode, consumer mode,
// spin applicability, producer distance, then consumer distance. Numeric distance validation
// precedes applicability. Shared options document the wrapped error sentinels.
func NewMPMCQueue[T any](count int, options ...Option) (*MPMCQueue[T], error) {
	cfg, err := resolveConfig("NewMPMCQueue", count, modeMulti, modeMulti, true, options)
	if err != nil {
		return nil, err
	}
	return &MPMCQueue[T]{config: mpmcConfig[T]{
		entries:         make([]T, count),
		mask:            uint32(count - 1),
		maxEnqueueBatch: uint32(cfg.maxEnqueueBatch),
		maxDequeueBatch: uint32(cfg.maxDequeueBatch),
		prodHTDMax: resolveHeadTailDistance(
			cfg.prodMode,
			count,
			cfg.maxProducerHeadTailDistance,
			cfg.producerHeadTailDistanceSet,
		),
		consHTDMax: resolveHeadTailDistance(
			cfg.consMode,
			count,
			cfg.maxConsumerHeadTailDistance,
			cfg.consumerHeadTailDistanceSet,
		),
		spinsBeforeYield: uint32(cfg.spinsBeforeYield),
		producerRelaxed:  cfg.prodMode == modeRelaxed,
		consumerRelaxed:  cfg.consMode == modeRelaxed,
		clearOnDequeue:   !cfg.keepDequeued,
		exactBatchCounts: cfg.exactBatchCounts,
	}}, nil
}

// Capacity returns the number of usable slots.
func (r *MPMCQueue[T]) Capacity() int { return int(r.config.mask + 1) }

// MaxEnqueueBatch returns the configured enqueue batch limit.
func (r *MPMCQueue[T]) MaxEnqueueBatch() int { return int(r.config.maxEnqueueBatch) }

// MaxDequeueBatch returns the configured dequeue batch limit.
func (r *MPMCQueue[T]) MaxDequeueBatch() int { return int(r.config.maxDequeueBatch) }

// Count returns a bounded snapshot of consumer then producer publication.
// It excludes unpublished producer reservations and can include returned consumer intervals.
func (r *MPMCQueue[T]) Count() int {
	consumer := r.consumerPublication.published.Load()
	producer := r.producerPublication.published.Load()
	return int(min(producer-consumer, r.config.mask+1))
}

// Free returns Capacity minus a Count snapshot.
func (r *MPMCQueue[T]) Free() int { return r.Capacity() - r.Count() }

// Full reports whether a Count snapshot equals Capacity.
func (r *MPMCQueue[T]) Full() bool { return r.Count() == r.Capacity() }

// Empty reports whether a Count snapshot is zero.
func (r *MPMCQueue[T]) Empty() bool { return r.Count() == 0 }

// Reset clears all slots and active and inactive protocol words, preserving storage and config.
// Callers must synchronize exclusive access against all operations and observers and synchronize
// publication to future users, even when dequeue clearing is disabled.
func (r *MPMCQueue[T]) Reset() {
	clear(r.config.entries)
	r.producerReservation.headRTS.Store(0)
	r.producerReservation.head.Store(0)
	r.producerReservation.cachedConsumer.Store(0)
	r.producerPublication.tailRTS.Store(0)
	r.producerPublication.published.Store(0)
	r.consumerReservation.headRTS.Store(0)
	r.consumerReservation.head.Store(0)
	r.consumerReservation.cachedProducer.Store(0)
	r.consumerPublication.tailRTS.Store(0)
	r.consumerPublication.published.Store(0)
}

// Enqueue appends item or returns ErrFull unwrapped. RTS can return before visibility.
func (r *MPMCQueue[T]) Enqueue(item T) error {
	start, count, _ := r.mpmcReserveProducer(1, true, false)
	if count == 0 {
		return ErrFull
	}
	producer := &r.config
	producer.entries[start&producer.mask] = item
	if producer.producerRelaxed {
		r.mpmcPublishProducerRelaxed()
	} else {
		r.mpmcPublishProducerOrdered(start, count)
	}
	return nil
}

// Dequeue removes the oldest reserved published value or returns zero and ErrEmpty unwrapped.
func (r *MPMCQueue[T]) Dequeue() (T, error) {
	start, count, _ := r.mpmcReserveConsumer(1, true, false)
	if count == 0 {
		var zero T
		return zero, ErrEmpty
	}
	consumer := &r.config
	index := start & consumer.mask
	value := consumer.entries[index]
	if consumer.clearOnDequeue {
		var zero T
		consumer.entries[index] = zero
	}
	if consumer.consumerRelaxed {
		r.mpmcReleaseConsumerRelaxed()
	} else {
		r.mpmcReleaseConsumerOrdered(start, count)
	}
	return value, nil
}

// EnqueueBulk appends all items or none. Length errors wrap ErrExceedsCapacity before
// ErrExceedsMaxBatch. Valid failure returns ErrFull unwrapped. Free follows the batch count
// policy. Zero length refreshes without reservation. RTS can wait before admission.
func (r *MPMCQueue[T]) EnqueueBulk(items []T) (free int, err error) {
	producer := &r.config
	capacity := producer.mask + 1
	if len(items) > int(capacity) || len(items) > int(producer.maxEnqueueBatch) {
		head := r.mpmcProducerHead()
		fresh := capacity + r.consumerPublication.published.Load() - head
		return int(min(fresh, capacity)), batchLengthError(
			"enqueue",
			len(items),
			producer.maxEnqueueBatch,
			capacity,
		)
	}
	start, count, remaining := r.mpmcReserveProducer(
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
	r.mpmcWriteProducer(start, items)
	if producer.producerRelaxed {
		r.mpmcPublishProducerRelaxed()
	} else {
		r.mpmcPublishProducerOrdered(start, count)
	}
	return int(remaining), nil
}

// EnqueueBurst appends the admitted prefix, clamped to MaxEnqueueBatch, and returns moved
// and remaining admission counts. Zero length refreshes without reservation.
func (r *MPMCQueue[T]) EnqueueBurst(items []T) (enqueued, free int) {
	producer := &r.config
	length := min(len(items), int(producer.maxEnqueueBatch))
	start, count, remaining := r.mpmcReserveProducer(
		uint32(length),
		false,
		producer.exactBatchCounts,
	)
	if count == 0 {
		return 0, int(remaining)
	}
	r.mpmcWriteProducer(start, items[:count])
	if producer.producerRelaxed {
		r.mpmcPublishProducerRelaxed()
	} else {
		r.mpmcPublishProducerOrdered(start, count)
	}
	return int(count), int(remaining)
}

// DequeueBulk fills dst in reservation order or preserves it entirely. Length errors wrap
// ErrExceedsCapacity before ErrExceedsMaxBatch. Valid failure returns ErrEmpty unwrapped.
// Available follows the batch count policy. Zero length refreshes without reservation.
func (r *MPMCQueue[T]) DequeueBulk(dst []T) (available int, err error) {
	consumer := &r.config
	capacity := consumer.mask + 1
	if len(dst) > int(capacity) || len(dst) > int(consumer.maxDequeueBatch) {
		head := r.mpmcConsumerHead()
		fresh := r.producerPublication.published.Load() - head
		return int(min(fresh, capacity)), batchLengthError(
			"dequeue",
			len(dst),
			consumer.maxDequeueBatch,
			capacity,
		)
	}
	start, count, remaining := r.mpmcReserveConsumer(
		uint32(len(dst)),
		true,
		consumer.exactBatchCounts,
	)
	if count == 0 {
		if len(dst) == 0 {
			return int(remaining), nil
		}
		return int(remaining), ErrEmpty
	}
	r.mpmcReadConsumer(start, dst)
	if consumer.consumerRelaxed {
		r.mpmcReleaseConsumerRelaxed()
	} else {
		r.mpmcReleaseConsumerOrdered(start, count)
	}
	return int(remaining), nil
}

// DequeueBurst fills the admitted prefix, clamped to MaxDequeueBatch, preserving the suffix.
// It returns moved and remaining admission counts. Zero length refreshes without reservation.
func (r *MPMCQueue[T]) DequeueBurst(dst []T) (dequeued, available int) {
	consumer := &r.config
	length := min(len(dst), int(consumer.maxDequeueBatch))
	start, count, remaining := r.mpmcReserveConsumer(
		uint32(length),
		false,
		consumer.exactBatchCounts,
	)
	if count == 0 {
		return 0, int(remaining)
	}
	r.mpmcReadConsumer(start, dst[:count])
	if consumer.consumerRelaxed {
		r.mpmcReleaseConsumerRelaxed()
	} else {
		r.mpmcReleaseConsumerOrdered(start, count)
	}
	return int(count), int(remaining)
}

func (r *MPMCQueue[T]) mpmcReserveProducer(
	requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32) {
	start, count, remaining, _ = r.mpmcReserveProducerLoop(0, requested, fixed, exact, true)
	return start, count, remaining
}

func (r *MPMCQueue[T]) mpmcAttemptProducer(
	word uint64,
	requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32, retry bool) {
	return r.mpmcReserveProducerLoop(word, requested, fixed, exact, false)
}

// A retrying reservation owns head capture and RTS distance rechecks. A captured-head caller
// owns renewal after a conflict.
func (r *MPMCQueue[T]) mpmcReserveProducerLoop(
	word uint64,
	requested uint32,
	fixed, exact, retryOnConflict bool,
) (start, count, remaining uint32, retry bool) {
	for {
		if retryOnConflict {
			if r.config.producerRelaxed {
				word = waitHeadTailDistance(
					&r.producerReservation.headRTS,
					&r.producerPublication.tailRTS,
					r.config.prodHTDMax,
					int(r.config.spinsBeforeYield),
				)
			} else {
				word = uint64(r.producerReservation.head.Load())
			}
		}
		head := uint32(word)
		if r.config.producerRelaxed {
			head = unpackPos(word)
		}
		capacity := r.config.mask + 1
		cached := r.producerReservation.cachedConsumer.Load()
		published := cached
		if exact || requested == 0 || head-cached > capacity-requested {
			published = r.consumerPublication.published.Load()
			if published != cached {
				r.producerReservation.cachedConsumer.Store(published)
			}
		}
		free := capacity + published - head
		if free > capacity {
			if retryOnConflict && requested != 0 {
				continue
			}
			return head, 0, capacity, requested != 0
		}
		if fixed && free < requested {
			return head, 0, free, false
		}
		count = min(requested, free)
		if count == 0 {
			return head, 0, free, false
		}
		if r.config.producerRelaxed {
			next := pack(head+count, unpackCnt(word)+1)
			if !r.producerReservation.headRTS.CompareAndSwap(word, next) {
				if retryOnConflict {
					continue
				}
				return head, 0, 0, true
			}
		} else if !r.producerReservation.head.CompareAndSwap(head, head+count) {
			if retryOnConflict {
				continue
			}
			return head, 0, 0, true
		}
		return head, count, free - count, false
	}
}

func (r *MPMCQueue[T]) mpmcWriteProducer(start uint32, items []T) {
	producer := &r.config
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

func (r *MPMCQueue[T]) mpmcPublishProducer(start, count uint32) {
	if r.config.producerRelaxed {
		r.mpmcPublishProducerRelaxed()
		return
	}
	r.mpmcPublishProducerOrdered(start, count)
}

func (r *MPMCQueue[T]) mpmcPublishProducerOrdered(start, count uint32) {
	waitForTail(&r.producerPublication.published, start, int(r.config.spinsBeforeYield))
	r.producerPublication.published.Store(start + count)
}

func (r *MPMCQueue[T]) mpmcPublishProducerRelaxed() {
	publication := &r.producerPublication
	if publishRelaxed(&r.producerReservation.headRTS, &publication.tailRTS) {
		raiseMirror(&publication.published, &publication.tailRTS)
	}
}

func (r *MPMCQueue[T]) mpmcProducerHead() uint32 {
	if r.config.producerRelaxed {
		return unpackPos(r.producerReservation.headRTS.Load())
	}
	return r.producerReservation.head.Load()
}

func (r *MPMCQueue[T]) mpmcReserveConsumer(
	requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32) {
	start, count, remaining, _ = r.mpmcReserveConsumerLoop(0, requested, fixed, exact, true)
	return start, count, remaining
}

func (r *MPMCQueue[T]) mpmcAttemptConsumer(
	word uint64,
	requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32, retry bool) {
	return r.mpmcReserveConsumerLoop(word, requested, fixed, exact, false)
}

// A retrying reservation owns head capture and RTS distance rechecks. A captured-head caller
// owns renewal after a conflict.
func (r *MPMCQueue[T]) mpmcReserveConsumerLoop(
	word uint64,
	requested uint32,
	fixed, exact, retryOnConflict bool,
) (start, count, remaining uint32, retry bool) {
	for {
		if retryOnConflict {
			if r.config.consumerRelaxed {
				word = waitHeadTailDistance(
					&r.consumerReservation.headRTS,
					&r.consumerPublication.tailRTS,
					r.config.consHTDMax,
					int(r.config.spinsBeforeYield),
				)
			} else {
				word = uint64(r.consumerReservation.head.Load())
			}
		}
		head := uint32(word)
		if r.config.consumerRelaxed {
			head = unpackPos(word)
		}
		capacity := r.config.mask + 1
		cached := r.consumerReservation.cachedProducer.Load()
		published := cached
		if exact || requested == 0 || cached-head-requested > capacity-requested {
			published = r.producerPublication.published.Load()
			if published != cached {
				r.consumerReservation.cachedProducer.Store(published)
			}
		}
		available := published - head
		if available > capacity {
			if retryOnConflict && requested != 0 {
				continue
			}
			return head, 0, capacity, requested != 0
		}
		if fixed && available < requested {
			return head, 0, available, false
		}
		count = min(requested, available)
		if count == 0 {
			return head, 0, available, false
		}
		if r.config.consumerRelaxed {
			next := pack(head+count, unpackCnt(word)+1)
			if !r.consumerReservation.headRTS.CompareAndSwap(word, next) {
				if retryOnConflict {
					continue
				}
				return head, 0, 0, true
			}
		} else if !r.consumerReservation.head.CompareAndSwap(head, head+count) {
			if retryOnConflict {
				continue
			}
			return head, 0, 0, true
		}
		return head, count, available - count, false
	}
}

func (r *MPMCQueue[T]) mpmcReadConsumer(start uint32, dst []T) {
	consumer := &r.config
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

func (r *MPMCQueue[T]) mpmcReleaseConsumer(start, count uint32) {
	if r.config.consumerRelaxed {
		r.mpmcReleaseConsumerRelaxed()
		return
	}
	r.mpmcReleaseConsumerOrdered(start, count)
}

func (r *MPMCQueue[T]) mpmcReleaseConsumerOrdered(start, count uint32) {
	waitForTail(&r.consumerPublication.published, start, int(r.config.spinsBeforeYield))
	r.consumerPublication.published.Store(start + count)
}

func (r *MPMCQueue[T]) mpmcReleaseConsumerRelaxed() {
	if publishRelaxed(&r.consumerReservation.headRTS, &r.consumerPublication.tailRTS) {
		raiseMirror(&r.consumerPublication.published, &r.consumerPublication.tailRTS)
	}
}

func (r *MPMCQueue[T]) mpmcConsumerHead() uint32 {
	if r.config.consumerRelaxed {
		return unpackPos(r.consumerReservation.headRTS.Load())
	}
	return r.consumerReservation.head.Load()
}
