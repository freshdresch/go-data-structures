package ring

import (
	"fmt"
	"sync/atomic"
)

type mpscProducerConfig[T any] struct {
	entries          []T
	mask             uint32
	maxBatch         uint32
	spinsBeforeYield uint32
	exactBatchCounts bool
}

type mpscProducerReservation struct {
	head           atomic.Uint32
	cachedConsumer atomic.Uint32
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

// MPSC is a fixed-capacity FIFO with concurrent producers and one serialized consumer.
// Construct it with NewMPSC. Its zero value is unusable, and it must not be copied after use.
// Construction must complete before synchronized publication to callers. Every consumer call,
// including failed and zero-length calls, belongs to one serialized owner. Owner handoff
// requires caller synchronization, such as a channel or mutex. Producers, Count observers,
// and immutable configuration queries may run concurrently with both sides. Reset requires
// exclusive synchronized use against all operations and observers.
//
// Producer reservation order controls FIFO and ordered publication. Calls may return in any
// order. A stalled reservation can block useful progress on both sides. Producer CAS retries
// and publication waits have no fixed bound. MPSC is neither wait-free nor formally lock-free.
// Ordinary uint32 rollover is supported, but the shared cache must lag consumer publication
// by less than 2^32 - Capacity, and a producer head/CAS attempt must not span a complete
// 32-bit position cycle.
type MPSC[T any] struct {
	producerConfig      mpscProducerConfig[T]
	_                   [cacheLine]byte
	producerReservation mpscProducerReservation
	_                   [cacheLine]byte
	producerPublished   atomic.Uint32
	_                   [cacheLine]byte
	consumer            mpscConsumer[T]
	_                   [cacheLine]byte
	consumerPublished   atomic.Uint32
}

type mpscConfig struct {
	maxEnqueueBatch  int
	maxDequeueBatch  int
	keepDequeued     bool
	exactBatchCounts bool
	spinsBeforeYield int
}

// MPSCOption configures an MPSC during construction. Options apply in argument order.
type MPSCOption func(*mpscConfig)

// WithMPSCMaxBatch limits both batch directions to n. Later directional options override it.
// NewMPSC wraps ErrInvalidMaxBatch if a final limit is outside [1, capacity].
func WithMPSCMaxBatch(n int) MPSCOption {
	return func(cfg *mpscConfig) {
		cfg.maxEnqueueBatch = n
		cfg.maxDequeueBatch = n
	}
}

// WithMPSCMaxEnqueueBatch limits enqueue batches to n. Bulk rejects longer requests and burst
// clamps them. Single enqueue is unaffected. The last setting for this direction applies.
func WithMPSCMaxEnqueueBatch(n int) MPSCOption {
	return func(cfg *mpscConfig) { cfg.maxEnqueueBatch = n }
}

// WithMPSCMaxDequeueBatch limits dequeue batches to n. Bulk rejects longer requests and burst
// clamps them. Single dequeue is unaffected. The last setting for this direction applies.
func WithMPSCMaxDequeueBatch(n int) MPSCOption {
	return func(cfg *mpscConfig) { cfg.maxDequeueBatch = n }
}

// WithoutMPSCDequeueClearing retains consumed values until overwritten or Reset clears them.
func WithoutMPSCDequeueClearing() MPSCOption {
	return func(cfg *mpscConfig) { cfg.keepDequeued = true }
}

// WithMPSCExactBatchCounts makes each batch admission attempt load the opposite publication.
// Every batch count is its admission snapshot minus progress, bounded to [0, Capacity]. Neither
// exact counts nor Count are a global snapshot at return. Concurrent reservations can change
// availability. Cached dequeue counts can under-report producer progress. Zero-length batches
// always refresh. Single methods retain cached admission. Counts must not drive parking,
// wakeups, or subsequent request sizes.
func WithMPSCExactBatchCounts() MPSCOption {
	return func(cfg *mpscConfig) { cfg.exactBatchCounts = true }
}

// WithMPSCSpinsBeforeYield sets the tight-spin bound for ordered producer publication waits.
// Zero yields on every unsuccessful poll. NewMPSC wraps ErrInvalidSpinsBeforeYield for a final
// bound outside [0, 2^31-1]. Yielding does not guarantee fairness or completion.
func WithMPSCSpinsBeforeYield(n int) MPSCOption {
	return func(cfg *mpscConfig) { cfg.spinsBeforeYield = n }
}

// NewMPSC returns an empty queue using all count slots, with clearing enabled and cached counts.
// Count must be a power of two in [1, 2^30], otherwise it wraps ErrInvalidCount before applying
// options. Batch limits default to count and spins default to defaultSpinsBeforeYield. Final
// enqueue and dequeue limits outside [1, count] wrap ErrInvalidMaxBatch, in that order, before
// validating the spin bound. The ownership and synchronization requirements are documented on MPSC.
func NewMPSC[T any](count int, options ...MPSCOption) (*MPSC[T], error) {
	inRange := count >= 1 && count <= maxCount
	isPowerOfTwo := count&(count-1) == 0
	if !inRange || !isPowerOfTwo {
		return nil, fmt.Errorf("new ring with count %d: %w", count, ErrInvalidCount)
	}
	cfg := mpscConfig{
		maxEnqueueBatch:  count,
		maxDequeueBatch:  count,
		spinsBeforeYield: defaultSpinsBeforeYield,
	}
	for _, option := range options {
		option(&cfg)
	}
	if err := checkMaxBatch("enqueue", cfg.maxEnqueueBatch, count); err != nil {
		return nil, err
	}
	if err := checkMaxBatch("dequeue", cfg.maxDequeueBatch, count); err != nil {
		return nil, err
	}
	if err := checkSpinsBeforeYield(cfg.spinsBeforeYield, count); err != nil {
		return nil, err
	}
	entries := make([]T, count)
	queue := &MPSC[T]{
		producerConfig: mpscProducerConfig[T]{
			entries:          entries,
			mask:             uint32(count - 1),
			maxBatch:         uint32(cfg.maxEnqueueBatch),
			spinsBeforeYield: uint32(cfg.spinsBeforeYield),
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
func (r *MPSC[T]) Capacity() int { return int(r.producerConfig.mask + 1) }

// MaxEnqueueBatch returns the configured enqueue batch limit.
func (r *MPSC[T]) MaxEnqueueBatch() int { return int(r.producerConfig.maxBatch) }

// MaxDequeueBatch returns the configured dequeue batch limit.
func (r *MPSC[T]) MaxDequeueBatch() int { return int(r.consumer.maxBatch) }

// Count returns a bounded two-publication snapshot, excluding unpublished producer reservations.
// It is not a linearizable count of immediately reservable entries or a global snapshot at return.
func (r *MPSC[T]) Count() int {
	consumer := r.consumerPublished.Load()
	producer := r.producerPublished.Load()
	return int(min(producer-consumer, r.producerConfig.mask+1))
}

// Free returns Capacity minus a Count snapshot. Unpublished producer reservations can make
// this exceed immediately reservable space.
func (r *MPSC[T]) Free() int { return r.Capacity() - r.Count() }

// Full reports whether a Count snapshot equals Capacity.
func (r *MPSC[T]) Full() bool { return r.Count() == r.Capacity() }

// Empty reports whether a Count snapshot is zero.
func (r *MPSC[T]) Empty() bool { return r.Count() == 0 }

// Reset clears every slot and all positions and caches while preserving configuration and storage.
// Callers must exclude operations and observers, synchronize completion of prior use, and
// synchronize publication to future users, even when dequeue clearing is disabled.
func (r *MPSC[T]) Reset() {
	clear(r.producerConfig.entries)
	r.producerReservation.head.Store(0)
	r.producerReservation.cachedConsumer.Store(0)
	r.producerPublished.Store(0)
	r.consumer.cursor = 0
	r.consumer.cachedProducer = 0
	r.consumerPublished.Store(0)
}

// Enqueue appends item or returns ErrFull unwrapped. Concurrent producers reserve disjoint
// intervals, copy their values, and wait for earlier reservations before publishing.
func (r *MPSC[T]) Enqueue(item T) error {
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
func (r *MPSC[T]) Dequeue() (T, error) {
	consumer := &r.consumer
	capacity := consumer.mask + 1
	available := consumer.cachedProducer - consumer.cursor
	if available == 0 || available > capacity {
		consumer.cachedProducer = r.producerPublished.Load()
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
// Its free count follows WithMPSCExactBatchCounts. Zero length refreshes without reservation
// or publication. Producers may call it concurrently.
func (r *MPSC[T]) EnqueueBulk(items []T) (free int, err error) {
	producer := &r.producerConfig
	capacity := producer.mask + 1
	if len(items) > int(capacity) || len(items) > int(producer.maxBatch) {
		head := r.producerReservation.head.Load()
		fresh := capacity + r.consumerPublished.Load() - head
		return int(min(fresh, capacity)), batchLengthError(
			"enqueue", len(items), producer.maxBatch, capacity,
		)
	}
	start, count, remaining := r.mpscReserveProducer(
		uint32(len(items)), true, producer.exactBatchCounts,
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
// counts follow WithMPSCExactBatchCounts. Zero length refreshes without reservation or
// publication. Producers may call it concurrently.
func (r *MPSC[T]) EnqueueBurst(items []T) (enqueued, free int) {
	producer := &r.producerConfig
	length := min(len(items), int(producer.maxBatch))
	start, count, remaining := r.mpscReserveProducer(
		uint32(length), false, producer.exactBatchCounts,
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
// or unwrapped ErrEmpty respectively. Its available count follows WithMPSCExactBatchCounts.
// This is a serialized consumer operation, including failed and zero-length calls.
func (r *MPSC[T]) DequeueBulk(dst []T) (available int, err error) {
	consumer := &r.consumer
	capacity := consumer.mask + 1
	if len(dst) > int(capacity) || len(dst) > int(consumer.maxBatch) {
		cursor := consumer.cursor
		fresh := r.producerPublished.Load() - cursor
		return int(min(fresh, capacity)), batchLengthError(
			"dequeue", len(dst), consumer.maxBatch, capacity,
		)
	}
	moved, remaining := r.mpscDequeueBatch(dst, true)
	if moved != len(dst) {
		return remaining, ErrEmpty
	}
	return remaining, nil
}

// DequeueBurst fills the available prefix, limited to MaxDequeueBatch, and leaves the suffix
// unchanged. Its counts follow WithMPSCExactBatchCounts. Zero length refreshes without release.
// This is a serialized consumer operation, including zero-length calls.
func (r *MPSC[T]) DequeueBurst(dst []T) (dequeued, available int) {
	length := min(len(dst), int(r.consumer.maxBatch))
	return r.mpscDequeueBatch(dst[:length], false)
}

func (r *MPSC[T]) mpscReserveProducer(
	requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32) {
	for {
		head := r.producerReservation.head.Load()
		start, count, remaining, retry := r.mpscAttemptProducer(head, requested, fixed, exact)
		if !retry {
			return start, count, remaining
		}
	}
}

// mpscAttemptProducer admits and attempts one reservation using a previously loaded head.
// The caller must load head before this attempt and reload it after any retry, including
// a stale out-of-range observation.
func (r *MPSC[T]) mpscAttemptProducer(
	head, requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32, retry bool) {
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
	if !r.producerReservation.head.CompareAndSwap(head, head+count) {
		return head, 0, 0, true
	}
	return head, count, free - count, false
}

// mpscWriteProducer writes an already reserved interval. The caller must own the complete
// nonempty interval and finish copying before publishing it.
func (r *MPSC[T]) mpscWriteProducer(start uint32, items []T) {
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
func (r *MPSC[T]) mpscPublishProducer(start, count uint32) {
	waitForTail(&r.producerPublished, start, int(r.producerConfig.spinsBeforeYield))
	r.producerPublished.Store(start + count)
}

func (r *MPSC[T]) mpscDequeueBatch(dst []T, fixed bool) (int, int) {
	consumer := &r.consumer
	capacity := consumer.mask + 1
	length := uint32(len(dst))
	available := consumer.cachedProducer - consumer.cursor
	if consumer.exactBatchCounts || length == 0 || available < length || available > capacity {
		consumer.cachedProducer = r.producerPublished.Load()
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
func (r *MPSC[T]) mpscReadConsumer(start uint32, dst []T) {
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
func (r *MPSC[T]) mpscReleaseConsumer(count uint32) {
	r.consumer.cursor += count
	r.consumerPublished.Store(r.consumer.cursor)
}
