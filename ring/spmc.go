package ring

import (
	"fmt"
	"sync/atomic"
)

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
	clearOnDequeue   bool
	exactBatchCounts bool
}

type spmcConsumerReservation struct {
	head           atomic.Uint32
	cachedProducer atomic.Uint32
}

// SPMC is a fixed-capacity FIFO with one serialized producer and concurrent consumers.
// Construct it with NewSPMC. Its zero value is unusable, and it must not be copied after use.
// Construction must complete before synchronized publication to callers. Every producer call,
// including failed and zero-length calls, belongs to one serialized owner. Owner handoff
// requires caller synchronization, such as a channel or mutex. Consumers, Count observers,
// and immutable configuration queries may run concurrently with both sides. Reset requires
// exclusive synchronized use against all operations and observers.
//
// Reservation order controls consumer release and FIFO within each reserved interval.
// Calls may return in any order. A stalled reservation can block useful progress on both sides.
// Consumer CAS retries and ordered release waits have no fixed bound. Ordinary uint32 rollover
// is supported, but the shared cache must lag producer publication by less than 2^32 - Capacity,
// and a consumer head/CAS attempt must not span a complete 32-bit position cycle.
type SPMC[T any] struct {
	producer            spmcProducer[T]
	_                   [cacheLine]byte
	producerPublished   atomic.Uint32
	_                   [cacheLine]byte
	consumerConfig      spmcConsumerConfig[T]
	_                   [cacheLine]byte
	consumerReservation spmcConsumerReservation
	_                   [cacheLine]byte
	consumerPublished   atomic.Uint32
}

type spmcConfig struct {
	maxEnqueueBatch  int
	maxDequeueBatch  int
	keepDequeued     bool
	exactBatchCounts bool
	spinsBeforeYield int
}

// SPMCOption configures an SPMC during construction. Options apply in argument order.
type SPMCOption func(*spmcConfig)

// WithSPMCMaxBatch limits both batch directions to n. Later directional options override it.
// NewSPMC wraps ErrInvalidMaxBatch if a final limit is outside [1, capacity].
func WithSPMCMaxBatch(n int) SPMCOption {
	return func(cfg *spmcConfig) {
		cfg.maxEnqueueBatch = n
		cfg.maxDequeueBatch = n
	}
}

// WithSPMCMaxEnqueueBatch limits enqueue batches to n. Bulk rejects longer requests and burst
// clamps them. Single enqueue is unaffected. The last setting for this direction applies.
func WithSPMCMaxEnqueueBatch(n int) SPMCOption {
	return func(cfg *spmcConfig) { cfg.maxEnqueueBatch = n }
}

// WithSPMCMaxDequeueBatch limits dequeue batches to n. Bulk rejects longer requests and burst
// clamps them. Single dequeue is unaffected. The last setting for this direction applies.
func WithSPMCMaxDequeueBatch(n int) SPMCOption {
	return func(cfg *spmcConfig) { cfg.maxDequeueBatch = n }
}

// WithoutSPMCDequeueClearing retains consumed values until overwritten or Reset clears them.
func WithoutSPMCDequeueClearing() SPMCOption {
	return func(cfg *spmcConfig) { cfg.keepDequeued = true }
}

// WithSPMCExactBatchCounts makes each batch admission attempt load the opposite publication.
// Every batch count is its admission snapshot minus progress, bounded to [0, Capacity]. Neither
// exact counts nor Count are a global snapshot at return. Concurrent reservations can change
// availability. Cached producer counts can under-report consumer progress. Zero-length batches
// always refresh. Single methods retain cached admission. Counts must not drive parking,
// wakeups, or subsequent request sizes.
func WithSPMCExactBatchCounts() SPMCOption {
	return func(cfg *spmcConfig) { cfg.exactBatchCounts = true }
}

// WithSPMCSpinsBeforeYield sets the tight-spin bound for ordered consumer release waits.
// Zero yields on every unsuccessful poll. NewSPMC wraps ErrInvalidSpinsBeforeYield for a final
// bound outside [0, 2^31-1]. Yielding does not guarantee fairness or completion.
func WithSPMCSpinsBeforeYield(n int) SPMCOption {
	return func(cfg *spmcConfig) { cfg.spinsBeforeYield = n }
}

// NewSPMC returns an empty queue using all count slots, with clearing enabled and cached counts.
// Count must be a power of two in [1, 2^30], otherwise it wraps ErrInvalidCount before applying
// options. Batch limits default to count and spins default to defaultSpinsBeforeYield. Final
// enqueue and dequeue limits outside [1, count] wrap ErrInvalidMaxBatch, in that order, before
// validating the spin bound. The ownership and synchronization requirements are documented on SPMC.
func NewSPMC[T any](count int, options ...SPMCOption) (*SPMC[T], error) {
	inRange := count >= 1 && count <= maxCount
	isPowerOfTwo := count&(count-1) == 0
	if !inRange || !isPowerOfTwo {
		return nil, fmt.Errorf("new ring with count %d: %w", count, ErrInvalidCount)
	}
	cfg := spmcConfig{
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
	queue := &SPMC[T]{
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
			clearOnDequeue:   !cfg.keepDequeued,
			exactBatchCounts: cfg.exactBatchCounts,
		},
	}
	return queue, nil
}

// Capacity returns the number of usable slots.
func (r *SPMC[T]) Capacity() int { return int(r.producer.mask + 1) }

// MaxEnqueueBatch returns the configured enqueue batch limit.
func (r *SPMC[T]) MaxEnqueueBatch() int { return int(r.producer.maxBatch) }

// MaxDequeueBatch returns the configured dequeue batch limit.
func (r *SPMC[T]) MaxDequeueBatch() int { return int(r.consumerConfig.maxBatch) }

// Count returns a bounded two-publication snapshot, including reservations not yet released.
// It is not a linearizable count of immediately reservable entries or a global snapshot at return.
func (r *SPMC[T]) Count() int {
	consumer := r.consumerPublished.Load()
	producer := r.producerPublished.Load()
	return int(min(producer-consumer, r.producer.mask+1))
}

// Free returns Capacity minus a Count snapshot.
func (r *SPMC[T]) Free() int { return r.Capacity() - r.Count() }

// Full reports whether a Count snapshot equals Capacity.
func (r *SPMC[T]) Full() bool { return r.Count() == r.Capacity() }

// Empty reports whether a Count snapshot is zero.
func (r *SPMC[T]) Empty() bool { return r.Count() == 0 }

// Reset clears every slot and all positions and caches while preserving configuration and storage.
// Callers must exclude operations and observers, synchronize completion of prior use, and
// synchronize publication to future users, even when dequeue clearing is disabled.
func (r *SPMC[T]) Reset() {
	clear(r.producer.entries)
	r.producer.cursor = 0
	r.producer.cachedConsumer = 0
	r.producerPublished.Store(0)
	r.consumerReservation.head.Store(0)
	r.consumerReservation.cachedProducer.Store(0)
	r.consumerPublished.Store(0)
}

// Enqueue appends item or returns ErrFull unwrapped. It belongs to the serialized producer side.
func (r *SPMC[T]) Enqueue(item T) error {
	producer := &r.producer
	capacity := producer.mask + 1
	free := capacity + producer.cachedConsumer - producer.cursor
	if free == 0 || free > capacity {
		producer.cachedConsumer = r.consumerPublished.Load()
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
// releases after reading and optional clearing, and callers may return out of reservation order.
func (r *SPMC[T]) Dequeue() (T, error) {
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
// Its free count follows WithSPMCExactBatchCounts. Zero length refreshes without publication.
// This is a producer operation, including failed and zero-length calls.
func (r *SPMC[T]) EnqueueBulk(items []T) (free int, err error) {
	producer := &r.producer
	capacity := producer.mask + 1
	if len(items) > int(capacity) || len(items) > int(producer.maxBatch) {
		cursor := producer.cursor
		fresh := capacity + r.consumerPublished.Load() - cursor
		return int(min(fresh, capacity)), spmcLengthError(
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
// free counts following WithSPMCExactBatchCounts. Zero length refreshes without publication.
// This is a producer operation, including zero-length calls.
func (r *SPMC[T]) EnqueueBurst(items []T) (enqueued, free int) {
	length := min(len(items), int(r.producer.maxBatch))
	return r.spmcEnqueueBatch(items[:length], false)
}

// DequeueBulk fills dst in FIFO order or leaves it entirely unchanged. It checks capacity,
// batch limit, then data, returning wrapped ErrExceedsCapacity, wrapped ErrExceedsMaxBatch, or
// unwrapped ErrEmpty respectively. Its available count follows WithSPMCExactBatchCounts.
// Zero length refreshes without reservation or publication. Consumers may call it concurrently.
func (r *SPMC[T]) DequeueBulk(dst []T) (available int, err error) {
	consumer := &r.consumerConfig
	capacity := consumer.mask + 1
	if len(dst) > int(capacity) || len(dst) > int(consumer.maxBatch) {
		head := r.consumerReservation.head.Load()
		fresh := r.producerPublished.Load() - head
		return int(min(fresh, capacity)), spmcLengthError(
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
// the suffix unchanged. Its moved and available counts follow WithSPMCExactBatchCounts.
// Zero length refreshes without reservation or publication. Consumers may call it concurrently.
func (r *SPMC[T]) DequeueBurst(dst []T) (dequeued, available int) {
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

func spmcLengthError(op string, length int, maxBatch, capacity uint32) error {
	if length > int(capacity) {
		return fmt.Errorf(
			"%s %d elements, capacity %d: %w",
			op, length, capacity, ErrExceedsCapacity,
		)
	}
	return fmt.Errorf(
		"%s %d elements, max %s batch %d: %w",
		op, length, op, maxBatch, ErrExceedsMaxBatch,
	)
}

func (r *SPMC[T]) spmcEnqueueBatch(items []T, fixed bool) (int, int) {
	producer := &r.producer
	capacity := producer.mask + 1
	length := uint32(len(items))
	free := capacity + producer.cachedConsumer - producer.cursor
	if producer.exactBatchCounts || length == 0 || free < length || free > capacity {
		producer.cachedConsumer = r.consumerPublished.Load()
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

func (r *SPMC[T]) spmcReserveConsumer(
	requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32) {
	for {
		head := r.consumerReservation.head.Load()
		start, count, remaining, retry := r.spmcAttemptConsumer(head, requested, fixed, exact)
		if !retry {
			return start, count, remaining
		}
	}
}

// spmcAttemptConsumer admits and attempts one reservation using a previously loaded head.
// The caller must load head before this attempt and reload it after any retry, including
// a stale out-of-range observation.
func (r *SPMC[T]) spmcAttemptConsumer(
	head, requested uint32,
	fixed, exact bool,
) (start, count, remaining uint32, retry bool) {
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
	if !r.consumerReservation.head.CompareAndSwap(head, head+count) {
		return head, 0, 0, true
	}
	return head, count, available - count, false
}

// spmcReadConsumer modifies dst and its already reserved backing slots when clearing is enabled.
// The caller must own the complete nonempty interval and finish movement before releasing it.
func (r *SPMC[T]) spmcReadConsumer(start uint32, dst []T) {
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
func (r *SPMC[T]) spmcReleaseConsumer(start, count uint32) {
	waitForTail(&r.consumerPublished, start, int(r.consumerConfig.spinsBeforeYield))
	r.consumerPublished.Store(start + count)
}
