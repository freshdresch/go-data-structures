package frontier

import (
	"sync/atomic"

	"github.com/freshdresch/go-data-structures/ring/internal/shared"
)

type spmcProducer[T any] struct {
	entries          []T
	mask             uint32
	cursor           uint64
	released         uint64
	maxBatch         uint32
	exactBatchCounts bool
}

type spmcConsumerConfig[T any] struct {
	entries          []T
	mask             uint32
	maxBatch         uint32
	clearOnDequeue   bool
	exactBatchCounts bool
}

type spmcConsumerReservation struct {
	head           atomic.Uint64
	cachedProducer atomic.Uint64
}

type spmcReleases struct {
	records []atomic.Uint64
}

// SPMCQueue is a fixed-capacity FIFO with one serialized producer and concurrent consumers.
// Construct it with NewSPMCQueue. Its zero value is unusable, and it must not be copied after use.
//
// Consumers never wait for one another. Each reserves an interval with one compare-and-swap,
// copies out of it, and releases it on its own. The producer reuses slots up to the first
// reservation that has not been released, so a returned dequeue does not mean its slots are
// reusable yet: a consumer stalled mid-dequeue holds back reuse of every slot released after it
// until it finishes. Consumer compare-and-swap retries have no fixed bound, so the queue is
// neither wait-free nor formally lock-free.
//
// Every producer call, including failed and zero-length calls, belongs to one serialized owner,
// and handing that role to another goroutine needs a happens-before edge such as a channel or
// mutex. Consumers, Count, Free, Full, Empty, and the configuration queries may run concurrently
// with both sides. Reset needs exclusive access against every operation and observer, and
// synchronized publication to later users.
//
// Positions are 64-bit, and no position may overflow between construction or Resets. Platforms
// may implement 64-bit atomics with locks.
type SPMCQueue[T any] struct {
	producer            spmcProducer[T]
	_                   [128]byte
	producerPublished   atomic.Uint64
	_                   [128]byte
	consumerConfig      spmcConsumerConfig[T]
	_                   [128]byte
	consumerReservation spmcConsumerReservation
	_                   [128]byte
	releases            spmcReleases
}

// NewSPMCQueue returns an empty queue using all count slots, with clearing enabled and cached
// batch counts. Count must be a power of two in [1, 2^30], otherwise it wraps ErrInvalidCount
// before applying options. Batch limits default to count, and invalid final limits wrap
// ErrInvalidMaxBatch.
func NewSPMCQueue[T any](count int, options ...Option) (*SPMCQueue[T], error) {
	cfg, err := resolveConfig("NewSPMCQueue", count, options)
	if err != nil {
		return nil, err
	}
	entries := make([]T, count)
	return &SPMCQueue[T]{
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
			clearOnDequeue:   !cfg.keepDequeued,
			exactBatchCounts: cfg.exactBatchCounts,
		},
		releases: spmcReleases{records: make([]atomic.Uint64, count)},
	}, nil
}

// Capacity returns the number of usable slots.
func (r *SPMCQueue[T]) Capacity() int { return int(r.producer.mask + 1) }

// MaxEnqueueBatch returns the configured enqueue batch limit.
func (r *SPMCQueue[T]) MaxEnqueueBatch() int { return int(r.producer.maxBatch) }

// MaxDequeueBatch returns the configured dequeue batch limit.
func (r *SPMCQueue[T]) MaxDequeueBatch() int { return int(r.consumerConfig.maxBatch) }

// Count returns how many published values no consumer has reserved yet, the backlog consumers can
// still claim. It excludes values consumers have reserved but not finished, so unlike
// MPSCQueue.Count it does not include slots awaiting release. It loads two positions and is a
// snapshot, not a linearizable count.
func (r *SPMCQueue[T]) Count() int {
	head := r.consumerReservation.head.Load()
	published := r.producerPublished.Load()
	return int(min(published-head, uint64(r.producer.mask+1)))
}

// Free returns Capacity minus Count. Reserved and unreleased intervals can make it exceed the room
// the producer can use immediately.
func (r *SPMCQueue[T]) Free() int { return r.Capacity() - r.Count() }

// Full reports whether Count equals Capacity.
func (r *SPMCQueue[T]) Full() bool { return r.Count() == r.Capacity() }

// Empty reports whether Count is zero.
func (r *SPMCQueue[T]) Empty() bool { return r.Count() == 0 }

// Reset clears every slot, release record, and position while preserving configuration and
// storage. Callers must exclude operations and observers, synchronize completion of prior use,
// and synchronize publication to future users, even when dequeue clearing is disabled.
func (r *SPMCQueue[T]) Reset() {
	clear(r.producer.entries)
	for index := range r.releases.records {
		r.releases.records[index].Store(0)
	}
	r.producer.cursor = 0
	r.producer.released = 0
	r.producerPublished.Store(0)
	r.consumerReservation.head.Store(0)
	r.consumerReservation.cachedProducer.Store(0)
}

// Enqueue appends item or returns ErrFull unwrapped. It belongs to the serialized producer side.
func (r *SPMCQueue[T]) Enqueue(item T) error {
	producer := &r.producer
	capacity := uint64(producer.mask + 1)
	if producer.cursor-producer.released >= capacity {
		r.spmcDiscoverReleases()
		if producer.cursor-producer.released >= capacity {
			return ErrFull
		}
	}
	producer.entries[uint32(producer.cursor)&producer.mask] = item
	producer.cursor++
	r.producerPublished.Store(producer.cursor)
	return nil
}

// Dequeue removes the oldest unreserved value or returns the zero value and ErrEmpty unwrapped.
// Consumers may call it concurrently.
func (r *SPMCQueue[T]) Dequeue() (T, error) {
	start, count, _ := r.spmcReserveConsumer(1, true, false)
	if count == 0 {
		var zero T
		return zero, ErrEmpty
	}
	consumer := &r.consumerConfig
	index := uint32(start) & consumer.mask
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
		r.spmcDiscoverReleases()
		fresh := uint64(capacity) - (producer.cursor - producer.released)
		return int(fresh), shared.BatchLengthError(
			"enqueue",
			len(items),
			producer.maxBatch,
			capacity,
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
// Zero length refreshes without reservation or release. Consumers may call it concurrently.
func (r *SPMCQueue[T]) DequeueBulk(dst []T) (available int, err error) {
	consumer := &r.consumerConfig
	capacity := consumer.mask + 1
	if len(dst) > int(capacity) || len(dst) > int(consumer.maxBatch) {
		head := r.consumerReservation.head.Load()
		fresh := r.producerPublished.Load() - head
		return int(min(fresh, uint64(capacity))), shared.BatchLengthError(
			"dequeue",
			len(dst),
			consumer.maxBatch,
			capacity,
		)
	}
	start, count, remaining := r.spmcReserveConsumer(
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
	r.spmcReadConsumer(start, dst)
	r.spmcReleaseConsumer(start, count)
	return int(remaining), nil
}

// DequeueBurst fills only the available prefix of dst, limited to MaxDequeueBatch, and leaves
// the suffix unchanged. Its moved and available counts follow WithExactBatchCounts.
// Zero length refreshes without reservation or release. Consumers may call it concurrently.
func (r *SPMCQueue[T]) DequeueBurst(dst []T) (dequeued, available int) {
	consumer := &r.consumerConfig
	length := min(len(dst), int(consumer.maxBatch))
	start, count, remaining := r.spmcReserveConsumer(
		uint32(length),
		false,
		consumer.exactBatchCounts,
	)
	if count == 0 {
		return 0, int(remaining)
	}
	r.spmcReadConsumer(start, dst[:count])
	r.spmcReleaseConsumer(start, count)
	return int(count), int(remaining)
}

// spmcDiscoverReleases advances the producer's released frontier through the contiguous run of
// released intervals. No consumer can reserve past the producer's cursor, so the cursor bounds
// discovery. It belongs to the serialized producer side.
func (r *SPMCQueue[T]) spmcDiscoverReleases() {
	producer := &r.producer
	window := producer.cursor - producer.released
	producer.released = discoverRecords(
		r.releases.records,
		producer.mask,
		producer.released,
		window,
		producer.released,
		window,
	)
}

func (r *SPMCQueue[T]) spmcEnqueueBatch(items []T, fixed bool) (int, int) {
	producer := &r.producer
	capacity := uint64(producer.mask + 1)
	length := uint64(len(items))
	free := capacity - (producer.cursor - producer.released)
	if producer.exactBatchCounts || length == 0 || free < length {
		r.spmcDiscoverReleases()
		free = capacity - (producer.cursor - producer.released)
	}
	if fixed && free < length {
		return 0, int(free)
	}
	length = min(length, free)
	if length == 0 {
		return 0, int(free)
	}
	base := uint32(producer.cursor) & producer.mask
	if length == 1 {
		producer.entries[base] = items[0]
	} else {
		first := min(uint32(length), producer.mask+1-base)
		copy(producer.entries[base:base+first], items[:first])
		copy(producer.entries[:uint32(length)-first], items[first:length])
	}
	producer.cursor += length
	r.producerPublished.Store(producer.cursor)
	return int(length), int(free - length)
}

func (r *SPMCQueue[T]) spmcReserveConsumer(
	requested uint32,
	fixed, exact bool,
) (start uint64, count, remaining uint32) {
	for {
		head := r.consumerReservation.head.Load()
		start, count, remaining, retry := r.spmcAttemptConsumer(head, requested, fixed, exact)
		if !retry {
			return start, count, remaining
		}
	}
}

// spmcAttemptConsumer admits and attempts one reservation using a previously loaded head. The
// caller must reload head after any retry, including a stale out-of-range observation.
func (r *SPMCQueue[T]) spmcAttemptConsumer(
	head uint64,
	requested uint32,
	fixed, exact bool,
) (start uint64, count, remaining uint32, retry bool) {
	capacity := uint64(r.consumerConfig.mask + 1)
	cached := r.consumerReservation.cachedProducer.Load()
	published := cached
	if exact || requested == 0 || cached < head || cached-head < uint64(requested) {
		published = r.producerPublished.Load()
	}
	available := published - head
	// A head loaded before other consumers advanced it past a lap of reuse sees more than a
	// capacity of values, so it reloads.
	if available > capacity {
		return head, 0, uint32(capacity), requested != 0
	}
	if fixed && available < uint64(requested) {
		return head, 0, uint32(available), false
	}
	count = uint32(min(uint64(requested), available))
	if count == 0 {
		return head, 0, uint32(available), false
	}
	// Only an attempt that goes on to reserve shares its refreshed position, so consumers
	// polling an empty queue never write the line the others are trying to CAS.
	if published != cached {
		r.consumerReservation.cachedProducer.Store(published)
	}
	if !r.consumerReservation.head.CompareAndSwap(head, head+uint64(count)) {
		return head, 0, 0, true
	}
	return head, count, uint32(available) - count, false
}

// spmcReadConsumer fills dst from an owned nonempty reservation and optionally clears it. The
// caller must finish reading and clearing before releasing the reservation.
func (r *SPMCQueue[T]) spmcReadConsumer(start uint64, dst []T) {
	consumer := &r.consumerConfig
	length := uint32(len(dst))
	base := uint32(start) & consumer.mask
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

// spmcReleaseConsumer releases a nonempty reservation after all its reading and clearing by
// recording its absolute end at its start.
func (r *SPMCQueue[T]) spmcReleaseConsumer(start uint64, count uint32) {
	r.releases.records[uint32(start)&r.consumerConfig.mask].Store(start + uint64(count))
}
