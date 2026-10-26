package frontier

import (
	"sync/atomic"

	"github.com/freshdresch/go-data-structures/ring/internal/shared"
)

type mpscProducerConfig[T any] struct {
	entries          []T
	mask             uint32
	maxBatch         uint32
	exactBatchCounts bool
}

type mpscProducerReservation struct {
	head           atomic.Uint64
	cachedConsumer atomic.Uint32
}

type mpscCompletion struct {
	records []atomic.Uint64
}

type mpscConsumer[T any] struct {
	entries          []T
	mask             uint32
	cursor           uint64
	frontier         uint64
	clearOnDequeue   bool
	maxBatch         uint32
	exactBatchCounts bool
}

// mpscBase is the transfer core of MPSCQueue: reservation, range publication, discovery, and
// release. MPSCQueue embeds it and adds the observer cache that keeps repeated Count calls cheap.
// It is exercised directly only by tests. Its own Count, Free, Full, Empty, and Reset are the
// uncached forms, which MPSCQueue shadows.
//
// Producers reserve a disjoint interval with one CAS on the 64-bit head, copy into it, and publish
// it by storing its absolute end in the record at its start. The consumer discovers the contiguous
// completed prefix by following those ends from its frontier. Records are never cleared during
// normal operation, because a record left from an earlier lap ends at or before its boundary and
// fails validation. No absolute position may overflow uint64 between construction or exclusive
// Resets. No producer CAS attempt or Count observation may span a complete 2^32 position cycle,
// and the shared release cache must lag the actual release by less than 2^32-Capacity.
// Zero-length and failed Bulk calls may advance the private frontier without releasing payloads.
type mpscBase[T any] struct {
	producerConfig      mpscProducerConfig[T]
	_                   [128]byte
	producerReservation mpscProducerReservation
	_                   [128]byte
	completion          mpscCompletion
	_                   [128]byte
	consumer            mpscConsumer[T]
	_                   [128]byte
	consumerPublished   atomic.Uint64
}

// newMPSCBase returns an empty transfer core using all count slots. Tests use it to exercise the
// core without the observer cache.
func newMPSCBase[T any](count int, options ...Option) (*mpscBase[T], error) {
	cfg, err := resolveConfig("newMPSCBase", count, options)
	if err != nil {
		return nil, err
	}
	r := &mpscBase[T]{}
	r.init(count, cfg)
	return r, nil
}

// init sets up an unpublished zero core for count slots under a validated configuration.
func (r *mpscBase[T]) init(count int, cfg config) {
	entries := make([]T, count)
	r.producerConfig = mpscProducerConfig[T]{
		entries:          entries,
		mask:             uint32(count - 1),
		maxBatch:         uint32(cfg.maxEnqueueBatch),
		exactBatchCounts: cfg.exactBatchCounts,
	}
	r.completion = mpscCompletion{records: make([]atomic.Uint64, count)}
	r.consumer = mpscConsumer[T]{
		entries:          entries,
		mask:             uint32(count - 1),
		clearOnDequeue:   !cfg.keepDequeued,
		maxBatch:         uint32(cfg.maxDequeueBatch),
		exactBatchCounts: cfg.exactBatchCounts,
	}
}

func mpscConsumerWord(cursor, frontier uint32) uint64 {
	return uint64(frontier)<<32 | uint64(cursor)
}

func mpscReleased(word uint64) uint32 { return uint32(word) }
func mpscFrontier(word uint64) uint32 { return uint32(word >> 32) }

// Capacity returns the number of usable slots.
func (r *mpscBase[T]) Capacity() int { return int(r.producerConfig.mask + 1) }

// MaxEnqueueBatch returns the configured enqueue batch limit.
func (r *mpscBase[T]) MaxEnqueueBatch() int { return int(r.producerConfig.maxBatch) }

// MaxDequeueBatch returns the configured dequeue batch limit.
func (r *mpscBase[T]) MaxDequeueBatch() int { return int(r.consumer.maxBatch) }

// Count scans the contiguous completed prefix while consumer state stays stable.
// If state changes, it returns only the latest acquired prefix, without retry or rescan.
// It excludes reservations behind a hole and is not a linearizable global snapshot.
func (r *mpscBase[T]) Count() int {
	return r.mpscCountFromSnapshot(r.consumerPublished.Load())
}

func (r *mpscBase[T]) mpscCountFromSnapshot(before uint64) int {
	cursorLow := mpscReleased(before)
	head := r.producerReservation.head.Load()
	distance := uint32(head) - cursorLow
	acquired := mpscFrontier(before) - cursorLow
	capacity := r.producerConfig.mask + 1
	result := uint64(min(acquired, capacity))
	if uint64(distance) <= head {
		cursor := head - uint64(distance)
		limit := min(uint64(distance), uint64(capacity))
		if uint64(acquired) <= limit {
			end := cursor + uint64(acquired)
			for end-cursor < limit {
				recordEnd := r.completion.records[uint32(end)&r.producerConfig.mask].Load()
				if recordEnd <= end || recordEnd-end > limit-(end-cursor) {
					break
				}
				end = recordEnd
			}
			result = end - cursor
		}
	}

	after := r.consumerPublished.Load()
	if before != after {
		return int(min(
			mpscFrontier(after)-mpscReleased(after),
			capacity,
		))
	}
	return int(result)
}

// Free returns Capacity minus a Count snapshot. Unpublished producer reservations can make
// this exceed immediately reservable space.
func (r *mpscBase[T]) Free() int { return r.Capacity() - r.Count() }

// Full reports whether a Count snapshot equals Capacity.
func (r *mpscBase[T]) Full() bool { return r.Count() == r.Capacity() }

// Empty reports whether a Count snapshot is zero.
func (r *mpscBase[T]) Empty() bool { return r.Count() == 0 }

// Reset clears every slot and all positions and caches while preserving configuration and storage.
// Callers must exclude operations and observers, synchronize completion of prior use, and
// synchronize publication to future users, even when dequeue clearing is disabled.
func (r *mpscBase[T]) Reset() {
	clear(r.producerConfig.entries)
	for index := range r.completion.records {
		r.completion.records[index].Store(0)
	}
	r.producerReservation.head.Store(0)
	r.producerReservation.cachedConsumer.Store(0)
	r.consumer.cursor = 0
	r.consumer.frontier = 0
	r.consumerPublished.Store(0)
}

// Enqueue appends item or returns ErrFull unwrapped. Concurrent producers reserve disjoint
// intervals and copy before completing publication. Later reservations may finish first.
func (r *mpscBase[T]) Enqueue(item T) error {
	start, count, _ := r.mpscReserveProducer(1, true, false)
	if count == 0 {
		return ErrFull
	}
	producer := &r.producerConfig
	producer.entries[uint32(start)&producer.mask] = item
	r.mpscPublishProducer(start, count)
	return nil
}

// Dequeue removes the oldest published value or returns zero and ErrEmpty unwrapped.
// It belongs to the serialized consumer side, including failed calls.
func (r *mpscBase[T]) Dequeue() (T, error) {
	consumer := &r.consumer
	if consumer.frontier == consumer.cursor {
		end := r.mpscDiscover(consumer.cursor, consumer.frontier, true, 1)
		consumer.frontier = end
		if consumer.frontier == consumer.cursor {
			var zero T
			return zero, ErrEmpty
		}
	}
	index := uint32(consumer.cursor) & consumer.mask
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
// Its free count follows WithExactBatchCounts. Zero length refreshes without reservation
// or publication. Producers may call it concurrently.
func (r *mpscBase[T]) EnqueueBulk(items []T) (free int, err error) {
	producer := &r.producerConfig
	capacity := producer.mask + 1
	if len(items) > int(capacity) || len(items) > int(producer.maxBatch) {
		head := r.producerReservation.head.Load()
		fresh := capacity + mpscReleased(r.consumerPublished.Load()) - uint32(head)
		return int(min(fresh, capacity)), shared.BatchLengthError(
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
func (r *mpscBase[T]) EnqueueBurst(items []T) (enqueued, free int) {
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
func (r *mpscBase[T]) DequeueBulk(dst []T) (available int, err error) {
	consumer := &r.consumer
	capacity := consumer.mask + 1
	if len(dst) > int(capacity) || len(dst) > int(consumer.maxBatch) {
		cursor := consumer.cursor
		fresh := r.mpscDiscover(cursor, consumer.frontier, false, 0) - cursor
		return int(min(fresh, uint64(capacity))), shared.BatchLengthError(
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
// unchanged. Its counts follow WithExactBatchCounts. Zero length may acquire a larger frontier
// without releasing payloads.
// This is a serialized consumer operation, including zero-length calls.
func (r *mpscBase[T]) DequeueBurst(dst []T) (dequeued, available int) {
	length := min(len(dst), int(r.consumer.maxBatch))
	return r.mpscDequeueBatch(dst[:length], false)
}

func (r *mpscBase[T]) mpscReserveProducer(
	requested uint32,
	fixed, exact bool,
) (start uint64, count, remaining uint32) {
	for {
		head := r.producerReservation.head.Load()
		start, count, remaining, retry := r.mpscAttemptProducer(
			head,
			requested,
			fixed,
			exact,
		)
		if !retry {
			return start, count, remaining
		}
	}
}

func (r *mpscBase[T]) mpscAttemptProducer(
	head uint64,
	requested uint32,
	fixed, exact bool,
) (start uint64, count, remaining uint32, retry bool) {
	capacity := r.producerConfig.mask + 1
	headLow := uint32(head)
	cached := r.producerReservation.cachedConsumer.Load()
	released := cached
	if exact || requested == 0 || headLow-cached > capacity-requested {
		released = mpscReleased(r.consumerPublished.Load())
	}
	free := capacity + released - headLow
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
	// Only an attempt that goes on to reserve shares its refreshed release. The cache shares the
	// head's cache line, so producers failing against a full ring would otherwise write the line
	// the other producers are trying to CAS on every failed attempt.
	if released != cached {
		r.producerReservation.cachedConsumer.Store(released)
	}
	if !r.producerReservation.head.CompareAndSwap(head, head+uint64(count)) {
		return head, 0, 0, true
	}
	return head, count, free - count, false
}

// mpscWriteProducer copies into an owned nonempty reservation.
// The caller must finish the complete copy before publishing it.
func (r *mpscBase[T]) mpscWriteProducer(start uint64, items []T) {
	producer := &r.producerConfig
	length := uint32(len(items))
	base := uint32(start) & producer.mask
	if length == 1 {
		producer.entries[base] = items[0]
		return
	}
	first := min(length, producer.mask+1-base)
	copy(producer.entries[base:base+first], items[:first])
	copy(producer.entries[:length-first], items[first:])
}

// mpscPublishProducer publishes a nonempty interval after all its writes.
func (r *mpscBase[T]) mpscPublishProducer(start uint64, count uint32) {
	r.completion.records[uint32(start)&r.producerConfig.mask].Store(start + uint64(count))
}

// mpscDiscover extends frontier through contiguous completed records, stopping at the first
// hole, the capacity window, or once requested fits when stopWhenRequestFits.
func (r *mpscBase[T]) mpscDiscover(
	cursor, frontier uint64,
	stopWhenRequestFits bool,
	requested uint32,
) uint64 {
	capacity := uint64(r.consumer.mask + 1)
	stopAt := capacity
	if stopWhenRequestFits {
		stopAt = uint64(requested)
	}
	return discoverRecords(r.completion.records, r.consumer.mask, cursor, capacity, frontier, stopAt)
}

func (r *mpscBase[T]) mpscDequeueBatch(dst []T, fixed bool) (int, int) {
	consumer := &r.consumer
	capacity := uint64(consumer.mask + 1)
	length := uint32(len(dst))
	previousFrontier := consumer.frontier
	available := consumer.frontier - consumer.cursor
	if consumer.exactBatchCounts || length == 0 || available < uint64(length) || available > capacity {
		consumer.frontier = r.mpscDiscover(consumer.cursor, consumer.frontier, false, 0)
		available = consumer.frontier - consumer.cursor
	}
	if available > capacity || (fixed && available < uint64(length)) || length == 0 || available == 0 {
		if consumer.frontier > previousFrontier {
			r.consumerPublished.Store(mpscConsumerWord(
				uint32(consumer.cursor),
				uint32(consumer.frontier),
			))
		}
		return 0, int(min(available, capacity))
	}
	length = min(length, uint32(available))
	r.mpscReadConsumer(consumer.cursor, dst[:length])
	r.mpscReleaseConsumer(length)
	return int(length), int(available - uint64(length))
}

// mpscReadConsumer fills dst and optionally clears an acquired interval.
// All reading and clearing must finish before release permits producer reuse.
func (r *mpscBase[T]) mpscReadConsumer(start uint64, dst []T) {
	consumer := &r.consumer
	capacity := consumer.mask + 1
	length := uint32(len(dst))
	base := uint32(start) & consumer.mask
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

// mpscReleaseConsumer publishes reuse after all payload reads and clearing.
// The consumer must finish reading and clearing the complete nonempty interval first.
func (r *mpscBase[T]) mpscReleaseConsumer(count uint32) {
	r.consumer.cursor += uint64(count)
	r.consumerPublished.Store(mpscConsumerWord(
		uint32(r.consumer.cursor),
		uint32(r.consumer.frontier),
	))
}
