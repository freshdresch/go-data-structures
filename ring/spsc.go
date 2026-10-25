package ring

import "sync/atomic"

type spscProducer[T any] struct {
	entries          []T
	mask             uint32
	cursor           uint32
	cachedConsumer   uint32
	maxBatch         uint32
	exactBatchCounts bool
	// The full gap separates private writes from remote-polled publication lines
	// at every base phase for line widths up to cacheLine.
	_         [cacheLine]byte
	published atomic.Uint32
}

type spscConsumer[T any] struct {
	entries          []T
	mask             uint32
	cursor           uint32
	cachedProducer   uint32
	clearOnDequeue   bool
	maxBatch         uint32
	exactBatchCounts bool
	// The full gap separates private writes from remote-polled publication lines
	// at every base phase for line widths up to cacheLine.
	_         [cacheLine]byte
	published atomic.Uint32
}

// SPSCQueue is a fixed-capacity FIFO with one serialized producer and one serialized consumer.
// Construct it with NewSPSCQueue. Its zero value is unusable, and it must not be copied after use.
// Each side serializes all its methods, including failed and zero-length calls that can refresh
// private caches. Ownership changes require caller synchronization, such as a channel or mutex.
// The opposite side and Count, Free, Full, and Empty may run concurrently. Construction must
// complete before synchronized publication to callers. Reset requires exclusive synchronized use.
// Capacity and batch-limit queries read immutable configuration and may also run concurrently.
type SPSCQueue[T any] struct {
	producer spscProducer[T]
	_        [128]byte
	consumer spscConsumer[T]
}

// NewSPSCQueue returns an empty queue using all count slots.
// Clearing is enabled and batch counts use cached admission by default.
// Count must be a power of two in [1, 2^30], otherwise it wraps ErrInvalidCount. Batch limits
// default to count. A final limit outside [1, count] wraps ErrInvalidMaxBatch. Shared options
// apply in order. Multi-side modes, explicit spins, and explicit distances are unsupported.
// Incompatible modes, explicit spins, and valid distances wrap ErrUnsupportedOption.
// Numeric spin errors wrap ErrInvalidSpinsBeforeYield, and numeric distance errors wrap
// ErrInvalidMaxHeadTailDistance. Validation checks batch limits and spin numbers before topology,
// then explicit spins and
// producer/consumer distances. Numeric distance errors precede distance applicability errors.
func NewSPSCQueue[T any](count int, options ...Option) (*SPSCQueue[T], error) {
	cfg, err := resolveConfig("NewSPSCQueue", count, modeSingle, modeSingle, true, options)
	if err != nil {
		return nil, err
	}
	entries := make([]T, count)
	queue := &SPSCQueue[T]{
		producer: spscProducer[T]{
			entries:          entries,
			mask:             uint32(count - 1),
			maxBatch:         uint32(cfg.maxEnqueueBatch),
			exactBatchCounts: cfg.exactBatchCounts,
		},
		consumer: spscConsumer[T]{
			entries:          entries,
			mask:             uint32(count - 1),
			clearOnDequeue:   !cfg.keepDequeued,
			maxBatch:         uint32(cfg.maxDequeueBatch),
			exactBatchCounts: cfg.exactBatchCounts,
		},
	}
	return queue, nil
}

// Capacity returns the number of usable slots.
func (r *SPSCQueue[T]) Capacity() int { return int(r.producer.mask + 1) }

// MaxEnqueueBatch returns the configured enqueue batch limit.
func (r *SPSCQueue[T]) MaxEnqueueBatch() int { return int(r.producer.maxBatch) }

// MaxDequeueBatch returns the configured dequeue batch limit.
func (r *SPSCQueue[T]) MaxDequeueBatch() int { return int(r.consumer.maxBatch) }

// Count returns a bounded two-publication snapshot. It is not a linearizable count query.
func (r *SPSCQueue[T]) Count() int {
	consumer := r.consumer.published.Load()
	producer := r.producer.published.Load()
	return int(min(producer-consumer, r.producer.mask+1))
}

// Free returns Capacity minus a Count snapshot.
func (r *SPSCQueue[T]) Free() int { return r.Capacity() - r.Count() }

// Full reports whether a Count snapshot equals Capacity.
func (r *SPSCQueue[T]) Full() bool { return r.Count() == r.Capacity() }

// Empty reports whether a Count snapshot is zero.
func (r *SPSCQueue[T]) Empty() bool { return r.Count() == 0 }

// Reset clears every slot and all positions and caches while preserving configuration.
// Callers must exclude operations and observers, synchronize completion of prior use, and
// synchronize publication to future users, even when dequeue clearing is disabled.
func (r *SPSCQueue[T]) Reset() {
	clear(r.producer.entries)
	r.producer.cursor = 0
	r.producer.cachedConsumer = 0
	r.producer.published.Store(0)
	r.consumer.cursor = 0
	r.consumer.cachedProducer = 0
	r.consumer.published.Store(0)
}

// Enqueue appends item or returns ErrFull unwrapped. It belongs to the serialized producer side.
func (r *SPSCQueue[T]) Enqueue(item T) error {
	producer := &r.producer
	capacity := producer.mask + 1
	free := capacity + producer.cachedConsumer - producer.cursor
	if free == 0 || free > capacity {
		producer.cachedConsumer = r.consumer.published.Load()
		free = capacity + producer.cachedConsumer - producer.cursor
		if free == 0 || free > capacity {
			return ErrFull
		}
	}
	producer.entries[producer.cursor&producer.mask] = item
	producer.cursor++
	producer.published.Store(producer.cursor)
	return nil
}

// Dequeue removes the oldest value or returns the zero value and ErrEmpty unwrapped.
// It belongs to the serialized consumer side and clears the slot before release when enabled.
func (r *SPSCQueue[T]) Dequeue() (T, error) {
	consumer := &r.consumer
	capacity := consumer.mask + 1
	available := consumer.cachedProducer - consumer.cursor
	if available == 0 || available > capacity {
		consumer.cachedProducer = r.producer.published.Load()
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
	consumer.cursor++
	consumer.published.Store(consumer.cursor)
	return value, nil
}

// EnqueueBulk appends all items or none. It checks capacity, batch limit, then room, returning
// wrapped ErrExceedsCapacity, wrapped ErrExceedsMaxBatch, or unwrapped ErrFull respectively.
// It returns post-call free slots on every path. Counts follow WithExactBatchCounts.
// A zero-length call refreshes its count without publication. This is a producer operation.
func (r *SPSCQueue[T]) EnqueueBulk(items []T) (free int, err error) {
	producer := &r.producer
	if len(items) > int(producer.maxBatch) {
		fresh := producer.mask + 1 + r.consumer.published.Load() - producer.cursor
		return int(min(fresh, producer.mask+1)), batchLengthError(
			"enqueue",
			len(items),
			producer.maxBatch,
			producer.mask+1,
		)
	}
	if len(items) == 1 {
		capacity := producer.mask + 1
		free := capacity + producer.cachedConsumer - producer.cursor
		if producer.exactBatchCounts || free == 0 || free > capacity {
			producer.cachedConsumer = r.consumer.published.Load()
			free = capacity + producer.cachedConsumer - producer.cursor
		}
		if free > capacity {
			return int(capacity), ErrFull
		}
		if free == 0 {
			return 0, ErrFull
		}
		producer.entries[producer.cursor&producer.mask] = items[0]
		producer.cursor++
		producer.published.Store(producer.cursor)
		return int(free - 1), nil
	}
	moved, remaining := r.enqueueBatch(items, true)
	if moved != len(items) {
		return remaining, ErrFull
	}
	return remaining, nil
}

// EnqueueBurst appends the available prefix, limited to MaxEnqueueBatch, and returns moved and
// post-call free counts. Counts follow WithExactBatchCounts. A zero-length call refreshes
// its count without publication. This is a producer operation.
func (r *SPSCQueue[T]) EnqueueBurst(items []T) (enqueued, free int) {
	length := min(len(items), int(r.producer.maxBatch))
	if length == 1 {
		producer := &r.producer
		capacity := producer.mask + 1
		free := capacity + producer.cachedConsumer - producer.cursor
		if producer.exactBatchCounts || free == 0 || free > capacity {
			producer.cachedConsumer = r.consumer.published.Load()
			free = capacity + producer.cachedConsumer - producer.cursor
		}
		if free > capacity {
			return 0, int(capacity)
		}
		if free == 0 {
			return 0, 0
		}
		producer.entries[producer.cursor&producer.mask] = items[0]
		producer.cursor++
		producer.published.Store(producer.cursor)
		return 1, int(free - 1)
	}
	return r.enqueueBatch(items[:length], false)
}

// DequeueBulk fills dst in FIFO order or leaves it entirely unchanged. It checks capacity,
// batch limit, then data, returning wrapped ErrExceedsCapacity, wrapped ErrExceedsMaxBatch, or
// unwrapped ErrEmpty respectively. It returns post-call available entries on every path.
// Counts follow WithExactBatchCounts. Zero length refreshes without publication.
// This is a consumer operation, including failures and zero-length calls.
func (r *SPSCQueue[T]) DequeueBulk(dst []T) (available int, err error) {
	consumer := &r.consumer
	if len(dst) > int(consumer.maxBatch) {
		fresh := r.producer.published.Load() - consumer.cursor
		return int(min(fresh, consumer.mask+1)), batchLengthError(
			"dequeue",
			len(dst),
			consumer.maxBatch,
			consumer.mask+1,
		)
	}
	if len(dst) == 1 {
		capacity := consumer.mask + 1
		available := consumer.cachedProducer - consumer.cursor
		if consumer.exactBatchCounts || available == 0 || available > capacity {
			consumer.cachedProducer = r.producer.published.Load()
			available = consumer.cachedProducer - consumer.cursor
		}
		if available > capacity {
			return int(capacity), ErrEmpty
		}
		if available == 0 {
			return 0, ErrEmpty
		}
		index := consumer.cursor & consumer.mask
		dst[0] = consumer.entries[index]
		if consumer.clearOnDequeue {
			var zero T
			consumer.entries[index] = zero
		}
		consumer.cursor++
		consumer.published.Store(consumer.cursor)
		return int(available - 1), nil
	}
	moved, remaining := r.dequeueBatch(dst, true)
	if moved != len(dst) {
		return remaining, ErrEmpty
	}
	return remaining, nil
}

// DequeueBurst fills only the available prefix of dst, limited to MaxDequeueBatch, and leaves
// the suffix unchanged. It returns moved and post-call available counts, following
// WithExactBatchCounts. Zero length refreshes without publication. This is a consumer call.
func (r *SPSCQueue[T]) DequeueBurst(dst []T) (dequeued, available int) {
	length := min(len(dst), int(r.consumer.maxBatch))
	if length == 1 {
		consumer := &r.consumer
		capacity := consumer.mask + 1
		available := consumer.cachedProducer - consumer.cursor
		if consumer.exactBatchCounts || available == 0 || available > capacity {
			consumer.cachedProducer = r.producer.published.Load()
			available = consumer.cachedProducer - consumer.cursor
		}
		if available > capacity {
			return 0, int(capacity)
		}
		if available == 0 {
			return 0, 0
		}
		index := consumer.cursor & consumer.mask
		dst[0] = consumer.entries[index]
		if consumer.clearOnDequeue {
			var zero T
			consumer.entries[index] = zero
		}
		consumer.cursor++
		consumer.published.Store(consumer.cursor)
		return 1, int(available - 1)
	}
	return r.dequeueBatch(dst[:length], false)
}

func (r *SPSCQueue[T]) enqueueBatch(items []T, fixed bool) (int, int) {
	producer := &r.producer
	capacity := producer.mask + 1
	length := uint32(len(items))
	free := capacity + producer.cachedConsumer - producer.cursor
	if producer.exactBatchCounts || length == 0 || free < length || free > capacity {
		producer.cachedConsumer = r.consumer.published.Load()
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
	first := min(length, capacity-base)
	copy(producer.entries[base:base+first], items[:first])
	copy(producer.entries[:length-first], items[first:length])
	producer.cursor += length
	producer.published.Store(producer.cursor)
	return int(length), int(free - length)
}

func (r *SPSCQueue[T]) dequeueBatch(dst []T, fixed bool) (int, int) {
	consumer := &r.consumer
	capacity := consumer.mask + 1
	length := uint32(len(dst))
	available := consumer.cachedProducer - consumer.cursor
	if consumer.exactBatchCounts || length == 0 || available < length || available > capacity {
		consumer.cachedProducer = r.producer.published.Load()
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
	base := consumer.cursor & consumer.mask
	first := min(length, capacity-base)
	copy(dst[:first], consumer.entries[base:base+first])
	copy(dst[first:length], consumer.entries[:length-first])
	if consumer.clearOnDequeue {
		clear(consumer.entries[base : base+first])
		clear(consumer.entries[:length-first])
	}
	consumer.cursor += length
	consumer.published.Store(consumer.cursor)
	return int(length), int(available - length)
}
