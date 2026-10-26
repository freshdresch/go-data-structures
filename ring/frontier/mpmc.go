package frontier

import (
	"runtime"
	"sync/atomic"

	"github.com/freshdresch/go-data-structures/ring/internal/shared"
)

type mpmcConfig[T any] struct {
	entries          []T
	mask             uint32
	maxEnqueueBatch  uint32
	maxDequeueBatch  uint32
	clearOnDequeue   bool
	exactBatchCounts bool
	completions      []atomic.Uint64
	releases         []atomic.Uint64
}

// MPMCQueue is a fixed-capacity FIFO with concurrent producers and concurrent consumers.
// Construct it with NewMPMCQueue. Its zero value is unusable, and it must not be copied after use.
//
// Neither side waits for earlier callers on its side. Each caller reserves an interval with one
// compare-and-swap, copies, and records the interval's end on its own, and FIFO follows
// reservation order. While an earlier producer is unfinished, a returned enqueue's values may not
// be visible to consumers yet. While an earlier consumer is unfinished, a returned dequeue's slots
// may not be reusable yet. Producers and consumers call runtime.Gosched before every retry.
//
// A retry means another caller on that side moved its head, so the queue as a whole makes
// progress, but a single caller's retries have no fixed bound, so the queue is not wait-free. A
// producer stalled between its reservation and its completion record holds back consumers past it,
// and a consumer stalled between its reservation and its release record holds back reuse past it,
// so the queue is not lock-free either.
//
// The enqueue methods can advance the shared released frontier, and the dequeue methods, Count,
// Free, Full, and Empty can advance the shared completed frontier. Both advance by
// compare-and-swap, including in calls that reserve nothing.
//
// Every call may run concurrently with every other call. Reset needs exclusive access against
// every operation and observer, and synchronized publication to later users.
//
// Positions are 64-bit, and no position may overflow between construction or Resets. Platforms
// may implement 64-bit atomics with locks.
type MPMCQueue[T any] struct {
	config       mpmcConfig[T]
	_            [128]byte
	producerHead atomic.Uint64
	_            [128]byte
	completed    atomic.Uint64
	_            [128]byte
	consumerHead atomic.Uint64
	_            [128]byte
	released     atomic.Uint64
}

// NewMPMCQueue returns an empty queue using all count slots, with clearing enabled. Count must be a
// power of two in [1, 2^30], otherwise it wraps ErrInvalidCount before applying options. Batch
// limits default to count, and invalid final limits wrap ErrInvalidMaxBatch.
func NewMPMCQueue[T any](count int, options ...Option) (*MPMCQueue[T], error) {
	cfg, err := resolveConfig("NewMPMCQueue", count, options)
	if err != nil {
		return nil, err
	}
	return &MPMCQueue[T]{
		config: mpmcConfig[T]{
			entries:          make([]T, count),
			mask:             uint32(count - 1),
			maxEnqueueBatch:  uint32(cfg.maxEnqueueBatch),
			maxDequeueBatch:  uint32(cfg.maxDequeueBatch),
			clearOnDequeue:   !cfg.keepDequeued,
			exactBatchCounts: cfg.exactBatchCounts,
			completions:      make([]atomic.Uint64, count),
			releases:         make([]atomic.Uint64, count),
		},
	}, nil
}

// Capacity returns the number of usable slots.
func (r *MPMCQueue[T]) Capacity() int { return int(r.config.mask + 1) }

// MaxEnqueueBatch returns the configured enqueue batch limit.
func (r *MPMCQueue[T]) MaxEnqueueBatch() int { return int(r.config.maxEnqueueBatch) }

// MaxDequeueBatch returns the configured dequeue batch limit.
func (r *MPMCQueue[T]) MaxDequeueBatch() int { return int(r.config.maxDequeueBatch) }

// Count returns how many published values no consumer has reserved yet, the backlog consumers can
// still claim. It excludes values consumers have reserved but not finished, and it stops at the
// first reservation a producer has not finished. It follows the completion records past the
// completed frontier and can advance that frontier by compare-and-swap. It is a snapshot, not a
// linearizable count.
func (r *MPMCQueue[T]) Count() int {
	// The head is loaded first, so the completed frontier never trails it and the difference
	// cannot wrap.
	head := r.consumerHead.Load()
	frontier := r.mpmcAdvanceCompleted()
	return int(min(frontier-head, uint64(r.config.mask+1)))
}

// Free returns Capacity minus Count, and can advance the completed frontier as Count does.
// Unfinished producer reservations, the values behind them, and unreleased consumer reservations
// can make it exceed the room producers can use immediately.
func (r *MPMCQueue[T]) Free() int { return r.Capacity() - r.Count() }

// Full reports whether Count equals Capacity, and can advance the completed frontier as Count does.
func (r *MPMCQueue[T]) Full() bool { return r.Count() == r.Capacity() }

// Empty reports whether Count is zero, and can advance the completed frontier as Count does.
func (r *MPMCQueue[T]) Empty() bool { return r.Count() == 0 }

// Reset clears every slot, both record arrays, and all four positions while preserving
// configuration and storage. Callers must exclude operations and observers, synchronize completion
// of prior use, and synchronize publication to future users, even when dequeue clearing is
// disabled.
func (r *MPMCQueue[T]) Reset() {
	clear(r.config.entries)
	for index := range r.config.completions {
		r.config.completions[index].Store(0)
	}
	for index := range r.config.releases {
		r.config.releases[index].Store(0)
	}
	r.producerHead.Store(0)
	r.completed.Store(0)
	r.consumerHead.Store(0)
	r.released.Store(0)
}

// Enqueue appends item or returns ErrFull unwrapped. Producers may call it concurrently.
func (r *MPMCQueue[T]) Enqueue(item T) error {
	start, count, _ := r.mpmcReserveProducer(1, true, false)
	if count == 0 {
		return ErrFull
	}
	r.config.entries[uint32(start)&r.config.mask] = item
	r.mpmcPublishCompletion(start, count)
	return nil
}

// Dequeue removes the oldest unreserved value or returns the zero value and ErrEmpty unwrapped.
// Consumers may call it concurrently.
func (r *MPMCQueue[T]) Dequeue() (T, error) {
	start, count, _ := r.mpmcReserveConsumer(1, true, false)
	if count == 0 {
		var zero T
		return zero, ErrEmpty
	}
	config := &r.config
	index := uint32(start) & config.mask
	value := config.entries[index]
	if config.clearOnDequeue {
		var zero T
		config.entries[index] = zero
	}
	r.mpmcPublishRelease(start, count)
	return value, nil
}

// EnqueueBulk appends all items or none. It checks capacity, batch limit, then room, returning
// wrapped ErrExceedsCapacity, wrapped ErrExceedsMaxBatch, or unwrapped ErrFull respectively.
// Its free count follows WithExactBatchCounts. An invalid length returns a fresh free count, and
// zero length discovers releases and reserves nothing. Both can advance the released frontier.
// Producers may call it concurrently.
func (r *MPMCQueue[T]) EnqueueBulk(items []T) (free int, err error) {
	config := &r.config
	capacity := config.mask + 1
	if len(items) > int(capacity) || len(items) > int(config.maxEnqueueBatch) {
		_, _, remaining := r.mpmcReserveProducer(0, true, false)
		return int(remaining), shared.BatchLengthError(
			"enqueue",
			len(items),
			config.maxEnqueueBatch,
			capacity,
		)
	}
	start, count, remaining := r.mpmcReserveProducer(
		uint32(len(items)),
		true,
		config.exactBatchCounts,
	)
	if count == 0 {
		if len(items) == 0 {
			return int(remaining), nil
		}
		return int(remaining), ErrFull
	}
	r.mpmcWriteProducer(start, items)
	r.mpmcPublishCompletion(start, count)
	return int(remaining), nil
}

// EnqueueBurst appends the available prefix, limited to MaxEnqueueBatch, and returns moved and
// free counts following WithExactBatchCounts. Zero length discovers releases, which can advance
// the released frontier, and reserves nothing. Producers may call it concurrently.
func (r *MPMCQueue[T]) EnqueueBurst(items []T) (enqueued, free int) {
	config := &r.config
	length := min(len(items), int(config.maxEnqueueBatch))
	start, count, remaining := r.mpmcReserveProducer(
		uint32(length),
		false,
		config.exactBatchCounts,
	)
	if count == 0 {
		return 0, int(remaining)
	}
	r.mpmcWriteProducer(start, items[:count])
	r.mpmcPublishCompletion(start, count)
	return int(count), int(remaining)
}

// DequeueBulk fills dst in FIFO order or leaves it entirely unchanged. It checks capacity,
// batch limit, then data, returning wrapped ErrExceedsCapacity, wrapped ErrExceedsMaxBatch, or
// unwrapped ErrEmpty respectively. Its available count follows WithExactBatchCounts. An invalid
// length returns a fresh available count, and zero length discovers completions and reserves
// nothing. Both can advance the completed frontier. Consumers may call it concurrently.
func (r *MPMCQueue[T]) DequeueBulk(dst []T) (available int, err error) {
	config := &r.config
	capacity := config.mask + 1
	if len(dst) > int(capacity) || len(dst) > int(config.maxDequeueBatch) {
		_, _, remaining := r.mpmcReserveConsumer(0, true, false)
		return int(remaining), shared.BatchLengthError(
			"dequeue",
			len(dst),
			config.maxDequeueBatch,
			capacity,
		)
	}
	start, count, remaining := r.mpmcReserveConsumer(
		uint32(len(dst)),
		true,
		config.exactBatchCounts,
	)
	if count == 0 {
		if len(dst) == 0 {
			return int(remaining), nil
		}
		return int(remaining), ErrEmpty
	}
	r.mpmcReadConsumer(start, dst)
	r.mpmcPublishRelease(start, count)
	return int(remaining), nil
}

// DequeueBurst fills only the available prefix of dst, limited to MaxDequeueBatch, and leaves
// the suffix unchanged. Its moved and available counts follow WithExactBatchCounts. Zero length
// discovers completions, which can advance the completed frontier, and reserves nothing.
// Consumers may call it concurrently.
func (r *MPMCQueue[T]) DequeueBurst(dst []T) (dequeued, available int) {
	config := &r.config
	length := min(len(dst), int(config.maxDequeueBatch))
	start, count, remaining := r.mpmcReserveConsumer(
		uint32(length),
		false,
		config.exactBatchCounts,
	)
	if count == 0 {
		return 0, int(remaining)
	}
	r.mpmcReadConsumer(start, dst[:count])
	r.mpmcPublishRelease(start, count)
	return int(count), int(remaining)
}

func (r *MPMCQueue[T]) mpmcReserveProducer(
	requested uint32,
	fixed, exact bool,
) (start uint64, count, remaining uint32) {
	for {
		head := r.producerHead.Load()
		start, count, remaining, retry := r.mpmcAttemptProducer(head, requested, fixed, exact)
		if !retry {
			return start, count, remaining
		}
		// Another producer moved the head first. Yielding takes the loser off the head's cache
		// line while the winner finishes, as consumers do. Unlike in MPSCQueue, this measurably
		// speeds up MPMC single-element transfers.
		runtime.Gosched()
	}
}

// mpmcAttemptProducer admits and attempts one producer reservation using a previously loaded
// head. The caller must reload head after any retry.
func (r *MPMCQueue[T]) mpmcAttemptProducer(
	head uint64,
	requested uint32,
	fixed, exact bool,
) (start uint64, count, remaining uint32, retry bool) {
	capacity := uint64(r.config.mask + 1)
	released := r.released.Load()
	// A fast path for a head that other producers moved past released positions, which also
	// keeps the free count below from wrapping.
	if released > head {
		return head, 0, 0, true
	}
	free := capacity - (head - released)
	if exact || requested == 0 || free < uint64(requested) {
		released = r.mpmcAdvanceReleased()
		// Discovery can raise the released frontier past a head more than a lap stale, and this
		// check keeps a count from coming from such a head.
		if released > head {
			return head, 0, 0, true
		}
		free = capacity - (head - released)
	}
	if fixed && free < uint64(requested) {
		return head, 0, uint32(free), false
	}
	count = uint32(min(uint64(requested), free))
	if count == 0 {
		return head, 0, uint32(free), false
	}
	if !r.producerHead.CompareAndSwap(head, head+uint64(count)) {
		return head, 0, 0, true
	}
	return head, count, uint32(free) - count, false
}

func (r *MPMCQueue[T]) mpmcReserveConsumer(
	requested uint32,
	fixed, exact bool,
) (start uint64, count, remaining uint32) {
	for {
		head := r.consumerHead.Load()
		start, count, remaining, retry := r.mpmcAttemptConsumer(head, requested, fixed, exact)
		if !retry {
			return start, count, remaining
		}
		// Another consumer moved the head first. Retrying at once keeps every losing consumer on
		// the head's cache line, and yielding takes the loser off the line while the winner
		// finishes, as in SPMCQueue.
		runtime.Gosched()
	}
}

// mpmcAttemptConsumer admits and attempts one consumer reservation using a previously loaded
// head. The caller must reload head after any retry.
func (r *MPMCQueue[T]) mpmcAttemptConsumer(
	head uint64,
	requested uint32,
	fixed, exact bool,
) (start uint64, count, remaining uint32, retry bool) {
	capacity := uint64(r.config.mask + 1)
	frontier := r.completed.Load()
	// A fast path for a head more than a lap stale, which also keeps the available count below
	// within the capacity.
	if frontier-head > capacity {
		return head, 0, 0, true
	}
	available := frontier - head
	if exact || requested == 0 || available < uint64(requested) {
		frontier = r.mpmcAdvanceCompleted()
		// Discovery can raise the completed frontier more than a lap past a stale head, and this
		// check keeps a count from coming from such a head.
		if frontier-head > capacity {
			return head, 0, 0, true
		}
		available = frontier - head
	}
	if fixed && available < uint64(requested) {
		return head, 0, uint32(available), false
	}
	count = uint32(min(uint64(requested), available))
	if count == 0 {
		return head, 0, uint32(available), false
	}
	if !r.consumerHead.CompareAndSwap(head, head+uint64(count)) {
		return head, 0, 0, true
	}
	return head, count, uint32(available) - count, false
}

// mpmcAdvanceCompleted raises the completed frontier through the contiguous run of completed
// producer reservations and returns the frontier freshly loaded.
func (r *MPMCQueue[T]) mpmcAdvanceCompleted() uint64 {
	start := r.completed.Load()
	// The bound is loaded after the frontier, so it is at least the frontier.
	bound := r.producerHead.Load()
	end := mpmcDiscover(r.config.completions, r.config.mask, start, bound)
	return raiseFrontier(&r.completed, start, end)
}

// mpmcAdvanceReleased raises the released frontier through the contiguous run of released
// consumer reservations and returns the frontier freshly loaded.
func (r *MPMCQueue[T]) mpmcAdvanceReleased() uint64 {
	start := r.released.Load()
	// The bound is loaded after the frontier, so it is at least the frontier.
	bound := r.consumerHead.Load()
	end := mpmcDiscover(r.config.releases, r.config.mask, start, bound)
	return raiseFrontier(&r.released, start, end)
}

// mpmcDiscover follows records from start toward bound and returns the end of the contiguous run
// of finished intervals. The window is capped at the capacity so that a record from a later lap,
// which a stale caller can load, fails validation.
func mpmcDiscover(records []atomic.Uint64, mask uint32, start, bound uint64) uint64 {
	window := min(bound-start, uint64(mask)+1)
	return discoverRecords(records, mask, start, window, start, window)
}

// raiseFrontier raises frontier from current to end unless another caller already raised it at
// least that far, and returns the frontier freshly loaded. A failed compare-and-swap must not end
// the loop, because a caller that proved a shorter range can win it, and the loser would then
// report less than it proved.
func raiseFrontier(frontier *atomic.Uint64, current, end uint64) uint64 {
	for current < end {
		if frontier.CompareAndSwap(current, end) {
			break
		}
		current = frontier.Load()
	}
	return frontier.Load()
}

// mpmcWriteProducer fills an owned nonempty producer reservation from items. The caller must
// finish writing before publishing the reservation's completion.
func (r *MPMCQueue[T]) mpmcWriteProducer(start uint64, items []T) {
	config := &r.config
	length := uint32(len(items))
	base := uint32(start) & config.mask
	if length == 1 {
		config.entries[base] = items[0]
		return
	}
	first := min(length, config.mask+1-base)
	copy(config.entries[base:base+first], items[:first])
	copy(config.entries[:length-first], items[first:])
}

// mpmcReadConsumer fills dst from an owned nonempty consumer reservation and optionally clears
// it. The caller must finish reading and clearing before releasing the reservation.
func (r *MPMCQueue[T]) mpmcReadConsumer(start uint64, dst []T) {
	config := &r.config
	length := uint32(len(dst))
	base := uint32(start) & config.mask
	if length == 1 {
		dst[0] = config.entries[base]
		if config.clearOnDequeue {
			var zero T
			config.entries[base] = zero
		}
		return
	}
	first := min(length, config.mask+1-base)
	copy(dst[:first], config.entries[base:base+first])
	copy(dst[first:], config.entries[:length-first])
	if config.clearOnDequeue {
		clear(config.entries[base : base+first])
		clear(config.entries[:length-first])
	}
}

// mpmcPublishCompletion completes a nonempty producer reservation after all its writing by
// recording its absolute end at its start.
func (r *MPMCQueue[T]) mpmcPublishCompletion(start uint64, count uint32) {
	r.config.completions[uint32(start)&r.config.mask].Store(start + uint64(count))
}

// mpmcPublishRelease releases a nonempty consumer reservation after all its reading and clearing
// by recording its absolute end at its start.
func (r *MPMCQueue[T]) mpmcPublishRelease(start uint64, count uint32) {
	r.config.releases[uint32(start)&r.config.mask].Store(start + uint64(count))
}
