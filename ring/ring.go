// Package ring provides a fixed-size, lock-free ring buffer modeled on DPDK's rte_ring.
package ring

import (
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
)

// ErrInvalidCount is returned by New, wrapped, when the count is not a power of two, is less
// than 1, or exceeds 2^30.
var ErrInvalidCount = errors.New("ring: count must be a power of two between 1 and 2^30")

// ErrFull is returned unwrapped by Enqueue and EnqueueBulk when the ring does not have enough
// free slots for the request. The caller may retry.
var ErrFull = errors.New("ring: not enough free slots")

// ErrEmpty is returned unwrapped by Dequeue and DequeueBulk when the ring does not have enough
// occupied entries for the request. The caller may retry.
var ErrEmpty = errors.New("ring: not enough occupied entries")

// ErrExceedsCapacity is returned wrapped by EnqueueBulk when len(items) exceeds the ring's
// capacity, and by DequeueBulk when numElems does. Such a request can never succeed, so retrying
// it is pointless.
var ErrExceedsCapacity = errors.New("ring: requested count exceeds the ring's capacity")

// ErrInvalidMaxBatch is returned by New, wrapped, when a maximum enqueue or dequeue batch size,
// set with WithMaxBatch, WithMaxEnqueueBatch, or WithMaxDequeueBatch, is below 1 or above the
// count.
var ErrInvalidMaxBatch = errors.New("ring: max batch must be between 1 and the ring's count")

// ErrExceedsMaxBatch is returned wrapped by EnqueueBulk when len(items) exceeds the ring's maximum
// enqueue batch size, and by DequeueBulk when numElems exceeds its maximum dequeue batch size,
// while still fitting the capacity. Such a request can never succeed, so retrying it is
// pointless.
var ErrExceedsMaxBatch = errors.New("ring: requested count exceeds the ring's maximum batch size")

// Ring is a Go ring implementation heavily inspired by rte_ring from DPDK. It provides a
// fixed-size ring buffer that can function as anything between an SPSC and MPMC queue.
//
// However, there is a notable difference in that Go does not provide the relaxed memory model,
// acquire/release fences, and ordering guarantees that the DPDK C implementation leverages. We
// try to be as performant as we can, in the face of working only in the realm of sequential
// consistency.
//
// The ring defaults to an SPSC queue in the base implementation, and multi-producer/multi-consumer
// semantics can be added through functional options. For example: to create an MPMC queue,
// initialize the ring with both the multi-producer and multi-consumer options.
//
// It can incur a non-trivial performance hit in order to hold truly generic elements, so if
// bleeding-edge performance is desired, stick to the types required by the DPDK ring buffer
// implementation: a pointer, or 4-byte boundary aligned element. Otherwise, an element could be
// split between adjacent cache lines.
//
// The ring holds exactly count elements, where count is the count given to New.
type Ring[T any] struct {
	entries []T

	prodHead *uint32 // The head index for the producer: used for reservation.
	prodTail *uint32 // The tail index for the producer: used for publication.

	consHead *uint32 // The head index for the consumer: used for reservation.
	consTail *uint32 // The tail index for the consumer: used for publication.

	mask uint32 // Mask for bounding the index of the ring.

	maxEnqueueBatch uint32 // The most elements one enqueue batch call moves, at most the capacity.
	maxDequeueBatch uint32 // The most elements one dequeue batch call moves, at most the capacity.

	multiProd bool // Multi-Producer enqueue instead of single.
	multiCons bool // Multi-Consumer dequeue instead of single.

	_ [2]byte // padding for better cache alignment
}

type config struct {
	multiProd       bool
	multiCons       bool
	maxEnqueueBatch int
	maxDequeueBatch int
}

// Option configures a Ring at construction.
type Option func(*config)

// WithMultiProdEnqueue returns an Option that makes the enqueue methods safe for concurrent use by
// multiple producers.
func WithMultiProdEnqueue() Option {
	return func(cfg *config) {
		cfg.multiProd = true
	}
}

// WithMultiConsDequeue returns an Option that makes the dequeue methods safe for concurrent use by
// multiple consumers.
func WithMultiConsDequeue() Option {
	return func(cfg *config) {
		cfg.multiCons = true
	}
}

// WithMaxEnqueueBatch returns an Option that limits every enqueue batch call to at most n
// elements. EnqueueBurst moves at most n, and EnqueueBulk rejects a request for more than n with
// ErrExceedsMaxBatch. Enqueue moves one element, so the limit never affects it. Without a limit
// the enqueue batch may be as large as the capacity. New returns an error wrapping
// ErrInvalidMaxBatch when the limit is below 1 or above the count.
//
// The limit bounds how long one enqueue can make others wait. With multiple producers, a producer
// publishes only after every producer that reserved before it has published, so a long batch
// delays every producer that reserved after it until its copy finishes. With one producer or
// many, a batch is published all at once, so consumers see none of it until its whole copy is
// done. Choose n from the element size and the delay the ring's users can tolerate.
//
// When options set the same limit more than once, the last one given applies. New checks only the
// limit that applies.
func WithMaxEnqueueBatch(n int) Option {
	return func(cfg *config) {
		cfg.maxEnqueueBatch = n
	}
}

// WithMaxDequeueBatch returns an Option that limits every dequeue batch call to at most n
// elements. DequeueBurst moves at most n, and DequeueBulk rejects a request for more than n with
// ErrExceedsMaxBatch. Dequeue moves one element, so the limit never affects it. Without a limit
// the dequeue batch may be as large as the capacity. New returns an error wrapping
// ErrInvalidMaxBatch when the limit is below 1 or above the count.
//
// The limit bounds how long one dequeue can make others wait. With multiple consumers, a consumer
// releases its slots only after every consumer that reserved before it has released theirs, so a
// long batch delays every consumer that reserved after it until its copy finishes. With one
// consumer or many, a batch's slots are released all at once, so producers cannot reuse any of
// them until its whole copy is done. Choose n from the element size and the delay the ring's users
// can tolerate.
//
// When options set the same limit more than once, the last one given applies. New checks only the
// limit that applies.
func WithMaxDequeueBatch(n int) Option {
	return func(cfg *config) {
		cfg.maxDequeueBatch = n
	}
}

// WithMaxBatch returns an Option that sets both the maximum enqueue batch and the maximum dequeue
// batch to n. It is shorthand for WithMaxEnqueueBatch(n) and WithMaxDequeueBatch(n) together. When
// options set the same limit more than once, the last one given applies, so a later
// WithMaxEnqueueBatch or WithMaxDequeueBatch overrides this for its side.
func WithMaxBatch(n int) Option {
	return func(cfg *config) {
		cfg.maxEnqueueBatch = n
		cfg.maxDequeueBatch = n
	}
}

// maxCount is the largest count New accepts, the largest power of two DPDK accepts.
const maxCount = 1 << 30

// New returns an empty Ring with count slots. A ring of count n holds n elements. Without options,
// the ring allows one producer and one consumer at a time, and a batch call may move up to the
// whole capacity. It returns an error wrapping ErrInvalidCount when count is not a power of two
// between 1 and 2^30, and one wrapping ErrInvalidMaxBatch when a maximum enqueue or dequeue batch
// is below 1 or above count.
func New[T any](count int, options ...Option) (*Ring[T], error) {
	inRange := count >= 1 && count <= maxCount
	isPowerOfTwo := count&(count-1) == 0
	if !inRange || !isPowerOfTwo {
		return nil, fmt.Errorf("new ring with count %d: %w", count, ErrInvalidCount)
	}

	cfg := config{maxEnqueueBatch: count, maxDequeueBatch: count}
	for _, option := range options {
		option(&cfg)
	}
	if err := checkMaxBatch("enqueue", cfg.maxEnqueueBatch, count); err != nil {
		return nil, err
	}
	if err := checkMaxBatch("dequeue", cfg.maxDequeueBatch, count); err != nil {
		return nil, err
	}

	ring := &Ring[T]{
		entries:         make([]T, count),
		mask:            uint32(count - 1),
		maxEnqueueBatch: uint32(cfg.maxEnqueueBatch),
		maxDequeueBatch: uint32(cfg.maxDequeueBatch),
		prodHead:        new(uint32),
		prodTail:        new(uint32),
		consHead:        new(uint32),
		consTail:        new(uint32),
		multiProd:       cfg.multiProd,
		multiCons:       cfg.multiCons,
	}
	return ring, nil
}

// checkMaxBatch returns an error wrapping ErrInvalidMaxBatch when maxBatch, the limit for the
// direction op names, is below 1 or above count.
func checkMaxBatch(op string, maxBatch, count int) error {
	if maxBatch >= 1 && maxBatch <= count {
		return nil
	}
	return fmt.Errorf("new ring with count %d and max %s batch %d: %w", count, op, maxBatch,
		ErrInvalidMaxBatch)
}

// Capacity returns the number of elements the ring holds, which is the count given to New.
func (r *Ring[T]) Capacity() int {
	return int(r.capacity())
}

// MaxEnqueueBatch returns the most elements one enqueue batch call moves: the limit set by
// WithMaxEnqueueBatch or WithMaxBatch, or the capacity without one.
func (r *Ring[T]) MaxEnqueueBatch() int {
	return int(r.maxEnqueueBatch)
}

// MaxDequeueBatch returns the most elements one dequeue batch call moves: the limit set by
// WithMaxDequeueBatch or WithMaxBatch, or the capacity without one.
func (r *Ring[T]) MaxDequeueBatch() int {
	return int(r.maxDequeueBatch)
}

// capacity returns the number of elements the ring holds as a uint32, for the index arithmetic.
// The count is a power of two, so it is one more than the mask.
func (r *Ring[T]) capacity() uint32 {
	return r.mask + 1
}

// Reset empties the ring by resetting its indices.
//
// WARNING: Make sure to only do this while the ring is not in use!
//
// Reset does not change how the ring was configured. It only resets the producer and consumer
// indices so the ring is empty.
func (r *Ring[T]) Reset() {
	// Just use atomics so we don't have to worry about single or multi
	atomic.StoreUint32(r.prodHead, 0)
	atomic.StoreUint32(r.prodTail, 0)
	atomic.StoreUint32(r.consHead, 0)
	atomic.StoreUint32(r.consTail, 0)
}

// Count returns the number of occupied entries in the ring.
//
// Unlike DPDK's rte_ring_count, it does not mask the difference of the tails. The ring holds
// exactly Capacity elements, so a full ring's difference equals the capacity, and masking it would
// report a full ring as empty.
//
// It loads the consumer tail before the producer tail. The producer tail only grows and is never
// behind the consumer tail, so the producer tail loaded second is never behind the consumer tail
// loaded first, and the difference cannot underflow. Loading them in the other order lets
// consumers pass the producer tail that was read, and the underflow would report a nearly empty
// ring as full.
//
// Under concurrency the result is a snapshot. Dequeues that complete between the two loads, with
// the enqueues that refill the ring, can make it report more entries than the ring holds. It never
// exceeds Capacity.
func (r *Ring[T]) Count() int {
	consTail := atomic.LoadUint32(r.consTail)
	prodTail := atomic.LoadUint32(r.prodTail)
	return int(min(prodTail-consTail, r.capacity()))
}

// Free returns the number of empty slots in the ring. It derives from Count, so under concurrency
// it can report the ring as fuller than it is.
func (r *Ring[T]) Free() int {
	return r.Capacity() - r.Count()
}

// Full reports whether the ring is full. It derives from Count, so under concurrency it can report
// the ring as fuller than it is.
func (r *Ring[T]) Full() bool {
	return r.Free() == 0
}

// Empty reports whether the ring is empty, which is whether Count reports 0.
func (r *Ring[T]) Empty() bool {
	return r.Count() == 0
}

// Enqueue places item in the ring. It returns ErrFull, unwrapped, when the ring has no free slot.
//
// Enqueue is safe for concurrent use with other enqueues only when the ring was created with
// WithMultiProdEnqueue. It is always safe to run alongside dequeues.
func (r *Ring[T]) Enqueue(item T) error {
	items := [1]T{item}
	enqueued, _ := r.doEnqueue(items[:], true)
	if enqueued == 0 {
		return ErrFull
	}
	return nil
}

// EnqueueBulk places all of items in the ring, or none of them. It returns nil when all are
// enqueued. A zero-length items succeeds and enqueues nothing.
//
// It checks, in order, that len(items) is at most Capacity, that it is at most MaxEnqueueBatch,
// and that the ring has len(items) free slots. The first failing check decides the error. These
// return an error wrapping ErrExceedsCapacity, an error wrapping ErrExceedsMaxBatch, and ErrFull
// unwrapped. A failed call leaves the ring unchanged.
//
// free is returned on every path, errors included. It is the number of free slots after the call,
// between 0 and Capacity inclusive.
//
// EnqueueBulk is safe for concurrent use with other enqueues only when the ring was created with
// WithMultiProdEnqueue. It is always safe to run alongside dequeues.
func (r *Ring[T]) EnqueueBulk(items []T) (free int, err error) {
	// The limit is at most the capacity, so one comparison guards both checks on the success
	// path. It is in int, so a length beyond the uint32 range cannot truncate.
	if len(items) > int(r.maxEnqueueBatch) {
		return int(r.freeEntries()), r.bulkLengthError("enqueue", uint64(len(items)),
			r.maxEnqueueBatch)
	}

	enqueued, freeAfter := r.doEnqueue(items, true)
	if int(enqueued) != len(items) {
		return int(freeAfter), ErrFull
	}
	return int(freeAfter), nil
}

// EnqueueBurst places as many elements from the front of items in the ring as it has free slots
// for, and never more than MaxEnqueueBatch. It returns the number enqueued, between 0 and
// len(items) inclusive. A zero-length items enqueues nothing.
//
// free is returned on every path. It is the number of free slots after the call, between 0 and
// Capacity inclusive.
//
// EnqueueBurst is safe for concurrent use with other enqueues only when the ring was created with
// WithMultiProdEnqueue. It is always safe to run alongside dequeues.
func (r *Ring[T]) EnqueueBurst(items []T) (enqueued, free int) {
	// Clamping in int keeps a slice longer than 2^32 from truncating when its length becomes a
	// uint32 in doEnqueue. The limit is at most the capacity, so it bounds both.
	clamped := min(len(items), int(r.maxEnqueueBatch))
	moved, freeAfter := r.doEnqueue(items[:clamped], false)
	return int(moved), int(freeAfter)
}

// Dequeue removes and returns the oldest element in the ring. It returns the zero value and
// ErrEmpty, unwrapped, when the ring is empty.
//
// Dequeue is safe for concurrent use with other dequeues only when the ring was created with
// WithMultiConsDequeue. It is always safe to run alongside enqueues.
func (r *Ring[T]) Dequeue() (T, error) {
	elems, numDequeued := r.doDequeue(1, nil, true)
	if numDequeued != 1 {
		var zero T
		return zero, ErrEmpty
	}
	return elems[0], nil
}

// DequeueBulk removes and returns the numElems oldest elements in the ring. Either all of them are
// dequeued and it returns them with a nil error, or none are and it returns a nil slice and an
// error.
//
// It checks, in order, that numElems is at most Capacity, that it is at most MaxDequeueBatch, and
// that the ring has numElems occupied entries. The first failing check decides the error. These
// return an error wrapping ErrExceedsCapacity, an error wrapping ErrExceedsMaxBatch, and ErrEmpty
// unwrapped. A failed call leaves the ring unchanged. A numElems of zero succeeds with a nil
// slice.
//
// If avail is non-nil, it is written on every return with the number of occupied entries after
// the call.
//
// DequeueBulk is safe for concurrent use with other dequeues only when the ring was created with
// WithMultiConsDequeue. It is always safe to run alongside enqueues.
func (r *Ring[T]) DequeueBulk(
	numElems uint32,
	avail *uint32,
) ([]T, error) {
	// The limit is at most the capacity, so one comparison guards both checks on the success
	// path.
	if numElems > r.maxDequeueBatch {
		updateValidPtr(avail, r.occupiedEntries())
		return nil, r.bulkLengthError("dequeue", uint64(numElems), r.maxDequeueBatch)
	}

	elems, numDequeued := r.doDequeue(numElems, avail, true)
	if numDequeued != numElems {
		return nil, ErrEmpty
	}
	return elems, nil
}

// DequeueBurst removes and returns up to numElems of the oldest elements in the ring, taking all
// of them when the ring does not have enough, and never more than MaxDequeueBatch. It returns the
// elements and their count, which is between 0 and numElems inclusive. When nothing is dequeued,
// including for a numElems of zero, the slice is nil.
//
// If avail is non-nil, it is written on every return with the number of occupied entries after
// the call.
//
// DequeueBurst is safe for concurrent use with other dequeues only when the ring was created with
// WithMultiConsDequeue. It is always safe to run alongside enqueues.
func (r *Ring[T]) DequeueBurst(
	numElems uint32,
	avail *uint32,
) ([]T, uint32) {
	return r.doDequeue(
		min(numElems, r.maxDequeueBatch),
		avail,
		false,
	)
}

// bulkLengthError returns the error for a bulk call that asked to move length elements, more than
// maxBatch, its direction's limit. It wraps ErrExceedsCapacity when length also exceeds the
// capacity, and ErrExceedsMaxBatch otherwise. op names the call, enqueue or dequeue, in the
// message. Callers check the limit first, so this runs only on the error path. length is a uint64
// so that neither a slice length nor a uint32 count can wrap negative on a 32-bit target.
func (r *Ring[T]) bulkLengthError(op string, length uint64, maxBatch uint32) error {
	capacity := uint64(r.capacity())
	if length > capacity {
		return fmt.Errorf("%s %d elements, capacity %d: %w", op, length, capacity,
			ErrExceedsCapacity)
	}
	return fmt.Errorf("%s %d elements, max %s batch %d: %w", op, length, op, maxBatch,
		ErrExceedsMaxBatch)
}

// doEnqueue places elements from the front of items in the ring. fixed is true for a bulk
// enqueue, which enqueues all of items or nothing. It returns the number enqueued and the number
// of free slots after the call, which is at most the capacity.
//
// Callers pass a slice whose length is already at most the capacity, so converting len(items) to
// uint32 cannot truncate.
func (r *Ring[T]) doEnqueue(items []T, fixed bool) (enqueued, free uint32) {
	var (
		head uint32
		next uint32
	)

	// get the number of entries that we have space to accommodate
	numEntries, freeBefore := r.moveProdHead(uint32(len(items)), &head, &next, fixed)
	if numEntries == 0 {
		// Without a reservation the producer head moveProdHead read may be stale, so the count
		// can overshoot the capacity. With a reservation the head was current when the consumer
		// tail was loaded, because the single producer owns the head or the multi-producer CAS
		// succeeded on it, so capacity + tail - head is already between 0 and the capacity.
		return 0, min(freeBefore, r.capacity())
	}

	base := head & r.mask
	end := r.capacity() - base
	if end > numEntries {
		end = numEntries
	}

	// enqueue the elements -- first contiguous chunk
	for i := uint32(0); i < end; i++ {
		r.entries[int(base+i)] = items[int(i)]
	}

	// second chunk if wrapped
	for i := end; i < numEntries; i++ {
		r.entries[int(i-end)] = items[int(i)]
	}

	// ensure in-order publication
	if r.multiProd {
		for atomic.LoadUint32(r.prodTail) != head {
			runtime.Gosched()
		}
	}

	// atomic store acts as release barrier
	atomic.StoreUint32(r.prodTail, next)

	return numEntries, freeBefore - numEntries
}

func (r *Ring[T]) doDequeue(
	numElems uint32,
	avail *uint32,
	fixed bool,
) ([]T, uint32) {
	var (
		head uint32
		next uint32
	)

	// get the number of entries that we can actually dequeue
	numElems, filled := r.moveConsHead(numElems, &head, &next, fixed)
	if numElems == 0 {
		updateValidPtr(avail, filled)
		return nil, uint32(0)
	}

	elems := make([]T, int(numElems))

	base := head & r.mask
	end := r.capacity() - base
	if end > numElems {
		end = numElems
	}

	// dequeue the elements -- first chunk
	for i := uint32(0); i < end; i++ {
		elems[int(i)] = r.entries[int(base+i)]
	}

	// second chunk if wrapped
	for i := end; i < numElems; i++ {
		elems[int(i)] = r.entries[int(i-end)]
	}

	// ensure in-order consumption
	if r.multiCons {
		for atomic.LoadUint32(r.consTail) != head {
			runtime.Gosched()
		}
	}

	// update the tail after a successful dequeue
	atomic.StoreUint32(r.consTail, next)

	updateValidPtr(avail, filled-numElems)
	return elems, numElems
}

// moveProdHead moves the ring's producer head for an enqueue operation.
//
// numItems is the number of items to enqueue, which is how far the head should move. oldHead
// receives the producer head from before the move, where the enqueue starts, and newHead receives
// it from after the move, where the enqueue ends. fixed is true for a bulk enqueue, which enqueues
// the entire batch or nothing.
//
// It returns the number of items reserved, between 0 and numItems inclusive, which is 0 or
// numItems for a fixed enqueue. It also returns the number of free entries in the ring before the
// head was moved.
func (r *Ring[T]) moveProdHead(
	numItems uint32,
	oldHead *uint32,
	newHead *uint32,
	fixed bool,
) (uint32, uint32) {
	capacity := r.capacity()
	maxItems := numItems

	for {
		head := atomic.LoadUint32(r.prodHead)
		tail := atomic.LoadUint32(r.consTail)

		freeEntries := capacity + tail - head
		numItems = maxItems
		if numItems > freeEntries {
			if fixed {
				return 0, freeEntries
			}
			numItems = freeEntries
		}
		if numItems == 0 {
			return 0, freeEntries
		}

		next := head + numItems

		if r.multiProd {
			if atomic.CompareAndSwapUint32(r.prodHead, head, next) {
				*oldHead = head
				*newHead = next
				return numItems, freeEntries
			}
			continue
		}

		atomic.StoreUint32(r.prodHead, next)
		*oldHead = head
		*newHead = next
		return numItems, freeEntries
	}
}

// moveConsHead moves the ring's consumer head for a dequeue operation.
//
// numItems is the number of items to dequeue, which is how far the head should move. oldHead
// receives the consumer head from before the move, where the dequeue starts, and newHead receives
// it from after the move, where the dequeue ends. fixed is true for a bulk dequeue, which dequeues
// the entire batch or nothing.
//
// It returns the number of items reserved, between 0 and numItems inclusive, which is 0 or
// numItems for a fixed dequeue. It also returns the number of occupied entries in the ring before
// the head was moved.
func (r *Ring[T]) moveConsHead(
	numItems uint32,
	oldHead *uint32,
	newHead *uint32,
	fixed bool,
) (uint32, uint32) {
	var (
		tail   uint32
		head   uint32
		filled uint32
	)

	maxItems := numItems
	multiCons := r.multiCons

	for {
		head = atomic.LoadUint32(r.consHead)
		tail = atomic.LoadUint32(r.prodTail)

		filled = tail - head
		numItems = maxItems
		if numItems > filled {
			if fixed {
				return 0, filled
			}
			numItems = filled
		}
		if numItems == 0 {
			return 0, filled
		}

		next := head + numItems

		if multiCons {
			if atomic.CompareAndSwapUint32(r.consHead, head, next) {
				*oldHead = head
				*newHead = next
				return numItems, filled
			}
			continue
		}

		atomic.StoreUint32(r.consHead, next)
		*oldHead = head
		*newHead = next
		return numItems, filled
	}
}

func updateValidPtr(ptr *uint32, val uint32) {
	if ptr == nil {
		return
	}
	*ptr = val
}

// freeEntries returns the number of slots a producer could reserve right now. It loads the
// producer head before the consumer tail, as moveProdHead does. It clamps the result to the
// capacity, because a consumer tail that advances after the head was loaded can make the value
// overshoot.
func (r *Ring[T]) freeEntries() uint32 {
	head := atomic.LoadUint32(r.prodHead)
	capacity := r.capacity()
	free := capacity + atomic.LoadUint32(r.consTail) - head
	return min(free, capacity)
}

// occupiedEntries returns the number of entries a consumer could reserve right now. It loads the
// consumer head before the producer tail, as moveConsHead does. It clamps the result to the
// capacity, because a producer tail that advances after the head was loaded can make the value
// overshoot.
func (r *Ring[T]) occupiedEntries() uint32 {
	head := atomic.LoadUint32(r.consHead)
	occupied := atomic.LoadUint32(r.prodTail) - head
	return min(occupied, r.capacity())
}
