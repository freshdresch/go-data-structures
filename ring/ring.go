// Package ring provides a fixed-size, lock-free ring buffer modeled on DPDK's rte_ring.
package ring

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
)

// ErrInvalidCount is returned by New, wrapped, when the count is not a power of two, is less
// than 2, or exceeds math.MaxInt.
var ErrInvalidCount = errors.New("ring: count must be a power of two and at least 2")

// ErrFull is returned unwrapped by Enqueue and EnqueueBulk when the ring does not have enough
// free slots for the request. The caller may retry.
var ErrFull = errors.New("ring: not enough free slots")

// ErrEmpty is returned unwrapped by Dequeue and DequeueBulk when the ring does not have enough
// occupied entries for the request. The caller may retry.
var ErrEmpty = errors.New("ring: not enough occupied entries")

// ErrShortBatch is returned by EnqueueBulk, wrapped, when numElems exceeds the length of the
// batch.
var ErrShortBatch = errors.New("ring: batch is shorter than the requested count")

// ErrExceedsCapacity is returned by EnqueueBulk and DequeueBulk, wrapped, when numElems exceeds
// the ring's capacity. Such a request can never succeed, so retrying it is pointless.
var ErrExceedsCapacity = errors.New("ring: requested count exceeds the ring's capacity")

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
// The actual number of entries in the ring is *count - 1*, because the *count* entry is used to
// determine whether the ring is empty or not.
type Ring[T any] struct {
	entries []T

	prodHead *uint32 // The head index for the producer: used for reservation.
	prodTail *uint32 // The tail index for the producer: used for publication.

	consHead *uint32 // The head index for the consumer: used for reservation.
	consTail *uint32 // The tail index for the consumer: used for publication.

	size     uint32 // Backing array length.
	mask     uint32 // Mask for bounding the index of the ring.
	capacity uint32 // Usable slots in the ring.

	multiProd bool // Multi-Producer enqueue instead of single.
	multiCons bool // Multi-Consumer dequeue instead of single.

	_ [2]byte // padding for better cache alignment
}

type config struct {
	multiProd bool
	multiCons bool
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

// New returns an empty Ring with count slots, of which count - 1 are usable. Without options, the
// ring allows one producer and one consumer at a time. It returns an error wrapping
// ErrInvalidCount when count is not a power of two, is less than 2, or exceeds math.MaxInt.
func New[T any](count uint32, options ...Option) (*Ring[T], error) {
	isPowerOfTwo := count&(count-1) == 0
	if count < 2 || !isPowerOfTwo || uint64(count) > uint64(math.MaxInt) {
		return nil, fmt.Errorf("new ring with count %d: %w", count, ErrInvalidCount)
	}

	var cfg config
	for _, option := range options {
		option(&cfg)
	}

	mask := count - 1
	ring := &Ring[T]{
		entries:   make([]T, int(count)),
		size:      count,
		mask:      mask,
		capacity:  mask,
		prodHead:  new(uint32),
		prodTail:  new(uint32),
		consHead:  new(uint32),
		consTail:  new(uint32),
		multiProd: cfg.multiProd,
		multiCons: cfg.multiCons,
	}
	return ring, nil
}

// Capacity returns the number of usable slots in the ring, which is one less than the count
// given to New.
func (ring *Ring[T]) Capacity() uint32 {
	return ring.capacity
}

// Reset empties the ring by resetting its indices.
//
// WARNING: Make sure to only do this while the ring is not in use!
//
// Reset does not change how the ring was configured. It only resets the producer and consumer
// indices so the ring is empty.
func (ring *Ring[T]) Reset() {
	// Just use atomics so we don't have to worry about single or multi
	atomic.StoreUint32(ring.prodHead, 0)
	atomic.StoreUint32(ring.prodTail, 0)
	atomic.StoreUint32(ring.consHead, 0)
	atomic.StoreUint32(ring.consTail, 0)
}

// Count returns the number of occupied entries in the ring.
func (ring *Ring[T]) Count() uint32 {
	prodTail := atomic.LoadUint32(ring.prodTail)
	consTail := atomic.LoadUint32(ring.consTail)
	count := prodTail - consTail
	if count > ring.capacity {
		return ring.capacity
	}
	return count
}

// Free returns the number of empty entries in the ring.
func (ring *Ring[T]) Free() uint32 {
	return ring.capacity - ring.Count()
}

// Full reports whether the ring is full.
func (ring *Ring[T]) Full() bool {
	return ring.Free() == 0
}

// Empty reports whether the ring is empty.
func (ring *Ring[T]) Empty() bool {
	prodTail := atomic.LoadUint32(ring.prodTail)
	consTail := atomic.LoadUint32(ring.consTail)
	return consTail == prodTail
}

// Enqueue places one element in the ring. It returns ErrFull, unwrapped, when the ring has no
// free slot.
//
// Enqueue is safe for concurrent use with other enqueues only when the ring was created with
// WithMultiProdEnqueue. It is always safe to run alongside dequeues.
func (ring *Ring[T]) Enqueue(element T) error {
	var elems [1]T
	elems[0] = element

	numEnqueued := ring.doEnqueue(elems[:], 1, nil, true)
	if numEnqueued != 1 {
		return ErrFull
	}
	return nil
}

// EnqueueBulk places the first numElems elements of elems in the ring. Either all of them are
// enqueued and it returns nil, or none are and it returns an error.
//
// It checks, in order, that numElems is at most len(elems), that numElems is at most Capacity,
// and that the ring has numElems free slots. The first failing check decides the error. These
// return an error wrapping ErrShortBatch, an error wrapping ErrExceedsCapacity, and ErrFull
// unwrapped. A failed call leaves the ring unchanged. A numElems of zero succeeds.
//
// If freeSpace is non-nil, it is written on every return with the number of free slots after the
// call.
//
// EnqueueBulk is safe for concurrent use with other enqueues only when the ring was created with
// WithMultiProdEnqueue. It is always safe to run alongside dequeues.
func (ring *Ring[T]) EnqueueBulk(
	elems []T,
	numElems uint32,
	freeSpace *uint32,
) error {
	if uint64(numElems) > uint64(len(elems)) {
		updateValidPtr(freeSpace, ring.freeEntries())
		return fmt.Errorf(
			"enqueue %d elements from a batch of %d: %w",
			numElems,
			len(elems),
			ErrShortBatch,
		)
	}
	if numElems > ring.capacity {
		updateValidPtr(freeSpace, ring.freeEntries())
		return fmt.Errorf(
			"enqueue %d elements, capacity %d: %w",
			numElems,
			ring.capacity,
			ErrExceedsCapacity,
		)
	}

	numEnqueued := ring.doEnqueue(elems, numElems, freeSpace, true)
	if numEnqueued != numElems {
		return ErrFull
	}
	return nil
}

// EnqueueBurst places as many of the first numElems elements of elems in the ring as it has free
// slots for. It returns the number enqueued, which is between 0 and numElems inclusive.
//
// It panics when numElems exceeds len(elems), before touching the ring or freeSpace. Otherwise, if
// freeSpace is non-nil, it is written on every return with the number of free slots after the
// call.
//
// EnqueueBurst is safe for concurrent use with other enqueues only when the ring was created with
// WithMultiProdEnqueue. It is always safe to run alongside dequeues.
func (ring *Ring[T]) EnqueueBurst(
	elems []T,
	numElems uint32,
	freeSpace *uint32,
) uint32 {
	if uint64(numElems) > uint64(len(elems)) {
		panic(fmt.Sprintf("ring: burst count %d exceeds batch length %d", numElems, len(elems)))
	}
	return ring.doEnqueue(
		elems,
		numElems,
		freeSpace,
		false,
	)
}

// Dequeue removes and returns the oldest element in the ring. It returns the zero value and
// ErrEmpty, unwrapped, when the ring is empty.
//
// Dequeue is safe for concurrent use with other dequeues only when the ring was created with
// WithMultiConsDequeue. It is always safe to run alongside enqueues.
func (ring *Ring[T]) Dequeue() (T, error) {
	elems, numDequeued := ring.doDequeue(1, nil, true)
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
// It checks, in order, that numElems is at most Capacity and that the ring has numElems occupied
// entries. The first failing check decides the error. These return an error wrapping
// ErrExceedsCapacity and ErrEmpty unwrapped. A failed call leaves the ring unchanged. A numElems
// of zero succeeds with a nil slice.
//
// If avail is non-nil, it is written on every return with the number of occupied entries after
// the call.
//
// DequeueBulk is safe for concurrent use with other dequeues only when the ring was created with
// WithMultiConsDequeue. It is always safe to run alongside enqueues.
func (ring *Ring[T]) DequeueBulk(
	numElems uint32,
	avail *uint32,
) ([]T, error) {
	if numElems > ring.capacity {
		updateValidPtr(avail, ring.occupiedEntries())
		return nil, fmt.Errorf(
			"dequeue %d elements, capacity %d: %w",
			numElems,
			ring.capacity,
			ErrExceedsCapacity,
		)
	}

	elems, numDequeued := ring.doDequeue(numElems, avail, true)
	if numDequeued != numElems {
		return nil, ErrEmpty
	}
	return elems, nil
}

// DequeueBurst removes and returns up to numElems of the oldest elements in the ring, taking all
// of them when the ring does not have enough. It returns the elements and their count, which is
// between 0 and numElems inclusive. When nothing is dequeued, including for a numElems of zero,
// the slice is nil.
//
// If avail is non-nil, it is written on every return with the number of occupied entries after
// the call.
//
// DequeueBurst is safe for concurrent use with other dequeues only when the ring was created with
// WithMultiConsDequeue. It is always safe to run alongside enqueues.
func (ring *Ring[T]) DequeueBurst(
	numElems uint32,
	avail *uint32,
) ([]T, uint32) {
	return ring.doDequeue(
		numElems,
		avail,
		false,
	)
}

func (ring *Ring[T]) doEnqueue(
	elems []T,
	numElems uint32,
	freeSpace *uint32,
	fixed bool,
) uint32 {
	var (
		head uint32
		next uint32
	)

	// get the number of entries that we have space to accommodate
	numEntries, freeBefore := ring.moveProdHead(numElems, &head, &next, fixed)
	if numEntries == 0 {
		updateValidPtr(freeSpace, freeBefore)
		return 0
	}

	base := head & ring.mask
	end := ring.size - base
	if end > numEntries {
		end = numEntries
	}

	// enqueue the elements -- first contiguous chunk
	for i := uint32(0); i < end; i++ {
		ring.entries[int(base+i)] = elems[int(i)]
	}

	// second chunk if wrapped
	for i := end; i < numEntries; i++ {
		ring.entries[int(i-end)] = elems[int(i)]
	}

	// ensure in-order publication
	if ring.multiProd {
		for atomic.LoadUint32(ring.prodTail) != head {
			runtime.Gosched()
		}
	}

	// atomic store acts as release barrier
	atomic.StoreUint32(ring.prodTail, next)

	updateValidPtr(freeSpace, freeBefore-numEntries)
	return numEntries
}

func (ring *Ring[T]) doDequeue(
	numElems uint32,
	avail *uint32,
	fixed bool,
) ([]T, uint32) {
	var (
		head uint32
		next uint32
	)

	// get the number of entries that we can actually dequeue
	numElems, filled := ring.moveConsHead(numElems, &head, &next, fixed)
	if numElems == 0 {
		updateValidPtr(avail, filled)
		return nil, uint32(0)
	}

	elems := make([]T, int(numElems))

	base := head & ring.mask
	end := ring.size - base
	if end > numElems {
		end = numElems
	}

	// dequeue the elements -- first chunk
	for i := uint32(0); i < end; i++ {
		elems[int(i)] = ring.entries[int(base+i)]
	}

	// second chunk if wrapped
	for i := end; i < numElems; i++ {
		elems[int(i)] = ring.entries[int(i-end)]
	}

	// ensure in-order consumption
	if ring.multiCons {
		for atomic.LoadUint32(ring.consTail) != head {
			runtime.Gosched()
		}
	}

	// update the tail after a successful dequeue
	atomic.StoreUint32(ring.consTail, next)

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
func (ring *Ring[T]) moveProdHead(
	numItems uint32,
	oldHead *uint32,
	newHead *uint32,
	fixed bool,
) (uint32, uint32) {
	capacity := ring.capacity
	maxItems := numItems

	for {
		head := atomic.LoadUint32(ring.prodHead)
		tail := atomic.LoadUint32(ring.consTail)

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

		if ring.multiProd {
			if atomic.CompareAndSwapUint32(ring.prodHead, head, next) {
				*oldHead = head
				*newHead = next
				return numItems, freeEntries
			}
			continue
		}

		atomic.StoreUint32(ring.prodHead, next)
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
func (ring *Ring[T]) moveConsHead(
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
	multiCons := ring.multiCons

	for {
		head = atomic.LoadUint32(ring.consHead)
		tail = atomic.LoadUint32(ring.prodTail)

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
			if atomic.CompareAndSwapUint32(ring.consHead, head, next) {
				*oldHead = head
				*newHead = next
				return numItems, filled
			}
			continue
		}

		atomic.StoreUint32(ring.consHead, next)
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
func (ring *Ring[T]) freeEntries() uint32 {
	head := atomic.LoadUint32(ring.prodHead)
	free := ring.capacity + atomic.LoadUint32(ring.consTail) - head
	return min(free, ring.capacity)
}

// occupiedEntries returns the number of entries a consumer could reserve right now. It loads the
// consumer head before the producer tail, as moveConsHead does. It clamps the result to the
// capacity, because a producer tail that advances after the head was loaded can make the value
// overshoot.
func (ring *Ring[T]) occupiedEntries() uint32 {
	head := atomic.LoadUint32(ring.consHead)
	occupied := atomic.LoadUint32(ring.prodTail) - head
	return min(occupied, ring.capacity)
}
