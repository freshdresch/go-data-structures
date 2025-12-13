package ring

import (
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
)

// Ring is a Go ring implementation heavily inspired by rte_ring from DPDK. It provides a fixed-size
// ring buffer that can function as anything between an SPSC and MPMC queue.
//
// However, there is a notable difference in that Go does not provide the relaxed memory model,
// acquire/release fences, and ordering guarantees that the DPDK C implementation leverages. We try
// to be as performant as we can, in the face of working only in the realm of sequential consistency.
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

	prodHead *uint32 /* The head index for the producer: used for reservation. */
	prodTail *uint32 /* The tail index for the producer: used for publication. */

	consHead *uint32 /* The head index for the consumer: used for reservation*/
	consTail *uint32 /* The tail index for the consumer: used for publication. */

	size     uint32 /* Backing array length. */
	mask     uint32 /* Mask for bounding the index of the ring. */
	capacity uint32 /* Usable slots in the ring. */

	multiProd bool /* Multi-Producer enqueue instead of single. */
	multiCons bool /* Multi-Consumer dequeue instead of single. */

	_ [2]byte /* padding for better cache alignment */
}

type RingOption[T any] func(*Ring[T])

func WithMultiProdEnqueue[T any]() RingOption[T] {
	return func(ring *Ring[T]) {
		ring.multiProd = true
	}
}

func WithMultiConsDequeue[T any]() RingOption[T] {
	return func(ring *Ring[T]) {
		ring.multiCons = true
	}
}

func NewRing[T any](count uint32, opts ...RingOption[T]) (*Ring[T], error) {
	if count < 2 || (count&(count-1)) != 0 {
		return nil, fmt.Errorf("count must be a power of two and >= 2; got %d", count)
	}
	mask := count - 1

	ring := &Ring[T]{
		entries:  make([]T, int(count)),
		size:     count,
		mask:     mask,
		capacity: mask,
		prodHead: new(uint32),
		prodTail: new(uint32),
		consHead: new(uint32),
		consTail: new(uint32),
	}

	*ring.prodHead = 0
	*ring.prodTail = 0
	*ring.consHead = 0
	*ring.consTail = 0

	for _, opt := range opts {
		opt(ring)
	}

	return ring, nil
}

/**
 * Reset flushes the entries in the ring by resetting its state.
 *
 * WARNING: Make sure to only do this while the ring is not in use!
 *
 * This reset function does not change the params of how the ring was initialized, it only resets
 * the consumer and producer indices to show the ring is empty.
 */
func (ring *Ring[T]) Reset() {
	/* Just use atomics so we don't have to worry about single or multi */
	atomic.StoreUint32(ring.prodHead, 0)
	atomic.StoreUint32(ring.prodTail, 0)
	atomic.StoreUint32(ring.consHead, 0)
	atomic.StoreUint32(ring.consTail, 0)
}

/**
 * Count returns the number of occupied entries in the ring.
 *
 * Returns:
 *   The number of occupied entries.
 */
func (ring *Ring[T]) Count() uint32 {
	prodTail := atomic.LoadUint32(ring.prodTail)
	consTail := atomic.LoadUint32(ring.consTail)
	var count uint32 = prodTail - consTail
	if count > ring.capacity {
		return ring.capacity
	}
	return count
}

/**
 * Free returns the number of empty entries in the ring.
 *
 * Returns:
 *   The number of empty entries.
 */
func (ring *Ring[T]) Free() uint32 {
	return ring.capacity - ring.Count()
}

/**
 * Full returns whether the ring is full or not.
 *
 * Returns:
 *   `true` if the ring is full, or `false` if it is not full.
 */
func (ring *Ring[T]) Full() bool {
	return ring.Free() == 0
}

/**
 * Empty returns whether the ring is empty or not.
 *
 * Returns:
 *   `true` if the ring is empty, or `false` if it is not empty.
 */
func (ring *Ring[T]) Empty() bool {
	prodTail := atomic.LoadUint32(ring.prodTail)
	consTail := atomic.LoadUint32(ring.consTail)
	return consTail == prodTail
}

/**
 * Enqueue places one object in the ring.
 *
 * Params:
 *   element - the object we want to enqueue into the ring.
 *
 * Returns:
 *   `nil` if the element was successfully enqueued, or an error explaining why if not.
 */
func (ring *Ring[T]) Enqueue(element T) error {
	var elems [1]T
	elems[0] = element

	numEnqueued := ring.doEnqueue(elems[:], 1, nil, true)
	if numEnqueued != 1 {
		return errors.New("no space in the ring")
	}
	return nil
}

/**
 * EnqueueBulk places a slice of elements onto the ring. The `bulk` descriptor signifies that we do
 * not want any partial batches enqueued: either the entire batch gets enqueued, or we bail out and
 * enqueue nothing at all.
 *
 * Params:
 *   elems - the slice of elements we want to enqueue into the ring.
 *   numElems - the number of elements in the batch.
 *   freeSpace - a pointer where we will fill in the amount of available entries in the ring.
 *
 * Returns:
 *   `nil` if the entire batch was successfully enqueued, or an error explaining why we could not
 *   enqueue the batch.
 */
func (ring *Ring[T]) EnqueueBulk(
	elems []T,
	numElems uint32,
	freeSpace *uint32,
) error {
	numEnqueued := ring.doEnqueue(elems, numElems, freeSpace, true)
	if numEnqueued != numElems {
		return fmt.Errorf(
			"needed %d empty slots in the ring, but only %d were free",
			numElems,
			numEnqueued,
		)
	}
	return nil
}

/**
 * EnqueueBurst places a slice of elements onto the ring. The 'burst' descriptor signifies that,
 * in the circumstance where our ring does not have enough space for the whole batch, we will enqueue
 * as many elements in the batch as the ring can hold.
 *
 * Params:
 *   elems - the slice of elements we want to enqueue into the ring.
 *   numElems - the number of elements in the batch.
 *   freeSpace - a pointer where we will fill in the amount of available entries in the ring.
 *
 * Returns:
 *   the number of elements successfully enqueued (on the closed interval [0, numElems]).
 */
func (ring *Ring[T]) EnqueueBurst(
	elems []T,
	numElems uint32,
	freeSpace *uint32,
) uint32 {
	return ring.doEnqueue(
		elems,
		numElems,
		freeSpace,
		false,
	)
}

/**
 * Dequeue removes the oldest object from the ring.
 *
 * Returns:
 *   1. The dequeued element.
 *   2. `nil` if the element was successfully dequeued, or an error explaining why if not.
 */
func (ring *Ring[T]) Dequeue() (T, error) {
	elems, numDequeued := ring.doDequeue(1, nil, true)
	if numDequeued != 1 {
		return getZeroVal[T](), errors.New("no elements in the ring")
	}
	return elems[0], nil
}

/**
 * DequeueBulk removes a chunk of elements from the ring. The `bulk` descriptor signifies that we do
 * not want any partial batches dequeued: either the entire batch gets dequeued, or we bail out and
 * dequeue nothing at all.
 *
 * Params:
 *   numElems - the number of elements in the batch.
 *   avail - a pointer where we will fill in the amount of occupied entries in the ring.
 *
 * Returns:
 *   1. The slice of elements we dequeued from the ring.
 *   2. `nil` if the entire batch was successfully dequeued, or an error explaining why we could not
 *      dequeue the batch.
 */
func (ring *Ring[T]) DequeueBulk(
	numElems uint32,
	avail *uint32,
) ([]T, error) {
	elems, numDequeued := ring.doDequeue(numElems, avail, true)
	if numDequeued != numElems {
		return []T{}, fmt.Errorf(
			"needed %d occupied entries in the ring, but only %d were occupied",
			numElems,
			numDequeued,
		)
	}

	return elems, nil
}

/**
 * DequeueBurst remove a chunk of elements from the ring. The 'burst' descriptor signifies that,
 * in the circumstance where our ring does have enough entries to fulfill our requested batch size,
 * we will accept a partial batch and dequeue all of the ring's elements.
 *
 * Params:
 *   numElems - the number of elements in the batch.
 *   available - a pointer where we will fill in the amount of occupied entries in the ring.
 *
 * Returns:
 *   1. The slice of elements we dequeued from the ring.
 *   2. The number of elements successfully dequeued (on the closed interval [0, numElems]).
 */
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

/**
 * moveProdHead moves the ring's producer head for an enqueue operation.
 *
 * Params:
 *   numItems - The number of items we want to enqueue, i.e., how far the head should be moved.
 *   oldHead - Saves the producer head value from *before* the move (where the enqueue starts).
 *   newHead - Saves the producer head value from *after* the move (where the enqueue ends).
 *   fixed - If we are doing a bulk enqueue (where we enqueue the entire batch size, or don't enqueue
 *       anything).
 *
 * Returns:
 *   1. The actual number of items enqueued. This can lie anywhere between 0 and numItems
 *      (inclusive). If it is a fixed enqueue, then 0 or numItems are the only possible values.
 *   2. Returns the number of free entries in the ring before the head was moved.
 */
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

/**
 * moveConsHead moves the ring's consumer head for a dequeue operation.
 *
 * Params:
 *   numItems - The number of items we want to dequeue, i.e., how far the head should be moved.
 *   oldHead - Saves the consumer head value from *before* the move (where the dequeue starts).
 *   newHead - Saves the consumer head value from *after* the move (where the dequeue ends).
 *   fixed - If we are doing a bulk enqueue (where we dequeue the entire batch size, or don't
 *       dequeue anything).
 *
 * Returns:
 *   1. The actual number of items dequeued. This can lie anywhere between 0 and numItems
 *      (inclusive). If it is a fixed dequeue, then 0 or numItems are the only possible values.
 *   2. Returns the number of occupied entries in the ring before the head was moved.
 */
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

func getZeroVal[T any]() T {
	var zval T
	return zval
}
