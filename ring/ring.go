package ring

import (
        "errors"
        "fmt"
        "runtime"
        "sync/atomic"
)

// Ring is a Go port of rte_ring from DPDK. It provides a fixed-size ring buffer that can function
// as anything between an SPSC and MPMC queue.
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
        entries  []T

        prodHead *uint32 /* The head index for the producer. */
        consTail *uint32 /* The tail index for the consumer. */

        prodTail *uint32 /* The tail index for the producer. */
        consHead *uint32 /* The head index for the consumer. */

        size     uint32 /* Backing array length. */
        mask     uint32 /* Mask for bounding the index of the ring. */
        capacity uint32 /* Usable slots in the ring. */

        multiProdEnqueue bool /* Multi-Producer enqueue instead of single. */
        multiConsDequeue bool /* Multi-Consumer dequeue instead of single. */

        _ [2]byte /* padding for better cache alignment */
}

type RingOption[T any] func(*Ring[T])

func WithMultiProdEnqueue[T any]() RingOption[T] {
        return func(ring *Ring[T]) {
                ring.multiProdEnqueue = true
        }
}

func WithMultiConsDequeue[T any]() RingOption[T] {
        return func(ring *Ring[T]) {
                ring.multiConsDequeue = true
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
        elements := make([]T, 1)
        elements[0] = element

        numEnqueued := ring.doEnqueue(elements, 1, nil, true)
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
 *   elements - the slice of elements we want to enqueue into the ring.
 *   numElems - the number of elements in the batch.
 *   freeSpace - a pointer where we will fill in the amount of available entries in the ring.
 *
 * Returns:
 *   `nil` if the entire batch was successfully enqueued, or an error explaining why we could not
 *   enqueue the batch.
 */
func (ring *Ring[T]) EnqueueBulk(
        elements []T,
        numElems uint32,
        freeSpace *uint32,
) error {
        numEnqueued := ring.doEnqueue(elements, numElems, freeSpace, true)
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
 *   elements - the slice of elements we want to enqueue into the ring.
 *   numElems - the number of elements in the batch.
 *   freeSpace - a pointer where we will fill in the amount of available entries in the ring.
 *
 * Returns:
 *   the number of elements successfully enqueued (on the closed interval [0, numElems]).
 */
func (ring *Ring[T]) EnqueueBurst(
        elements []T,
        numElems uint32,
        freeSpace *uint32,
) uint32 {
        return ring.doEnqueue(
                elements,
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
        elements, numDequeued := ring.doDequeue(1, nil, true)
        if numDequeued != 1 {
                return getZeroVal[T](), errors.New("no elements in the ring")
        }
        return elements[0], nil
}

/**
 * DequeueBulk removes a chunk of elements from the ring. The `bulk` descriptor signifies that we do
 * not want any partial batches dequeued: either the entire batch gets dequeued, or we bail out and
 * dequeue nothing at all.
 *
 * Params:
 *   numElems - the number of elements in the batch.
 *   available - a pointer where we will fill in the amount of occupied entries in the ring.
 *
 * Returns:
 *   1. The slice of elements we dequeued from the ring.
 *   2. `nil` if the entire batch was successfully dequeued, or an error explaining why we could not
 *      dequeue the batch.
 */
func (ring *Ring[T]) DequeueBulk(
        numElems uint32,
        available *uint32,
) ([]T, error) {
        elements, numDequeued := ring.doDequeue(numElems, available, true)
        if numDequeued != numElems {
                return []T{}, fmt.Errorf(
                        "needed %d occupied entries in the ring, but only %d were occupied",
                        numElems,
                        numDequeued,
                )
        }

        return elements, nil
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
        available *uint32,
) ([]T, uint32) {
        return ring.doDequeue(
                numElems,
                available,
                false,
        )
}

func (ring *Ring[T]) doEnqueue(
        elements []T,
        numElems uint32,
        freeSpace *uint32,
        fixedEnqueue bool,
) uint32 {
        var (
                prodHead uint32
                prodNext uint32
        )

        // get the number of entries that we have space to accommodate
        numEntries, freeEntries := ring.moveProdHead(
                numElems,
                &prodHead,
                &prodNext,
                fixedEnqueue,
        )
        if numEntries == 0 {
                updateValidPtr(freeSpace, freeEntries)
                return numEntries
        }

        baseIdx := prodHead & ring.mask
        n1 := ring.size - baseIdx
        if n1 > numEntries {
                n1 = numEntries
        }

        // enqueue the elements -- first contiguous chunk
        for i := uint32(0); i < n1; i++ {
                ring.entries[int(baseIdx+i)] = elements[i]
        }

        // second chunk if wrapped
        for i := n1; i < numEntries; i++ {
                ring.entries[int(i - n1)] = elements[i]
        }

        // update the tail after a successful enqueue
        updateTail(ring.prodTail, ring.multiProdEnqueue, prodHead, prodNext)

        updateValidPtr(freeSpace, freeEntries - numEntries)
        return numEntries
}

func (ring *Ring[T]) doDequeue(
        numElems uint32,
        available *uint32,
        fixedDequeue bool,
) ([]T, uint32) {
        var (
                consHead uint32
                consNext uint32
        )

        // get the number of entries that we can actually dequeue
        numEntries, filledBefore := ring.moveConsHead(
                numElems,
                &consHead,
                &consNext,
                fixedDequeue,
        )

        elements := make([]T, int(numEntries))
        if numEntries == 0 {
                updateValidPtr(available, filledBefore)
                return elements, numEntries
        }

        baseIdx := consHead & ring.mask
        n1 := ring.size - baseIdx
        if n1 > numEntries {
                n1 = numEntries
        }

        // dequeue the elements -- first chunk
        for i := uint32(0); i < n1; i++ {
                elements[i] = ring.entries[int(baseIdx+i)]
        }

        // second chunk if wrapped
        for i := n1; i < numEntries; i++ {
                elements[i] = ring.entries[int(i - n1)]
        }

        // update the tail after a successful dequeue
        updateTail(ring.consTail, ring.multiConsDequeue, consHead, consNext)

        updateValidPtr(available, filledBefore - numEntries)
        return elements, numEntries
}

/**
 * moveProdHead moves the ring's producer head for an enqueue operation.
 *
 * Params:
 *   numItems - The number of items we want to enqueue, i.e., how far the head should be moved.
 *   oldHead - Saves the producer head value from *before* the move (where the enqueue starts).
 *   newHead - Saves the producer head value from *after* the move (where the enqueue ends).
 *   fixedEnqueue - If we are doing a bulk enqueue (where we enqueue the entire batch size, or don't
 *       enqueue anything).
 *
 * Returns:
 *   1. The actual number of items enqueued. This can lie anywhere between 0 and numItems
 *      (inclusive). If it is a fixedEnqueue, then 0 or numItems are the only possible values.
 *   2. Returns the number of free entries in the ring before the head was moved.
 */
func (ring *Ring[T]) moveProdHead(
        numItems uint32,
        oldHead *uint32,
        newHead *uint32,
        fixedEnqueue bool,
) (uint32, uint32) {
        var (
                consTail    uint32
                freeEntries uint32
                success     bool
        )

        multiProd := ring.multiProdEnqueue
        capacity := ring.capacity
        maxItems := numItems

        // Go doesn't have any relaxed memory ordering tools or the ability to leverage memory
        // fences (from what I can tell), so we will have to use sequentially consistent atomic
        // operations for each of these.
        *oldHead = atomic.LoadUint32(ring.prodHead)
        for {
                numItems = maxItems
                consTail = atomic.LoadUint32(ring.consTail)

                /* uint32 subtraction trickery makes this calculation always valid, even if
                 * `*oldHead > consTail`.
                 */
                freeEntries = (capacity + consTail - *oldHead)
                if numItems > freeEntries {
                        if fixedEnqueue {
                                return 0, freeEntries
                        }
                        numItems = freeEntries
                }

                if numItems == 0 {
                        return 0, 0
                }

                *newHead = *oldHead + numItems
                if multiProd {
                        success = atomic.CompareAndSwapUint32(ring.prodHead, *oldHead, *newHead)
                } else {
                        atomic.StoreUint32(ring.prodHead, *newHead)
                        success = true
                }

                if success == true {
                        break
                }
        }

        return numItems, freeEntries
}

/**
 * moveConsHead moves the ring's consumer head for a dequeue operation.
 *
 * Params:
 *   numItems - The number of items we want to dequeue, i.e., how far the head should be moved.
 *   oldHead - Saves the consumer head value from *before* the move (where the dequeue starts).
 *   newHead - Saves the consumer head value from *after* the move (where the dequeue ends).
 *   fixedDequeue - If we are doing a bulk enqueue (where we dequeue the entire batch size, or don't
 *       dequeue anything).
 *
 * Returns:
 *   1. The actual number of items dequeued. This can lie anywhere between 0 and numItems
 *      (inclusive). If it is a fixedDequeue, then 0 or numItems are the only possible values.
 *   2. Returns the number of occupied entries in the ring before the head was moved.
 */
func (ring *Ring[T]) moveConsHead(
        numItems uint32,
        oldHead *uint32,
        newHead *uint32,
        fixedDequeue bool,
) (uint32, uint32) {
        var (
                prodTail uint32
                filledEntries  uint32
                success  bool
        )

        multiCons := ring.multiConsDequeue
        maxItems := numItems

        *oldHead = atomic.LoadUint32(ring.consHead)
        for {
                numItems = maxItems
                prodTail = atomic.LoadUint32(ring.prodTail)

                /* uint32 subtraction trickery makes this calculation always valid, even if
                 * `*oldHead > prodTail`.
                 */
                filledEntries = prodTail - *oldHead
                if numItems > filledEntries {
                        if fixedDequeue {
                                return 0, filledEntries
                        }
                        numItems = filledEntries
                }

                if numItems == 0 {
                        return 0, 0
                }

                *newHead = *oldHead + numItems
                if multiCons {
                        success = atomic.CompareAndSwapUint32(ring.consHead, *oldHead, *newHead)
                } else {
                        *ring.consHead = *newHead
                        success = true
                }

                if success == true {
                        break
                }
        }

        return numItems, filledEntries
}

/**
 * updateTail updates the tail for the ring. Since it takes an arbitrary ring, it can handle both
 * enqueue (passing the producer tail) and dequeue (passing the consumer tail).
 *
 * Params:
 *   tailPtr - a pointer to ring's tail index that should be moved.
 *   multi - whether this ring operation supports multiple producers/consumers.
 *   oldTail - the tail value that we need to see in order to start our tail update. In the multiple
 *       producer/consumer case, we must wait for operations in front of us to update the tail before
 *       we can go.
 *   newTail - the tail value that we are updating the ring tail to reflect.
 */
func updateTail(
        tailPtr *uint32,
        multi bool,
        oldTail uint32,
        newTail uint32,
) {
        if multi {
                // need to wait for the other enqueues/dequeues that precede us
                // to complete, before we can go on.
                for !atomic.CompareAndSwapUint32(tailPtr, oldTail, newTail) {
                        runtime.Gosched()
                }
                return
        }

        atomic.StoreUint32(tailPtr, newTail)
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
