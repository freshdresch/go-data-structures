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

        size     uint32 /* Size of the ring. */
        mask     uint32 /* Mask of the ring (`size - 1`). */
        capacity uint32 /* Usable size of the ring. */

        multiProdEnqueue bool /* Multi-Producer enqueue instead of single. */
        multiConsDequeue bool /* Multi-Consumer dequeue instead of single. */

        _ [2]byte /* padding for better cache alignment */
}

func WithMultiProdEnqueue[T any]() func(*Ring[T]) {
        return func(ring *Ring[T]) {
                ring.multiProdEnqueue = true
        }
}

func WithMultiConsDequeue[T any]() func(*Ring[T]) {
        return func(ring *Ring[T]) {
                ring.multiConsDequeue = true
        }
}

func New[T any](count uint32, opts ...func(*Ring[T])) (*Ring[T], error) {
        if count % 2 != 0 {
                return &Ring[T]{}, errors.New("count is not a power of 2")
        }
        mask := count - 1

        ring := &Ring[T]{
                entries:  make([]T, count),
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
                        *freeSpace,
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
 *   the number of elements successfully enqueued (on the closed interval [0, n]).
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

func (ring *Ring[T]) doEnqueue(
        elements []T,
        numElems uint32,
        freeSpace *uint32,
        fixedEnqueue bool,
) uint32 {
        var (
                prodHead uint32
                prodNext uint32
                i        uint32
        )

        // get the number of entries that we have space to accommodate
        numEntries, freeEntries := ring.moveProdHead(
                numElems,
                &prodHead,
                &prodNext,
                fixedEnqueue,
        )
        if numEntries == 0 {
                goto end
        }

        // enqueue the elements
        if prodNext > prodHead {
                for i = 0; i < numEntries; i++ {
                        ring.entries[prodHead+i] = elements[i]
                }
        } else {
                var idx uint32
                // use more expensive wrappingAdd since we are wrapping around
                for i = 0; i < numEntries; i++ {
                        idx = wrappingAdd(prodHead, i, ring.mask)
                        ring.entries[idx] = elements[i]
                }
        }

        // update the tail after a successful enqueue
        updateTail(ring.prodTail, ring.multiProdEnqueue, prodHead, prodNext)

end:
        if freeSpace != nil {
                *freeSpace = freeEntries - numEntries
        }
        return numEntries
}

/**
 * moveProdHead moves the ring's producer head for an enqueue operation. This will automatically
 * wrap based on the ring's mask.
 *
 * Params:
 *   numItems - The number of items we want to enqueue, i.e., how far the head should be moved.
 *   oldHead - Saves the producer head value from *before* the move (where the enqueue starts).
 *   newHead - Saves the producer head value from *after* the move (where the enqueue ends).
 *   fixedEnqueue - If we are doing a bulk enqueue (where we enqueue the entire batch size, or don't
 *             enqueue anything).
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

                *newHead = wrappingAdd(*oldHead, numItems, ring.mask)
                if multiProd {
                        success = atomic.CompareAndSwapUint32(ring.prodHead, *oldHead, *newHead)
                } else {
                        *ring.prodHead = *newHead
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
 *   oldHead  - Saves the consumer head value from *before* the move (where the dequeue starts).
 *   newHead  - Saves the producer head value from *after* the move (where the dequeue ends).
 *
 * Returns:
 *   1. The actual number of items enqueued. This can lie anywhere between 0 and numItems
        (inclusive).
 *   2. Returns the number of free entries in the ring before the head was moved.
 */
func (ring *Ring[T]) moveConsHead(
        numItems uint32,
        // TODO I think we want the updates for these so they should stay pointers, but once I
        // start using the function, if we only care about feeding the value in, we could supply
        // straight up uint32s instead
        oldHead *uint32,
        newHead *uint32,
) (uint32, uint32) {
        var (
                prodTail uint32
                entries  uint32
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
                entries = prodTail - *oldHead
                if numItems > entries {
                        numItems = entries
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

        return numItems, entries
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

        // TODO why does the single-producer/single-consumer (aka serialized) case have an atomic op
        // here? Can't we just assign it straight up?
        // atomic.StoreUint32(ring.prodTail, newTail)
        *tailPtr = newTail
}

// Note: this is only valid if we can perform `base + addend` without overflowing the uint32.
// However, this is only a danger if someone is trying to create a ring of size 2^32 (which would
// be >4 billion entries) so we should be safe.
func wrappingAdd(base, addend, mask uint32) uint32 {
        return (base + addend) & mask
}
