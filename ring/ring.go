package ring

import (
        "runtime"
        "sync/atomic"
)

// Ring is a Go port of rte_ring from DPDK. It provides a fixed-size ring buffer
// that can function as anything between an SPSC and MPMC queue. It defaults to
// an SPSC queue in the base implementation, and multi-producer/multi-consumer
// semantics can be added through functional options. So an MPMC queue would be
// the ring initialized with both the multi-producer and multi-consumer options.
//
// It can incur a non-trivial performance hit in order to hold truly generic
// elements, so if bleeding-edge performance is desired, stick to the types
// required by the DPDK ring buffer implementation: a pointer, or 4-byte
// boundary aligned element. Otherwise, an element could be split between
// adjacent cache lines.
//
// The actual number of entries in the ring is *count - 1*, because the *count*
// entry is used to determine whether the ring is empty or not. If you
// specifically need *count* elements, see the friendlier-but-slower queue
// implementation in [TODO reference once I implement this].
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

        _  [2]byte /* padding for better cache alignment */
}

type RingOption func(*Ring)

func WithMultiProdEnqueue() RingOption {
        return func(ring *Ring) {
                ring.multiProdEnqueue = true
        }
}

func WithMultiConsDequeue() RingOption {
        return func(ring *Ring) {
                ring.multiConsDequeue = true
        }
}

func New[T any](count uint32, opts ...RingOption) (*Ring[T], error) {
        if count % 2 != 0 {
                return &Ring[T]{}, error.New("count is not a power of 2")
        }
        mask := count - 1

        ring := &Ring[T]{
                entries:  make([]T, count),
                size:     count,
                mask:     mask
                capacity: mask,
        }

        *ring.prodHead = 0
        *ring.prodTail = 0
        *ring.consHead = 0
        *ring.consTail = 0

        for _, opt := range opts {
                opt(ring)
        }

        return ring
}

/**
 * moveProdHead moves the ring's producer head for an enqueue operation.
 *
 * Params:
 *   numItems - The number of items we want to enqueue, i.e., how far the head
 *              should be moved.
 *   oldHead  - Saves the producer head value from *before* the move (where the
 *              enqueue starts).
 *   newHead  - Saves the producer head value from *after* the move (where the
 *              enqueue ends).
 *
 * Returns:
 *   1. The actual number of items enqueued. This can lie anywhere between 0
 *      and numItems (inclusive).
 *   2. Returns the number of free entries in the ring before the head was moved.
 */
func (ring *Ring[T]) moveProdHead(
        numItems uint32,
        oldHead *uint32,
        newHead *uint32,
) (uint32, uint32) {
        var (
                consTail    uint32
                freeEntries uint32
                success     bool
        )

        multiProd := ring.multiProdEnqueue
        capacity := ring.capacity
        maxItems := numItems

        // Go doesn't have any relaxed memory ordering tools or the ability to
        // leverage memory fences (from what I can tell), so we will have to 
        // use sequentially consistent atomic operations for each of these.
        *oldHead = atomic.LoadUint32(ring.prodHead)
        for {
                numItems = maxItems
                consTail = atomic.LoadUint32(ring.consTail)

                /* uint32 subtraction trickery makes this calculation always
                 * valid, even if *oldHead > consTail.
                 */
                freeEntries = (capacity + consTail - *oldHead)
                if numItems > freeEntries {
                        numItems = freeEntries
                }

                *newHead = *oldHead + numItems
                if multiProd {
                        success = atomic.CompareAndSwap(ring.prodHead, *oldHead, *newHead)
                } else {
                        ring.prodHead = *newHead
                        success = true
                }

                if (success == true) break
        }

        return numItems, freeEntries
}

/**
 * moveConsHead moves the ring's consumer head for a dequeue operation.
 *
 * Params:
 *   numItems - The number of items we want to dequeue, i.e., how far the head
 *              should be moved.
 *   oldHead  - Saves the consumer head value from *before* the move (where the
 *              dequeue starts).
 *   newHead  - Saves the producer head value from *after* the move (where the
 *              dequeue ends).
 *
 * Returns:
 *   1. The actual number of items enqueued. This can lie anywhere between 0
 *      and numItems (inclusive).
 *   2. Returns the number of free entries in the ring before the head was moved.
 */
func (ring *Ring[T]) moveConsHead(
        numItems uint32,
        // TODO I think we want the updates for these so they should stay pointers,
        // but once I start using the function, if we only care about feeding the
        // value in, we could supply straight up uint32s instead
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

                /* uint32 subtraction trickery makes this calculation always
                 * valid, even if *oldHead > prodTail.
                 */
                entries = prodTail - *oldHead
                if numItems > entries {
                        numItems = entries
                }

                *newHead = *oldHead + numItems
                if multiCons {
                        success = atomic.CompareAndSwap(ring.consHead, *oldHead, *newHead)
                } else {
                        ring.consHead = *newHead
                        success = true
                }

                if (success == true) break
        }

        return numItems, entries
}

/**
 * updateTail updates the tail for the ring. Since it takes an arbitrary ring,
 * it can handle both enqueue (passing the producer tail) and dequeue (passing
 * the consumer tail).
 *
 * Params:
 *   tailPtr - a pointer to ring's tail index that should be moved.
 *   multi   - whether this ring operation supports multiple producers/consumers.
 *   oldTail - the tail value that we need to see in order to start our tail
 *             update. In the multiple producer/consumer case, we must wait for
 *             operations in front of us to update the tail before we can go.
 *   newTail - the tail value that we are updating the ring tail to reflect.
 */
func updateTail(
        tailPtr *uint32,
        multi    bool,
        oldTail  uint32,
        newTail  uint32,
){
        if multi {
                // need to wait for the other enqueues/dequeues that precede us
                // to complete, before we can go on.
                for !atomic.CompareAndSwap(tailPtr, oldTail, newTail) {
                        runtime.Gosched()
                }
                return
        }

        // TODO why does the single-producer/single-consumer (aka serialized)
        // case have an atomic op here? Can't we just assign it straight up?
        // atomic.StoreUint32(ring.prodTail, newTail)
        *tailPtr = newTail
}
