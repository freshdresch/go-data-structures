package ring

import "sync/atomic"

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

func (ring *Ring[T]) moveProdHead(
        numItems uint32,
        oldHead *uint32,
        newHead *uint32,
        freeEntries *uint32,
) uint32 {
        var (
                consTail uint32
                success  bool
        )

        multiProd := ring.multiProdEnqueue
        capacity := ring.capacity
        maxItems := numItems

        // Go doesn't have any relaxed memory ordering utilities from what I
        // can tell, so these will all be strongly ordered.
        *oldHead = atomic.LoadUint32(ring.prodHead)
        for {
                numItems = maxItems
                consTail = atomic.LoadUint32(ring.consTail)

                /* uint32 subtractions trickery makes this calculation always
                 * valid, even if *oldHead > consTail.
                 */
                freeEntries = (capacity + consTail - *oldHead)
                if numItems > *freeEntries {
                        numItems = *freeEntries
                }

                newHead = *oldHead + numItems
                if multiProd {
                        success = atomic.CompareAndSwap(ring.prodHead, *oldHead, *newHead)
                } else {
                        ring.prodHead = *newHead
                        success = true
                }

                if (success == true) break
        }

        return numItems
}
