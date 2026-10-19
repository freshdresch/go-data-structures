package ring_test

import (
	"sync"

	"github.com/freshdresch/go-data-structures/ring"
)

// adapterImpl names an implementation that runs through the shared queue harness.
type adapterImpl[T any] struct {
	name     string
	newQueue func() (queue[T], error)
}

// adapterImpls returns every implementation that runs through the shared queue harness, each
// built for mode.
func adapterImpls[T any](mode benchMode) []adapterImpl[T] {
	return []adapterImpl[T]{
		{name: "ring", newQueue: func() (queue[T], error) {
			created, err := newRingQueue[T](mode)
			if err != nil {
				return nil, err
			}
			return created, nil
		}},
		{name: "chanpoll", newQueue: func() (queue[T], error) {
			return newChanPollQueue[T](), nil
		}},
		{name: "mutex", newQueue: func() (queue[T], error) {
			return newMutexQueue[T](), nil
		}},
	}
}

// ringQueue drives the ring through EnqueueBurst and DequeueBurst.
type ringQueue[T any] struct {
	ring *ring.Ring[T]
}

// newRingQueue returns a ringQueue whose ring has ringCapacity slots and allows as many
// producers and consumers as mode has.
func newRingQueue[T any](mode benchMode) (*ringQueue[T], error) {
	var options []ring.Option
	if mode.producers > 1 {
		options = append(options, ring.WithMultiProdEnqueue())
	}
	if mode.consumers > 1 {
		options = append(options, ring.WithMultiConsDequeue())
	}
	created, err := ring.New[T](ringCapacity, options...)
	if err != nil {
		return nil, err
	}
	return &ringQueue[T]{ring: created}, nil
}

func (q *ringQueue[T]) enqueue(items []T) int {
	return int(q.ring.EnqueueBurst(items, uint32(len(items)), nil))
}

// dequeue copies the elements DequeueBurst returns into dst, because today's DequeueBurst returns
// a new slice. The copy disappears with RING_DESIGN change 2, which dequeues into the caller's
// slice.
func (q *ringQueue[T]) dequeue(dst []T) int {
	elems, _ := q.ring.DequeueBurst(uint32(len(dst)), nil)
	return copy(dst, elems)
}

// chanPollQueue uses a channel as a polled, non-blocking queue, moving one element at a time
// with select and default.
type chanPollQueue[T any] struct {
	ch chan T
}

// newChanPollQueue returns a chanPollQueue over a channel of ringCapacity slots.
func newChanPollQueue[T any]() *chanPollQueue[T] {
	return &chanPollQueue[T]{ch: make(chan T, ringCapacity)}
}

func (q *chanPollQueue[T]) enqueue(items []T) int {
	for k, v := range items {
		select {
		case q.ch <- v:
		default:
			return k
		}
	}
	return len(items)
}

func (q *chanPollQueue[T]) dequeue(dst []T) int {
	for k := range dst {
		select {
		case dst[k] = <-q.ch:
		default:
			return k
		}
	}
	return len(dst)
}

// mutexQueue is a bounded FIFO of ringCapacity slots in a slice under a sync.Mutex, the common
// hand-rolled alternative to a ring. head and tail count every element ever dequeued and
// enqueued, and a slot's index is the count masked by ringCapacity - 1, which works because
// ringCapacity is a power of two.
type mutexQueue[T any] struct {
	mu    sync.Mutex
	slots []T
	head  int
	tail  int
}

// newMutexQueue returns an empty mutexQueue.
func newMutexQueue[T any]() *mutexQueue[T] {
	return &mutexQueue[T]{slots: make([]T, ringCapacity)}
}

func (q *mutexQueue[T]) enqueue(items []T) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	free := len(q.slots) - (q.tail - q.head)
	moved := min(len(items), free)
	for k := range moved {
		q.slots[(q.tail+k)&(ringCapacity-1)] = items[k]
	}
	q.tail += moved
	return moved
}

// dequeue zeroes each slot it empties, so the queue does not keep pointers alive.
func (q *mutexQueue[T]) dequeue(dst []T) int {
	var zero T
	q.mu.Lock()
	defer q.mu.Unlock()
	moved := min(len(dst), q.tail-q.head)
	for k := range moved {
		slot := (q.head + k) & (ringCapacity - 1)
		dst[k] = q.slots[slot]
		q.slots[slot] = zero
	}
	q.head += moved
	return moved
}
