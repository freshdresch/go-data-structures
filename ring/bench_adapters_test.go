package ring_test

import (
	"errors"
	"fmt"
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

// newModeRing returns a ring with ringCapacity slots that allows as many producers and consumers
// as mode has.
func newModeRing[T any](mode benchMode) (*ring.Ring[T], error) {
	var options []ring.Option
	if mode.producers > 1 {
		options = append(options, ring.WithMultiProdEnqueue())
	}
	if mode.consumers > 1 {
		options = append(options, ring.WithMultiConsDequeue())
	}
	return ring.New[T](ringCapacity, options...)
}

// newRingQueue returns a ringQueue over a ring built for mode.
func newRingQueue[T any](mode benchMode) (*ringQueue[T], error) {
	created, err := newModeRing[T](mode)
	if err != nil {
		return nil, err
	}
	return &ringQueue[T]{ring: created}, nil
}

func (q *ringQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *ringQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

// newRingAPIQueue returns the adapter that drives a ring built for mode through api.
func newRingAPIQueue[T any](api ringAPI, mode benchMode) (queue[T], error) {
	var wrap func(*ring.Ring[T]) queue[T]
	switch api {
	case apiSingle:
		wrap = func(base *ring.Ring[T]) queue[T] { return &ringSingleQueue[T]{ring: base} }
	case apiBurst:
		wrap = func(base *ring.Ring[T]) queue[T] { return &ringQueue[T]{ring: base} }
	case apiBulk:
		wrap = func(base *ring.Ring[T]) queue[T] { return &ringBulkQueue[T]{ring: base} }
	default:
		return nil, fmt.Errorf("unknown ring API %v", api)
	}
	created, err := newModeRing[T](mode)
	if err != nil {
		return nil, err
	}
	return wrap(created), nil
}

// ringSingleQueue drives the ring through Enqueue and Dequeue, moving at most one element per
// call. It is rewritten when the ring API changes.
type ringSingleQueue[T any] struct {
	ring *ring.Ring[T]
}

// enqueue moves only items[0], so it returns 0 or 1.
func (q *ringSingleQueue[T]) enqueue(items []T) int {
	return movedOrPanic(q.ring.Enqueue(items[0]), ring.ErrFull, 1)
}

// dequeue fills only dst[0], so it returns 0 or 1.
func (q *ringSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if movedOrPanic(err, ring.ErrEmpty, 1) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

// ringBulkQueue drives the ring through EnqueueBulk and DequeueBulk, which move all of the
// elements or none, so each call returns 0 or the full length. It is rewritten when the ring API
// changes.
type ringBulkQueue[T any] struct {
	ring *ring.Ring[T]
}

func (q *ringBulkQueue[T]) enqueue(items []T) int {
	_, err := q.ring.EnqueueBulk(items)
	return movedOrPanic(err, ring.ErrFull, len(items))
}

func (q *ringBulkQueue[T]) dequeue(dst []T) int {
	_, err := q.ring.DequeueBulk(dst)
	return movedOrPanic(err, ring.ErrEmpty, len(dst))
}

// movedOrPanic returns 0 when err is the call's expected failure, and moved when err is nil. Any
// other error is a test bug, so it panics with it.
func movedOrPanic(err, failure error, moved int) int {
	if errors.Is(err, failure) {
		return 0
	}
	if err != nil {
		panic(err)
	}
	return moved
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
