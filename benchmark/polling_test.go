package benchmark

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
	"github.com/freshdresch/go-data-structures/ring/frontier"
)

// queue is the shape the harness drives. Each method moves as many elements as it can right
// now, without blocking, and returns how many it moved.
type queue[T any] interface {
	enqueue(items []T) int
	dequeue(dst []T) int
}

// consume dequeues from source into dst until the producers are done and source is empty, or
// until the run aborts, passes each dequeued batch to recorder, and returns how many elements it
// received. dst must not be empty. It yields after every attempt that moved nothing.
//
// Important: it loads producersDone before each dequeue attempt, and returns only when the flag
// was already set before an attempt that moved nothing. The reverse order, dequeue and then load
// the flag, can return while a producer's last elements are still in flight. The dequeue finds the
// queue empty, then the producer enqueues its last elements and finishes, and then the flag load
// sees the flag set.
func consume[T any, S sink[T]](
	source queue[T],
	state *runState,
	producersDone *atomic.Bool,
	dst []T,
	recorder S,
) int {
	received := 0
	for {
		done := producersDone.Load()
		if state.aborted() {
			return received
		}
		moved := source.dequeue(dst)
		if moved > 0 {
			recorder.record(dst[:moved])
			received += moved
			continue
		}
		if done {
			return received
		}
		runtime.Gosched()
	}
}

// produce sends total elements to target in batches of len(buf), the last one shorter when total
// does not divide evenly. It refills buf before each batch and retries the unsent remainder,
// yielding whenever an attempt moves nothing. It returns early when the run aborts.
func produce[T any](
	target queue[T],
	state *runState,
	buf []T,
	total, producer int,
	fill fillFunc[T],
) {
	for sent := 0; sent < total; {
		if state.aborted() {
			return
		}
		batch := buf[:min(len(buf), total-sent)]
		fill(batch, producer, int64(sent))
		enqueueAll(target, state, batch)
		sent += len(batch)
	}
}

// enqueueAll enqueues every element of items on target, retrying the unsent remainder and
// yielding whenever an attempt moves nothing. A full queue is normal backpressure, so it retries.
// It returns early when the run aborts, because an aborted run's queue may never drain.
func enqueueAll[T any](target queue[T], state *runState, items []T) {
	for unsent := items; len(unsent) > 0; {
		if state.aborted() {
			return
		}
		moved := target.enqueue(unsent)
		if moved == 0 {
			runtime.Gosched()
		}
		unsent = unsent[moved:]
	}
}

// runPolling moves total elements from mode's producers to its consumers through target, in
// batches of batch, and returns how many the consumers received. Consumers yield on every empty
// poll. The error is the run's first adapter error, if any, and the caller decides how to fail.
func runPolling[T any, S sink[T]](
	timer benchTimer,
	state *runState,
	target queue[T],
	mode benchMode,
	batch, total int,
	fill fillFunc[T],
	sinks func(consumer int) S,
) (received int, err error) {
	bufs := newBuffers[T](mode.producers, batch)
	dsts := newBuffers[T](mode.consumers, batch)
	recorders := make([]S, mode.consumers)
	for c := range recorders {
		recorders[c] = sinks(c)
	}
	counts := make([]int, mode.consumers)

	runTimed(timer, func() {
		var producersDone atomic.Bool
		var producerGroup, consumerGroup sync.WaitGroup
		for c := range mode.consumers {
			consumerGroup.Go(func() {
				counts[c] = consume(
					target,
					state,
					&producersDone,
					dsts[c],
					recorders[c],
				)
			})
		}
		for p := range mode.producers {
			producerGroup.Go(func() {
				produce(target, state, bufs[p], producerShare(total, mode.producers, p), p, fill)
			})
		}
		producerGroup.Wait()
		producersDone.Store(true)
		consumerGroup.Wait()
	})
	for _, count := range counts {
		received += count
	}
	if state.firstError() != nil {
		return received, fmt.Errorf("mode=%s: %w", mode.name, state.firstError())
	}
	return received, nil
}

type spscBurstQueue[T any] struct {
	ring *dpdk.SPSCQueue[T]
}

func (q *spscBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *spscBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

type mpscBurstQueue[T any] struct {
	ring *dpdk.MPSCQueue[T]
}

func (q *mpscBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *mpscBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

type spmcBurstQueue[T any] struct {
	ring *dpdk.SPMCQueue[T]
}

func (q *spmcBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *spmcBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

type mpmcBurstQueue[T any] struct {
	ring *dpdk.MPMCQueue[T]
}

func (q *mpmcBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *mpmcBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

// newModeRing chooses and wraps a concrete public queue before timing starts.
// Each adapter calls its concrete queue directly so ring rows have one dispatch boundary,
// like the third-party adapters. Concurrent sides default to ordered publication.
func newModeRing[T any](
	mode benchMode,
	state *runState,
	single bool,
	extra ...dpdk.Option,
) (queue[T], error) {
	var options []dpdk.Option
	if mode.relaxed {
		if mode.producers > 1 {
			options = append(options, dpdk.WithRelaxedMultiProdEnqueue())
		}
		if mode.consumers > 1 {
			options = append(options, dpdk.WithRelaxedMultiConsDequeue())
		}
	}
	options = append(options, extra...)
	switch {
	case mode.producers > 1 && mode.consumers > 1:
		created, err := dpdk.NewMPMCQueue[T](ringCapacity, options...)
		if err != nil {
			return nil, err
		}
		if single {
			return &mpmcSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &mpmcBurstQueue[T]{ring: created}, nil
	case mode.producers > 1:
		created, err := dpdk.NewMPSCQueue[T](ringCapacity, options...)
		if err != nil {
			return nil, err
		}
		if single {
			return &mpscSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &mpscBurstQueue[T]{ring: created}, nil
	case mode.consumers > 1:
		created, err := dpdk.NewSPMCQueue[T](ringCapacity, options...)
		if err != nil {
			return nil, err
		}
		if single {
			return &spmcSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &spmcBurstQueue[T]{ring: created}, nil
	default:
		created, err := dpdk.NewSPSCQueue[T](ringCapacity, options...)
		if err != nil {
			return nil, err
		}
		if single {
			return &spscSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &spscBurstQueue[T]{ring: created}, nil
	}
}

// newModeFrontier builds the ring/frontier queue for mode before timing starts, wrapped like
// newModeRing's adapters. It supports only the modes in frontierModes.
func newModeFrontier[T any](mode benchMode, state *runState, single bool) (queue[T], error) {
	switch {
	case mode.producers > 1 && mode.consumers == 1:
		created, err := frontier.NewMPSCQueue[T](ringCapacity)
		if err != nil {
			return nil, err
		}
		if single {
			return &frontierMPSCSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &frontierMPSCBurstQueue[T]{ring: created}, nil
	case mode.producers == 1 && mode.consumers > 1:
		created, err := frontier.NewSPMCQueue[T](ringCapacity)
		if err != nil {
			return nil, err
		}
		if single {
			return &frontierSPMCSingleQueue[T]{
				ring: created, ringSingleQueue: ringSingleQueue{state: state},
			}, nil
		}
		return &frontierSPMCBurstQueue[T]{ring: created}, nil
	default:
		return nil, fmt.Errorf("frontier has no queue for mode %s", mode.name)
	}
}

type frontierSPMCBurstQueue[T any] struct {
	ring *frontier.SPMCQueue[T]
}

func (q *frontierSPMCBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *frontierSPMCBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

type frontierSPMCSingleQueue[T any] struct {
	ringSingleQueue
	ring *frontier.SPMCQueue[T]
}

func (q *frontierSPMCSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), frontier.ErrFull)
}

func (q *frontierSPMCSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, frontier.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

type frontierMPSCBurstQueue[T any] struct {
	ring *frontier.MPSCQueue[T]
}

func (q *frontierMPSCBurstQueue[T]) enqueue(items []T) int {
	enqueued, _ := q.ring.EnqueueBurst(items)
	return enqueued
}

func (q *frontierMPSCBurstQueue[T]) dequeue(dst []T) int {
	dequeued, _ := q.ring.DequeueBurst(dst)
	return dequeued
}

type frontierMPSCSingleQueue[T any] struct {
	ringSingleQueue
	ring *frontier.MPSCQueue[T]
}

func (q *frontierMPSCSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), frontier.ErrFull)
}

func (q *frontierMPSCSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, frontier.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

// rtsMode returns a copy of mode whose multi sides use relaxed tail sync.
func rtsMode(mode benchMode) benchMode {
	relaxed := mode
	relaxed.relaxed = true
	return relaxed
}

// ringSingleQueue shares run error handling without adding queue-method dispatch.
type ringSingleQueue struct {
	state *runState
}

type spscSingleQueue[T any] struct {
	ringSingleQueue
	ring *dpdk.SPSCQueue[T]
}

func (q *spscSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), dpdk.ErrFull)
}

func (q *spscSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, dpdk.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

type mpscSingleQueue[T any] struct {
	ringSingleQueue
	ring *dpdk.MPSCQueue[T]
}

func (q *mpscSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), dpdk.ErrFull)
}

func (q *mpscSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, dpdk.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

type spmcSingleQueue[T any] struct {
	ringSingleQueue
	ring *dpdk.SPMCQueue[T]
}

func (q *spmcSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), dpdk.ErrFull)
}

func (q *spmcSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, dpdk.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

type mpmcSingleQueue[T any] struct {
	ringSingleQueue
	ring *dpdk.MPMCQueue[T]
}

func (q *mpmcSingleQueue[T]) enqueue(items []T) int {
	return q.movedOrFail(q.ring.Enqueue(items[0]), dpdk.ErrFull)
}

func (q *mpmcSingleQueue[T]) dequeue(dst []T) int {
	elem, err := q.ring.Dequeue()
	if q.movedOrFail(err, dpdk.ErrEmpty) == 0 {
		return 0
	}
	dst[0] = elem
	return 1
}

// movedOrFail returns 1 when err is nil, and 0 when err is the expected error. Any other error
// fails the run, and it returns 0. It compares with == because the ring returns ErrFull and
// ErrEmpty unwrapped.
func (q *ringSingleQueue) movedOrFail(err, expected error) int {
	if err == nil {
		return 1
	}
	if err == expected {
		return 0
	}
	q.state.fail(err)
	return 0
}

// xsyncQueue drives xsync's MPMCQueue through TryEnqueue and TryDequeue, one element per call.
type xsyncQueue[T any] struct {
	mpmc *xsync.MPMCQueue[T]
}

// newXsyncQueue returns an xsyncQueue over a queue of ringCapacity slots.
func newXsyncQueue[T any]() *xsyncQueue[T] {
	return &xsyncQueue[T]{mpmc: xsync.NewMPMCQueue[T](ringCapacity)}
}

// enqueue moves only items[0], so it returns 0 or 1.
func (q *xsyncQueue[T]) enqueue(items []T) int {
	if !q.mpmc.TryEnqueue(items[0]) {
		return 0
	}
	return 1
}

// dequeue fills only dst[0], so it returns 0 or 1.
func (q *xsyncQueue[T]) dequeue(dst []T) int {
	elem, ok := q.mpmc.TryDequeue()
	if !ok {
		return 0
	}
	dst[0] = elem
	return 1
}

// mutexQueue is a bounded FIFO of ringCapacity slots in a slice under a sync.Mutex, the common
// hand-rolled alternative to a ring. head and tail count every element ever dequeued and
// enqueued, and a slot's index is the count masked by ringCapacity - 1, which works because
// ringCapacity is a power of two. It moves at most one element per call, like the libraries'
// single-element calls.
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

// enqueue moves only items[0], so it returns 0 or 1.
func (q *mutexQueue[T]) enqueue(items []T) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.tail-q.head == len(q.slots) {
		return 0
	}
	q.slots[q.tail&(ringCapacity-1)] = items[0]
	q.tail++
	return 1
}

// dequeue fills only dst[0], so it returns 0 or 1. It zeroes the slot it empties, so the queue
// does not keep pointers alive.
func (q *mutexQueue[T]) dequeue(dst []T) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.tail == q.head {
		return 0
	}
	slot := q.head & (ringCapacity - 1)
	dst[0] = q.slots[slot]
	var zero T
	q.slots[slot] = zero
	q.head++
	return 1
}
