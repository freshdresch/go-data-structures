package benchmark

import (
	"errors"
	"fmt"
	"sync"

	workiva "github.com/Workiva/go-datastructures/queue"
)

// blockingQueue is the shape the blocking runner drives. put and get block until they can move
// one element. get reports false once the queue has stopped. stop, called after every element has
// been put, tells that many consumers to stop.
type blockingQueue[T any] interface {
	put(item T)
	get() (item T, ok bool)
	stop(consumers int)
}

// consumeBlocking gets from source until it reports false, passes each element to recorder
// through dst, which holds one element, and returns how many it received. Recording through dst
// keeps the consumer from allocating, where a slice literal would escape to the heap.
func consumeBlocking[T any, S sink[T]](source blockingQueue[T], dst []T, recorder S) int {
	received := 0
	for {
		elem, ok := source.get()
		if !ok {
			return received
		}
		dst[0] = elem
		recorder.record(dst)
		received++
	}
}

// produceBlocking puts total elements on target, refilling buf before each batch of len(buf), the
// last one shorter when total does not divide evenly. It returns early when the run aborts.
func produceBlocking[T any](
	target blockingQueue[T],
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
		for _, elem := range batch {
			target.put(elem)
		}
		sent += len(batch)
	}
}

// runBlocking moves total elements from mode's producers to its consumers through target, and
// returns how many the consumers received. Producers fill batch elements at a time and put each
// one. Once every producer is done, it stops target, unless the run has aborted, in which case
// the adapter has already freed the consumers. The error is the run's first adapter error, if
// any, and the caller decides how to fail.
func runBlocking[T any, S sink[T]](
	timer benchTimer,
	state *runState,
	target blockingQueue[T],
	mode benchMode,
	batch, total int,
	fill fillFunc[T],
	sinks func(consumer int) S,
) (received int, err error) {
	bufs := newBuffers[T](mode.producers, batch)
	dsts := newBuffers[T](mode.consumers, 1)
	recorders := make([]S, mode.consumers)
	for c := range recorders {
		recorders[c] = sinks(c)
	}
	counts := make([]int, mode.consumers)

	runTimed(timer, func() {
		var producerGroup, consumerGroup sync.WaitGroup
		for c := range mode.consumers {
			consumerGroup.Go(func() {
				counts[c] = consumeBlocking(target, dsts[c], recorders[c])
			})
		}
		for p := range mode.producers {
			producerGroup.Go(func() {
				produceBlocking(
					target,
					state,
					bufs[p],
					producerShare(total, mode.producers, p),
					p,
					fill,
				)
			})
		}
		producerGroup.Wait()
		if !state.aborted() {
			target.stop(mode.consumers)
		}
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

// chanQueue drives a channel of ringCapacity slots with blocking sends and receives. stop closes
// the channel. It never calls state.fail, because a channel operation cannot fail, which is why a
// chan run needs no abort path.
type chanQueue[T any] struct {
	ch chan T
}

// newChanQueue returns a chanQueue over a channel of ringCapacity slots.
func newChanQueue[T any]() *chanQueue[T] {
	return &chanQueue[T]{ch: make(chan T, ringCapacity)}
}

func (q *chanQueue[T]) put(item T) {
	q.ch <- item
}

// get reports false once the channel is closed and drained.
func (q *chanQueue[T]) get() (item T, ok bool) {
	item, ok = <-q.ch
	return item, ok
}

func (q *chanQueue[T]) stop(_ int) {
	close(q.ch)
}

// stopToken is the element workivaQueue puts once per consumer to tell it to stop.
type stopToken struct{}

// workivaQueue drives Workiva's RingBuffer through Put and Get. Its run disposes of the buffer
// when it aborts, which frees every goroutine blocked in Put or Get with ErrDisposed.
//
// ErrDisposed after an abort is the abort's own effect, not a new failure, so the adapter never
// passes it to state.fail. Because fail keeps only the first error, passing it would do no harm,
// but skipping it keeps the run's error the one that caused the abort.
type workivaQueue[T any] struct {
	buffer *workiva.RingBuffer
	state  *runState
}

// newWorkivaRun builds a RingBuffer of ringCapacity slots, a run that disposes of it on abort,
// and the adapter over both.
func newWorkivaRun[T any]() (*workiva.RingBuffer, *runState, *workivaQueue[T]) {
	buffer := workiva.NewRingBuffer(ringCapacity)
	state := newRunState(buffer.Dispose)
	return buffer, state, &workivaQueue[T]{buffer: buffer, state: state}
}

func (q *workivaQueue[T]) put(item T) {
	err := q.buffer.Put(item)
	if err == nil {
		return
	}
	if errors.Is(err, workiva.ErrDisposed) && q.state.aborted() {
		return
	}
	q.state.fail(err)
}

// get reports false on a stopToken, and on any error. An error after an abort is expected.
func (q *workivaQueue[T]) get() (item T, ok bool) {
	got, err := q.buffer.Get()
	if err != nil {
		if !q.state.aborted() {
			q.state.fail(err)
		}
		return item, false
	}
	switch elem := got.(type) {
	case T:
		return elem, true
	case stopToken:
		return item, false
	default:
		q.state.fail(fmt.Errorf("workiva get: unexpected element type %T", got))
		return item, false
	}
}

// stop puts one stopToken per consumer, and returns at the first error.
func (q *workivaQueue[T]) stop(consumers int) {
	for range consumers {
		err := q.buffer.Put(stopToken{})
		if err == nil {
			continue
		}
		if !q.state.aborted() {
			q.state.fail(err)
		}
		return
	}
}
