package ring_test

import (
	"runtime"
	"sync/atomic"
)

// spinPolls is how many dequeue attempts a spinThenYield wait cycle makes, the first included,
// before it yields the processor.
const spinPolls = 64

// queue is the shape the harness drives. Each method moves as many elements as it can right
// now, without blocking, and returns how many it moved.
type queue[T any] interface {
	enqueue(items []T) int
	dequeue(dst []T) int
}

// pollPolicy decides what a consumer does after a dequeue attempt that moved nothing.
type pollPolicy int

const (
	// yieldOnly yields with runtime.Gosched after every empty poll, so a waiting goroutine never
	// burns its time slice. The throughput and RingAPI benchmarks use it.
	yieldOnly pollPolicy = iota
	// spinThenYield polls up to spinPolls times before it yields, so a waiting goroutine sees a
	// new element without paying for a yield. The latency benchmarks use it, because yielding on
	// every empty poll mostly measures the yield.
	spinThenYield
)

// waiter holds the bounded spin that every polling loop shares. yieldOnly makes one dequeue
// attempt per wait cycle, and yields when that attempt moved nothing. spinThenYield makes up to
// spinPolls attempts per wait cycle, the first included, and yields when the last of them moved
// nothing. A successful dequeue starts a new cycle. yield is injectable, so tests can observe the
// wait cycle.
type waiter struct {
	policy     pollPolicy
	emptyPolls int
	yield      func()
}

// newWaiter returns a waiter that follows policy and yields with runtime.Gosched.
func newWaiter(policy pollPolicy) waiter {
	return waiter{policy: policy, yield: runtime.Gosched}
}

// afterEmptyPoll counts a dequeue attempt that moved nothing, and yields when that attempt was
// the last of the wait cycle, so the next attempt starts a new cycle.
func (w *waiter) afterEmptyPoll() {
	w.emptyPolls++
	if w.policy == spinThenYield && w.emptyPolls < spinPolls {
		return
	}
	w.emptyPolls = 0
	w.yield()
}

// restart starts a new wait cycle after a dequeue attempt that moved elements.
func (w *waiter) restart() {
	w.emptyPolls = 0
}

// sink receives each batch a consumer dequeues. The batch is only valid during the call, because
// the consumer reuses its buffer.
type sink[T any] interface {
	record(batch []T)
}

// recordSink keeps a copy of every element it receives, so tests can check what was dequeued.
type recordSink[T any] struct {
	items []T
}

func (r *recordSink[T]) record(batch []T) {
	r.items = append(r.items, batch...)
}

// consume dequeues from source into dst until the producers are done and source is empty, passes
// each dequeued batch to recorder, and returns how many elements it received. dst must not be
// empty. The caller supplies wait, built with newWaiter, so tests can observe the wait cycle.
//
// Important: it loads producersDone before each dequeue attempt, and returns only when the flag
// was already set before an attempt that moved nothing. The reverse order, dequeue and then load
// the flag, can return while a producer's last elements are still in flight. The dequeue finds the
// queue empty, then the producer enqueues its last elements and finishes, and then the flag load
// sees the flag set.
func consume[T any, S sink[T]](source queue[T], producersDone *atomic.Bool, dst []T,
	recorder S, wait waiter,
) int {
	received := 0
	for {
		done := producersDone.Load()
		moved := source.dequeue(dst)
		if moved > 0 {
			recorder.record(dst[:moved])
			received += moved
			wait.restart()
			continue
		}
		if done {
			return received
		}
		wait.afterEmptyPoll()
	}
}
