package ring_test

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

// stopOrderQueue is a stub queue that is empty on every dequeue call except one. During call
// number trigger it sets producersDone and makes one element available to the next call.
type stopOrderQueue struct {
	producersDone *atomic.Bool
	trigger       int
	calls         int
	available     int
}

func (q *stopOrderQueue) enqueue(_ []int64) int {
	panic("stopOrderQueue: enqueue is never called")
}

func (q *stopOrderQueue) dequeue(dst []int64) int {
	q.calls++
	if q.available > 0 {
		dst[0] = 1
		q.available--
		return 1
	}
	if q.calls == q.trigger {
		q.producersDone.Store(true)
		q.available = 1
	}
	return 0
}

// consumeReversed is consume with the stopping order reversed: it dequeues first and loads
// producersDone after. It exists only to prove that TestConsumeStopOrder tells the orders apart.
func consumeReversed[T any, S sink[T]](source queue[T], producersDone *atomic.Bool, dst []T,
	recorder S, wait waiter,
) int {
	received := 0
	for {
		moved := source.dequeue(dst)
		done := producersDone.Load()
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

// TestConsumeStopOrder checks the stop order and that consume's loop yields where its policy says.
// The stub sets producersDone during the empty dequeue itself, so the reversed order sees the flag
// at once and returns 0 for any trigger. This reversed case is what proves the stop order. The
// trigger is placed at the last poll before a yield to mirror the real race, and TestWaiterCycle
// covers the waiter on its own.
func TestConsumeStopOrder(t *testing.T) {
	cases := []struct {
		name    string
		poll    pollPolicy
		trigger int
	}{
		{name: "yieldOnly", poll: yieldOnly, trigger: 1},
		{name: "spinThenYield", poll: spinThenYield, trigger: spinPolls},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var producersDone atomic.Bool
			source := &stopOrderQueue{producersDone: &producersDone, trigger: tc.trigger}
			recorded := &recordSink[int64]{}
			var yields []int
			wait := newWaiter(tc.poll)
			wait.yield = func() { yields = append(yields, source.calls) }
			received := consume(source, &producersDone, make([]int64, 1), recorded, wait)
			assert.Equal(t, 1, received)
			assert.Equal(t, []int64{1}, recorded.items)
			assert.Equal(t, []int{tc.trigger}, yields)

			var reversedDone atomic.Bool
			reversedSource := &stopOrderQueue{producersDone: &reversedDone, trigger: tc.trigger}
			reversedRecorded := &recordSink[int64]{}
			reversedReceived := consumeReversed(reversedSource, &reversedDone,
				make([]int64, 1), reversedRecorded, newWaiter(tc.poll))
			assert.Equal(t, 0, reversedReceived)
			assert.Empty(t, reversedRecorded.items)
		})
	}
}

// TestWaiterCycle checks when each policy yields. Each subtest replaces the waiter's yield with a
// function that records the 1-based index of the empty poll it was called after, so the wait
// cycle is observed without the scheduler.
func TestWaiterCycle(t *testing.T) {
	t.Run("yieldOnly", func(t *testing.T) {
		w := newWaiter(yieldOnly)
		var polls int
		var yields []int
		w.yield = func() { yields = append(yields, polls) }
		for range 5 {
			polls++
			w.afterEmptyPoll()
		}
		assert.Equal(t, []int{1, 2, 3, 4, 5}, yields)
	})

	t.Run("spinThenYield", func(t *testing.T) {
		w := newWaiter(spinThenYield)
		var polls int
		var yields []int
		w.yield = func() { yields = append(yields, polls) }
		for range 2 * spinPolls {
			polls++
			w.afterEmptyPoll()
		}
		assert.Equal(t, []int{spinPolls, 2 * spinPolls}, yields)
	})

	t.Run("spinThenYieldRestart", func(t *testing.T) {
		w := newWaiter(spinThenYield)
		var polls int
		var yields []int
		w.yield = func() { yields = append(yields, polls) }
		for range spinPolls - 1 {
			polls++
			w.afterEmptyPoll()
		}
		w.restart()
		for range spinPolls - 1 {
			polls++
			w.afterEmptyPoll()
		}
		assert.Empty(t, yields)
		polls++
		w.afterEmptyPoll()
		assert.Equal(t, []int{2*spinPolls - 1}, yields)
	})
}
