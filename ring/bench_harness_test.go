package ring_test

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// noopTimer lets tests call the runs without a benchmark timer.
type noopTimer struct{}

func (noopTimer) ResetTimer() {}

func (noopTimer) StopTimer() {}

// accountingCase is one implementation and batch size that TestHarnessAccounting checks. run
// moves total int64 elements, recording through the sinks it is given.
type accountingCase struct {
	name  string
	batch int
	run   func(total int, sinks func(consumer int) *recordSink[int64]) (throughputResult, error)
}

// accountingCases returns the adapters at batch sizes 1 and 32, and chan at batch size 1, each
// running in mode.
func accountingCases(mode benchMode) []accountingCase {
	var cases []accountingCase
	for _, batch := range []int{1, 32} {
		for _, impl := range adapterImpls[int64](mode) {
			cases = append(cases, accountingCase{
				name:  impl.name,
				batch: batch,
				run: func(total int, sinks func(consumer int) *recordSink[int64],
				) (throughputResult, error) {
					return runThroughput(noopTimer{}, impl.newQueue, mode.producers,
						mode.consumers, batch, total, fillInt64, sinks)
				},
			})
		}
	}
	cases = append(cases, accountingCase{
		name:  "chan",
		batch: 1,
		run: func(total int, sinks func(consumer int) *recordSink[int64],
		) (throughputResult, error) {
			return runChanThroughput(noopTimer{}, mode.producers, mode.consumers, total,
				fillInt64, sinks), nil
		},
	})
	return cases
}

// accountingError checks what the consumers recorded when producers sent total elements, and
// returns an error describing the first problem it finds. Every (producer, sequence) pair must
// be received exactly once, and within each consumer each producer's sequence numbers must
// strictly increase.
func accountingError(sinks []*recordSink[int64], producers, total int) error {
	seen := make([][]bool, producers)
	for p := range seen {
		seen[p] = make([]bool, producerShare(total, producers, p))
	}
	received := 0
	for c, recorded := range sinks {
		lastSeq := make([]int64, producers)
		for p := range lastSeq {
			lastSeq[p] = -1
		}
		for _, v := range recorded.items {
			producer, seq := unpackInt64(v)
			if producer < 0 || producer >= producers || seq >= int64(len(seen[producer])) {
				return fmt.Errorf("consumer %d received producer %d sequence %d, never sent",
					c, producer, seq)
			}
			if seen[producer][seq] {
				return fmt.Errorf("consumer %d received producer %d sequence %d again",
					c, producer, seq)
			}
			if seq <= lastSeq[producer] {
				return fmt.Errorf("consumer %d received producer %d sequence %d after %d",
					c, producer, seq, lastSeq[producer])
			}
			seen[producer][seq] = true
			lastSeq[producer] = seq
			received++
		}
	}
	if received != total {
		return fmt.Errorf("received %d elements, want %d", received, total)
	}
	return nil
}

// TestHarnessAccounting checks that every run moves each element exactly once, in order per
// producer within each consumer, and counts them correctly, for each implementation, mode, batch
// size, and total, at GOMAXPROCS 1 and the default. Each case repeats 20 times, or 3 under -race,
// because a stopping-order bug shows only in some interleavings. It takes about a minute, and 1
// to 3 minutes under -race, because contended batch-1 ring cases cost several microseconds per
// element today.
func TestHarnessAccounting(t *testing.T) {
	repeats := 20
	if raceEnabled {
		repeats = 3
	}
	settings := []struct {
		name       string
		gomaxprocs int
	}{
		{name: "GOMAXPROCS=1", gomaxprocs: 1},
		{name: "GOMAXPROCS=default"},
	}
	for _, setting := range settings {
		t.Run(setting.name, func(t *testing.T) {
			if setting.gomaxprocs > 0 {
				previous := runtime.GOMAXPROCS(setting.gomaxprocs)
				t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
			}
			for _, mode := range benchModes {
				for _, tc := range accountingCases(mode) {
					for _, total := range []int{1, 10_007, 100_000} {
						name := fmt.Sprintf("impl=%s/mode=%s/batch=%d/total=%d",
							tc.name, mode.name, tc.batch, total)
						t.Run(name, func(t *testing.T) {
							for range repeats {
								checkAccounting(t, tc, mode, total)
							}
						})
					}
				}
			}
		})
	}
}

// accountingDeadline bounds one accounting run, so a stop-order regression fails fast with the
// case named instead of waiting for the global test timeout. The slowest case takes about a second
// today.
const accountingDeadline = 30 * time.Second

// checkAccounting runs tc once with a recordSink per consumer and checks what they recorded. The
// run has accountingDeadline to finish.
func checkAccounting(t *testing.T, tc accountingCase, mode benchMode, total int) {
	t.Helper()
	sinks := make([]*recordSink[int64], mode.consumers)
	newSink := func(consumer int) *recordSink[int64] {
		sinks[consumer] = &recordSink[int64]{}
		return sinks[consumer]
	}
	type runOutcome struct {
		result throughputResult
		err    error
	}
	outcomes := make(chan runOutcome, 1)
	go func() {
		result, err := tc.run(total, newSink)
		outcomes <- runOutcome{result: result, err: err}
	}()
	var outcome runOutcome
	select {
	case outcome = <-outcomes:
	case <-time.After(accountingDeadline):
		// The test is failing anyway, so the stuck goroutine is abandoned, not joined.
		t.Fatalf("%s batch=%d mode=%s total=%d: run did not finish within %v, "+
			"likely a consumer exited early", tc.name, tc.batch, mode.name, total,
			accountingDeadline)
	}
	require.NoError(t, outcome.err)
	require.Equal(t, total, outcome.result.received)
	require.NoError(t, accountingError(sinks, mode.producers, total))
}

func TestGCPerMillion(t *testing.T) {
	cases := []struct {
		name     string
		cycles   uint64
		elements int
		want     float64
	}{
		{name: "none", cycles: 0, elements: 1_000_000, want: 0},
		{name: "perMillion", cycles: 3, elements: 1_000_000, want: 3},
		{name: "fraction", cycles: 5, elements: 10_000_000, want: 0.5},
		{name: "underMillion", cycles: 1, elements: 500_000, want: 2},
		{name: "noElements", cycles: 2, elements: 0, want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.InDelta(t, tc.want, gcPerMillion(tc.cycles, tc.elements), 1e-9)
		})
	}
}

// allocQueue is a queue that allocates a new int64 on the heap for every element it enqueues,
// so a run through it triggers garbage collection. It is single-producer and single-consumer,
// because each side reuses its own buffer.
type allocQueue struct {
	inner    *chanPollQueue[*int64]
	sending  []*int64
	received []*int64
}

func newAllocQueue(batch int) *allocQueue {
	return &allocQueue{
		inner:    newChanPollQueue[*int64](),
		sending:  make([]*int64, batch),
		received: make([]*int64, batch),
	}
}

func (q *allocQueue) enqueue(items []int64) int {
	boxed := q.sending[:len(items)]
	for k, v := range items {
		boxed[k] = new(int64)
		*boxed[k] = v
	}
	return q.inner.enqueue(boxed)
}

func (q *allocQueue) dequeue(dst []int64) int {
	moved := q.inner.dequeue(q.received[:len(dst)])
	for k := range moved {
		dst[k] = *q.received[k]
		q.received[k] = nil
	}
	return moved
}

// TestGCMetric checks that the GC metric tells a run that allocates per element from one that
// does not. It asserts ranges, because collections follow heap growth, not element counts. It
// is skipped under -short and -race, because it moves 10M elements.
func TestGCMetric(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("moves 10M elements")
	}
	const (
		total = 10_000_000
		batch = 32
	)
	spsc := benchModes[0]

	allocating, err := runThroughput(noopTimer{}, func() (queue[int64], error) {
		return newAllocQueue(batch), nil
	}, spsc.producers, spsc.consumers, batch, total, fillInt64, noopSinks[int64])
	require.NoError(t, err)
	require.Equal(t, total, allocating.received)
	assert.GreaterOrEqual(t, allocating.gcPerMelem, 1.0)

	plain, err := runThroughput(noopTimer{}, func() (queue[int64], error) {
		return newChanPollQueue[int64](), nil
	}, spsc.producers, spsc.consumers, batch, total, fillInt64, noopSinks[int64])
	require.NoError(t, err)
	require.Equal(t, total, plain.received)
	assert.Less(t, plain.gcPerMelem, 1.0)
}
