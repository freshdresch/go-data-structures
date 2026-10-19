package ring_test

import (
	"fmt"
	"math"
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

// TestRingAPIAccounting checks that every run BenchmarkRingAPI makes through the queue harness
// moves each element exactly once, in order per producer within each consumer, for each api,
// batch size, and mode it runs. The requested totals are rounded as the benchmark rounds them,
// and 10,007 needs rounding for every bulk run except SPSC at batch size 1.
func TestRingAPIAccounting(t *testing.T) {
	for _, mode := range ringAPIModes {
		for _, api := range ringAPIs {
			for _, batch := range api.batches() {
				tc := accountingCase{
					name:  api.String(),
					batch: batch,
					run: func(total int, sinks func(consumer int) *recordSink[int64],
					) (throughputResult, error) {
						newQueue := func() (queue[int64], error) {
							return newRingAPIQueue[int64](api, mode)
						}
						return runThroughput(noopTimer{}, newQueue, mode.producers,
							mode.consumers, batch, total, fillInt64, sinks)
					},
				}
				for _, requested := range []int{1, 10_007} {
					total := api.total(requested, batch, mode.producers)
					name := fmt.Sprintf("api=%s/mode=%s/batch=%d/total=%d",
						tc.name, mode.name, batch, total)
					t.Run(name, func(t *testing.T) {
						checkAccounting(t, tc, mode, total)
					})
				}
			}
		}
	}
}

// TestRingAPIAdapters checks that each adapter uses the API it claims, which accounting cannot
// tell apart, because an adapter that moved one element at a time would still move every element
// exactly once. It builds SPSC rings through newModeRing, the constructor the adapters use, and
// checks what each call returns and what Count reports afterward.
func TestRingAPIAdapters(t *testing.T) {
	t.Run("bulk", func(t *testing.T) {
		created, err := newModeRing[int64](spscMode)
		require.NoError(t, err)
		q := &ringBulkQueue[int64]{ring: created}
		assert.Equal(t, 3, q.enqueue([]int64{1, 2, 3}))
		assert.Equal(t, 0, q.dequeue(make([]int64, 8)))
		assert.Equal(t, uint32(3), created.Count())
		assert.Equal(t, 3, q.dequeue(make([]int64, 3)))
	})

	t.Run("single", func(t *testing.T) {
		created, err := newModeRing[int64](spscMode)
		require.NoError(t, err)
		q := &ringSingleQueue[int64]{ring: created}
		assert.Equal(t, 1, q.enqueue([]int64{1, 2}))
		assert.Equal(t, uint32(1), created.Count())
		assert.Equal(t, 1, q.dequeue(make([]int64, 4)))
	})

	t.Run("unknownAPI", func(t *testing.T) {
		_, err := newRingAPIQueue[int64](ringAPI(99), spscMode)
		assert.Error(t, err)
	})
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

// assertWithinHistogramError checks that got is the histogram's report of want: never above it,
// and below it by less than 1/(subBuckets+1) of want, the bound derived on histogram. A zero want
// must be reported exactly.
func assertWithinHistogramError(t *testing.T, want, got time.Duration) {
	t.Helper()
	if want == 0 {
		assert.Equal(t, time.Duration(0), got)
		return
	}
	assert.LessOrEqual(t, got, want)
	if got > want {
		return
	}
	// For integers, want-got < want/(subBuckets+1) is the same as want-got <= (want-1)/(subBuckets+1).
	// This form stays exact and cannot overflow, unlike multiplying the difference by subBuckets+1.
	assert.LessOrEqual(t, want-got, (want-1)/(subBuckets+1), "want %v, got %v", want, got)
}

// TestHistogram checks that percentiles come back within 1/(subBuckets+1) from 1 ns to 1 s and at
// the edges of the value range, and that merge, count, and max agree with what was recorded.
func TestHistogram(t *testing.T) {
	t.Run("magnitudes", func(t *testing.T) {
		for value := time.Duration(1); value <= time.Second; value = value*5/4 + 1 {
			var h histogram
			h.record(value)
			assert.Equal(t, 1, h.count())
			assert.Equal(t, value, h.max())
			assertWithinHistogramError(t, value, h.percentile(50))
		}
	})
	t.Run("exact", func(t *testing.T) {
		for value := range time.Duration(2 * subBuckets) {
			var h histogram
			h.record(value)
			assert.Equal(t, value, h.percentile(50))
		}
	})
	t.Run("ranks", func(t *testing.T) {
		var h histogram
		for value := time.Duration(1); value <= 1000; value++ {
			h.record(value)
		}
		assert.Equal(t, 1000, h.count())
		assert.Equal(t, time.Duration(1000), h.max())
		assert.Equal(t, time.Duration(1), h.percentile(0))
		assertWithinHistogramError(t, 500, h.percentile(50))
		assertWithinHistogramError(t, 900, h.percentile(90))
		assertWithinHistogramError(t, 990, h.percentile(99))
		assertWithinHistogramError(t, 999, h.percentile(99.9))
		assertWithinHistogramError(t, 1000, h.percentile(100))
	})
	t.Run("exactRank", func(t *testing.T) {
		// Catches a Floor in place of Ceil: the exact rank is 1.5, so the rank must be 2.
		var h histogram
		h.record(1)
		h.record(1000)
		h.record(1000)
		assert.Equal(t, histogramLowest(histogramIndex(1000)), h.percentile(50))

		// Checks that a fractional rank of .001 rounds up: the exact rank 998.001 must give 999.
		var tail histogram
		for range 998 {
			tail.record(1)
		}
		tail.record(1000)
		assert.Equal(t, histogramLowest(histogramIndex(1000)), tail.percentile(99.9))

		// Catches an epsilon as large as 1e-6: exact rank 1997.001 must still round up to 1998.
		var wide histogram
		for range 1997 {
			wide.record(1)
		}
		wide.record(1000)
		wide.record(1000)
		assert.Equal(t, histogramLowest(histogramIndex(1000)), wide.percentile(99.9))
	})
	t.Run("largeCount", func(t *testing.T) {
		// ceil(0.999 * 20,972,000) is exactly 20,951,028. Float rounding must not push the
		// rank to 20,951,029, which would land in the bucket of 1000 instead of 1.
		const total = 20972000
		const lowCount = 20951028
		var h histogram
		for range lowCount {
			h.record(1)
		}
		for range total - lowCount {
			h.record(1000)
		}
		assert.Equal(t, total, h.count())
		assert.Equal(t, time.Duration(1), h.percentile(99.9))
	})
	t.Run("largeCountFraction", func(t *testing.T) {
		// Catches an epsilon above about 1e-10: the exact rank is 20,951,026.002, so it must
		// round up to 20,951,027, the first rank of the bucket of 1000.
		const total = 20971998
		const lowCount = 20951026
		var h histogram
		for range lowCount {
			h.record(1)
		}
		for range total - lowCount {
			h.record(1000)
		}
		assert.Equal(t, total, h.count())
		assert.Equal(t, histogramLowest(histogramIndex(1000)), h.percentile(99.9))
	})
	t.Run("edges", func(t *testing.T) {
		var h histogram
		assert.Equal(t, 0, h.count())
		assert.Equal(t, time.Duration(0), h.max())
		assert.Equal(t, time.Duration(0), h.percentile(50))
		h.record(0)
		h.record(-time.Second)
		h.record(math.MaxInt64)
		assert.Equal(t, 3, h.count())
		assert.Equal(t, time.Duration(math.MaxInt64), h.max())
		assert.Equal(t, time.Duration(0), h.percentile(50))
		assertWithinHistogramError(t, math.MaxInt64, h.percentile(100))

		var descending histogram
		descending.record(1000)
		descending.record(1)
		assert.Equal(t, time.Duration(1000), descending.max())
	})
	t.Run("clamp", func(t *testing.T) {
		var h histogram
		for value := time.Duration(1); value <= 100; value++ {
			h.record(value)
		}
		assert.Equal(t, h.percentile(0), h.percentile(-1))
		assert.Equal(t, h.percentile(100), h.percentile(101))
		assert.Equal(t, h.percentile(0), h.percentile(math.NaN()))
	})
	t.Run("merge", func(t *testing.T) {
		var low, high histogram
		for range 90 {
			low.record(100 * time.Nanosecond)
		}
		for range 10 {
			high.record(time.Millisecond)
		}
		low.merge(&high)
		assert.Equal(t, 100, low.count())
		assert.Equal(t, time.Millisecond, low.max())
		assertWithinHistogramError(t, 100*time.Nanosecond, low.percentile(90))
		assertWithinHistogramError(t, time.Millisecond, low.percentile(91))
		assert.Equal(t, 10, high.count())
	})
	t.Run("boundaries", func(t *testing.T) {
		for k := range histogramBuckets {
			lowest := histogramLowest(k)
			assert.Equal(t, k, histogramIndex(lowest))
			if k >= 1 {
				assert.Greater(t, lowest, histogramLowest(k-1))
			}
		}
		// The top of each bucket is where the error is largest, so this pins the bound.
		for k := range histogramBuckets - 1 {
			top := histogramLowest(k+1) - 1
			assert.Equal(t, k, histogramIndex(top))
			var h histogram
			h.record(top)
			assertWithinHistogramError(t, top, h.percentile(50))
		}
		for e := 1; e <= durationValueBits-1; e++ {
			below := time.Duration(1)<<e - 1
			at := time.Duration(1) << e
			var belowHist, atHist histogram
			belowHist.record(below)
			atHist.record(at)
			assertWithinHistogramError(t, below, belowHist.percentile(50))
			assertWithinHistogramError(t, at, atHist.percentile(50))
		}
	})
}
