package benchmark

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/freshdresch/go-data-structures/ring"
)

// abortTotal is how many elements each abort test sends, far more than any implementation holds,
// so goroutines are still blocked or retrying when the run aborts.
const abortTotal = 100_000

// goroutineDeadline bounds how long an abort test waits for every goroutine to return.
const goroutineDeadline = 5 * time.Second

// TestCompareAccounting checks that every implementation moves each element exactly once, in
// order per producer within each consumer, and counts them correctly, in every mode it runs, at
// batch size 1, and also 32 where it runs at 32. A total of 1 checks a run with fewer elements
// than producers.
func TestCompareAccounting(t *testing.T) {
	impls := compareImpls[int64, *recordSink[int64]]()
	for _, row := range compareRows {
		impl, ok := impls[row.name]
		if !ok {
			t.Fatalf("compareRows names %s, which has no compareImpls entry", row.name)
		}
		for _, mode := range row.modes {
			for _, batch := range row.batches {
				for _, total := range []int{1, 10_007} {
					name := fmt.Sprintf(
						"impl=%s/mode=%s/batch=%d/total=%d",
						row.name,
						mode.name,
						batch,
						total,
					)
					t.Run(name, func(t *testing.T) {
						checkAccounting(t, impl, mode, batch, total)
					})
				}
			}
		}
	}
}

// checkAccounting runs impl once with a recordSink per consumer and checks what they recorded.
// The run has accountingDeadline to finish.
func checkAccounting(
	t *testing.T,
	impl compareImpl[int64, *recordSink[int64]],
	mode benchMode,
	batch, total int,
) {
	t.Helper()
	sinks := make([]*recordSink[int64], mode.consumers)
	for c := range sinks {
		sinks[c] = &recordSink[int64]{}
	}
	sinkFor := func(consumer int) *recordSink[int64] {
		return sinks[consumer]
	}
	stuck := fmt.Sprintf("mode=%s batch=%d total=%d", mode.name, batch, total)
	received := runWithin(t, stuck, func() (int, error) {
		return impl.run(noopTimer{}, mode, batch, total, fillInt64, sinkFor)
	})
	if received != total {
		t.Fatalf("received %d elements, want %d", received, total)
	}
	if err := accountingError(sinks, mode.producers, total); err != nil {
		t.Fatal(err)
	}
}

// TestCompareCapacity logs how many elements each implementation holds before it reports full.
func TestCompareCapacity(t *testing.T) {
	impls := compareImpls[int64, noopSink[int64]]()
	for _, row := range compareRows {
		impl, ok := impls[row.name]
		if !ok {
			t.Fatalf("compareRows names %s, which has no compareImpls entry", row.name)
		}
		for _, mode := range row.modes {
			t.Run(row.name+"/"+mode.name, func(t *testing.T) {
				slots := impl.checkCapacity(t, mode)
				if slots != ringCapacity {
					t.Fatalf("impl=%s mode=%s slots=%d, want %d", row.name, mode.name, slots, ringCapacity)
				}
				t.Logf("capacity: impl=%s mode=%s slots=%d reuse=true", row.name, mode.name, slots)
			})
		}
	}
}

func TestModeRingTopology(t *testing.T) {
	for _, mode := range benchModes {
		for _, single := range []bool{false, true} {
			for _, relaxed := range []bool{false, true} {
				testMode := mode
				testMode.relaxed = relaxed
				target := newTestRing[int64](t, testMode, newRunState(nil), single)
				api := "Burst"
				if single {
					api = "Single"
				}
				want := fmt.Sprintf("*benchmark.%s%sQueue[int64]", strings.ToLower(mode.name), api)
				if got := fmt.Sprintf("%T", target); got != want {
					t.Errorf(
						"mode=%s single=%v relaxed=%v: got %s, want %s",
						mode.name, single, relaxed, got, want,
					)
				}
			}
		}
	}
}

func TestRingRTSRelaxed(t *testing.T) {
	for _, mode := range []benchMode{mpscMode, spmcMode, mpmcMode} {
		if !rtsMode(mode).relaxed {
			t.Errorf("rtsMode(%s).relaxed = false, want true", mode.name)
		}
	}
}

// TestWorkivaAbort checks that a workiva adapter error aborts the run, reports that error, and
// frees every goroutine, including the consumers blocked in Get and the producers blocked in Put.
// A value of the wrong type put before the run starts is the error.
func TestWorkivaAbort(t *testing.T) {
	buffer, state, target := newWorkivaRun[int64]()
	if err := buffer.Put(int(1)); err != nil {
		t.Fatalf("workiva put: %v", err)
	}
	before := runtime.NumGoroutine()
	runErr := runWithin(t, "workiva abort did not free every goroutine", func() (error, error) {
		_, err := runBlocking(
			noopTimer{},
			state,
			target,
			mpmcMode,
			1,
			abortTotal,
			fillInt64,
			noopSinks[int64],
		)
		return err, nil
	})
	checkRunError(t, runErr, "mode=MPMC: workiva get: unexpected element type int")
	waitForGoroutines(t, before)
}

// errStubAbort is the error abortingQueue fails its run with.
var errStubAbort = errors.New("stub queue failed")

// abortAfter is how many elements abortingQueue accepts before it fails its run.
const abortAfter = 500

// abortingQueue moves at most one element per call through a channel of ringCapacity slots until
// abortAfter have been enqueued, then fails its run and moves nothing more.
type abortingQueue struct {
	ch       chan int64
	enqueued atomic.Int64
	state    *runState
}

func (q *abortingQueue) enqueue(items []int64) int {
	if q.enqueued.Load() >= abortAfter {
		return 0
	}
	select {
	case q.ch <- items[0]:
	default:
		return 0
	}
	if q.enqueued.Add(1) == abortAfter {
		q.state.fail(errStubAbort)
	}
	return 1
}

func (q *abortingQueue) dequeue(dst []int64) int {
	select {
	case dst[0] = <-q.ch:
		return 1
	default:
		return 0
	}
}

// TestPollingAbort checks that an adapter error ends a polling run. It runs in SPSC at batch 32,
// and abortAfter is not a multiple of 32, so the producer is always partway through an
// enqueueAll batch when the stub fails. The stub moves nothing after that, so a producer whose
// enqueueAll ignored the abort would retry forever.
func TestPollingAbort(t *testing.T) {
	state := newRunState(nil)
	target := &abortingQueue{ch: make(chan int64, ringCapacity), state: state}
	before := runtime.NumGoroutine()
	runErr := runWithin(t, "polling abort did not free every goroutine", func() (error, error) {
		_, err := runPolling(
			noopTimer{},
			state,
			target,
			spscMode,
			32,
			abortTotal,
			fillInt64,
			noopSinks[int64],
		)
		return err, nil
	})
	checkRunError(t, runErr, "mode=SPSC: "+errStubAbort.Error())
	waitForGoroutines(t, before)
}

// checkRunError fails the test unless err's whole message is want.
func checkRunError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("run returned no error, want %q", want)
	}
	if err.Error() != want {
		t.Fatalf("run returned %q, want %q", err.Error(), want)
	}
}

// waitForGoroutines polls until no more than limit goroutines are running, and fails the test if
// that does not happen within goroutineDeadline. No test in this package runs in parallel, so the
// count reflects only the calling test.
func waitForGoroutines(t *testing.T, limit int) {
	t.Helper()
	deadline := time.Now().Add(goroutineDeadline)
	for runtime.NumGoroutine() > limit {
		if time.Now().After(deadline) {
			t.Fatalf(
				"%d goroutines still running after %v, want at most %d",
				runtime.NumGoroutine(),
				goroutineDeadline,
				limit,
			)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRingAdapterOutcomes(t *testing.T) {
	for _, expected := range []error{ring.ErrFull, ring.ErrEmpty} {
		opposite := ring.ErrFull
		if expected == opposite {
			opposite = ring.ErrEmpty
		}
		for _, original := range []error{nil, expected, opposite, errors.New("unexpected"),
			fmt.Errorf("wrapped: %w", expected)} {
			state := newRunState(nil)
			target := &ringSingleQueue{state: state}
			want := 0
			if original == nil {
				want = 1
			}
			if got := target.movedOrFail(original, expected); got != want {
				t.Errorf("moved = %d, want %d", got, want)
			}
			wantErr := original
			if original == expected {
				wantErr = nil
			}
			if state.firstError() != wantErr {
				t.Errorf("stored error = %v, want %v", state.firstError(), wantErr)
			}
			if state.aborted() != (wantErr != nil) {
				t.Errorf("abort = %v", state.aborted())
			}
		}
	}
}

func checkCompareCapacity(t *testing.T, name string, mode benchMode) int {
	t.Helper()
	if name == "chan" {
		return checkChannelCapacity(t)
	}
	if name == "workiva" {
		return checkWorkivaCapacity(t)
	}
	state := newRunState(nil)
	var target queue[int64]
	switch name {
	case "ring", "ring-exact", "ring-rts":
		var options []ring.Option
		if name == "ring-exact" {
			options = append(options, ring.WithExactBatchCounts())
		}
		if name == "ring-rts" {
			mode = rtsMode(mode)
		}
		target = newTestRing[int64](t, mode, state, false, options...)
	case "ring-single":
		target = newTestRing[int64](t, mode, state, true)
	case "frontier", "frontier-single":
		created, err := newModeFrontier[int64](mode, state, name == "frontier-single")
		if err != nil {
			t.Fatal(err)
		}
		target = created
	case "mutex":
		target = newMutexQueue[int64]()
	case "xsync":
		target = newXsyncQueue[int64]()
	default:
		t.Fatalf("unknown capacity implementation %q", name)
	}
	return checkCapacityCycles(t, func(value int64) bool {
		moved := target.enqueue([]int64{value})
		if err := state.firstError(); err != nil {
			t.Fatal(err)
		}
		if moved != 0 && moved != 1 {
			t.Fatalf("enqueue moved %d", moved)
		}
		return moved == 1
	}, func() (int64, bool) {
		dst := []int64{-1}
		moved := target.dequeue(dst)
		if err := state.firstError(); err != nil {
			t.Fatal(err)
		}
		if moved != 0 && moved != 1 {
			t.Fatalf("dequeue moved %d", moved)
		}
		return dst[0], moved == 1
	})
}

func checkChannelCapacity(t *testing.T) int {
	t.Helper()
	target := newChanQueue[int64]()
	t.Cleanup(func() { close(target.ch) })
	return checkCapacityCycles(t, func(value int64) bool {
		select {
		case target.ch <- value:
			return true
		default:
			return false
		}
	}, func() (int64, bool) {
		select {
		case value := <-target.ch:
			return value, true
		default:
			return 0, false
		}
	})
}

func checkWorkivaCapacity(t *testing.T) int {
	t.Helper()
	buffer, _, _ := newWorkivaRun[int64]()
	t.Cleanup(buffer.Dispose)
	return checkCapacityCycles(t, func(value int64) bool {
		accepted, err := buffer.Offer(value)
		if err != nil {
			t.Fatal(err)
		}
		return accepted
	}, func() (int64, bool) {
		if buffer.Len() == 0 {
			return 0, false
		}
		value, err := buffer.Poll(0)
		if err != nil {
			t.Fatal(err)
		}
		typed, ok := value.(int64)
		if !ok {
			t.Fatalf("workiva returned %T", value)
		}
		return typed, true
	})
}

func checkCapacityCycles(
	t *testing.T,
	put func(int64) bool,
	get func() (int64, bool),
) int {
	t.Helper()
	if _, ok := get(); ok {
		t.Fatal("new queue is not empty")
	}
	for cycle := range 2 {
		for index := range ringCapacity + 1 {
			accepted := put(int64(cycle*ringCapacity + index))
			if accepted != (index < ringCapacity) {
				t.Fatalf("cycle %d put %d accepted=%v", cycle, index, accepted)
			}
		}
		for index := range ringCapacity + 1 {
			value, ok := get()
			if ok != (index < ringCapacity) {
				t.Fatalf("cycle %d get %d ok=%v", cycle, index, ok)
			}
			if ok && value != int64(cycle*ringCapacity+index) {
				t.Fatalf("cycle %d get %d value=%d", cycle, index, value)
			}
		}
	}
	return ringCapacity
}
