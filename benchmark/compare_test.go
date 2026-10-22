package benchmark

import (
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
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
		slots := impl.fillUntilFull(t)
		if slots <= 0 {
			t.Fatalf("impl=%s held %d elements, want a positive count", row.name, slots)
		}
		t.Logf("capacity: impl=%s slots=%d", row.name, slots)
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
