package benchmark

import (
	"fmt"
	"slices"
	"testing"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
)

// compareRow is one implementation's name and the modes and batch sizes it runs at.
type compareRow struct {
	name    string
	modes   []benchMode
	batches []int
}

// multiModes are the modes with a multi side.
var multiModes = []benchMode{mpscMode, spmcMode, mpmcMode}

// frontierModes are the modes ring/frontier implements so far.
var frontierModes = []benchMode{mpscMode, spmcMode}

// compareRows lists every implementation, in the order the benchmark runs them. The benchmark,
// the accounting test, and the runner script's table all follow it.
var compareRows = []compareRow{
	{name: "ring", modes: benchModes, batches: []int{1, 32}},
	{name: "ring-exact", modes: benchModes, batches: []int{1, 32}},
	{name: "ring-single", modes: benchModes, batches: []int{1}},
	{name: "ring-rts", modes: multiModes, batches: []int{1, 32}},
	{name: "frontier", modes: frontierModes, batches: []int{1, 32}},
	{name: "frontier-single", modes: frontierModes, batches: []int{1}},
	{name: "chan", modes: benchModes, batches: []int{1}},
	{name: "mutex", modes: benchModes, batches: []int{1}},
	{name: "xsync", modes: benchModes, batches: []int{1}},
	{name: "workiva", modes: benchModes, batches: []int{1}},
}

// runs reports whether row runs in mode at batch.
func (row compareRow) runs(mode benchMode, batch int) bool {
	return slices.Contains(row.modes, mode) && slices.Contains(row.batches, batch)
}

// compareRun builds a fresh run, queue, and adapter for mode, and moves total elements through
// them in batches of batch.
type compareRun[T any, S sink[T]] func(
	timer benchTimer,
	mode benchMode,
	batch, total int,
	fill fillFunc[T],
	sinks func(consumer int) S,
) (received int, err error)

// compareImpl supplies the timed run and the exact capacity and reuse gate.
type compareImpl[T any, S sink[T]] struct {
	run           compareRun[T, S]
	checkCapacity func(t *testing.T, mode benchMode) int
}

// pollingRun returns a compareRun that builds a fresh queue and run state for each run with build,
// and moves the elements through them with runPolling.
func pollingRun[T any, S sink[T]](
	build func(mode benchMode) (queue[T], *runState, error),
) compareRun[T, S] {
	return func(
		timer benchTimer,
		mode benchMode,
		batch, total int,
		fill fillFunc[T],
		sinks func(consumer int) S,
	) (int, error) {
		target, state, err := build(mode)
		if err != nil {
			return 0, err
		}
		return runPolling(timer, state, target, mode, batch, total, fill, sinks)
	}
}

// blockingRun returns a compareRun that builds a fresh queue and run state for each run with
// build, and moves the elements through them with runBlocking.
func blockingRun[T any, S sink[T]](build func() (blockingQueue[T], *runState)) compareRun[T, S] {
	return func(
		timer benchTimer,
		mode benchMode,
		batch, total int,
		fill fillFunc[T],
		sinks func(consumer int) S,
	) (int, error) {
		target, state := build()
		return runBlocking(timer, state, target, mode, batch, total, fill, sinks)
	}
}

// compareImpls maps each compareRows name to its implementation.
func compareImpls[T any, S sink[T]]() map[string]compareImpl[T, S] {
	return map[string]compareImpl[T, S]{
		"ring": {
			run: pollingRun[T, S](func(mode benchMode) (queue[T], *runState, error) {
				created, err := newModeRing[T](mode, nil, false)
				if err != nil {
					return nil, nil, err
				}
				return created, newRunState(nil), nil
			}),
			checkCapacity: func(t *testing.T, mode benchMode) int {
				return checkCompareCapacity(t, "ring", mode)
			},
		},
		"ring-exact": {
			run: pollingRun[T, S](func(mode benchMode) (queue[T], *runState, error) {
				created, err := newModeRing[T](mode, nil, false, dpdk.WithExactBatchCounts())
				if err != nil {
					return nil, nil, err
				}
				return created, newRunState(nil), nil
			}),
			checkCapacity: func(t *testing.T, mode benchMode) int {
				return checkCompareCapacity(t, "ring-exact", mode)
			},
		},
		"ring-single": {
			run: pollingRun[T, S](func(mode benchMode) (queue[T], *runState, error) {
				state := newRunState(nil)
				created, err := newModeRing[T](mode, state, true)
				if err != nil {
					return nil, nil, err
				}
				return created, state, nil
			}),
			checkCapacity: func(t *testing.T, mode benchMode) int {
				return checkCompareCapacity(t, "ring-single", mode)
			},
		},
		"ring-rts": {
			run: pollingRun[T, S](func(mode benchMode) (queue[T], *runState, error) {
				created, err := newModeRing[T](rtsMode(mode), nil, false)
				if err != nil {
					return nil, nil, err
				}
				return created, newRunState(nil), nil
			}),
			checkCapacity: func(t *testing.T, mode benchMode) int {
				return checkCompareCapacity(t, "ring-rts", mode)
			},
		},
		"frontier": {
			run: pollingRun[T, S](func(mode benchMode) (queue[T], *runState, error) {
				created, err := newModeFrontier[T](mode, nil, false)
				if err != nil {
					return nil, nil, err
				}
				return created, newRunState(nil), nil
			}),
			checkCapacity: func(t *testing.T, mode benchMode) int {
				return checkCompareCapacity(t, "frontier", mode)
			},
		},
		"frontier-single": {
			run: pollingRun[T, S](func(mode benchMode) (queue[T], *runState, error) {
				state := newRunState(nil)
				created, err := newModeFrontier[T](mode, state, true)
				if err != nil {
					return nil, nil, err
				}
				return created, state, nil
			}),
			checkCapacity: func(t *testing.T, mode benchMode) int {
				return checkCompareCapacity(t, "frontier-single", mode)
			},
		},
		"chan": {
			run: blockingRun[T, S](func() (blockingQueue[T], *runState) {
				return newChanQueue[T](), newRunState(nil)
			}),
			checkCapacity: func(t *testing.T, mode benchMode) int {
				return checkCompareCapacity(t, "chan", mode)
			},
		},
		"mutex": {
			run: pollingRun[T, S](func(_ benchMode) (queue[T], *runState, error) {
				return newMutexQueue[T](), newRunState(nil), nil
			}),
			checkCapacity: func(t *testing.T, mode benchMode) int {
				return checkCompareCapacity(t, "mutex", mode)
			},
		},
		"xsync": {
			run: pollingRun[T, S](func(_ benchMode) (queue[T], *runState, error) {
				return newXsyncQueue[T](), newRunState(nil), nil
			}),
			checkCapacity: func(t *testing.T, mode benchMode) int {
				return checkCompareCapacity(t, "xsync", mode)
			},
		},
		"workiva": {
			run: blockingRun[T, S](func() (blockingQueue[T], *runState) {
				_, state, target := newWorkivaRun[T]()
				return target, state
			}),
			checkCapacity: func(t *testing.T, mode benchMode) int {
				return checkCompareCapacity(t, "workiva", mode)
			},
		},
	}
}

// newTestRing returns a ring built for mode with the extra options, failing the test if it cannot
// be built.
func newTestRing[T any](
	t *testing.T,
	mode benchMode,
	state *runState,
	single bool,
	extra ...dpdk.Option,
) queue[T] {
	t.Helper()
	created, err := newModeRing[T](mode, state, single, extra...)
	if err != nil {
		t.Fatalf("build ring for %s: %v", mode.name, err)
	}
	return created
}

// BenchmarkCompare moves b.N elements from producers to consumers through each implementation, in
// every mode and batch size compareRows allows, so ns/op is nanoseconds per element transferred
// end to end.
func BenchmarkCompare(b *testing.B) {
	for _, mode := range benchModes {
		b.Run("mode="+mode.name, func(b *testing.B) {
			for _, batch := range compareBatches(mode) {
				b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
					b.Run("elem=int64", func(b *testing.B) {
						benchmarkCompareImpls(b, mode, batch, fillInt64)
					})
					b.Run("elem=ptr", func(b *testing.B) {
						benchmarkCompareImpls(b, mode, batch, fillPtr(newItemPool()))
					})
				})
			}
		})
	}
}

// compareBatches returns, in increasing order, every batch size some row runs at in mode.
func compareBatches(mode benchMode) []int {
	var batches []int
	for _, row := range compareRows {
		if !slices.Contains(row.modes, mode) {
			continue
		}
		for _, batch := range row.batches {
			if !slices.Contains(batches, batch) {
				batches = append(batches, batch)
			}
		}
	}
	slices.Sort(batches)
	return batches
}

// benchmarkCompareImpls runs one impl sub-benchmark for each row that runs in mode at batch.
func benchmarkCompareImpls[T any](b *testing.B, mode benchMode, batch int, fill fillFunc[T]) {
	impls := compareImpls[T, noopSink[T]]()
	for _, row := range compareRows {
		if !row.runs(mode, batch) {
			continue
		}
		impl, ok := impls[row.name]
		if !ok {
			b.Fatalf("compareRows names %s, which has no compareImpls entry", row.name)
		}
		b.Run("impl="+row.name, func(b *testing.B) {
			setElemBytes[T](b)
			received, err := impl.run(b, mode, batch, b.N, fill, noopSinks[T])
			if err != nil {
				b.Fatalf("run failed: %v", err)
			}
			if received != b.N {
				b.Fatalf("received %d elements, want %d", received, b.N)
			}
		})
	}
}
