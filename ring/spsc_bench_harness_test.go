package ring_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSPSCAdapterCapacity(t *testing.T) {
	for _, api := range ringAPIs {
		t.Run(api.String(), func(t *testing.T) {
			target, err := newSPSCAPIQueue[int64](api)
			require.NoError(t, err)
			checkHarnessCapacity(t, target)
		})
	}
}

func TestSPSCAdapterAccounting(t *testing.T) {
	for _, api := range ringAPIs {
		for _, batch := range []int{1, 32} {
			if api == apiSingle && batch != 1 {
				continue
			}
			for _, requested := range []int{0, 1, 31, 32, 33, 10007} {
				name := fmt.Sprintf("api=%s/batch=%d/total=%d", api, batch, requested)
				t.Run(name, func(t *testing.T) {
					total := api.total(requested, batch, 1)
					testCase := accountingCase{
						name: name, batch: batch,
						run: func(total int, sinks func(int) *recordSink[int64]) (throughputResult, error) {
							return runThroughput(
								noopTimer{},
								func() (queue[int64], error) { return newSPSCAPIQueue[int64](api) },
								1,
								1,
								batch,
								total,
								fillInt64,
								sinks,
							)
						},
					}
					checkAccounting(t, testCase, spscMode, total)
				})
			}
		}
	}
}

// Each queue has one consumer, so its recorder is inspected only after runHandoff joins.
type spscHandoffRecorder struct {
	queue[int64]
	recorded recordSink[int64]
}

func (q *spscHandoffRecorder) dequeue(dst []int64) int {
	moved := q.queue.dequeue(dst)
	q.recorded.record(dst[:moved])
	return moved
}

func TestSPSCComparisonHandoffProtocol(t *testing.T) {
	for _, impl := range spscComparisonQueues() {
		for _, total := range []int{0, 1, 33} {
			t.Run(fmt.Sprintf("%s/total=%d", impl.name, total), func(t *testing.T) {
				var queues []*spscHandoffRecorder
				factory := func() (queue[int64], error) {
					target, err := impl.newQueue()
					if err != nil {
						return nil, err
					}
					recorded := &spscHandoffRecorder{queue: target}
					queues = append(queues, recorded)
					return recorded, nil
				}
				result := runWithin(t, impl.name, func() (handoffResult, error) {
					return runHandoff(noopTimer{}, factory, total)
				})
				require.Equal(t, total, result.recorded)
				require.Equal(t, total, result.latency.count())
				if total == 0 {
					require.Zero(t, result.roundTripTotal)
				}
				require.Len(t, queues, 2)
				expected := []int64{warmUpPing}
				for index := range total {
					expected = append(expected, int64(index))
				}
				for _, target := range queues {
					require.Equal(t, expected, target.recorded.items)
				}
			})
		}
	}
}

func TestSPSCManifestRows(t *testing.T) {
	// These literal workload suffixes are independent of the manifest builders.
	suffixes := []string{
		"single/elem=int64", "single/elem=ptr",
		"bulk/batch=1/elem=int64", "bulk/batch=1/elem=ptr",
		"bulk/batch=32/elem=int64", "bulk/batch=32/elem=ptr",
		"burst/batch=1/elem=int64", "burst/batch=1/elem=ptr",
		"burst/batch=32/elem=int64", "burst/batch=32/elem=ptr",
	}
	var concurrent []string
	for _, root := range []string{"BenchmarkRingAPI", "BenchmarkSPSCAPI"} {
		for _, suffix := range suffixes {
			concurrent = append(concurrent, root+"/mode=SPSC/api="+suffix)
		}
	}
	concurrent = append(
		concurrent,
		"BenchmarkSPSCComparisonHandoff/impl=ring",
		"BenchmarkSPSCComparisonHandoff/impl=spsc",
	)
	sequential := []string{
		"BenchmarkSPSCConcrete/impl=ring/api=seq/elem=int64",
		"BenchmarkSPSCConcrete/impl=ring/api=seq/elem=ptr",
		"BenchmarkSPSCConcrete/impl=spsc/api=seq/elem=int64",
		"BenchmarkSPSCConcrete/impl=spsc/api=seq/elem=ptr",
	}
	for selection, expected := range map[string][]string{
		"concurrent": concurrent, "sequential": sequential,
	} {
		t.Run(selection, func(t *testing.T) {
			manifest, err := selectedBenchmarkManifest(selection)
			require.NoError(t, err)
			repeated, err := selectedBenchmarkManifest(selection)
			require.NoError(t, err)
			require.Equal(t, manifest, repeated)
			var actual []string
			for _, row := range manifest.Rows {
				actual = append(actual, row.Name)
				require.Equal(t, 1024, row.UsableCapacity)
				require.Empty(t, row.RingOptions)
				require.NotEmpty(t, row.TimingBoundary)
				require.NotEmpty(t, row.Storage)
				if row.Element == "ptr" {
					require.NotNil(t, row.PointerPoolSize)
					require.Equal(t, 1024, *row.PointerPoolSize)
				} else {
					require.Nil(t, row.PointerPoolSize)
					require.NotEmpty(t, row.PointerPoolReason)
				}
				metric := "ns/elem"
				if selection == "sequential" || row.Family == "SPSCSharedHandoff" {
					metric = "ns/op"
				}
				require.Equal(t, metric, row.PrimaryMetric)
			}
			slices.Sort(actual)
			slices.Sort(expected)
			require.Equal(t, expected, actual)
		})
	}
	require.Len(t, buildBenchmarkManifest().Rows, 24)
	_, err := selectedBenchmarkManifest("unknown")
	require.Error(t, err)
}

func TestSPSCPointerStorageContracts(t *testing.T) {
	for _, testCase := range []struct {
		selection string
		storage   string
		perImpl   int
	}{
		{
			selection: "concurrent",
			storage:   "typed *item pointing into a rotating pool of 1024 items",
			perImpl:   5,
		},
		{
			selection: "sequential",
			storage: "typed *item repeatedly pointing to the first item " +
				"of an allocated 1024-item pool",
			perImpl: 1,
		},
	} {
		t.Run(testCase.selection, func(t *testing.T) {
			manifest, err := selectedBenchmarkManifest(testCase.selection)
			require.NoError(t, err)
			pointerRows := make(map[string]int)
			for _, row := range manifest.Rows {
				if row.Element != "ptr" {
					continue
				}
				pointerRows[row.Implementation]++
				t.Run(row.Name, func(t *testing.T) {
					require.NotNil(t, row.PointerPoolSize)
					require.Equal(t, 1024, *row.PointerPoolSize)
					require.Equal(t, testCase.storage, row.Storage)
				})
			}
			require.Equal(t, map[string]int{
				"ring": testCase.perImpl,
				"spsc": testCase.perImpl,
			}, pointerRows)
		})
	}
}
