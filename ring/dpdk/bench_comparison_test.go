package dpdk_test

import (
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
)

type comparisonScenario struct {
	family       string
	specialized  string
	mode         benchMode
	layoutPrefix string
	ringOptions  []string
	defaults     comparisonDefaults
}

var (
	spmcComparison = comparisonScenario{
		family: "SPMCComparison", specialized: "spmc", mode: spmcMode,
		layoutPrefix: "SPMC_COMPARISON_LAYOUT ",
		ringOptions:  []string{"WithMultiConsDequeue"},
		defaults: comparisonDefaults{
			MaxEnqueueBatch: 1024, MaxDequeueBatch: 1024,
			ClearOnDequeue: true, ExactBatchCounts: false,
			ProducerAdmission: "cached counts", ConsumerAdmission: "shared cached counts",
			ConsumerRelease: "ordered", SpinsBeforeYield: 8192,
		},
	}
	mpscComparison = comparisonScenario{
		family: "MPSCComparison", specialized: "mpsc", mode: mpscMode,
		layoutPrefix: "MPSC_COMPARISON_LAYOUT ",
		ringOptions:  []string{"WithMultiProdEnqueue"},
		defaults: comparisonDefaults{
			MaxEnqueueBatch: 1024, MaxDequeueBatch: 1024,
			ClearOnDequeue: true, ExactBatchCounts: false,
			ProducerAdmission: "shared cached counts", ConsumerAdmission: "cached counts",
			ProducerPublication: "ordered", ConsumerRelease: "ordinary", SpinsBeforeYield: 8192,
		},
	}
	mpmcComparison = comparisonScenario{
		family: "MPMCComparison", specialized: "mpmc", mode: mpmcMode,
		layoutPrefix: "MPMC_COMPARISON_LAYOUT ",
		ringOptions:  []string{"WithMultiProdEnqueue", "WithMultiConsDequeue"},
		defaults: comparisonDefaults{
			MaxEnqueueBatch: 1024, MaxDequeueBatch: 1024,
			ClearOnDequeue: true, ExactBatchCounts: false,
			ProducerAdmission: "shared cached counts", ConsumerAdmission: "shared cached counts",
			ProducerPublication: "ordered", ConsumerRelease: "ordered", SpinsBeforeYield: 8192,
		},
	}
)

func benchmarkComparison(
	b *testing.B,
	scenario comparisonScenario,
	newInt64Queue func(string, ringAPI) (queue[int64], error),
	newPtrQueue func(string, ringAPI) (queue[*item], error),
) {
	for _, implementation := range []string{"ring", scenario.specialized} {
		b.Run("impl="+implementation+"/mode="+scenario.mode.name, func(b *testing.B) {
			for _, api := range []ringAPI{apiSingle, apiBulk, apiBurst} {
				b.Run("api="+api.String(), func(b *testing.B) {
					for _, batch := range []int{1, 32} {
						if api == apiSingle && batch != 1 {
							continue
						}
						run := func(b *testing.B) {
							b.Run("elem=int64", func(b *testing.B) {
								benchmarkComparisonRun(
									b,
									scenario,
									implementation,
									api,
									batch,
									fillInt64,
									newInt64Queue,
								)
							})
							b.Run("elem=ptr", func(b *testing.B) {
								benchmarkComparisonRun(
									b,
									scenario,
									implementation,
									api,
									batch,
									fillPtr(newItemPool()),
									newPtrQueue,
								)
							})
						}
						if api == apiSingle {
							run(b)
							continue
						}
						b.Run(fmt.Sprintf("batch=%d", batch), run)
					}
				})
			}
		})
	}
}

func benchmarkComparisonRun[T any](
	b *testing.B,
	scenario comparisonScenario,
	implementation string,
	api ringAPI,
	batch int,
	fill fillFunc[T],
	newQueue func(string, ringAPI) (queue[T], error),
) {
	setElemBytes[T](b)
	total := api.total(b.N, batch, scenario.mode.producers)
	var actual queue[T]
	factory := func() (queue[T], error) {
		created, err := newQueue(implementation, api)
		actual = created
		return created, err
	}
	result, err := runThroughput(
		b,
		factory,
		scenario.mode.producers,
		scenario.mode.consumers,
		batch,
		total,
		fill,
		noopSinks[T],
	)
	if err != nil {
		b.Fatal(err)
	}
	reportConcurrentAPIMetrics(b, api, result, total)
	// runThroughput has stopped the timer before any layout inspection or serialization.
	evidence := comparisonLayoutRecord{
		Name: b.Name(), Iterations: b.N, VerifiedTotal: result.received,
		Layout: comparisonQueueLayout(actual, "actual benchmark queue"),
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		b.Fatal(err)
	}
	b.Log(scenario.layoutPrefix + string(encoded))
	runtime.KeepAlive(actual)
}

func reportConcurrentAPIMetrics(
	b *testing.B,
	api ringAPI,
	result throughputResult,
	total int,
) {
	reportThroughput(b, result, total)
	nsPerElem, elemPerOp, err := ringAPIMetrics(b.Elapsed(), b.N, total, result.received)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(nsPerElem, "ns/elem")
	if api == apiBulk {
		b.ReportMetric(elemPerOp, "elem/op")
	}
}
