package ring_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/ring"
)

type mpmcComparisonDefaults struct {
	MaxEnqueueBatch     int    `json:"max_enqueue_batch"`
	MaxDequeueBatch     int    `json:"max_dequeue_batch"`
	ClearOnDequeue      bool   `json:"clear_on_dequeue"`
	ExactBatchCounts    bool   `json:"exact_batch_counts"`
	ProducerAdmission   string `json:"producer_admission"`
	ConsumerAdmission   string `json:"consumer_admission"`
	ProducerPublication string `json:"producer_publication"`
	ConsumerRelease     string `json:"consumer_release"`
	SpinsBeforeYield    int    `json:"spins_before_yield"`
}

type mpmcComparisonManifestRow struct {
	benchmarkManifestRow
	ExplicitOptions []string               `json:"explicit_options"`
	Defaults        mpmcComparisonDefaults `json:"defaults"`
	Metrics         []string               `json:"metrics"`
	Layout          spmcComparisonLayout   `json:"layout"`
}

type mpmcComparisonManifest struct {
	Schema     int                         `json:"schema"`
	GOOS       string                      `json:"goos"`
	GOARCH     string                      `json:"goarch"`
	GoVersion  string                      `json:"go_version"`
	GOMAXPROCS int                         `json:"gomaxprocs"`
	Rows       []mpmcComparisonManifestRow `json:"rows"`
}

func buildMPMCComparisonManifest() (mpmcComparisonManifest, error) {
	manifest := mpmcComparisonManifest{
		Schema: 1, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
	for _, implementation := range []string{"ring", "mpmc"} {
		for _, api := range []ringAPI{apiSingle, apiBulk, apiBurst} {
			for _, batch := range []int{1, 32} {
				if api == apiSingle && batch != 1 {
					continue
				}
				for _, element := range []string{"int64", "ptr"} {
					name := "BenchmarkMPMCComparison/impl=" + implementation + "/mode=MPMC/api=" + api.String()
					if api != apiSingle {
						name += fmt.Sprintf("/batch=%d", batch)
					}
					row := manifestRow(name+"/elem="+element, implementation, mpmcMode, batch)
					row.Family, row.PrimaryMetric = "MPMCComparison", "ns/elem"
					row.API, row.APIReason = manifestPointer(api.String()), ""
					row.Participants = 8
					row.RingOptions = []string{}
					if implementation == "ring" {
						row.RingOptions = []string{"WithMultiProdEnqueue", "WithMultiConsDequeue"}
					}
					row = spscElementContract(row, element)
					var exemplar any
					if element == "int64" {
						created, err := newMPMCComparisonQueue[int64](implementation, api)
						if err != nil {
							return mpmcComparisonManifest{}, err
						}
						exemplar = created
					} else {
						created, err := newMPMCComparisonQueue[*item](implementation, api)
						if err != nil {
							return mpmcComparisonManifest{}, err
						}
						exemplar = created
					}
					metrics := []string{"ns/op", "ns/elem", "gc-cycles", "gc/Melem", "B/op", "allocs/op"}
					if api == apiBulk {
						metrics = append(metrics, "elem/op")
					}
					manifest.Rows = append(manifest.Rows, mpmcComparisonManifestRow{
						benchmarkManifestRow: row, ExplicitOptions: slices.Clone(row.RingOptions),
						Defaults: mpmcComparisonDefaults{
							MaxEnqueueBatch: 1024, MaxDequeueBatch: 1024,
							ClearOnDequeue: true, ExactBatchCounts: false,
							ProducerAdmission: "shared cached counts", ConsumerAdmission: "shared cached counts",
							ProducerPublication: "ordered", ConsumerRelease: "ordered", SpinsBeforeYield: 8192,
						},
						Metrics: metrics,
						Layout:  spmcComparisonQueueLayout(exemplar, "separately allocated layout exemplar"),
					})
				}
			}
		}
	}
	return manifest, nil
}

func TestMPMCComparisonManifest(t *testing.T) {
	manifest, err := buildMPMCComparisonManifest()
	require.NoError(t, err)
	require.Equal(t, 1, manifest.Schema)
	require.Equal(t, runtime.GOOS, manifest.GOOS)
	require.Equal(t, runtime.GOARCH, manifest.GOARCH)
	require.Equal(t, runtime.Version(), manifest.GoVersion)
	require.Equal(t, runtime.GOMAXPROCS(0), manifest.GOMAXPROCS)
	suffixes := []string{
		"single/elem=int64", "single/elem=ptr",
		"bulk/batch=1/elem=int64", "bulk/batch=1/elem=ptr",
		"bulk/batch=32/elem=int64", "bulk/batch=32/elem=ptr",
		"burst/batch=1/elem=int64", "burst/batch=1/elem=ptr",
		"burst/batch=32/elem=int64", "burst/batch=32/elem=ptr",
	}
	expected := make(map[string]bool)
	for _, prefix := range []string{
		"BenchmarkMPMCComparison/impl=ring/mode=MPMC/api=",
		"BenchmarkMPMCComparison/impl=mpmc/mode=MPMC/api=",
	} {
		for _, suffix := range suffixes {
			expected[prefix+suffix] = true
		}
	}
	require.Len(t, manifest.Rows, 20)
	for _, row := range manifest.Rows {
		t.Run(row.Name, func(t *testing.T) {
			require.True(t, expected[row.Name], "unknown or duplicate leaf")
			delete(expected, row.Name)
			checkMPMCComparisonRow(t, row)
		})
	}
	require.Empty(t, expected)
	encoded, err := json.Marshal(manifest)
	require.NoError(t, err)
	t.Log("MPMC_COMPARISON_MANIFEST " + string(encoded))
}

func checkMPMCComparisonRow(t *testing.T, row mpmcComparisonManifestRow) {
	t.Helper()
	require.Equal(t, "MPMCComparison", row.Family)
	implementation := "ring"
	if strings.Contains(row.Name, "/impl=mpmc/") {
		implementation = "mpmc"
	}
	require.Equal(t, implementation, row.Implementation)
	api := apiSingle
	if strings.Contains(row.Name, "/api=bulk/") {
		api = apiBulk
	}
	if strings.Contains(row.Name, "/api=burst/") {
		api = apiBurst
	}
	require.NotNil(t, row.API)
	require.Equal(t, api.String(), *row.API)
	require.Empty(t, row.APIReason)
	require.Empty(t, row.WorkerReason)
	require.Empty(t, row.WaitReason)
	require.Empty(t, row.RingOptionsReason)
	batch := 1
	if strings.Contains(row.Name, "/batch=32/") {
		batch = 32
	}
	require.Equal(t, batch, row.Batch)
	if api == apiSingle {
		require.NotContains(t, row.Name, "/batch=")
	}
	require.Equal(t, 1024, row.UsableCapacity)
	require.NotNil(t, row.Producers)
	require.NotNil(t, row.Consumers)
	require.Equal(t, 4, *row.Producers)
	require.Equal(t, 4, *row.Consumers)
	require.Zero(t, row.Callers)
	require.Equal(t, 8, row.Participants)
	require.Equal(t, 1, row.QueueCount)
	options := []string{}
	if implementation == "ring" {
		options = []string{"WithMultiProdEnqueue", "WithMultiConsDequeue"}
	}
	require.Equal(t, options, row.ExplicitOptions)
	require.Equal(t, options, row.RingOptions)
	require.Equal(t, "ns/elem", row.PrimaryMetric)
	wait := "runtime.Gosched after each unsuccessful "
	require.NotNil(t, row.ProducerWait)
	require.NotNil(t, row.ConsumerWait)
	require.Equal(t, wait+"enqueue attempt", *row.ProducerWait)
	require.Equal(t, wait+"dequeue attempt", *row.ConsumerWait)
	require.Equal(
		t,
		"excludes queue/buffer setup and pre-run GC; includes worker startup, "+
			"payload fill, operations, retries, producer completion, drain, and joins",
		row.TimingBoundary,
	)
	metrics := []string{"ns/op", "ns/elem", "gc-cycles", "gc/Melem", "B/op", "allocs/op"}
	if api == apiBulk {
		metrics = append(metrics, "elem/op")
	}
	require.Equal(t, metrics, row.Metrics)
	require.Equal(t, mpmcComparisonDefaults{
		MaxEnqueueBatch: 1024, MaxDequeueBatch: 1024, ClearOnDequeue: true,
		ProducerAdmission: "shared cached counts", ConsumerAdmission: "shared cached counts",
		ProducerPublication: "ordered", ConsumerRelease: "ordered", SpinsBeforeYield: 8192,
	}, row.Defaults)
	var actual any
	if strings.HasSuffix(row.Name, "/elem=ptr") {
		require.Equal(t, "ptr", row.Element)
		require.NotNil(t, row.PointerPoolSize)
		require.Equal(t, 1024, *row.PointerPoolSize)
		require.Empty(t, row.PointerPoolReason)
		require.Equal(t, "typed *item pointing into a rotating pool of 1024 items", row.Storage)
		created, err := newMPMCComparisonQueue[*item](implementation, api)
		require.NoError(t, err)
		actual = created
	} else {
		require.Equal(t, "int64", row.Element)
		require.Nil(t, row.PointerPoolSize)
		require.Equal(t, "int64 values do not use a pointer pool", row.PointerPoolReason)
		require.Equal(t, "typed int64", row.Storage)
		created, err := newMPMCComparisonQueue[int64](implementation, api)
		require.NoError(t, err)
		actual = created
	}
	checkMPMCComparisonDefaults(t, actual, implementation)
	checkMPMCComparisonLayout(t, row.Layout, actual, implementation)
}

func checkMPMCComparisonDefaults(t *testing.T, target any, implementation string) {
	t.Helper()
	concrete := reflect.ValueOf(target).Elem().FieldByName("ring").Elem()
	if implementation == "ring" {
		config := concrete.FieldByName("readOnly")
		require.EqualValues(t, 1024, config.FieldByName("maxEnqueueBatch").Uint())
		require.EqualValues(t, 1024, config.FieldByName("maxDequeueBatch").Uint())
		require.True(t, config.FieldByName("clearOnDequeue").Bool())
		require.False(t, config.FieldByName("exactBatchCounts").Bool())
		require.EqualValues(t, 8192, config.FieldByName("spinsBeforeYield").Uint())
		require.EqualValues(t, 1, config.FieldByName("prodMode").Uint())
		require.EqualValues(t, 1, config.FieldByName("consMode").Uint())
		require.Zero(t, config.FieldByName("prodHTDMax").Uint())
		require.Zero(t, config.FieldByName("consHTDMax").Uint())
		require.EqualValues(t, 1023, config.FieldByName("mask").Uint())
		for _, name := range []string{"prodHeadLine", "consHeadLine"} {
			block := concrete.FieldByName(name)
			cache := "cachedConsTail"
			if name == "consHeadLine" {
				cache = "cachedProdTail"
			}
			require.Equal(t, "atomic.Uint32", block.FieldByName(cache).Type().String())
		}
		for _, name := range []string{"prodTailLine", "consTailLine"} {
			block := concrete.FieldByName(name)
			tail := "prodTail"
			if name == "consTailLine" {
				tail = "consTail"
			}
			require.Equal(t, "atomic.Uint32", block.FieldByName(tail).Type().String())
		}
		require.Equal(t, reflect.Slice, config.FieldByName("entries").Kind())
		require.Equal(t, 1024, config.FieldByName("entries").Len())
		return
	}
	config := concrete.FieldByName("config")
	require.EqualValues(t, 1024, config.FieldByName("maxEnqueueBatch").Uint())
	require.EqualValues(t, 1024, config.FieldByName("maxDequeueBatch").Uint())
	require.EqualValues(t, 1023, config.FieldByName("mask").Uint())
	require.Zero(t, config.FieldByName("prodHTDMax").Uint())
	require.Zero(t, config.FieldByName("consHTDMax").Uint())
	require.True(t, config.FieldByName("clearOnDequeue").Bool())
	require.False(t, config.FieldByName("exactBatchCounts").Bool())
	require.False(t, config.FieldByName("producerRelaxed").Bool())
	require.False(t, config.FieldByName("consumerRelaxed").Bool())
	require.EqualValues(t, 8192, config.FieldByName("spinsBeforeYield").Uint())
	require.Equal(t, reflect.Slice, config.FieldByName("entries").Kind())
	require.Equal(t, 1024, config.FieldByName("entries").Len())
	require.False(t, config.FieldByName("entries").IsNil())
	for _, block := range []string{"producerReservation", "consumerReservation"} {
		reservation := concrete.FieldByName(block)
		require.Equal(t, "atomic.Uint64", reservation.FieldByName("headRTS").Type().String())
		require.Equal(t, "atomic.Uint32", reservation.FieldByName("head").Type().String())
		cache := "cachedConsumer"
		if block == "consumerReservation" {
			cache = "cachedProducer"
		}
		require.Equal(t, "atomic.Uint32", reservation.FieldByName(cache).Type().String())
		require.Zero(t, reservation.FieldByName(cache).FieldByName("v").Uint())
	}
	for _, block := range []string{"producerPublication", "consumerPublication"} {
		publication := concrete.FieldByName(block)
		require.Equal(t, "atomic.Uint64", publication.FieldByName("tailRTS").Type().String())
		require.Equal(t, "atomic.Uint32", publication.FieldByName("published").Type().String())
		require.Zero(t, publication.FieldByName("published").FieldByName("v").Uint())
	}
}

func checkMPMCComparisonLayout(
	t *testing.T,
	layout spmcComparisonLayout,
	target any,
	implementation string,
) {
	t.Helper()
	require.Equal(t, "separately allocated layout exemplar", layout.Source)
	concrete := reflect.ValueOf(target).Elem().FieldByName("ring").Type().Elem()
	if implementation == "mpmc" {
		checkMPMCComparisonBlocks(t, concrete)
	}
	require.Equal(t, concrete.String(), layout.Type)
	argument := "int64"
	if strings.HasSuffix(concrete.String(), ".item]") {
		elementType := reflect.TypeFor[item]()
		argument = "*" + elementType.PkgPath() + "." + elementType.Name()
	}
	queueType := "Ring"
	if implementation == "mpmc" {
		queueType = "MPMCQueue"
	}
	require.Equal(t, "ring."+queueType+"["+argument+"]", layout.Type)
	require.Equal(t, concrete.Size(), layout.Size)
	require.Equal(t, concrete.Align(), layout.Alignment)
	require.Less(t, layout.BaseModulo64, uintptr(64))
	require.Less(t, layout.BaseModulo128, uintptr(128))
	require.Equal(t, layout.BaseModulo64, layout.BaseModulo128%64)
	require.Zero(t, layout.BaseModulo128%uintptr(concrete.Align()))
	var inspect func(reflect.Type, []spmcComparisonField)
	inspect = func(kind reflect.Type, fields []spmcComparisonField) {
		require.Len(t, fields, kind.NumField())
		for index, field := range fields {
			expected := kind.Field(index)
			require.Equal(t, expected.Name, field.Name)
			require.Equal(t, expected.Type.String(), field.Type)
			require.Equal(t, expected.Offset, field.Offset)
			require.Equal(t, expected.Type.Size(), field.Size)
			require.Equal(t, expected.Type.Align(), field.Alignment)
			if expected.Type.Kind() == reflect.Struct {
				inspect(expected.Type, field.Fields)
			} else {
				require.Empty(t, field.Fields)
			}
		}
	}
	inspect(concrete, layout.Fields)
	if reflect.TypeFor[uintptr]().Size() != 8 {
		return
	}
	if implementation == "ring" {
		require.Equal(t, uintptr(640), layout.Size)
		for index, name := range []string{
			"readOnly", "prodHeadLine", "prodTailLine", "consHeadLine", "consTailLine",
		} {
			field, found := concrete.FieldByName(name)
			require.True(t, found)
			require.Equal(t, uintptr(index*128), field.Offset)
		}
		return
	}
	require.Equal(t, uintptr(632), layout.Size)
	require.Equal(t, 8, layout.Alignment)
	require.Equal(t, uintptr(56), concrete.Field(0).Type.Size())
	for index, offset := range []uintptr{0, 184, 328, 472, 616} {
		require.Equal(t, offset, concrete.Field(2*index).Offset)
	}
	for _, index := range []int{1, 3, 5, 7} {
		gap := concrete.Field(index)
		require.Equal(t, "_", gap.Name)
		require.Equal(t, reflect.TypeFor[[128]byte](), gap.Type)
	}
}

func TestMPMCComparisonAccounting(t *testing.T) {
	for _, implementation := range []string{"ring", "mpmc"} {
		for _, api := range []ringAPI{apiSingle, apiBulk, apiBurst} {
			for _, batch := range []int{1, 32} {
				if api == apiSingle && batch != 1 {
					continue
				}
				for _, requested := range []int{0, 1, 3, 4, 31, 32, 33, 127, 128, 129, 10007} {
					name := fmt.Sprintf("impl=%s/api=%s/batch=%d/total=%d", implementation, api, batch, requested)
					expectedTotal := requested
					if api == apiBulk {
						multiple := batch * 4
						expectedTotal = (requested + multiple - 1) / multiple * multiple
					}
					total := api.total(requested, batch, 4)
					t.Run(name+"/elem=int64", func(t *testing.T) {
						require.Equal(t, expectedTotal, total)
						recorders := make([]*recordSink[int64], 4)
						result := runWithin(t, t.Name(), func() (throughputResult, error) {
							return runThroughput(
								noopTimer{},
								func() (queue[int64], error) {
									return newMPMCComparisonQueue[int64](implementation, api)
								},
								4,
								4,
								batch,
								total,
								fillInt64,
								func(consumer int) *recordSink[int64] {
									recorders[consumer] = &recordSink[int64]{}
									return recorders[consumer]
								},
							)
						})
						require.Equal(t, expectedTotal, result.received)
						require.NoError(t, accountingError(recorders, 4, expectedTotal))
						expected := make(map[int64]int)
						for producer := range 4 {
							for sequence := range producerShare(expectedTotal, 4, producer) {
								expected[packInt64(producer, int64(sequence))]++
							}
						}
						actual := make(map[int64]int)
						for _, recorder := range recorders {
							for _, value := range recorder.items {
								actual[value]++
							}
						}
						require.Equal(t, expected, actual)
					})
					t.Run(name+"/elem=ptr", func(t *testing.T) {
						require.Equal(t, expectedTotal, total)
						pool := newItemPool()
						require.Len(t, pool, 1024)
						recorders := make([]*recordSink[*item], 4)
						result := runWithin(t, t.Name(), func() (throughputResult, error) {
							return runThroughput(
								noopTimer{},
								func() (queue[*item], error) {
									return newMPMCComparisonQueue[*item](implementation, api)
								},
								4,
								4,
								batch,
								total,
								fillPtr(pool),
								func(consumer int) *recordSink[*item] {
									recorders[consumer] = &recordSink[*item]{}
									return recorders[consumer]
								},
							)
						})
						require.Equal(t, expectedTotal, result.received)
						expected := make(map[*item]int)
						for producer := range 4 {
							for sequence := range producerShare(expectedTotal, 4, producer) {
								expected[&pool[sequence%1024]]++
							}
						}
						actual := make(map[*item]int)
						for _, recorder := range recorders {
							for _, value := range recorder.items {
								require.NotNil(t, value)
								require.Contains(t, expected, value)
								actual[value]++
							}
						}
						require.Equal(t, expected, actual)
						for index := range pool {
							require.Equal(t, int64(index), pool[index].value)
						}
					})
				}
			}
		}
	}
}

func mpmcComparisonRaw(target queue[int64]) spmcComparisonRawQueue {
	switch adapter := target.(type) {
	case *ringSingleQueue[int64]:
		return adapter.ring
	case *ringBulkQueue[int64]:
		return adapter.ring
	case *ringQueue[int64]:
		return adapter.ring
	case *mpmcSingleQueue[int64]:
		return adapter.ring
	case *mpmcBulkQueue[int64]:
		return adapter.ring
	case *mpmcBurstQueue[int64]:
		return adapter.ring
	default:
		panic(fmt.Sprintf("unknown comparison adapter %T", target))
	}
}

func TestMPMCComparisonAdapterContracts(t *testing.T) {
	for _, implementation := range []string{"ring", "mpmc"} {
		for _, api := range []ringAPI{apiSingle, apiBulk, apiBurst} {
			t.Run(implementation+"/"+api.String(), func(t *testing.T) {
				target, err := newMPMCComparisonQueue[int64](implementation, api)
				require.NoError(t, err)
				checkHarnessCapacity(t, target)
				raw := mpmcComparisonRaw(target)
				_, err = raw.Dequeue()
				require.Same(t, ring.ErrEmpty, err)
				dst := []int64{-1, -2, -3, -4}
				_, err = raw.DequeueBulk(dst)
				require.Same(t, ring.ErrEmpty, err)
				require.Equal(t, []int64{-1, -2, -3, -4}, dst)
				require.Zero(t, target.dequeue(dst))
				require.Equal(t, []int64{-1, -2, -3, -4}, dst)
				if api != apiSingle {
					require.Zero(t, target.enqueue(nil))
					require.Zero(t, target.dequeue(nil))
					require.Equal(t, []int64{-1, -2, -3, -4}, dst)
				}
				require.Equal(t, 1, target.enqueue([]int64{7}))
				if api == apiBulk {
					require.Zero(t, target.dequeue(dst))
					require.Equal(t, []int64{-1, -2, -3, -4}, dst)
					require.Equal(t, 1, target.dequeue(dst[:1]))
				} else {
					require.Equal(t, 1, target.dequeue(dst))
				}
				require.Equal(t, []int64{7, -2, -3, -4}, dst)
				if api == apiSingle {
					require.Equal(t, 1, target.enqueue([]int64{11, 12}))
					require.Equal(t, 1, target.dequeue(dst))
					require.Equal(t, []int64{11, -2, -3, -4}, dst)
					require.Zero(t, target.dequeue(dst))
				}
				values := make([]int64, 1023)
				moved, _ := raw.EnqueueBurst(values)
				require.Equal(t, 1023, moved)
				if api == apiBulk {
					require.Zero(t, target.enqueue([]int64{1, 2}))
				}
				if api == apiBurst {
					require.Equal(t, 1, target.enqueue([]int64{1, 2, 3}))
				} else {
					require.Equal(t, 1, target.enqueue([]int64{1}))
				}
				require.Zero(t, target.enqueue([]int64{9}))
				if api != apiSingle {
					require.Zero(t, target.enqueue(nil))
					require.Zero(t, target.dequeue(nil))
				}
				require.Same(t, ring.ErrFull, raw.Enqueue(9))
				_, err = raw.EnqueueBulk([]int64{9})
				require.Same(t, ring.ErrFull, err)
				_, err = raw.DequeueBulk(make([]int64, 1024))
				require.NoError(t, err)
				_, err = raw.Dequeue()
				require.Same(t, ring.ErrEmpty, err)
				oversized := make([]int64, 1025)
				_, err = raw.EnqueueBulk(oversized)
				require.ErrorIs(t, err, ring.ErrExceedsCapacity)
				enqueueError := err
				_, err = raw.DequeueBulk(oversized)
				require.ErrorIs(t, err, ring.ErrExceedsCapacity)
				dequeueError := err
				if api == apiBulk {
					require.PanicsWithError(t, enqueueError.Error(), func() { target.enqueue(oversized) })
					require.PanicsWithError(t, dequeueError.Error(), func() { target.dequeue(oversized) })
				}
				for index := range oversized {
					oversized[index] = int64(index + 1)
				}
				moved, _ = raw.EnqueueBurst(oversized)
				require.Equal(t, 1024, moved)
				burstDestination := make([]int64, 1025)
				burstDestination[1024] = -1
				moved, _ = raw.DequeueBurst(burstDestination)
				require.Equal(t, 1024, moved)
				require.Equal(t, oversized[:1024], burstDestination[:1024])
				require.Equal(t, int64(-1), burstDestination[1024])
				// Single methods have no unexpected-error input, so exercise their shared translator.
				for _, unexpected := range []error{enqueueError, dequeueError} {
					require.PanicsWithValue(t, unexpected, func() {
						movedOrPanic(unexpected, ring.ErrFull, 1)
					})
					require.PanicsWithValue(t, unexpected, func() {
						movedOrPanic(unexpected, ring.ErrEmpty, 1)
					})
				}
			})
		}
		t.Run(implementation+"/directionalLimits", func(t *testing.T) {
			var target queue[int64]
			if implementation == "ring" {
				created, err := ring.New[int64](
					1024,
					ring.WithMultiProdEnqueue(),
					ring.WithMultiConsDequeue(),
					ring.WithMaxEnqueueBatch(2),
					ring.WithMaxDequeueBatch(2),
				)
				require.NoError(t, err)
				target = &ringBulkQueue[int64]{ring: created}
			} else {
				created, err := ring.NewMPMCQueue[int64](
					1024,
					ring.WithMaxEnqueueBatch(2),
					ring.WithMaxDequeueBatch(2),
				)
				require.NoError(t, err)
				target = &mpmcBulkQueue[int64]{ring: created}
			}
			raw := mpmcComparisonRaw(target)
			oversized := []int64{1, 2, 3}
			_, err := raw.EnqueueBulk(oversized)
			require.ErrorIs(t, err, ring.ErrExceedsMaxBatch)
			require.PanicsWithError(t, err.Error(), func() { target.enqueue(oversized) })
			dst := []int64{-1, -2, -3}
			_, err = raw.DequeueBulk(dst)
			require.ErrorIs(t, err, ring.ErrExceedsMaxBatch)
			require.PanicsWithError(t, err.Error(), func() { target.dequeue(dst) })
			require.Equal(t, []int64{-1, -2, -3}, dst)
			moved, _ := raw.EnqueueBurst(oversized)
			require.Equal(t, 2, moved)
			moved, _ = raw.DequeueBurst(dst)
			require.Equal(t, 2, moved)
			require.Equal(t, []int64{1, 2, -3}, dst)
		})
		_, err := newMPMCComparisonQueue[int64](implementation, ringAPI(99))
		require.Error(t, err)
	}
}

func TestMPMCComparisonUnknownImplementation(t *testing.T) {
	_, err := newMPMCComparisonQueue[int64]("unknown", apiSingle)
	require.Error(t, err)
}

func checkMPMCComparisonBlocks(t *testing.T, concrete reflect.Type) {
	t.Helper()
	groupNames := []string{
		"config", "producerReservation", "producerPublication",
		"consumerReservation", "consumerPublication",
	}
	require.Equal(t, 9, concrete.NumField())
	for index, name := range groupNames {
		require.Equal(t, name, concrete.Field(index*2).Name)
	}
	for _, index := range []int{1, 3, 5, 7} {
		require.Equal(t, "_", concrete.Field(index).Name)
		require.Equal(t, reflect.TypeFor[[128]byte](), concrete.Field(index).Type)
	}
	configNames := []string{
		"entries", "mask", "maxEnqueueBatch", "maxDequeueBatch", "prodHTDMax",
		"consHTDMax", "spinsBeforeYield", "producerRelaxed", "consumerRelaxed",
		"clearOnDequeue", "exactBatchCounts",
	}
	config := concrete.Field(0).Type
	require.Equal(t, len(configNames), config.NumField())
	var end uintptr
	for index, name := range configNames {
		field := config.Field(index)
		require.Equal(t, name, field.Name)
		expectedType := reflect.TypeFor[uint32]()
		if index == 0 {
			expectedType = reflect.TypeFor[[]int64]()
			if strings.Contains(concrete.String(), ".item]") {
				expectedType = reflect.TypeFor[[]*item]()
			}
		}
		if index >= 7 {
			expectedType = reflect.TypeFor[bool]()
		}
		require.Equal(t, expectedType, field.Type)
		alignment := uintptr(expectedType.Align())
		offset := (end + alignment - 1) / alignment * alignment
		require.Equal(t, offset, field.Offset)
		end = offset + expectedType.Size()
	}
	alignment := uintptr(config.Align())
	require.Equal(t, (end+alignment-1)/alignment*alignment, config.Size())
	for index, names := range [][]string{
		{"headRTS", "head", "cachedConsumer"}, {"tailRTS", "published"},
		{"headRTS", "head", "cachedProducer"}, {"tailRTS", "published"},
	} {
		block := concrete.Field(2 + index*2).Type
		require.Equal(t, len(names), block.NumField())
		require.Equal(t, uintptr(16), block.Size())
		require.Equal(t, 8, block.Align())
		for fieldIndex, name := range names {
			field := block.Field(fieldIndex)
			require.Equal(t, name, field.Name)
			expectedType := reflect.TypeFor[atomic.Uint32]()
			offset := uintptr(4 + fieldIndex*4)
			if fieldIndex == 0 {
				expectedType = reflect.TypeFor[atomic.Uint64]()
				offset = 0
			}
			require.Equal(t, expectedType, field.Type)
			require.Equal(t, offset, field.Offset)
		}
	}
	end = 0
	for index := range concrete.NumField() {
		field := concrete.Field(index)
		alignment := uintptr(field.Type.Align())
		offset := (end + alignment - 1) / alignment * alignment
		require.Equal(t, offset, field.Offset)
		end = offset + field.Type.Size()
	}
	alignment = uintptr(concrete.Align())
	require.Equal(t, (end+alignment-1)/alignment*alignment, concrete.Size())
}
