package ring_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/ring"
)

type spmcComparisonField struct {
	Name      string                `json:"name"`
	Type      string                `json:"type"`
	Offset    uintptr               `json:"offset"`
	Size      uintptr               `json:"size"`
	Alignment int                   `json:"alignment"`
	Fields    []spmcComparisonField `json:"fields,omitempty"`
}

type spmcComparisonLayout struct {
	Source        string                `json:"source"`
	Type          string                `json:"type"`
	Size          uintptr               `json:"size"`
	Alignment     int                   `json:"alignment"`
	BaseModulo64  uintptr               `json:"base_modulo_64"`
	BaseModulo128 uintptr               `json:"base_modulo_128"`
	Fields        []spmcComparisonField `json:"fields"`
}

type spmcComparisonLayoutRecord struct {
	Name          string               `json:"name"`
	Iterations    int                  `json:"iterations"`
	VerifiedTotal int                  `json:"verified_total"`
	Layout        spmcComparisonLayout `json:"layout"`
}

func spmcComparisonFields(target reflect.Type) []spmcComparisonField {
	var fields []spmcComparisonField
	for index := range target.NumField() {
		field := target.Field(index)
		entry := spmcComparisonField{
			Name: field.Name, Type: field.Type.String(), Offset: field.Offset,
			Size: field.Type.Size(), Alignment: field.Type.Align(),
		}
		if field.Type.Kind() == reflect.Struct {
			entry.Fields = spmcComparisonFields(field.Type)
		}
		fields = append(fields, entry)
	}
	return fields
}

func spmcComparisonQueueLayout(target any, source string) spmcComparisonLayout {
	pointer := reflect.ValueOf(target).Elem().FieldByName("ring")
	concrete := pointer.Type().Elem()
	layout := spmcComparisonLayout{
		Source: source, Type: concrete.String(), Size: concrete.Size(), Alignment: concrete.Align(),
		BaseModulo64: pointer.Pointer() % 64, BaseModulo128: pointer.Pointer() % 128,
		Fields: spmcComparisonFields(concrete),
	}
	runtime.KeepAlive(target)
	return layout
}

type spmcComparisonDefaults struct {
	MaxEnqueueBatch   int    `json:"max_enqueue_batch"`
	MaxDequeueBatch   int    `json:"max_dequeue_batch"`
	ClearOnDequeue    bool   `json:"clear_on_dequeue"`
	ExactBatchCounts  bool   `json:"exact_batch_counts"`
	ProducerAdmission string `json:"producer_admission"`
	ConsumerAdmission string `json:"consumer_admission"`
	ConsumerRelease   string `json:"consumer_release"`
	SpinsBeforeYield  int    `json:"spins_before_yield"`
}

type spmcComparisonManifestRow struct {
	benchmarkManifestRow
	ExplicitOptions []string               `json:"explicit_options"`
	Defaults        spmcComparisonDefaults `json:"defaults"`
	Metrics         []string               `json:"metrics"`
	Layout          spmcComparisonLayout   `json:"layout"`
}

type spmcComparisonManifest struct {
	Schema     int                         `json:"schema"`
	GOOS       string                      `json:"goos"`
	GOARCH     string                      `json:"goarch"`
	GoVersion  string                      `json:"go_version"`
	GOMAXPROCS int                         `json:"gomaxprocs"`
	Rows       []spmcComparisonManifestRow `json:"rows"`
}

func buildSPMCComparisonManifest() (spmcComparisonManifest, error) {
	manifest := spmcComparisonManifest{
		Schema: 1, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
	for _, implementation := range []string{"ring", "spmc"} {
		for _, api := range []ringAPI{apiSingle, apiBulk, apiBurst} {
			for _, batch := range []int{1, 32} {
				if api == apiSingle && batch != 1 {
					continue
				}
				for _, element := range []string{"int64", "ptr"} {
					name := "BenchmarkSPMCComparison/impl=" + implementation + "/mode=SPMC/api=" + api.String()
					if api != apiSingle {
						name += fmt.Sprintf("/batch=%d", batch)
					}
					row := manifestRow(name+"/elem="+element, implementation, spmcMode, batch)
					row.Family, row.PrimaryMetric = "SPMCComparison", "ns/elem"
					row.API, row.APIReason = manifestPointer(api.String()), ""
					row.Participants = 5
					row.RingOptions = []string{}
					if implementation == "ring" {
						row.RingOptions = []string{"WithMultiConsDequeue"}
					}
					row = spscElementContract(row, element)
					var exemplar any
					if element == "int64" {
						created, err := newSPMCComparisonQueue[int64](implementation, api)
						if err != nil {
							return spmcComparisonManifest{}, err
						}
						exemplar = created
					} else {
						created, err := newSPMCComparisonQueue[*item](implementation, api)
						if err != nil {
							return spmcComparisonManifest{}, err
						}
						exemplar = created
					}
					metrics := []string{"ns/op", "ns/elem", "gc-cycles", "gc/Melem", "B/op", "allocs/op"}
					if api == apiBulk {
						metrics = append(metrics, "elem/op")
					}
					manifest.Rows = append(manifest.Rows, spmcComparisonManifestRow{
						benchmarkManifestRow: row, ExplicitOptions: slices.Clone(row.RingOptions),
						Defaults: spmcComparisonDefaults{
							MaxEnqueueBatch: 1024, MaxDequeueBatch: 1024,
							ClearOnDequeue: true, ExactBatchCounts: false,
							ProducerAdmission: "cached counts", ConsumerAdmission: "shared cached counts",
							ConsumerRelease: "ordered", SpinsBeforeYield: 8192,
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

func TestSPMCComparisonManifest(t *testing.T) {
	manifest, err := buildSPMCComparisonManifest()
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
		"BenchmarkSPMCComparison/impl=ring/mode=SPMC/api=",
		"BenchmarkSPMCComparison/impl=spmc/mode=SPMC/api=",
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
			checkSPMCComparisonRow(t, row)
		})
	}
	require.Empty(t, expected)
	encoded, err := json.Marshal(manifest)
	require.NoError(t, err)
	t.Log("SPMC_COMPARISON_MANIFEST " + string(encoded))
}

func checkSPMCComparisonRow(t *testing.T, row spmcComparisonManifestRow) {
	t.Helper()
	require.Equal(t, "SPMCComparison", row.Family)
	implementation := "ring"
	if strings.Contains(row.Name, "/impl=spmc/") {
		implementation = "spmc"
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
	require.Equal(t, 1, *row.Producers)
	require.Equal(t, 4, *row.Consumers)
	require.Zero(t, row.Callers)
	require.Equal(t, 5, row.Participants)
	require.Equal(t, 1, row.QueueCount)
	options := []string{}
	if implementation == "ring" {
		options = []string{"WithMultiConsDequeue"}
	}
	require.Equal(t, options, row.ExplicitOptions)
	require.Equal(t, options, row.RingOptions)
	require.Equal(t, "ns/elem", row.PrimaryMetric)
	wait := "runtime.Gosched after each unsuccessful "
	require.NotNil(t, row.ProducerWait)
	require.NotNil(t, row.ConsumerWait)
	require.Equal(t, wait+"enqueue attempt", *row.ProducerWait)
	require.Equal(t, wait+"dequeue attempt", *row.ConsumerWait)
	require.Equal(t,
		"excludes queue/buffer setup and pre-run GC; includes worker startup, "+
			"payload fill, operations, retries, producer completion, drain, and joins",
		row.TimingBoundary,
	)
	metrics := []string{"ns/op", "ns/elem", "gc-cycles", "gc/Melem", "B/op", "allocs/op"}
	if api == apiBulk {
		metrics = append(metrics, "elem/op")
	}
	require.Equal(t, metrics, row.Metrics)
	require.Equal(t, spmcComparisonDefaults{
		MaxEnqueueBatch: 1024, MaxDequeueBatch: 1024, ClearOnDequeue: true,
		ProducerAdmission: "cached counts", ConsumerAdmission: "shared cached counts",
		ConsumerRelease: "ordered", SpinsBeforeYield: 8192,
	}, row.Defaults)
	var actual any
	if strings.HasSuffix(row.Name, "/elem=ptr") {
		require.Equal(t, "ptr", row.Element)
		require.NotNil(t, row.PointerPoolSize)
		require.Equal(t, 1024, *row.PointerPoolSize)
		require.Empty(t, row.PointerPoolReason)
		require.Equal(t, "typed *item pointing into a rotating pool of 1024 items", row.Storage)
		created, err := newSPMCComparisonQueue[*item](implementation, api)
		require.NoError(t, err)
		actual = created
	} else {
		require.Equal(t, "int64", row.Element)
		require.Nil(t, row.PointerPoolSize)
		require.Equal(t, "int64 values do not use a pointer pool", row.PointerPoolReason)
		require.Equal(t, "typed int64", row.Storage)
		created, err := newSPMCComparisonQueue[int64](implementation, api)
		require.NoError(t, err)
		actual = created
	}
	checkSPMCComparisonDefaults(t, actual, implementation)
	checkSPMCComparisonLayout(t, row.Layout, actual, implementation)
}

func checkSPMCComparisonDefaults(t *testing.T, target any, implementation string) {
	t.Helper()
	concrete := reflect.ValueOf(target).Elem().FieldByName("ring").Elem()
	if implementation == "ring" {
		config := concrete.FieldByName("readOnly")
		require.EqualValues(t, 1024, config.FieldByName("maxEnqueueBatch").Uint())
		require.EqualValues(t, 1024, config.FieldByName("maxDequeueBatch").Uint())
		require.True(t, config.FieldByName("clearOnDequeue").Bool())
		require.False(t, config.FieldByName("exactBatchCounts").Bool())
		require.EqualValues(t, 8192, config.FieldByName("spinsBeforeYield").Uint())
		require.EqualValues(t, 0, config.FieldByName("prodMode").Uint())
		require.EqualValues(t, 1, config.FieldByName("consMode").Uint())
		require.Equal(t, reflect.Slice, config.FieldByName("entries").Kind())
		require.Equal(t, 1024, config.FieldByName("entries").Len())
		return
	}
	producer := concrete.FieldByName("producer")
	consumer := concrete.FieldByName("consumerConfig")
	require.EqualValues(t, 1024, producer.FieldByName("maxBatch").Uint())
	require.EqualValues(t, 1024, consumer.FieldByName("maxBatch").Uint())
	require.False(t, producer.FieldByName("exactBatchCounts").Bool())
	require.False(t, consumer.FieldByName("exactBatchCounts").Bool())
	require.True(t, consumer.FieldByName("clearOnDequeue").Bool())
	require.EqualValues(t, 8192, consumer.FieldByName("spinsBeforeYield").Uint())
	require.Equal(t, 1024, producer.FieldByName("entries").Len())
	require.Equal(t, 1024, consumer.FieldByName("entries").Len())
	require.Equal(t,
		producer.FieldByName("entries").Pointer(), consumer.FieldByName("entries").Pointer(),
	)
	require.Equal(t, reflect.Uint32,
		concrete.FieldByName("consumerReservation").FieldByName("cachedProducer").
			FieldByName("v").Kind(),
	)
}

func checkSPMCComparisonLayout(
	t *testing.T,
	layout spmcComparisonLayout,
	target any,
	implementation string,
) {
	t.Helper()
	require.Equal(t, "separately allocated layout exemplar", layout.Source)
	concrete := reflect.ValueOf(target).Elem().FieldByName("ring").Type().Elem()
	require.Equal(t, concrete.String(), layout.Type)
	require.Equal(t, concrete.Size(), layout.Size)
	require.Equal(t, concrete.Align(), layout.Alignment)
	require.Less(t, layout.BaseModulo64, uintptr(64))
	require.Less(t, layout.BaseModulo128, uintptr(128))
	require.Equal(t, layout.BaseModulo64, layout.BaseModulo128%64)
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
	require.Equal(t, uintptr(624), layout.Size)
	for index, offset := range []uintptr{0, 176, 312, 480, 616} {
		require.Equal(t, offset, concrete.Field(2*index).Offset)
	}
}

func TestSPMCComparisonAccounting(t *testing.T) {
	for _, implementation := range []string{"ring", "spmc"} {
		for _, api := range []ringAPI{apiSingle, apiBulk, apiBurst} {
			for _, batch := range []int{1, 32} {
				if api == apiSingle && batch != 1 {
					continue
				}
				for _, requested := range []int{0, 1, 31, 32, 33, 10007} {
					name := fmt.Sprintf("impl=%s/api=%s/batch=%d/total=%d", implementation, api, batch, requested)
					t.Run(name+"/elem=int64", func(t *testing.T) {
						expectedTotal := requested
						if api == apiBulk && requested%batch != 0 {
							expectedTotal += batch - requested%batch
						}
						total := api.total(requested, batch, 1)
						require.Equal(t, expectedTotal, total)
						recorders := make([]*recordSink[int64], 4)
						result := runWithin(t, t.Name(), func() (throughputResult, error) {
							return runThroughput(
								noopTimer{},
								func() (queue[int64], error) {
									return newSPMCComparisonQueue[int64](implementation, api)
								},
								1, 4, batch, total, fillInt64,
								func(consumer int) *recordSink[int64] {
									recorders[consumer] = &recordSink[int64]{}
									return recorders[consumer]
								},
							)
						})
						require.Equal(t, expectedTotal, result.received)
						expected := make(map[int64]int)
						for sequence := range expectedTotal {
							expected[int64(sequence)]++
						}
						actual := make(map[int64]int)
						for _, recorder := range recorders {
							for _, value := range recorder.items {
								producer, sequence := unpackInt64(value)
								require.Zero(t, producer)
								require.Equal(t, sequence, value)
								actual[value]++
							}
						}
						require.Equal(t, expected, actual)
					})
					t.Run(name+"/elem=ptr", func(t *testing.T) {
						expectedTotal := requested
						if api == apiBulk && requested%batch != 0 {
							expectedTotal += batch - requested%batch
						}
						total := api.total(requested, batch, 1)
						require.Equal(t, expectedTotal, total)
						pool := newItemPool()
						require.Len(t, pool, 1024)
						recorders := make([]*recordSink[*item], 4)
						result := runWithin(t, t.Name(), func() (throughputResult, error) {
							return runThroughput(
								noopTimer{},
								func() (queue[*item], error) {
									return newSPMCComparisonQueue[*item](implementation, api)
								},
								1, 4, batch, total, fillPtr(pool),
								func(consumer int) *recordSink[*item] {
									recorders[consumer] = &recordSink[*item]{}
									return recorders[consumer]
								},
							)
						})
						require.Equal(t, expectedTotal, result.received)
						expected := make(map[*item]int)
						for sequence := range expectedTotal {
							expected[&pool[sequence%1024]]++
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

// Both concrete implementations expose the same errors before adapter translation.
type spmcComparisonRawQueue interface {
	Enqueue(int64) error
	Dequeue() (int64, error)
	EnqueueBulk([]int64) (int, error)
	DequeueBulk([]int64) (int, error)
	EnqueueBurst([]int64) (int, int)
	DequeueBurst([]int64) (int, int)
}

func spmcComparisonRaw(target queue[int64]) spmcComparisonRawQueue {
	switch adapter := target.(type) {
	case *ringSingleQueue[int64]:
		return adapter.ring
	case *ringBulkQueue[int64]:
		return adapter.ring
	case *ringQueue[int64]:
		return adapter.ring
	case *spmcSingleQueue[int64]:
		return adapter.ring
	case *spmcBulkQueue[int64]:
		return adapter.ring
	case *spmcBurstQueue[int64]:
		return adapter.ring
	default:
		panic(fmt.Sprintf("unknown comparison adapter %T", target))
	}
}

func TestSPMCComparisonAdapterContracts(t *testing.T) {
	for _, implementation := range []string{"ring", "spmc"} {
		for _, api := range []ringAPI{apiSingle, apiBulk, apiBurst} {
			t.Run(implementation+"/"+api.String(), func(t *testing.T) {
				target, err := newSPMCComparisonQueue[int64](implementation, api)
				require.NoError(t, err)
				checkHarnessCapacity(t, target)
				raw := spmcComparisonRaw(target)
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
					ring.WithMultiConsDequeue(),
					ring.WithMaxEnqueueBatch(2),
					ring.WithMaxDequeueBatch(2),
				)
				require.NoError(t, err)
				target = &ringBulkQueue[int64]{ring: created}
			} else {
				created, err := ring.NewSPMC[int64](
					1024,
					ring.WithSPMCMaxEnqueueBatch(2),
					ring.WithSPMCMaxDequeueBatch(2),
				)
				require.NoError(t, err)
				target = &spmcBulkQueue[int64]{ring: created}
			}
			raw := spmcComparisonRaw(target)
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
		_, err := newSPMCComparisonQueue[int64](implementation, ringAPI(99))
		require.Error(t, err)
	}
}
