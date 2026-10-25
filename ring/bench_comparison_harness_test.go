package ring_test

import (
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type comparisonField struct {
	Name      string            `json:"name"`
	Type      string            `json:"type"`
	Offset    uintptr           `json:"offset"`
	Size      uintptr           `json:"size"`
	Alignment int               `json:"alignment"`
	Fields    []comparisonField `json:"fields,omitempty"`
}

type comparisonLayout struct {
	Source        string            `json:"source"`
	Type          string            `json:"type"`
	Size          uintptr           `json:"size"`
	Alignment     int               `json:"alignment"`
	BaseModulo64  uintptr           `json:"base_modulo_64"`
	BaseModulo128 uintptr           `json:"base_modulo_128"`
	Fields        []comparisonField `json:"fields"`
}

type comparisonLayoutRecord struct {
	Name          string           `json:"name"`
	Iterations    int              `json:"iterations"`
	VerifiedTotal int              `json:"verified_total"`
	Layout        comparisonLayout `json:"layout"`
}

func comparisonFields(target reflect.Type) []comparisonField {
	var fields []comparisonField
	for index := range target.NumField() {
		field := target.Field(index)
		entry := comparisonField{
			Name: field.Name, Type: field.Type.String(), Offset: field.Offset,
			Size: field.Type.Size(), Alignment: field.Type.Align(),
		}
		if field.Type.Kind() == reflect.Struct {
			entry.Fields = comparisonFields(field.Type)
		}
		fields = append(fields, entry)
	}
	return fields
}

func comparisonQueueLayout(target any, source string) comparisonLayout {
	pointer := reflect.ValueOf(target).Elem().FieldByName("ring")
	concrete := pointer.Type().Elem()
	layout := comparisonLayout{
		Source: source, Type: concrete.String(), Size: concrete.Size(), Alignment: concrete.Align(),
		BaseModulo64: pointer.Pointer() % 64, BaseModulo128: pointer.Pointer() % 128,
		Fields: comparisonFields(concrete),
	}
	runtime.KeepAlive(target)
	return layout
}

type comparisonDefaults struct {
	MaxEnqueueBatch     int    `json:"max_enqueue_batch"`
	MaxDequeueBatch     int    `json:"max_dequeue_batch"`
	ClearOnDequeue      bool   `json:"clear_on_dequeue"`
	ExactBatchCounts    bool   `json:"exact_batch_counts"`
	ProducerAdmission   string `json:"producer_admission"`
	ConsumerAdmission   string `json:"consumer_admission"`
	ProducerPublication string `json:"producer_publication,omitempty"`
	ConsumerRelease     string `json:"consumer_release"`
	SpinsBeforeYield    int    `json:"spins_before_yield"`
}

type comparisonManifestRow struct {
	benchmarkManifestRow
	ExplicitOptions []string           `json:"explicit_options"`
	Defaults        comparisonDefaults `json:"defaults"`
	Metrics         []string           `json:"metrics"`
	Layout          comparisonLayout   `json:"layout"`
}

type comparisonManifest struct {
	Schema     int                     `json:"schema"`
	GOOS       string                  `json:"goos"`
	GOARCH     string                  `json:"goarch"`
	GoVersion  string                  `json:"go_version"`
	GOMAXPROCS int                     `json:"gomaxprocs"`
	Rows       []comparisonManifestRow `json:"rows"`
}

func buildComparisonManifest(
	scenario comparisonScenario,
	newInt64Queue func(string, ringAPI) (queue[int64], error),
	newPtrQueue func(string, ringAPI) (queue[*item], error),
) (comparisonManifest, error) {
	manifest := comparisonManifest{
		Schema: 1, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
	for _, implementation := range []string{"ring", scenario.specialized} {
		for _, api := range []ringAPI{apiSingle, apiBulk, apiBurst} {
			for _, batch := range []int{1, 32} {
				if api == apiSingle && batch != 1 {
					continue
				}
				for _, element := range []string{"int64", "ptr"} {
					name := "Benchmark" + scenario.family + "/impl=" + implementation +
						"/mode=" + scenario.mode.name + "/api=" + api.String()
					if api != apiSingle {
						name += fmt.Sprintf("/batch=%d", batch)
					}
					row := manifestRow(name+"/elem="+element, implementation, scenario.mode, batch)
					row.Family, row.PrimaryMetric = scenario.family, "ns/elem"
					row.API, row.APIReason = manifestPointer(api.String()), ""
					row.Participants = scenario.mode.producers + scenario.mode.consumers
					row.RingOptions = []string{}
					if implementation == "ring" {
						row.RingOptions = slices.Clone(scenario.ringOptions)
					}
					row = spscElementContract(row, element)
					var exemplar any
					if element == "int64" {
						created, err := newInt64Queue(implementation, api)
						if err != nil {
							return comparisonManifest{}, err
						}
						exemplar = created
					} else {
						created, err := newPtrQueue(implementation, api)
						if err != nil {
							return comparisonManifest{}, err
						}
						exemplar = created
					}
					metrics := []string{"ns/op", "ns/elem", "gc-cycles", "gc/Melem", "B/op", "allocs/op"}
					if api == apiBulk {
						metrics = append(metrics, "elem/op")
					}
					manifest.Rows = append(
						manifest.Rows,
						comparisonManifestRow{
							benchmarkManifestRow: row, ExplicitOptions: slices.Clone(row.RingOptions),
							Defaults: scenario.defaults,
							Metrics:  metrics,
							Layout:   comparisonQueueLayout(exemplar, "separately allocated layout exemplar"),
						},
					)
				}
			}
		}
	}
	return manifest, nil
}

func checkComparisonLayout(
	t *testing.T,
	layout comparisonLayout,
	target any,
) reflect.Type {
	t.Helper()
	require.Equal(t, "separately allocated layout exemplar", layout.Source)
	concrete := reflect.ValueOf(target).Elem().FieldByName("ring").Type().Elem()
	require.Equal(t, concrete.String(), layout.Type)
	require.Equal(t, concrete.Size(), layout.Size)
	require.Equal(t, concrete.Align(), layout.Alignment)
	require.Less(t, layout.BaseModulo64, uintptr(64))
	require.Less(t, layout.BaseModulo128, uintptr(128))
	require.Equal(t, layout.BaseModulo64, layout.BaseModulo128%64)
	var inspect func(reflect.Type, []comparisonField)
	inspect = func(kind reflect.Type, fields []comparisonField) {
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
	return concrete
}

func checkComparisonRow(
	t *testing.T,
	row comparisonManifestRow,
	implementation string,
	producers, consumers int,
	options []string,
) ringAPI {
	t.Helper()
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
	require.Equal(t, producers, *row.Producers)
	require.Equal(t, consumers, *row.Consumers)
	require.Zero(t, row.Callers)
	require.Equal(t, producers+consumers, row.Participants)
	require.Equal(t, 1, row.QueueCount)
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
	return api
}

func comparisonRowQueue(
	t *testing.T,
	row comparisonManifestRow,
	implementation string,
	api ringAPI,
	newInt64Queue func(string, ringAPI) (queue[int64], error),
	newPtrQueue func(string, ringAPI) (queue[*item], error),
) any {
	t.Helper()
	var actual any
	if strings.HasSuffix(row.Name, "/elem=ptr") {
		require.Equal(t, "ptr", row.Element)
		require.NotNil(t, row.PointerPoolSize)
		require.Equal(t, 1024, *row.PointerPoolSize)
		require.Empty(t, row.PointerPoolReason)
		require.Equal(t, "typed *item pointing into a rotating pool of 1024 items", row.Storage)
		created, err := newPtrQueue(implementation, api)
		require.NoError(t, err)
		actual = created
	} else {
		require.Equal(t, "int64", row.Element)
		require.Nil(t, row.PointerPoolSize)
		require.Equal(t, "int64 values do not use a pointer pool", row.PointerPoolReason)
		require.Equal(t, "typed int64", row.Storage)
		created, err := newInt64Queue(implementation, api)
		require.NoError(t, err)
		actual = created
	}
	return actual
}

func checkComparisonAccounting(
	t *testing.T,
	specialized string,
	producers, consumers int,
	requestedTotals []int,
	newInt64Queue func(string, ringAPI) (queue[int64], error),
	newPtrQueue func(string, ringAPI) (queue[*item], error),
) {
	t.Helper()
	for _, implementation := range []string{"ring", specialized} {
		for _, api := range []ringAPI{apiSingle, apiBulk, apiBurst} {
			for _, batch := range []int{1, 32} {
				if api == apiSingle && batch != 1 {
					continue
				}
				for _, requested := range requestedTotals {
					name := fmt.Sprintf(
						"impl=%s/api=%s/batch=%d/total=%d",
						implementation,
						api,
						batch,
						requested,
					)
					expectedTotal := requested
					if api == apiBulk {
						multiple := batch * producers
						expectedTotal = (requested + multiple - 1) / multiple * multiple
					}
					total := api.total(requested, batch, producers)
					t.Run(name+"/elem=int64", func(t *testing.T) {
						require.Equal(t, expectedTotal, total)
						recorders := runComparisonAccounting(
							t,
							implementation,
							api,
							producers,
							consumers,
							batch,
							total,
							fillInt64,
							newInt64Queue,
						)
						if producers > 1 {
							require.NoError(t, accountingError(recorders, producers, expectedTotal))
						}
						expected := make(map[int64]int)
						for producer := range producers {
							for sequence := range producerShare(expectedTotal, producers, producer) {
								expected[packInt64(producer, int64(sequence))]++
							}
						}
						actual := make(map[int64]int)
						for _, recorder := range recorders {
							for _, value := range recorder.items {
								if producers == 1 {
									producer, sequence := unpackInt64(value)
									require.Zero(t, producer)
									require.Equal(t, sequence, value)
								}
								actual[value]++
							}
						}
						require.Equal(t, expected, actual)
					})
					t.Run(name+"/elem=ptr", func(t *testing.T) {
						require.Equal(t, expectedTotal, total)
						pool := newItemPool()
						require.Len(t, pool, 1024)
						recorders := runComparisonAccounting(
							t,
							implementation,
							api,
							producers,
							consumers,
							batch,
							total,
							fillPtr(pool),
							newPtrQueue,
						)
						expected := make(map[*item]int)
						for producer := range producers {
							for sequence := range producerShare(expectedTotal, producers, producer) {
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

func runComparisonAccounting[T any](
	t *testing.T,
	implementation string,
	api ringAPI,
	producers, consumers, batch, total int,
	fill fillFunc[T],
	newQueue func(string, ringAPI) (queue[T], error),
) []*recordSink[T] {
	t.Helper()
	recorders := make([]*recordSink[T], consumers)
	result := runWithin(t, t.Name(), func() (throughputResult, error) {
		return runThroughput(
			noopTimer{},
			func() (queue[T], error) {
				return newQueue(implementation, api)
			},
			producers,
			consumers,
			batch,
			total,
			fill,
			func(consumer int) *recordSink[T] {
				recorders[consumer] = &recordSink[T]{}
				return recorders[consumer]
			},
		)
	})
	require.Equal(t, total, result.received)
	return recorders
}
