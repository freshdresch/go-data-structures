package ring_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/ring"
)

func buildSPMCComparisonManifest() (comparisonManifest, error) {
	return buildComparisonManifest(
		spmcComparison,
		newSPMCComparisonQueue[int64],
		newSPMCComparisonQueue[*item],
	)
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

func checkSPMCComparisonRow(t *testing.T, row comparisonManifestRow) {
	t.Helper()
	require.Equal(t, "SPMCComparison", row.Family)
	implementation := "ring"
	if strings.Contains(row.Name, "/impl=spmc/") {
		implementation = "spmc"
	}
	options := []string{}
	if implementation == "ring" {
		options = []string{"WithMultiConsDequeue"}
	}
	api := checkComparisonRow(t, row, implementation, 1, 4, options)
	require.Equal(
		t,
		comparisonDefaults{
			MaxEnqueueBatch: 1024, MaxDequeueBatch: 1024, ClearOnDequeue: true,
			ProducerAdmission: "cached counts", ConsumerAdmission: "shared cached counts",
			ConsumerRelease: "ordered", SpinsBeforeYield: 8192,
		},
		row.Defaults,
	)
	actual := comparisonRowQueue(
		t,
		row,
		implementation,
		api,
		newSPMCComparisonQueue[int64],
		newSPMCComparisonQueue[*item],
	)
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
	require.Equal(
		t,
		producer.FieldByName("entries").Pointer(),
		consumer.FieldByName("entries").Pointer(),
	)
	require.Equal(
		t,
		reflect.Uint32,
		concrete.FieldByName("consumerReservation").FieldByName("cachedProducer").
			FieldByName("v").Kind(),
	)
}

func checkSPMCComparisonLayout(
	t *testing.T,
	layout comparisonLayout,
	target any,
	implementation string,
) {
	t.Helper()
	concrete := checkComparisonLayout(t, layout, target)

	if implementation == "ring" {
		if reflect.TypeFor[uintptr]().Size() != 8 {
			return
		}
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
	size := uintptr(648)
	offsets := []uintptr{0, 176, 312, 488, 632}
	entries := concrete.Field(0).Type.Field(0).Type
	switch {
	case reflect.TypeFor[uintptr]().Size() == 8 && entries.Size() == 24 && entries.Align() == 8:
	case reflect.TypeFor[uintptr]().Size() == 4 && entries.Size() == 12 && entries.Align() == 4:
		size = 616
		offsets = []uintptr{0, 160, 292, 456, 600}
	default:
		return
	}
	require.Equal(t, size, layout.Size)
	for index, offset := range offsets {
		require.Equal(t, offset, concrete.Field(2*index).Offset)
	}
}

func TestSPMCComparisonAccounting(t *testing.T) {
	checkComparisonAccounting(
		t,
		"spmc",
		1,
		4,
		[]int{0, 1, 31, 32, 33, 10007},
		newSPMCComparisonQueue[int64],
		newSPMCComparisonQueue[*item],
	)
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
				created, err := ring.NewSPMCQueue[int64](
					1024,
					ring.WithMaxEnqueueBatch(2),
					ring.WithMaxDequeueBatch(2),
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
