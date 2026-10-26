package dpdk_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
)

func buildMPMCComparisonManifest() (comparisonManifest, error) {
	return buildComparisonManifest(
		mpmcComparison,
		newMPMCComparisonQueue[int64],
		newMPMCComparisonQueue[*item],
	)
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

func checkMPMCComparisonRow(t *testing.T, row comparisonManifestRow) {
	t.Helper()
	require.Equal(t, "MPMCComparison", row.Family)
	implementation := "ring"
	if strings.Contains(row.Name, "/impl=mpmc/") {
		implementation = "mpmc"
	}
	options := []string{}
	if implementation == "ring" {
		options = []string{"WithMultiProdEnqueue", "WithMultiConsDequeue"}
	}
	api := checkComparisonRow(t, row, implementation, 4, 4, options)
	require.Equal(t, comparisonDefaults{
		MaxEnqueueBatch: 1024, MaxDequeueBatch: 1024, ClearOnDequeue: true,
		ProducerAdmission: "shared cached counts", ConsumerAdmission: "shared cached counts",
		ProducerPublication: "ordered", ConsumerRelease: "ordered", SpinsBeforeYield: 8192,
	}, row.Defaults)
	actual := comparisonRowQueue(
		t,
		row,
		implementation,
		api,
		newMPMCComparisonQueue[int64],
		newMPMCComparisonQueue[*item],
	)
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
	layout comparisonLayout,
	target any,
	implementation string,
) {
	t.Helper()
	concrete := checkComparisonLayout(t, layout, target)
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
	require.Equal(t, "dpdk."+queueType+"["+argument+"]", layout.Type)
	require.Zero(t, layout.BaseModulo128%uintptr(concrete.Align()))

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
	checkComparisonAccounting(
		t,
		"mpmc",
		4,
		4,
		[]int{0, 1, 3, 4, 31, 32, 33, 127, 128, 129, 10007},
		newMPMCComparisonQueue[int64],
		newMPMCComparisonQueue[*item],
	)
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
				require.Same(t, dpdk.ErrEmpty, err)
				dst := []int64{-1, -2, -3, -4}
				_, err = raw.DequeueBulk(dst)
				require.Same(t, dpdk.ErrEmpty, err)
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
				require.Same(t, dpdk.ErrFull, raw.Enqueue(9))
				_, err = raw.EnqueueBulk([]int64{9})
				require.Same(t, dpdk.ErrFull, err)
				_, err = raw.DequeueBulk(make([]int64, 1024))
				require.NoError(t, err)
				_, err = raw.Dequeue()
				require.Same(t, dpdk.ErrEmpty, err)
				oversized := make([]int64, 1025)
				_, err = raw.EnqueueBulk(oversized)
				require.ErrorIs(t, err, dpdk.ErrExceedsCapacity)
				enqueueError := err
				_, err = raw.DequeueBulk(oversized)
				require.ErrorIs(t, err, dpdk.ErrExceedsCapacity)
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
						movedOrPanic(unexpected, dpdk.ErrFull, 1)
					})
					require.PanicsWithValue(t, unexpected, func() {
						movedOrPanic(unexpected, dpdk.ErrEmpty, 1)
					})
				}
			})
		}
		t.Run(implementation+"/directionalLimits", func(t *testing.T) {
			var target queue[int64]
			if implementation == "ring" {
				created, err := dpdk.New[int64](
					1024,
					dpdk.WithMultiProdEnqueue(),
					dpdk.WithMultiConsDequeue(),
					dpdk.WithMaxEnqueueBatch(2),
					dpdk.WithMaxDequeueBatch(2),
				)
				require.NoError(t, err)
				target = &ringBulkQueue[int64]{ring: created}
			} else {
				created, err := dpdk.NewMPMCQueue[int64](
					1024,
					dpdk.WithMaxEnqueueBatch(2),
					dpdk.WithMaxDequeueBatch(2),
				)
				require.NoError(t, err)
				target = &mpmcBulkQueue[int64]{ring: created}
			}
			raw := mpmcComparisonRaw(target)
			oversized := []int64{1, 2, 3}
			_, err := raw.EnqueueBulk(oversized)
			require.ErrorIs(t, err, dpdk.ErrExceedsMaxBatch)
			require.PanicsWithError(t, err.Error(), func() { target.enqueue(oversized) })
			dst := []int64{-1, -2, -3}
			_, err = raw.DequeueBulk(dst)
			require.ErrorIs(t, err, dpdk.ErrExceedsMaxBatch)
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
