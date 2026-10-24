package ring

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestMPSCLayout(t *testing.T) {
	checkMPSCLayout[*int](t)
	checkMPSCLayout[[512]byte](t)
}

func checkMPSCLayout[T any](t *testing.T) {
	t.Helper()
	producer := reflect.TypeOf(mpscProducerConfig[T]{})
	consumer := reflect.TypeOf(mpscConsumer[T]{})
	reservation := reflect.TypeOf(mpscProducerReservation{})
	outer := reflect.TypeOf(MPSC[T]{})
	entriesType := reflect.TypeOf([]T(nil))
	uint32Type := reflect.TypeOf(uint32(0))
	boolType := reflect.TypeOf(false)
	atomicType := reflect.TypeOf(atomic.Uint32{})
	gapType := reflect.TypeOf([cacheLine]byte{})
	if cacheLine != 128 || gapType.Size() != 128 {
		t.Fatal("cacheLine must preserve all four full 128-byte gaps")
	}
	checkMPSCBlockLayout(
		t,
		producer,
		[]string{"entries", "mask", "maxBatch", "spinsBeforeYield", "exactBatchCounts"},
		[]reflect.Type{entriesType, uint32Type, uint32Type, uint32Type, boolType},
	)
	checkMPSCBlockLayout(
		t,
		consumer,
		[]string{"entries", "mask", "cursor", "cachedProducer", "clearOnDequeue",
			"maxBatch", "exactBatchCounts"},
		[]reflect.Type{entriesType, uint32Type, uint32Type, uint32Type, boolType,
			uint32Type, boolType},
	)
	checkMPSCBlockLayout(
		t,
		reservation,
		[]string{"head", "cachedConsumer"},
		[]reflect.Type{atomicType, atomicType},
	)
	checkMPSCBlockLayout(
		t,
		outer,
		[]string{"producerConfig", "_", "producerReservation", "_", "producerPublished", "_",
			"consumer", "_", "consumerPublished"},
		[]reflect.Type{producer, gapType, reservation, gapType, atomicType, gapType,
			consumer, gapType, atomicType},
	)
	t.Logf(
		"GOARCH=%s pointer=%d slice=%d/%d atomic=%d/%d",
		runtime.GOARCH, unsafe.Sizeof(uintptr(0)), entriesType.Size(), entriesType.Align(),
		atomicType.Size(), atomicType.Align(),
	)
	groups := []int{0, 2, 4, 6, 8}
	for firstIndex, firstField := range groups {
		for _, secondField := range groups[firstIndex+1:] {
			checkMPSCGroupSeparation(t, outer.Field(firstField), outer.Field(secondField))
		}
	}
	queue := newMPSCForTest[T](t, 8)
	producerEntries := queue.producerConfig.entries
	consumerEntries := queue.consumer.entries
	if unsafe.SliceData(producerEntries) != unsafe.SliceData(consumerEntries) ||
		len(producerEntries) != 8 || cap(producerEntries) != 8 ||
		len(consumerEntries) != 8 || cap(consumerEntries) != 8 {
		t.Fatal("entry headers must describe the same eight-slot backing array")
	}
	if queue.producerConfig.mask != 7 || queue.consumer.mask != 7 {
		t.Fatal("entry headers must have matching capacity masks")
	}
	if unsafe.Sizeof(uintptr(0)) != 8 || entriesType.Size() != 24 || entriesType.Align() != 8 ||
		atomicType.Size() != 4 || atomicType.Align() != 4 {
		t.Log("numeric 64-bit layout expectations do not apply to this target")
		return
	}
	for _, expected := range []struct {
		block     reflect.Type
		offsets   []uintptr
		size      uintptr
		alignment int
	}{
		{producer, []uintptr{0, 24, 28, 32, 36}, 40, 8},
		{consumer, []uintptr{0, 24, 28, 32, 36, 40, 44}, 48, 8},
		{reservation, []uintptr{0, 4}, 8, 4},
		{outer, []uintptr{0, 40, 168, 176, 304, 308, 440, 488, 616}, 624, 8},
	} {
		if expected.block.Size() != expected.size ||
			expected.block.Align() != expected.alignment {
			t.Fatalf("%s numeric size/alignment mismatch", expected.block)
		}
		for index, offset := range expected.offsets {
			if expected.block.Field(index).Offset != offset {
				t.Fatalf("%s field %d numeric offset mismatch", expected.block, index)
			}
		}
	}
}

func checkMPSCBlockLayout(
	t *testing.T,
	block reflect.Type,
	names []string,
	types []reflect.Type,
) {
	t.Helper()
	if block.NumField() != len(names) {
		t.Fatalf("%s fields=%d want=%d", block, block.NumField(), len(names))
	}
	var end uintptr
	alignment := uintptr(1)
	for index, name := range names {
		field := block.Field(index)
		expectedType := types[index]
		fieldAlignment := uintptr(expectedType.Align())
		offset := (end + fieldAlignment - 1) / fieldAlignment * fieldAlignment
		if field.Name != name || field.Type != expectedType || field.Offset != offset {
			t.Fatalf(
				"%s field %d name/type/offset=%s/%s/%d want=%s/%s/%d",
				block, index, field.Name, field.Type, field.Offset, name, expectedType, offset,
			)
		}
		end = offset + expectedType.Size()
		alignment = max(alignment, fieldAlignment)
		t.Logf(
			"%s.%s offset=%d size=%d alignment=%d",
			block, name, offset, expectedType.Size(), fieldAlignment,
		)
	}
	size := (end + alignment - 1) / alignment * alignment
	if block.Size() != size || uintptr(block.Align()) != alignment {
		t.Fatalf(
			"%s size/alignment=%d/%d want=%d/%d",
			block, block.Size(), block.Align(), size, alignment,
		)
	}
	t.Logf("%s size=%d alignment=%d", block, size, alignment)
}

func checkMPSCGroupSeparation(t *testing.T, first, second reflect.StructField) {
	t.Helper()
	// Complete block sizes include immutable configuration and trailing padding.
	for _, lineWidth := range []uintptr{64, 128} {
		for phase := uintptr(0); phase < 128; phase++ {
			firstStart := (phase + first.Offset) / lineWidth
			firstEnd := (phase + first.Offset + first.Type.Size() - 1) / lineWidth
			secondStart := (phase + second.Offset) / lineWidth
			secondEnd := (phase + second.Offset + second.Type.Size() - 1) / lineWidth
			if firstStart <= secondEnd && secondStart <= firstEnd {
				t.Fatalf(
					"%s/%s share lines at width=%d phase=%d: [%d,%d]/[%d,%d]",
					first.Name, second.Name, lineWidth, phase,
					firstStart, firstEnd, secondStart, secondEnd,
				)
			}
		}
	}
}
