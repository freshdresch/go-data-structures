package ring

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestSPMCLayout(t *testing.T) {
	checkSPMCLayout[*int](t)
	checkSPMCLayout[[512]byte](t)
}

func checkSPMCLayout[T any](t *testing.T) {
	t.Helper()
	producer := reflect.TypeOf(spmcProducer[T]{})
	consumer := reflect.TypeOf(spmcConsumerConfig[T]{})
	reservation := reflect.TypeOf(spmcConsumerReservation{})
	publication := reflect.TypeOf(spmcConsumerPublication{})
	outer := reflect.TypeOf(SPMCQueue[T]{})
	entriesType := reflect.TypeOf([]T(nil))
	uint32Type := reflect.TypeOf(uint32(0))
	boolType := reflect.TypeOf(false)
	atomicType := reflect.TypeOf(atomic.Uint32{})
	packedType := reflect.TypeOf(atomic.Uint64{})
	gapType := reflect.TypeOf([cacheLine]byte{})
	if cacheLine != 128 || gapType.Size() != 128 {
		t.Fatal("cacheLine must preserve all four full 128-byte gaps")
	}
	checkSPMCBlockLayout(
		t,
		producer,
		[]string{"entries", "mask", "cursor", "cachedConsumer", "maxBatch", "exactBatchCounts"},
		[]reflect.Type{entriesType, uint32Type, uint32Type, uint32Type, uint32Type, boolType},
	)
	checkSPMCBlockLayout(
		t,
		consumer,
		[]string{"entries", "mask", "maxBatch", "spinsBeforeYield",
			"htdMax", "clearOnDequeue", "exactBatchCounts", "relaxed"},
		[]reflect.Type{entriesType, uint32Type, uint32Type, uint32Type,
			uint32Type, boolType, boolType, boolType},
	)
	checkSPMCBlockLayout(
		t,
		reservation,
		[]string{"headRTS", "head", "cachedProducer"},
		[]reflect.Type{packedType, atomicType, atomicType},
	)
	checkSPMCBlockLayout(
		t,
		outer,
		[]string{"producer", "_", "producerPublished", "_", "consumerConfig", "_",
			"consumerReservation", "_", "consumerPublication"},
		[]reflect.Type{producer, gapType, atomicType, gapType, consumer, gapType,
			reservation, gapType, publication},
	)
	checkSPMCBlockLayout(
		t,
		publication,
		[]string{"tailRTS", "published"},
		[]reflect.Type{packedType, atomicType},
	)
	for _, offset := range []uintptr{
		unsafe.Offsetof(SPMCQueue[T]{}.consumerReservation),
		unsafe.Offsetof(SPMCQueue[T]{}.consumerPublication),
	} {
		if offset%8 != 0 {
			t.Fatal("packed offset is not aligned", offset)
		}
	}
	t.Logf(
		"GOARCH=%s pointer=%d slice=%d/%d atomic=%d/%d",
		runtime.GOARCH, unsafe.Sizeof(uintptr(0)), entriesType.Size(), entriesType.Align(),
		atomicType.Size(), atomicType.Align(),
	)
	groups := []int{0, 2, 4, 6, 8}
	for firstIndex, firstField := range groups {
		for _, secondField := range groups[firstIndex+1:] {
			checkSPMCGroupSeparation(t, outer.Field(firstField), outer.Field(secondField))
		}
	}
	queue := newSPMCForTest[T](t, 8)
	producerEntries := queue.producer.entries
	consumerEntries := queue.consumerConfig.entries
	if unsafe.SliceData(producerEntries) != unsafe.SliceData(consumerEntries) ||
		len(producerEntries) != 8 || cap(producerEntries) != 8 ||
		len(consumerEntries) != 8 || cap(consumerEntries) != 8 {
		t.Fatal("entry headers must describe the same eight-slot backing array")
	}
	if queue.producer.mask != 7 || queue.consumerConfig.mask != 7 {
		t.Fatal("entry headers must have matching capacity masks")
	}
	if uintptr(unsafe.Pointer(&queue.consumerReservation.headRTS))%8 != 0 ||
		uintptr(unsafe.Pointer(&queue.consumerPublication.tailRTS))%8 != 0 {
		t.Fatal("allocated packed words are not aligned")
	}
	native := unsafe.Sizeof(uintptr(0)) == 8 && entriesType.Size() == 24 && entriesType.Align() == 8
	x86 := unsafe.Sizeof(uintptr(0)) == 4 && entriesType.Size() == 12 && entriesType.Align() == 4
	if (!native && !x86) || atomicType.Size() != 4 || atomicType.Align() != 4 ||
		packedType.Size() != 8 || packedType.Align() != 8 {
		return
	}
	producerOffsets := []uintptr{0, 24, 28, 32, 36, 40}
	consumerOffsets := []uintptr{0, 24, 28, 32, 36, 40, 41, 42}
	outerOffsets := []uintptr{0, 48, 176, 180, 312, 360, 488, 504, 632}
	producerSize, consumerSize, outerSize := uintptr(48), uintptr(48), uintptr(648)
	if x86 {
		producerOffsets = []uintptr{0, 12, 16, 20, 24, 28}
		consumerOffsets = []uintptr{0, 12, 16, 20, 24, 28, 29, 30}
		outerOffsets = []uintptr{0, 32, 160, 164, 292, 324, 456, 472, 600}
		producerSize, consumerSize, outerSize = 32, 32, 616
	}
	blockAlignment := producer.Align()
	for _, expected := range []struct {
		block     reflect.Type
		offsets   []uintptr
		size      uintptr
		alignment int
	}{
		{producer, producerOffsets, producerSize, blockAlignment},
		{consumer, consumerOffsets, consumerSize, blockAlignment},
		{reservation, []uintptr{0, 8, 12}, 16, 8},
		{publication, []uintptr{0, 8}, 16, 8},
		{outer, outerOffsets, outerSize, 8},
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

func checkSPMCBlockLayout(
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

func checkSPMCGroupSeparation(t *testing.T, first, second reflect.StructField) {
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
