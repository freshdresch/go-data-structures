package dpdk

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestMPMCLayout(t *testing.T) {
	mpmcCheckLayout[byte](t)
	mpmcCheckLayout[*int](t)
	mpmcCheckLayout[[512]byte](t)
}
func mpmcCheckLayout[T any](t *testing.T) {
	t.Helper()
	config := reflect.TypeOf(mpmcConfig[T]{})
	prodReservation := reflect.TypeOf(mpmcProducerReservation{})
	prodPublication := reflect.TypeOf(mpmcProducerPublication{})
	consReservation := reflect.TypeOf(mpmcConsumerReservation{})
	consPublication := reflect.TypeOf(mpmcConsumerPublication{})
	outer := reflect.TypeOf(MPMCQueue[T]{})
	word := reflect.TypeOf(uint32(0))
	flag := reflect.TypeOf(false)
	atomicWord := reflect.TypeOf(atomic.Uint32{})
	packed := reflect.TypeOf(atomic.Uint64{})
	gap := reflect.TypeOf([128]byte{})
	mpmcCheckBlockLayout(
		t,
		config,
		[]string{"entries", "mask", "maxEnqueueBatch", "maxDequeueBatch", "prodHTDMax",
			"consHTDMax", "spinsBeforeYield", "producerRelaxed", "consumerRelaxed",
			"clearOnDequeue", "exactBatchCounts"},
		[]reflect.Type{reflect.TypeOf([]T(nil)), word, word, word, word, word, word,
			flag, flag, flag, flag},
	)
	for _, reservation := range []reflect.Type{prodReservation, consReservation} {
		cache := "cachedConsumer"
		if reservation == consReservation {
			cache = "cachedProducer"
		}
		mpmcCheckBlockLayout(
			t,
			reservation,
			[]string{"headRTS", "head", cache},
			[]reflect.Type{packed, atomicWord, atomicWord},
		)
	}
	for _, publication := range []reflect.Type{prodPublication, consPublication} {
		mpmcCheckBlockLayout(
			t,
			publication,
			[]string{"tailRTS", "published"},
			[]reflect.Type{packed, atomicWord},
		)
	}
	mpmcCheckBlockLayout(
		t,
		outer,
		[]string{"config", "_", "producerReservation", "_", "producerPublication", "_",
			"consumerReservation", "_", "consumerPublication"},
		[]reflect.Type{config, gap, prodReservation, gap, prodPublication, gap,
			consReservation, gap, consPublication},
	)
	for _, index := range []int{1, 3, 5, 7} {
		if outer.Field(index).Type.Size() != 128 {
			t.Fatal("gap must be 128 bytes")
		}
	}
	groups := []int{0, 2, 4, 6, 8}
	for first, group := range groups {
		for _, second := range groups[first+1:] {
			mpmcCheckGroupSeparation(t, outer.Field(group), outer.Field(second))
		}
	}
	queue := mpmcNewForTest[T](t, 8)
	for _, group := range groups[1:] {
		offset := outer.Field(group).Offset
		if offset%8 != 0 || (uintptr(unsafe.Pointer(queue))+offset)%8 != 0 {
			t.Fatal("packed field offset/address must be eight-byte aligned")
		}
	}
	runtime.KeepAlive(queue)
	var offsets, configOffsets []uintptr
	var size uintptr
	switch unsafe.Sizeof(uintptr(0)) {
	case 8:
		offsets = []uintptr{0, 56, 184, 200, 328, 344, 472, 488, 616}
		configOffsets = []uintptr{0, 24, 28, 32, 36, 40, 44, 48, 49, 50, 51}
		size = 632
	case 4:
		offsets = []uintptr{0, 40, 168, 184, 312, 328, 456, 472, 600}
		configOffsets = []uintptr{0, 12, 16, 20, 24, 28, 32, 36, 37, 38, 39}
		size = 616
	default:
		return
	}
	for index, offset := range offsets {
		if outer.Field(index).Offset != offset {
			t.Fatalf("outer field %d offset=%d want=%d", index, outer.Field(index).Offset, offset)
		}
	}
	for index, offset := range configOffsets {
		if config.Field(index).Offset != offset {
			t.Fatalf("config field %d offset mismatch", index)
		}
	}
	if outer.Size() != size || outer.Align() != 8 {
		t.Fatalf("queue size/alignment=%d/%d want=%d/8", outer.Size(), outer.Align(), size)
	}
	for _, block := range []reflect.Type{
		prodReservation, prodPublication, consReservation, consPublication,
	} {
		if block.Size() != 16 || block.Align() != 8 || block.Field(1).Offset != 8 {
			t.Fatal("protocol block layout", block)
		}
	}
	t.Logf("GOARCH=%s queue size=%d alignment=%d", runtime.GOARCH, outer.Size(), outer.Align())
}
func mpmcCheckBlockLayout(
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
				block,
				index,
				field.Name,
				field.Type,
				field.Offset,
				name,
				expectedType,
				offset,
			)
		}
		end = offset + expectedType.Size()
		alignment = max(alignment, fieldAlignment)
		t.Logf(
			"%s.%s offset=%d size=%d alignment=%d",
			block,
			name,
			offset,
			expectedType.Size(),
			fieldAlignment,
		)
	}
	size := (end + alignment - 1) / alignment * alignment
	if block.Size() != size || uintptr(block.Align()) != alignment {
		t.Fatalf(
			"%s size/alignment=%d/%d want=%d/%d",
			block,
			block.Size(),
			block.Align(),
			size,
			alignment,
		)
	}
	t.Logf("%s size=%d alignment=%d", block, size, alignment)
}

func mpmcCheckGroupSeparation(t *testing.T, first, second reflect.StructField) {
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
					first.Name,
					second.Name,
					lineWidth,
					phase,
					firstStart,
					firstEnd,
					secondStart,
					secondEnd,
				)
			}
		}
	}
}
