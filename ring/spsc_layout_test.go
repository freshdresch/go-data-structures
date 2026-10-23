package ring

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestSPSCLayout(t *testing.T) {
	checkSPSCLayout[*int](t)
	checkSPSCLayout[[512]byte](t)
}

func checkSPSCLayout[T any](t *testing.T) {
	t.Helper()
	producer := reflect.TypeOf(spscProducer[T]{})
	consumer := reflect.TypeOf(spscConsumer[T]{})
	outer := reflect.TypeOf(SPSC[T]{})
	t.Logf(
		"GOARCH=%s pointer=%d producer=%d/%d consumer=%d/%d outer=%d/%d",
		runtime.GOARCH,
		unsafe.Sizeof(uintptr(0)),
		producer.Size(),
		producer.Align(),
		consumer.Size(),
		consumer.Align(),
		outer.Size(),
		outer.Align(),
	)
	for _, block := range []reflect.Type{producer, consumer, outer} {
		for index := range block.NumField() {
			field := block.Field(index)
			t.Logf(
				"%s.%s offset=%d size=%d alignment=%d",
				block.Name(), field.Name, field.Offset, field.Type.Size(), field.Type.Align(),
			)
		}
	}
	checkSPSCFieldOrder[T](t, producer, consumer, outer)
	for _, block := range []reflect.Type{producer, consumer} {
		cached := block.Field(3)
		padding := block.Field(4)
		publication := block.Field(5)
		prefixWidth := cached.Offset + cached.Type.Size()
		if padding.Offset != prefixWidth {
			t.Fatalf("%s padding does not directly follow private prefix", block)
		}
		if publication.Offset != padding.Offset+padding.Type.Size() {
			t.Fatalf("%s publication does not directly follow full padding", block)
		}
		ownerOffset := outer.Field(0).Offset
		if block == consumer {
			ownerOffset = outer.Field(2).Offset
		}
		checkSPSCLineSeparation(
			t,
			block.String()+" private prefix/publication",
			ownerOffset,
			prefixWidth,
			ownerOffset+publication.Offset,
			publication.Type.Size(),
		)
	}
	gap := outer.Field(1)
	if gap.Offset != producer.Size() || outer.Field(2).Offset != gap.Offset+gap.Type.Size() {
		t.Fatal("full gap does not directly separate producer and consumer")
	}
	checkSPSCLineSeparation(
		t,
		"producer/consumer blocks",
		outer.Field(0).Offset,
		producer.Size(),
		outer.Field(2).Offset,
		consumer.Size(),
	)
	// Inspect the actual target header without constructing an aliased slice representation.
	entries := make([]T, 2, 4)
	header := *(*[3]uintptr)(unsafe.Pointer(&entries))
	if header[0] != uintptr(unsafe.Pointer(unsafe.SliceData(entries))) ||
		header[1] != 2 || header[2] != 4 {
		t.Fatal("slice header data/length/capacity positions")
	}
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Log("exact 64-bit layout checks do not apply")
		return
	}
	checkFields := func(block reflect.Type, offsets, widths, alignments []uintptr) {
		t.Helper()
		if block.NumField() != len(offsets) {
			t.Fatalf("%s fields=%d", block, block.NumField())
		}
		for index, want := range offsets {
			field := block.Field(index)
			if field.Offset != want || field.Type.Size() != widths[index] ||
				uintptr(field.Type.Align()) != alignments[index] {
				t.Fatalf(
					"%s.%s offset/size/alignment=%d/%d/%d want=%d/%d/%d",
					block, field.Name, field.Offset, field.Type.Size(), field.Type.Align(),
					want, widths[index], alignments[index],
				)
			}
		}
	}
	checkFields(
		producer,
		[]uintptr{0, 24, 28, 32, 36, 164, 168, 172},
		[]uintptr{24, 4, 4, 4, 128, 4, 4, 1},
		[]uintptr{8, 4, 4, 4, 1, 4, 4, 1},
	)
	checkFields(
		consumer,
		[]uintptr{0, 24, 28, 32, 36, 164, 168, 172, 176},
		[]uintptr{24, 4, 4, 4, 128, 4, 1, 4, 1},
		[]uintptr{8, 4, 4, 4, 1, 4, 1, 4, 1},
	)
	checkFields(
		outer,
		[]uintptr{0, 176, 304},
		[]uintptr{176, 128, 184},
		[]uintptr{8, 1, 8},
	)
	if producer.Size() != 176 || consumer.Size() != 184 || outer.Size() != 488 ||
		producer.Align() != 8 || consumer.Align() != 8 || outer.Align() != 8 {
		t.Fatal("block size or alignment")
	}
	for _, block := range []reflect.Type{producer, consumer} {
		cached := block.Field(3)
		if cached.Offset+cached.Type.Size() != 36 {
			t.Fatalf("%s private prefix width is not 36", block)
		}
	}
	checkSPSCFieldPhases(t, producer, 0, 0, []uintptr{0, 24, 28, 32, 36, 40, 44})
	checkSPSCFieldPhases(t, consumer, 304, 176, []uintptr{0, 24, 28, 32, 36, 40, 44, 48})
	producerLast := producer.Field(producer.NumField() - 1)
	consumerLast := consumer.Field(consumer.NumField() - 1)
	if producerLast.Offset+producerLast.Type.Size() != 173 ||
		consumerLast.Offset+consumerLast.Type.Size() != 177 {
		t.Fatal("occupied width")
	}
}

func checkSPSCFieldOrder[T any](t *testing.T, producer, consumer, outer reflect.Type) {
	t.Helper()
	uint32Type := reflect.TypeOf(uint32(0))
	boolType := reflect.TypeOf(false)
	entriesType := reflect.TypeOf([]T(nil))
	paddingType := reflect.TypeOf([128]byte{})
	publicationType := reflect.TypeOf(atomic.Uint32{})
	check := func(block reflect.Type, names []string, types []reflect.Type) {
		t.Helper()
		if block.NumField() != len(names) {
			t.Fatalf("%s fields=%d want=%d with publication padding", block, block.NumField(), len(names))
		}
		for index, name := range names {
			field := block.Field(index)
			if field.Name != name || field.Type != types[index] {
				t.Fatalf(
					"%s field %d name/type=%s/%s want=%s/%s",
					block, index, field.Name, field.Type, name, types[index],
				)
			}
		}
	}
	check(
		producer,
		[]string{"entries", "mask", "cursor", "cachedConsumer", "_", "published",
			"maxBatch", "exactBatchCounts"},
		[]reflect.Type{entriesType, uint32Type, uint32Type, uint32Type, paddingType,
			publicationType, uint32Type, boolType},
	)
	check(
		consumer,
		[]string{"entries", "mask", "cursor", "cachedProducer", "_", "published",
			"clearOnDequeue", "maxBatch", "exactBatchCounts"},
		[]reflect.Type{entriesType, uint32Type, uint32Type, uint32Type, paddingType,
			publicationType, boolType, uint32Type, boolType},
	)
	check(
		outer,
		[]string{"producer", "_", "consumer"},
		[]reflect.Type{producer, paddingType, consumer},
	)
}

func checkSPSCLineSeparation(
	t *testing.T,
	name string,
	firstOffset, firstWidth, secondOffset, secondWidth uintptr,
) {
	t.Helper()
	// Enumerating virtual base phases proves separation without assuming allocator alignment.
	for _, lineWidth := range []uintptr{64, 128} {
		for phase := uintptr(0); phase < 128; phase++ {
			firstStart := (phase + firstOffset) / lineWidth
			firstEnd := (phase + firstOffset + firstWidth - 1) / lineWidth
			secondStart := (phase + secondOffset) / lineWidth
			secondEnd := (phase + secondOffset + secondWidth - 1) / lineWidth
			if firstStart <= secondEnd && secondStart <= firstEnd {
				t.Fatalf(
					"%s shares virtual line: width=%d phase=%d intervals=[%d,%d]/[%d,%d]",
					name, lineWidth, phase, firstStart, firstEnd, secondStart, secondEnd,
				)
			}
		}
	}
}

func checkSPSCFieldPhases(
	t *testing.T,
	block reflect.Type,
	ownerOffset, originalOwnerOffset uintptr,
	originalOffsets []uintptr,
) {
	t.Helper()
	originalIndex := 0
	for index := range block.NumField() {
		field := block.Field(index)
		if field.Name == "_" {
			continue
		}
		originalOffset := originalOffsets[originalIndex]
		if field.Offset%128 != originalOffset%128 ||
			(ownerOffset+field.Offset)%128 != (originalOwnerOffset+originalOffset)%128 {
			t.Fatalf("%s.%s changed original relative or absolute phase", block, field.Name)
		}
		originalIndex++
	}
}
