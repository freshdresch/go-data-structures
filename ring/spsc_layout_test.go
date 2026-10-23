package ring

import (
	"reflect"
	"runtime"
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
	t.Logf("GOARCH=%s pointer=%d producer=%d/%d consumer=%d/%d outer=%d/%d",
		runtime.GOARCH, unsafe.Sizeof(uintptr(0)), producer.Size(), producer.Align(),
		consumer.Size(), consumer.Align(), outer.Size(), outer.Align())
	for _, block := range []reflect.Type{producer, consumer, outer} {
		for index := range block.NumField() {
			field := block.Field(index)
			t.Logf("%s.%s offset=%d size=%d alignment=%d", block.Name(), field.Name,
				field.Offset, field.Type.Size(), field.Type.Align())
		}
	}
	gap := outer.Field(1)
	if gap.Type.Size() != 128 || gap.Offset != producer.Size() ||
		outer.Field(2).Offset-gap.Offset != 128 {
		t.Fatal("full gap does not follow producer")
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
				t.Fatalf("%s.%s offset/size/alignment=%d/%d/%d want=%d/%d/%d",
					block, field.Name, field.Offset, field.Type.Size(), field.Type.Align(),
					want, widths[index], alignments[index])
			}
		}
	}
	checkFields(producer,
		[]uintptr{0, 24, 28, 32, 36, 40, 44},
		[]uintptr{24, 4, 4, 4, 4, 4, 1},
		[]uintptr{8, 4, 4, 4, 4, 4, 1})
	checkFields(consumer,
		[]uintptr{0, 24, 28, 32, 36, 40, 44, 48},
		[]uintptr{24, 4, 4, 4, 4, 1, 4, 1},
		[]uintptr{8, 4, 4, 4, 4, 1, 4, 1})
	checkFields(outer, []uintptr{0, 48, 176}, []uintptr{48, 128, 56}, []uintptr{8, 1, 8})
	if producer.Size() != 48 || consumer.Size() != 56 || outer.Size() != 232 ||
		producer.Align() != 8 || consumer.Align() != 8 || outer.Align() != 8 {
		t.Fatal("block size or alignment")
	}
	producerLast := producer.Field(producer.NumField() - 1)
	consumerLast := consumer.Field(consumer.NumField() - 1)
	if producerLast.Offset+producerLast.Type.Size() != 45 ||
		consumerLast.Offset+consumerLast.Type.Size() != 49 {
		t.Fatal("occupied width")
	}
	// Inspect the actual target header without constructing an aliased slice representation.
	entries := make([]T, 2, 4)
	header := *(*[3]uintptr)(unsafe.Pointer(&entries))
	if header[0] != uintptr(unsafe.Pointer(unsafe.SliceData(entries))) ||
		header[1] != 2 || header[2] != 4 {
		t.Fatal("slice header data/length/capacity positions")
	}
}
