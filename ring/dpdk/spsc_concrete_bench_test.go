package dpdk_test

import (
	"testing"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
)

// BenchmarkSPSCConcrete measures direct concrete enqueue/dequeue pairs in one goroutine.
func BenchmarkSPSCConcrete(b *testing.B) {
	b.Run("impl=ring/api=seq", func(b *testing.B) {
		b.Run("elem=int64", func(b *testing.B) {
			benchmarkRingAPISeq(b, fillInt64)
		})
		b.Run("elem=ptr", func(b *testing.B) {
			benchmarkRingAPISeq(b, fillPtr(newItemPool()))
		})
	})
	b.Run("impl=spsc/api=seq", func(b *testing.B) {
		b.Run("elem=int64", func(b *testing.B) {
			benchmarkSPSCConcreteSeq(b, fillInt64)
		})
		b.Run("elem=ptr", func(b *testing.B) {
			benchmarkSPSCConcreteSeq(b, fillPtr(newItemPool()))
		})
	})
}

func benchmarkSPSCConcreteSeq[T any](b *testing.B, fill fillFunc[T]) {
	setElemBytes[T](b)
	created, err := dpdk.NewSPSCQueue[T](ringCapacity)
	if err != nil {
		b.Fatal(err)
	}
	elems := make([]T, 1)
	fill(elems, 0, 0)
	elem := elems[0]
	iterations := 0
	gcCycles := runTimed(b, func() {
		for b.Loop() {
			if err := created.Enqueue(elem); err != nil {
				b.Fatal(err)
			}
			if _, err := created.Dequeue(); err != nil {
				b.Fatal(err)
			}
			iterations++
		}
	})
	result := newThroughputResult([]int{iterations}, gcCycles, iterations)
	reportThroughput(b, result, iterations)
}
