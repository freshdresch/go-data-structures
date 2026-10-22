package ring

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// exhaustedSpinBounds are the spin bounds BenchmarkExhaustedSpin compares.
var exhaustedSpinBounds = []int{1024, 4096, 16384, 65536}

// BenchmarkExhaustedSpin measures the CPU one waiter wastes when the goroutine it waits for has
// been descheduled, which is the cost a larger tailSpins adds. The tail never reaches want, so
// every spin runs to its bound. Both cases use a ring from New, so the tail and the writer's
// field sit on the ring's real index line. The quiet case is the lower bound, where every load
// hits the waiter's cache. The contended case is the upper bound, where another core keeps
// writing the same line. Run it at -cpu 2 or higher on a machine with at least two CPUs,
// because the contended case needs a second CPU. The larger bounds give the truest ns/load,
// because the smallest includes a fixed per-call overhead.
func BenchmarkExhaustedSpin(b *testing.B) {
	b.Run("line=quiet", func(b *testing.B) {
		for _, spins := range exhaustedSpinBounds {
			b.Run(fmt.Sprintf("spins=%d", spins), func(b *testing.B) {
				r, err := New[int](8)
				require.NoError(b, err)
				exhaustSpins(b, &r.prodTail, spins)
			})
		}
	})
	b.Run("line=contended", func(b *testing.B) {
		if runtime.GOMAXPROCS(0) < 2 || runtime.NumCPU() < 2 {
			b.Skip("the writer needs GOMAXPROCS and NumCPU of at least 2 to contend for the line")
		}
		for _, spins := range exhaustedSpinBounds {
			b.Run(fmt.Sprintf("spins=%d", spins), func(b *testing.B) {
				r, err := New[int](8)
				require.NoError(b, err)
				var stop atomic.Bool
				var writer sync.WaitGroup
				defer writer.Wait()
				defer stop.Store(true)
				writer.Go(func() {
					for count := uint32(1); !stop.Load(); count++ {
						r.prodHead.Store(count)
					}
				})
				// Timing starts only once the writer is running.
				for r.prodHead.Load() == 0 {
					runtime.Gosched()
				}
				exhaustSpins(b, &r.prodTail, spins)
			})
		}
	})
}

// exhaustSpins times spinForTail on a tail that must never equal 1, and reports ns/load.
func exhaustSpins(b *testing.B, tail *atomic.Uint32, spins int) {
	for b.Loop() {
		if spinForTail(tail, 1, spins) {
			b.Fatal("spinForTail saw a tail that was never stored")
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*spins), "ns/load")
}
