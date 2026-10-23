// Package benchmark compares the ring with other Go queues. It is a separate module, so the
// ring's module gains no dependency on the queues it is compared with.
//
// Its harness is adapted from ring/bench_test.go and ring/bench_adapters_test.go, so a change
// there should be carried over here. Run it with scripts/ring-compare-bench.sh.
package benchmark
