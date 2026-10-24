# ring

The `ring` package is a fixed-size, lock-free ring buffer modeled on DPDK's `rte_ring`. Its
documentation is in the package's doc comments.

[`benchmark/README.md`](../benchmark/README.md) compares the ring with other Go queues.

Each single side permits one operation at a time across all its methods, including failed and
zero-length calls. Owners may change goroutines, but callers must synchronize every handoff
with a happens-before edge, such as a mutex or channel. Opposite-side operations and `Count`,
`Free`, `Full`, and `Empty` may run concurrently.

`Reset` requires exclusive access. Callers must synchronize completion of all prior use and
publication of the reset ring to future users. Reset clears every slot, including when
`WithoutDequeueClearing` is set, and zeroes all positions and caches.

## MPSC performance

The specialized `MPSC` is retained with a known pointer batch-32 throughput regression against
`Ring` configured with `WithMultiProdEnqueue`. The initial four-producer, one-consumer comparison
measured 4.6–6.3% longer times for pointer bulk and burst batches of 32. Follow-up experiments
reproduced the burst penalty, while bulk comparisons varied across binaries. The cause remains
unresolved, and further investigation is deferred.

The [matched comparison](../benchmark/results/2026-10-24-mpsc-ring/README.md) and
[diagnostic results](../benchmark/results/2026-10-24-mpsc-pointer32-alignment/README.md) preserve
the measurements and their limits.
