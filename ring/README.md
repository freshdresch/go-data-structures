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
