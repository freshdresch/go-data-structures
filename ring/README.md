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

The specialized types are `SPSCQueue`, `MPSCQueue`, `SPMCQueue`, and `MPMCQueue`, constructed
with `NewSPSCQueue`, `NewMPSCQueue`, `NewSPMCQueue`, and `NewMPMCQueue`. Prefer a specialized
queue for a known topology because its API expresses ownership. Performance depends on the
workload. `Ring` remains the generalized comparison reference.

All constructors accept the shared `Option` family. This directly replaces the old
`SPSC`, `MPSC`, and `SPMC` names, their constructors, and their prefixed option families.
Migrate calls to the `Queue` names and common options, such as `WithMaxBatch` and
`WithoutDequeueClearing`. There are no compatibility aliases. Historical results retain
the API names used when they were captured.

```go
queue, err := ring.NewMPSCQueue[int](1024,
    ring.WithMaxBatch(32),
    ring.WithRelaxedMultiProdEnqueue(),
    ring.WithMaxProducerHeadTailDistance(128),
    ring.WithSpinsBeforeYield(0),
)
if err != nil {
    return err
}
_ = queue
```

`MPSCQueue` producers and `SPMCQueue` consumers default to ordered publication. Select
relaxed tail synchronization (RTS) with `WithRelaxedMultiProdEnqueue` or
`WithRelaxedMultiConsDequeue` on the corresponding concurrent side. `MPMCQueue` defaults
to ordered publication on both sides and accepts either selector independently, supporting
ordered/ordered, RTS/ordered, ordered/RTS, and RTS/RTS producer/consumer pairs. Compatible
ordered selectors are also accepted. `SPSCQueue` rejects all multi-side selectors, explicit
spins, and explicit distances. `MPSCQueue` rejects consumer multi modes and distances.
`SPMCQueue` rejects producer multi modes and distances. Batch limits, clearing policy,
and exact batch counts apply to every queue. Options apply in order, and constructors
validate only final values. A distance option never selects RTS.

`WithMaxProducerHeadTailDistance` and `WithMaxConsumerHeadTailDistance` configure their
corresponding RTS sides on `Ring` and specialized queues. Distances count element
positions, not reservations, bytes, or goroutines. Valid values are
`[0, math.MaxUint32]`, limited to values representable as `int` on the target platform.
Omission uses `count / 8`, rounded down, which is zero below capacity eight. Explicit
zero allows the next reservation only after earlier reservations finish publication.
A limit at or above capacity effectively removes distance throttling under normal
index invariants. Capacity admission still applies. Large limits can postpone RTS
publication indefinitely under overlapping operations and guarantee neither speed nor
completion.

Before every RTS reservation attempt, including retries, the side waits while head
minus tail exceeds the configured limit. Equality admits an attempt. Its own batch can
cross the limit by its length. The wait precedes admission, so full, empty, and valid
zero-length calls can wait. Bulk requests rejected for length return before that wait.
Failed admission and zero-length calls create no reservation. `WithSpinsBeforeYield`
controls tight spinning before yielding, with a default of 8192. Waits and CAS retries
have no fixed bound and provide no timeout or cancellation.

A later RTS MPSC or MPMC enqueue can return while an earlier reservation holds back visibility
of both intervals. A later RTS SPMC or MPMC dequeue can return while an earlier reservation
holds back slot reuse. `Count` can include values already returned by a dequeue. FIFO
follows reservation order rather than completion order. Batch counts describe bounded
admission snapshots, including with `WithExactBatchCounts`, and must not drive parking
or wakeup decisions. The specialized queues promise neither wait-freedom nor formal
lock-freedom. Platform 64-bit atomics may use locks.

Constructors return nil on validation errors. Invalid capacities wrap `ErrInvalidCount`
before options run. Invalid batches wrap `ErrInvalidMaxBatch`, and invalid spin numbers
wrap `ErrInvalidSpinsBeforeYield`. Incompatible final topology options and explicit
SPSC spins wrap `ErrUnsupportedOption`. Invalid distance numbers wrap
`ErrInvalidMaxHeadTailDistance`. Valid distances on serialized specialized sides wrap
`ErrUnsupportedOption`. Distances on ordered specialized sides or any non-RTS `Ring`
side wrap `ErrHeadTailDistanceRequiresRelaxed`. Use `errors.Is` to inspect these errors.
Validation checks enqueue batch, dequeue batch, spin number, producer mode, consumer
mode, explicit SPSC spin, producer distance, then consumer distance. Each distance's
numeric check precedes its applicability check.

`MPMCQueue` uses five groups separated by four full 128-byte gaps. Its Go object size
is 632 bytes on the native 64-bit ABI and 616 bytes on 386, with 8-byte alignment on both.
These sizes exclude backing storage and allocator overhead.

## MPSC performance

The specialized `MPSCQueue` is retained with a known pointer batch-32 throughput regression against
`Ring` configured with `WithMultiProdEnqueue`. The initial four-producer, one-consumer comparison
measured 4.6–6.3% longer times for pointer bulk and burst batches of 32. Follow-up experiments
reproduced the burst penalty, while bulk comparisons varied across binaries. The cause remains
unresolved, and further investigation is deferred.

The [matched comparison](../benchmark/results/2026-10-24-mpsc-ring/README.md) and
[diagnostic results](../benchmark/results/2026-10-24-mpsc-pointer32-alignment/README.md) preserve
the measurements and their limits.

## MPMC performance

The [matched ordered 4P/4C comparison](../benchmark/results/2026-10-24-mpmc-ring/README.md)
measured 9.5–14.7% lower time per element for `MPMCQueue` than generalized `Ring` across
single, bulk-1/32 and burst-1/32 for int64 and pointers in both interleaved seeds.
All 20 individual comparison intervals excluded zero. These results apply to the recorded
machine and workload. RTS performance was not measured.
