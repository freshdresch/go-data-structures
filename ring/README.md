# Ring queues

Two packages provide fixed-capacity FIFO queues. They differ in how concurrent callers on one side
publish their progress to the other side.

| | [`ring/dpdk`](dpdk/README.md) | [`ring/frontier`](#frontier) |
|---|---|---|
| Model | DPDK's `rte_ring` | Concurrent callers publish completed ranges independently |
| Concurrent callers | Publish in reservation order, or with RTS | Never wait for one another |
| Returned call | Effect visible (ordered mode) | Visible once earlier reservations finish |
| Topologies | `SPSCQueue`, `SPMCQueue`, `MPSCQueue`, `MPMCQueue` | `SPMCQueue`, `MPSCQueue`, `MPMCQueue` |
| Options | Batches, clearing, exact counts, RTS, spins, distances | Batches, clearing, counts |

`ring/frontier` has no SPSC queue. With one caller on each side there is nothing to wait for,
so `dpdk.SPSCQueue` is already the right choice.

The errors both packages share are defined in package `ring`, such as `ring.ErrFull` and
`ring.ErrEmpty`. Each package re-exports them under the same names, so `errors.Is` matches them
whichever package returned one. Options are per package. `ring/frontier` has no RTS, spin, or
head-tail distance options, because it has no publication wait for them to configure.

Choose `ring/dpdk` when a returned call must mean its effect is visible to the other side, for
example when a producer enqueues and then signals another goroutine that the value is there.
Choose `ring/frontier` for throughput on a concurrent side. On an AMD Ryzen 7 5800X, single-element
transfers with four producers and one consumer measured about 31 to 34 ns per element through
`frontier.MPSCQueue` and about 88 ns through ordered `dpdk.MPSCQueue`. With one producer and four
consumers they measured about 32 to 34 ns through `frontier.SPMCQueue` and about 90 ns through
ordered `dpdk.SPMCQueue`. Burst batches of 32 measured about 1.7 to 1.9 ns for `int64` in both
topologies, against 2.7 and 3.0 ns.

## Frontier

```go
queue, err := frontier.NewMPSCQueue[int](1024)
if err != nil {
	return err
}
// Any number of producers may call Enqueue, EnqueueBulk, and EnqueueBurst concurrently.
if err := queue.Enqueue(42); errors.Is(err, frontier.ErrFull) {
	// Retry later.
}
// One consumer at a time calls Dequeue, DequeueBulk, and DequeueBurst.
value, err := queue.Dequeue()
```

In `MPSCQueue`, each producer reserves an interval with one compare-and-swap, copies into it, and
records where it ends. The consumer follows those records from its frontier and sees every value up
to the first reservation that has not finished. FIFO follows reservation order. A producer stalled
between its reservation and its record holds back every value reserved after it, so a returned
`Enqueue` does not guarantee its value is visible. `MPSCQueue.Count` reports values from the
consumer's released position through that contiguous completed prefix, and keeps the furthest
frontier any call has proven, so repeated calls on a stable backlog stay cheap. The first call
after many new single-element enqueues still reads one record per enqueue. `Count`, `Free`, `Full`,
and `Empty` may run alongside both sides in every frontier queue.

`SPMCQueue` mirrors this for consumers. Each consumer reserves with one compare-and-swap, copies
out, and records the end of what it released, and the producer reuses slots up to the first
reservation that has not been released. A consumer stalled mid-dequeue holds back reuse of every
slot released after it. A consumer that loses its compare-and-swap to another consumer calls
`runtime.Gosched` before retrying, which took four-consumer single-element transfers from about 47
to 33 ns. Retrying at once kept every losing consumer contending for the head's cache line.
`SPMCQueue.Count` counts published values that no consumer has reserved yet, the backlog consumers
can still claim, in two loads.

`MPMCQueue` applies both mechanisms. Producers record completions and consumers record
releases. Neither side has a single owner, so the completed and released frontiers are shared
atomics that any caller advances with a compare-and-swap once it has followed the records past
them. `MPMCQueue.Count` counts published values that no consumer has reserved, as
`SPMCQueue.Count` does, and it can advance the completed frontier itself. Dequeue calls can
advance the completed frontier and enqueue calls the released frontier, including zero-length
and invalid-length calls.

Ownership follows `ring/dpdk`: synchronize every handoff of a serialized role, and give `Reset`
exclusive access against every operation and observer. Positions are 64-bit and must not overflow
between construction or `Reset`, which takes centuries at these rates.

### Clearing against a full ring

Clearing costs throughput when producers keep the ring full. The consumer then sets the transfer
rate, and clearing adds a store to every consumed slot before its release. Four producers moving
Bulk batches of 32 pointers through a full 1024-slot `frontier.MPSCQueue` measured about 2.9 ns per
element with clearing and 2.1 ns without it, which matches ordered `dpdk.MPSCQueue`. Pointer Burst
batches of 32 went from 2.0 to 1.6 ns, and `int64` elements changed by about 5%. These were
measured on an AMD Ryzen 7 5800X under WSL2 with Go 1.27.3.

For pointer elements against a full ring, consider `WithoutDequeueClearing`. Its retention cost
matters mostly once the queue drains, because producers overwrite drained slots almost immediately
while the ring stays full.
