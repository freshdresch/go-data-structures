# Ring queues

Two packages provide fixed-capacity FIFO queues with the same type names, constructors, and
methods, so switching between them is a change of import. They differ in how concurrent
producers publish what they enqueued.

| | [`ring/dpdk`](dpdk/README.md) | [`ring/frontier`](#frontier) |
|---|---|---|
| Model | DPDK's `rte_ring` | Producers publish completed ranges independently |
| Concurrent producers | Publish in reservation order, or with RTS | Never wait for one another |
| Returned `Enqueue` | Value visible (ordered mode) | Visible once earlier reservations finish |
| Topologies | `SPSCQueue`, `SPMCQueue`, `MPSCQueue`, `MPMCQueue` | `MPSCQueue` so far |
| Options | Batches, clearing, exact counts, RTS, spins, distances | Batches, clearing, counts |

The errors both packages share are defined in package `ring`, such as `ring.ErrFull` and
`ring.ErrEmpty`. Each package re-exports them under the same names, so `errors.Is` matches them
whichever package returned one. Options are per package. `ring/frontier` has no RTS, spin, or
head-tail distance options, because it has no publication wait for them to configure.

Choose `ring/dpdk` when a returned enqueue must mean its value is visible, for example when a
producer enqueues and then signals another goroutine that the value is there. Choose
`ring/frontier` for multi-producer throughput. With four producers and one consumer on an AMD
Ryzen 7 5800X, single-element transfers measured about 31 to 34 ns per element through
`frontier.MPSCQueue` and about 88 ns through ordered `dpdk.MPSCQueue`. Burst batches of 32 measured
about 1.9 ns for `int64` and 2.1 ns for pointers, against 2.5 to 2.6 ns.

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

Each producer reserves an interval with one compare-and-swap, copies into it, and records where it
ends. The consumer follows those records from its frontier and sees every value up to the first
reservation that has not finished. FIFO follows reservation order. A producer stalled between its
reservation and its record holds back every value reserved after it, so a returned `Enqueue` does
not guarantee its value is visible. `Count`, `Free`, `Full`, and `Empty` report that contiguous
completed prefix and may run alongside both sides. `Count` keeps the furthest frontier any call has
proven, so repeated calls on a stable backlog stay cheap. The first call after many new
single-element enqueues still reads one record per enqueue.

Ownership follows `ring/dpdk`: synchronize every handoff of the consumer role, and give `Reset`
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
