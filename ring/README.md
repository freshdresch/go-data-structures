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

## Choosing a queue

`ring/frontier` is the best all-round choice for a concurrent side. It is the fastest queue
measured here whenever callers move several values per call. For single values, only the
Vyukov-style queues described below beat it.

A single-value-only workload may run faster on a Vyukov-style queue such as xsync's `MPMCQueue`.
Expect the largest gain with several producers and several consumers.

Choose `ring/dpdk` in these cases:

- A returned enqueue must mean consumers can see the value, for example when a producer enqueues
  and then signals another goroutine that the value is there.
- You need its tuning: relaxed tail sync (RTS), spin counts, head-tail distance limits, and exact
  batch counts.
- You need an SPSC queue.
- Your batches run to hundreds of values. At that size the copy dominates, and `ring/dpdk` matched
  or slightly beat `ring/frontier`.

### Vyukov-style queues

[xsync](https://github.com/puzpuzpuz/xsync)'s `MPMCQueue` follows Erik Rigtorp's `MPMCQueue`, and
[Workiva](https://github.com/Workiva/go-datastructures)'s `RingBuffer` follows Dmitry Vyukov's
bounded MPMC queue. Both use Vyukov's design. Each slot carries its own sequence word next to the
value. A caller claims a position on a shared head, then checks and sets only that slot's word.
Because readiness lives in the slot next to the value, a single-value call touches only the head
and one slot. That makes it very fast.

Batches are where the design suffers. Neither library has a batch API, so moving 32 values costs
32 full handoffs. A batch would still have to set every slot's word, because each slot carries
its own readiness word. Both rings in this repository reserve a whole batch at once and
publish it at once. A batch of 32 then costs 2 to 3 ns per value. That is 7 to 15 times less
than the fastest single-value call in each topology.

Vyukov queues are not lock-free either. A producer stalled after claiming a slot leaves that slot
unready, and the consumer assigned to it cannot proceed past it. This is close to how a stalled
`ring/frontier` caller holds back later values.

### Measurements

The table lists median ns per value over 10 rotating rounds, for `int64` and pointer values. The
machine was an AMD Ryzen 7 5800X under WSL2 with Go 1.27.3, with GOMAXPROCS 8 and one CPU per
physical core. Each queue has 1024 slots. Each concurrent side runs four goroutines.

The "single" columns call `Enqueue` and `Dequeue`. The "batch 32" columns call `EnqueueBurst`
and `DequeueBurst` with 32 values. xsync, Workiva, and `chan` move one value per call.

| Topology | frontier single | frontier batch 32 | dpdk single | dpdk batch 32 | xsync | Workiva | `chan` |
|---|---|---|---|---|---|---|---|
| SPSC | — | — | 10.7 / 11.3 | 1.4 / 2.6 | 8.9 / 9.9 | 19.7 / 10.1 | 31.6 / 31.7 |
| MPSC (4P/1C) | 31.4 / 33.4 | 1.9 / 2.0 | 87.7 / 77.3 | 2.7 / 2.0 | 29.3 / 30.7 | 35.6 / 21.8 | 43.7 / 44.0 |
| SPMC (1P/4C) | 31.7 / 31.6 | 1.7 / 2.7 | 89.4 / 88.7 | 3.0 / 3.0 | 25.0 / 26.4 | 24.9 / 18.8 | 43.9 / 43.2 |
| MPMC (4P/4C) | 30.8 / 36.8 | 2.0 / 2.6 | 79.9 / 81.0 | 3.1 / 3.0 | 18.0 / 19.7 | 30.0 / 18.4 | 49.6 / 49.6 |

The dpdk columns use ordered publication. `ring/frontier` has no SPSC queue. The SPSC paragraph
below explains why.

**Single values.** `ring/frontier` was 2 to 3 ns behind xsync in MPSC and 5 to 7 ns behind
in SPMC. In MPMC it was 13 to 17 ns behind. Workiva was faster still for pointers, but slower
than `ring/frontier` for `int64` in MPSC. `ring/frontier` was 2.2 to 2.8 times faster than
ordered `ring/dpdk`, and faster than `chan` everywhere.

**Batches of 32.** `ring/frontier` was 7 to 15 times faster per value than the fastest
single-value queue in each topology. It also beat ordered `ring/dpdk` everywhere except MPSC
pointers, where the two tied.

**SPSC.** `ring/frontier` exists so that concurrent callers on one side can publish without
waiting for one another. With one producer and one consumer, no caller has anyone to wait for. A
frontier SPSC queue would reduce to the two private cursors that `dpdk.SPSCQueue` already uses.
For single values, `dpdk.SPSCQueue` was about 2 ns behind xsync. It was about 1 ns behind Workiva
for pointers and nearly twice as fast for `int64`. A batch of 32 cost 1.4 ns per value for
`int64` and 2.6 ns for pointers. That is about 4 to 6 times cheaper per value than xsync's
single-value calls.

**Larger batches.** A separate six-round run raised the batch size to 256 and 1024. At those
sizes the copy dominates, and the two packages stayed within about 0.25 ns per value. At 256,
`ring/dpdk` was ahead in five of six rows, by 0.02 to 0.24 ns, and `ring/frontier` was ahead
for MPSC pointers by 0.11 ns. At 1024 the two were within 0.1 ns of each other.

**Workiva's `int64` rows.** Workiva stores `interface{}` values, so these rows box nearly every
value.

The [benchmark methodology](../benchmark/README.md) has the workloads, the harness's waiting
behavior, and the limits of these comparisons.

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
atomics. Consumers advance the completed frontier and producers advance the released frontier,
each with a compare-and-swap once it has followed the records past it. Dequeue calls, `Count`,
`Free`, `Full`, and `Empty` can advance the completed frontier, and enqueue calls the released
frontier, including zero-length and invalid-length calls. `MPMCQueue.Count` counts published
values that no consumer has reserved, as `SPMCQueue.Count` does. Producers and consumers that
lose a compare-and-swap call `runtime.Gosched` before retrying. With four producers and four
consumers, the producer yield took single-element transfers from about 53 to 30 ns for `int64`.

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
