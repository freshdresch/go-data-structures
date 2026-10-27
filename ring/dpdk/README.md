# DPDK-style ring queues

`dpdk` provides four generic, fixed-capacity FIFO queues modeled on DPDK's `rte_ring`.
Choose a queue for your producer and consumer topology, then transfer individual values or batches.
The queues allocate their backing storage at construction and use every slot in that storage.

## Choose a queue

A producer calls enqueue methods. A consumer calls dequeue methods. Choose the row that matches
which sides your application serializes.

| Topology | Public type | Constructor | Producers | Consumers |
|---|---|---|---|---|
| SPSC | `SPSCQueue[T]` | `NewSPSCQueue[T]` | One at a time | One at a time |
| SPMC | `SPMCQueue[T]` | `NewSPMCQueue[T]` | One at a time | Concurrent |
| MPSC | `MPSCQueue[T]` | `NewMPSCQueue[T]` | Concurrent | One at a time |
| MPMC | `MPMCQueue[T]` | `NewMPMCQueue[T]` | Concurrent | Concurrent |

SPSC means single producer, single consumer. SPMC means single producer, multiple consumers.
MPSC means multiple producers, single consumer. MPMC means multiple producers, multiple consumers.
A single side permits one call at a time across all its methods, including failed and zero-length
calls. Opposite sides can run concurrently in every topology.

Every constructor takes `count int, options ...Option` and returns a queue pointer and an error.
Capacity must be a power of two from 1 through `2^30`, inclusive.
All `count` slots are usable. Capacity never grows.

## Quickstart

Use the Go version declared in the repository's [go.mod](../../go.mod). Add the package to your
module:

```sh
go get github.com/freshdresch/go-data-structures/ring/dpdk
```

This program uses one producer and one consumer sequentially. It prints `42` and then `empty`.

```go
package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
)

func main() {
	queue, err := dpdk.NewSPSCQueue[int](1024)
	if err != nil {
		log.Fatal(err)
	}
	if err := queue.Enqueue(42); err != nil {
		log.Fatal(err)
	}
	value, err := queue.Dequeue()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value)

	_, err = queue.Dequeue()
	if errors.Is(err, dpdk.ErrEmpty) {
		fmt.Println("empty")
		return
	}
	if err != nil {
		log.Fatal(err)
	}
}
```

Use constructors rather than zero values. Do not copy a queue after use. Complete construction
before publishing its pointer to other goroutines through normal Go synchronization.

## Shared methods and batch behavior

All four queue types expose the same methods. These signatures use `T` for the element type:

```text
Enqueue(item T) error
Dequeue() (T, error)
EnqueueBulk(items []T) (free int, err error)
DequeueBulk(dst []T) (available int, err error)
EnqueueBurst(items []T) (enqueued, free int)
DequeueBurst(dst []T) (dequeued, available int)
Capacity() int
MaxEnqueueBatch() int
MaxDequeueBatch() int
Count() int
Free() int
Full() bool
Empty() bool
Reset()
```

`Enqueue` transfers one value or returns `ErrFull`. `Dequeue` transfers one value or returns the
zero value of `T` with `ErrEmpty`. Both errors are returned unwrapped. Use `errors.Is` to match
these and the wrapped validation errors below.

Bulk methods transfer the whole slice or nothing. A failed `DequeueBulk` leaves the entire
destination unchanged. A request longer than capacity wraps `ErrExceedsCapacity`. Otherwise,
a request longer than its batch limit wraps `ErrExceedsMaxBatch`. These checks precede admission.
A valid bulk request that cannot fit returns `ErrFull` or `ErrEmpty` without transferring values.
Retrying a length error cannot help. Reduce the request or construct a queue with larger limits.

Burst methods transfer a prefix, limited by the slice length, the configured batch limit, and
currently admissible slots or entries. They return no error. `DequeueBurst` leaves the destination
suffix unchanged. Use the returned transfer count to select the part to process or retry.

For example, this function shows all-or-nothing bulk and partial burst transfers:

```go
func batchExample() error {
	queue, err := dpdk.NewSPSCQueue[int](4, dpdk.WithMaxBatch(2))
	if err != nil {
		return err
	}
	free, err := queue.EnqueueBulk([]int{10, 20})
	if err != nil {
		return err
	}
	fmt.Println(free) // 2

	enqueued, free := queue.EnqueueBurst([]int{30, 40, 50})
	fmt.Println(enqueued, free) // 2 0

	dst := []int{-1, -1, -1}
	dequeued, available := queue.DequeueBurst(dst)
	fmt.Println(dst[:dequeued], available) // [10 20] 2
	fmt.Println(dst[dequeued:])            // [-1]

	available, err = queue.DequeueBulk(dst[:2])
	if err != nil {
		return err
	}
	fmt.Println(dst[:2], available) // [30 40] 0
	return nil
}
```

Use the imports from the quickstart and call this function from your program.
The burst enqueue transfers only the first two elements because the batch limit is two.

A bulk return count describes remaining free slots or available entries, even on failure.
A burst returns the number transferred first, then the remaining count. Remaining counts describe
the admission snapshot after subtracting this call's transfer. They do not promise the queue's
state when the caller reads them. Nil and empty slices request zero elements. Such calls refresh
their count and make no reservation or publication, but still belong to their respective side.

### Reading counts

`Count` reads consumer publication and producer publication and returns a value in
`[0, Capacity()]`. `Free` returns capacity minus its own `Count` snapshot. `Full` and `Empty`
compare a snapshot with capacity or zero. Separate calls need not describe the same instant.
These methods may run concurrently with operations. They are not linearizable queries.

Publication differs from reservation on concurrent sides. `Count` excludes producer reservations
that have not published and includes consumer reservations that have not released their slots.
Consequently, `Free` can exceed immediately reservable space, and `Count` can exceed immediately
reservable data. With relaxed consumers, `Count` can even include values already returned.

By default, batch methods may report remaining counts from a cached opposite-side publication.
The cache never causes a transfer to fail or move fewer elements than a fresh check would admit.
It can under-report the remaining count when the calling side is serialized. With concurrent
callers on that side, snapshots can be higher or lower than the state observed afterward.

`WithExactBatchCounts` loads the opposite side's publication on every batch call. Without it,
zero-length calls, length errors, `ErrFull`, and `ErrEmpty` paths already return fresh snapshots.
A burst that transfers less than its clamped request also refreshes. Transferring the whole
clamped request can return a cached count, even if the original slice was longer.

Do not use remaining counts to size the next request or make parking and wakeup decisions.
A cached zero can hide waiting data. A fresh snapshot can change before a goroutine parks.
`WithExactBatchCounts` provides no parking or wakeup guarantee. The package has no close, channel
notification, context, or timeout API. Build coordination into the caller when you need it.

## Options

Pass options only at construction. They configure policy without changing the queue's topology.
Options apply in argument order. Constructors validate only the final values, after checking
capacity. For example, `WithMaxBatch(32), WithMaxDequeueBatch(8)` leaves enqueue at 32 and
dequeue at 8, provided both fit the capacity. Configuration and batch limits stay fixed afterward.

For an MPSC queue with bounded batches, producer RTS, and immediate yielding during waits:

```go
func newWorkQueue() (*dpdk.MPSCQueue[int], error) {
	return dpdk.NewMPSCQueue[int](
		1024,
		dpdk.WithMaxBatch(32),
		dpdk.WithRelaxedMultiProdEnqueue(),
		dpdk.WithMaxProducerHeadTailDistance(128),
		dpdk.WithSpinsBeforeYield(0),
	)
}
```

### Batch limits, clearing, and counts

These options apply to every queue:

- `WithMaxEnqueueBatch(n)` limits enqueue batches. The default is capacity.
- `WithMaxDequeueBatch(n)` limits dequeue batches. The default is capacity.
- `WithMaxBatch(n)` sets both limits. A later side-specific option overrides its side.
  Each final limit must be from 1 through capacity. Invalid limits wrap `ErrInvalidMaxBatch`.
  Bulk rejects longer slices. Burst clamps them. Single-element methods are unaffected.
- `WithoutDequeueClearing()` disables clearing consumed slots. Clearing is enabled by default.
  For pointer-containing values, disabling it can retain up to capacity stale values until slot
  reuse, possibly indefinitely after the queue drains. `Reset` always clears every slot.
- `WithExactBatchCounts()` enables a fresh opposite-side publication load for every batch call.
  Cached counts are the default. Single-element methods return no counts and keep their cache
  behavior. See [Reading counts](#reading-counts) for the snapshot limits.

Batch limits bound the amount of copying in one call. A large producer batch delays visibility
of its values until copying finishes. A large consumer batch delays reuse of its slots until
reading and optional clearing finish. On concurrent sides, earlier batches can also delay later
reservations' publication or slot release.

### Publication and waiting

Concurrent sides use ordered publication by default:

- `WithRelaxedMultiProdEnqueue()` selects relaxed tail synchronization (RTS) for producers.
  Only `MPSCQueue` and `MPMCQueue` accept it.
- `WithRelaxedMultiConsDequeue()` selects RTS for consumers.
  Only `SPMCQueue` and `MPMCQueue` accept it.
- `WithSpinsBeforeYield(n)` sets how many checks a waiter makes before yielding through
  `runtime.Gosched`. The default is 8192. Valid values are 0 through `math.MaxInt32`.
  Zero skips tight spinning. Invalid values wrap `ErrInvalidSpinsBeforeYield`.
  This affects only concurrent sides, in either ordered or RTS mode. `SPSCQueue` rejects every
  explicit spin option, including zero. Yielding gives other goroutines a chance to run and does
  not park the caller until data or space arrives. Longer spinning can avoid scheduler overhead
  when the predecessor is running, but burns CPU when that goroutine is preempted.

`MPMCQueue` accepts each RTS selector independently. You can use ordered publication on both
sides, RTS on either side, or RTS on both. Selecting RTS on a serialized side wraps
`ErrUnsupportedOption`.

### RTS distance limits

- `WithMaxProducerHeadTailDistance(distance)` configures producer RTS on `MPSCQueue` or
  `MPMCQueue`.
- `WithMaxConsumerHeadTailDistance(distance)` configures consumer RTS on `SPMCQueue` or
  `MPMCQueue`.

Each option requires its matching RTS selector. It does not select RTS itself. An explicit limit
on an ordered concurrent side wraps `ErrHeadTailDistanceRequiresRelaxed`. A limit on a serialized
side wraps `ErrUnsupportedOption`. Numeric validation comes first and wraps
`ErrInvalidMaxHeadTailDistance` for values outside `[0, math.MaxUint32]`. The argument must also
be representable as an `int` on the target platform.

The default on each RTS side is `count / 8`, rounded down. Distances count element positions,
not reservations, bytes, or goroutines. The head marks reservations. The tail marks publication
or release. Their distance includes completed calls held back by outstanding reservations.

Before every reservation attempt, including retries, RTS waits while head minus tail exceeds
the limit. Equality admits an attempt, so that attempt's batch can take the head beyond the limit.
Explicit zero admits only when earlier reservations have published or released their slots.
The default is also zero below capacity eight. A limit at or above capacity removes distance
throttling while capacity checks still apply. Large limits can postpone publication indefinitely
when operations keep overlapping.

The distance wait precedes the free-slot or available-entry check. Full, empty, and valid
zero-length calls can therefore wait. Oversized bulk requests return their length error before
this wait. Failed admission and zero-length calls create no reservation.

Constructors return a nil queue on validation errors. Invalid capacity wraps `ErrInvalidCount`
before any option runs. After capacity, validation checks enqueue limit, dequeue limit, spin
number, producer mode, consumer mode, explicit SPSC spin, producer distance, then consumer
distance. Each distance's numeric check precedes its applicability check.

## How transfer and publication work

Each side reserves positions, copies values, then publishes its progress to the opposite side.
Consumer publication releases slots for reuse. On serialized sides, one owner advances a cursor.
On concurrent sides, callers atomically reserve disjoint intervals.

```text
Producer: reserve free slots -> copy items -> publish values
                   ^                              |
                   |                              v
          release slots <- copy out and clear <- Consumer: reserve values

Concurrent side, with A reserved before B:
Ordered: B finishes copying -> waits for A -> publishes B -> returns
RTS:     B finishes copying -> records completion -> can return
         A still unfinished -> neither interval advances the published tail
```

1. `Enqueue`, `EnqueueBulk`, and `EnqueueBurst` check or reserve free slots before writing.
   This prevents producers from overwriting entries that consumers still own.
2. The enqueue copies values before publishing them. A batch becomes visible together, so a
   consumer cannot read a partially copied batch.
3. `Dequeue`, `DequeueBulk`, and `DequeueBurst` check or reserve published entries before reading.
   They clear consumed slots by default, then release the interval so producers can reuse it.
4. Ordered concurrent sides wait for earlier reservations before publishing their own progress.
   RTS records completion without that wait. Its tail advances once all reservations made so far
   have completed. A later RTS enqueue can return before its values become visible. A later RTS
   dequeue can return before its slots become reusable.

FIFO follows reservation order. Concurrent calls can return in a different order in either mode.
RTS changes publication timing, not the order of values within reserved intervals.
It adds atomic read-modify-write work to record completion and update publication in exchange for
avoiding the ordered wait for earlier reservations. Measure that tradeoff for your batch sizes.

## Ownership and progress limits

- Synchronize every handoff of a serialized side with a happens-before edge, such as a channel or
  mutex. Merely avoiding simultaneous calls is insufficient because those calls update caches.
  `Count`, `Free`, `Full`, `Empty`, and immutable configuration queries may run alongside either
  side. `Reset` needs exclusive access against all operations and observers. Synchronize both
  completion of prior use and publication to future users. Reset preserves storage and options
  and clears every slot, position, and cache, even with `WithoutDequeueClearing`.
- Keep input slices stable for each enqueue call. Give concurrent dequeues nonoverlapping
  destinations unless the caller synchronizes access. Values of `T` are copied. Referenced
  objects are not deep-copied, so callers must coordinate subsequent access to shared data.
- Calls can wait internally even though admission reports `ErrFull` or `ErrEmpty`. Concurrent
  compare-and-swap retries, ordered publication waits, and RTS distance waits have no fixed bound
  or cancellation. A stalled reservation can block useful progress on both sides. The queues
  promise neither wait-freedom nor formal lock-freedom. Platforms may implement 64-bit atomics
  with locks. Choose spin bounds and RTS settings by measuring your workload.

## Benchmarks

Comparison reports cover [SPSCQueue](../../benchmark/ring/spsc.md),
[SPMCQueue](../../benchmark/ring/spmc.md), [MPSCQueue](../../benchmark/ring/mpsc.md), and
[MPMCQueue](../../benchmark/ring/mpmc.md). They currently publish no third-party measurements or
performance ranking. The [methodology](../../benchmark/README.md) describes workloads, waiting
behavior, timing boundaries, and measurement limits. Compare results from the same campaign
and record the machine, toolchain, batch sizes, and configuration.
