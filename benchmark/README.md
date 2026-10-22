# Queue comparison

This module compares the ring with other Go queues. Every row runs through one harness, in one
binary per machine, so the rows can be read against each other. The harness is a copy of the
ring's test harness, adapted so that this module builds without the ring's test files. The
comparison runs the ring through its exported API.

The comparison is a separate Go module, so the repository's root module gains no dependency on the
compared libraries, and the ring's own test binary does not change.

## Running it

```bash
scripts/ring-compare-bench.sh [-rounds N] [-benchtime D] [-pad N] [-seed N]
```

`-rounds` defaults to 10, and `-benchtime` defaults to 200ms. Each round runs every
implementation once, in an order shuffled for that round.

`-pad N` rebuilds the binary with N padding pointers, on the Mac only. Without it, the Mac pads
with 4 pointers only when `runtime.sched` lands at offset 96. `-seed N` sets the shuffle seed. A
seed repeats an order only on the same platform, because `awk`'s `srand` differs between macOS
and Linux.

Run the script only while the machine is plugged into AC power. On the Mac, the script stops
unless `pmset -g batt` reports AC Power. On battery, macOS throttles the CPU, and the results
are meaningless.

Results go to `~/ring-compare/<timestamp>-<pid>/`. The directory holds each round's raw output,
the comparison, and a `state.txt` that records how the run was made.

## Implementations

- `ring`: the ring, through `EnqueueBurst` and `DequeueBurst`. Batch 1 is the main row. Batch 32
  is labeled context, to show what batching gives the ring.
- `ring-exact`: the `ring` row built with `WithExactBatchCounts`. By default, the ring's batch
  calls check a cached copy of the other side's tail. `ring-exact` loads the real tail on every
  call and returns exact counts.
- `ring-single`: the ring, through `Enqueue` and `Dequeue`. It is the like-for-like comparison
  with the libraries, which have only single-element calls.
- `ring-rts`: the ring with relaxed tail sync on its sides that have several goroutines. It runs
  in the MPSC, SPMC, and MPMC modes.
- `chan`: a buffered Go channel.
- `xsync`: `xsync.NewMPMCQueue` from `github.com/puzpuzpuz/xsync/v4`, at v4.5.0. It is a bounded
  MPMC queue, and its `TryEnqueue` and `TryDequeue` return at once.
- `mutex`: a slice guarded by a `sync.Mutex`. It is the lock-based reference point.
- `workiva`: `queue.NewRingBuffer` from `github.com/Workiva/go-datastructures`, at v1.1.8. It is
  the most widely used Go ring buffer. It stores `interface{}` values.

Every implementation is built with 1024 slots. Each multi-goroutine side runs 4 goroutines, as
the ring's own benchmarks do.

## What each row measures

A row measures a queue together with the way its callers wait. In the list, our yield means that
the harness calls `runtime.Gosched` after a call that moves nothing. Each implementation waits as
follows.

- `ring` and `ring-single` wait with our yield on a full or empty call, and with the ring's
  bounded spin in the multi publish wait.
- `ring-exact` waits as `ring` does. Its difference from `ring` shows what exact batch counts cost.
- `ring-rts` waits with our yield on a full or empty call, and with the relaxed side's
  head-to-tail distance wait.
- `chan` parks in the runtime.
- `xsync` waits with our yield on a false result. `TryEnqueue` and `TryDequeue` also return false
  when they lose a compare-and-swap to another goroutine, so the yield also runs on contention,
  not only when the queue is full or empty.
- `mutex` waits on the lock, and with our yield on a full or empty call.
- `workiva` waits with `runtime.Gosched` in a loop inside the library. It never parks.

The following points also apply.

- **SPSC.** The ring runs its single-producer and single-consumer paths. xsync and workiva always
  run their MPMC algorithm, because neither has an SPSC mode. mutex takes the same lock in every
  mode.
- **Algorithms.** xsync and workiva are both bounded queues in the style of Dmitry Vyukov's
  design, which gives each slot a sequence number. They differ in generics against `interface{}`,
  and in where they wait, not in the algorithm.
- **Capacity.** `TestCompareCapacity` fills each implementation and logs how many elements it
  holds.
- **Allocations.** workiva stores `interface{}`, so its `int64` rows box nearly every element, at
  8 bytes each. The benchmark values carry a producer and a sequence number, so they are far above
  the small values that Go boxes without allocating. Go reports `allocs/op` as a whole number per
  element, rounded down. A workiva `int64` row can therefore show 0 `allocs/op` while it still
  boxes nearly every element. Read `B/op` for those rows, where a value near 8 shows the boxing.
  The `ptr` rows do not box, because a pointer fits in an interface directly, so 0 `allocs/op` is
  expected there.
- **Slot clearing.** The ring, xsync, workiva, and mutex clear a dequeued slot.
- **chan.** The `chan` rows go through the blocking runner's interface, so they differ from the
  ring package's own `impl=chan` rows.
- **Cross-reading.** Every producer and consumer loop here also loads the abort flag, which the
  ring package's harness does not. The binary also has its own layout. So none of these rows
  should be compared with the ring package's tables.
- **Layout.** Each binary places the Go scheduler's state, `runtime.sched`, at some offset modulo
  128 bytes. The implementations lean on the scheduler to different degrees. On Apple silicon,
  one placement moved the ring's MPMC rows by 30% or more. A different placement could change the
  numbers, and possibly a close ranking. The Mac pads the binary when the offset is 96, the
  placement known to slow the ring's MPMC rows. The AMD never pads, because no offset is known to
  be bad there.
- **Cores.** The M3 Pro mixes performance and efficiency cores, and macOS cannot pin goroutines.
  Which goroutines land on efficiency cores varies between invocations. Implementations that
  yield more may be affected differently. Under WSL2, `taskset` pins virtual CPUs rather than
  physical cores, and the Hyper-V host adds scheduling noise.
- **Statistics.** benchstat does not correct p-values across rows and metrics. A few rows will
  show a significant difference by chance.

## Results

Results are pending.
