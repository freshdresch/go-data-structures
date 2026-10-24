# Benchmarks

The benchmark module compares this repository's data structures with other implementations.
It is a separate Go module, so third-party benchmark dependencies stay outside the root module.
The current comparison covers the four [ring queues](../ring/README.md).

| Queue | Comparison report |
|---|---|
| SPSCQueue | [Single producer, single consumer](ring/spsc.md) |
| SPMCQueue | [Single producer, multiple consumers](ring/spmc.md) |
| MPSCQueue | [Multiple producers, single consumer](ring/mpsc.md) |
| MPMCQueue | [Multiple producers, multiple consumers](ring/mpmc.md) |

No third-party measurements for the current queue APIs are published yet. Crossbeam comparisons
are future work. The reports will describe measured workloads and their limits when results exist.

## Run the comparison

Run from the repository root with Go, Git, and `benchstat` on the path. Linux also needs `lscpu`
and `taskset`. macOS needs at least 10 CPUs. See [runner requirements](../scripts/README.md).

```bash
scripts/ring-compare-bench.sh -rounds 10 -benchtime 200ms -seed 12345
```

The script runs correctness gates before timing. Each round runs every implementation once,
in a shuffled order. Results go to `~/ring-compare/<timestamp>-<pid>/`. The directory contains
raw rounds, comparisons, and `state.txt` with the run's configuration.

Run one timing campaign at a time on an otherwise idle machine connected to AC power. On macOS,
the script requires `pmset -g batt` to report AC power. Battery power changes CPU behavior.

`-pad N` rebuilds the macOS binary with N padding pointers. Without this flag, macOS uses four
padding pointers only when `runtime.sched` lands at offset 96 modulo 128. Linux never pads.
A shuffle seed repeats an order only on the same platform because `awk` randomization differs
between macOS and Linux.

## Implementations and workloads

The `ring` row selects `SPSCQueue`, `MPSCQueue`, `SPMCQueue`, or `MPMCQueue` for its topology.
It calls `EnqueueBurst` and `DequeueBurst` with batches of 1 and 32. The `ring-exact` row adds
`WithExactBatchCounts`. The `ring-single` row calls `Enqueue` and `Dequeue` and is the
single-element comparison with the other queues. The `ring-rts` row selects relaxed tail
synchronization on concurrent sides in MPSC, SPMC, and MPMC workloads.

The other rows are a buffered Go channel (`chan`), a slice guarded by `sync.Mutex` (`mutex`),
`xsync.NewMPMCQueue` from `github.com/puzpuzpuz/xsync/v4` v4.5.0 (`xsync`), and
`queue.NewRingBuffer` from `github.com/Workiva/go-datastructures` v1.1.8 (`workiva`).
Dependency versions are recorded in [go.mod](go.mod).

Every queue has exactly 1024 usable elements. SPSC uses one producer and one consumer.
MPSC uses four producers and one consumer. SPMC uses one producer and four consumers.
MPMC uses four producers and four consumers. Worker counts are independent of GOMAXPROCS and
CPU affinity. Payloads are `int64` values or pointers rotated through a pool of 1024 items.

`TestCompareCapacity` checks capacity, a rejected extra put, ordered drain, empty behavior,
and reuse for each row and topology. Accounting tests check element transfers. Construction and
pre-run GC are outside the timer. Worker startup, payload fill, operations, retries or blocking
waits, completion, drain, stop, and joins are timed. Each row transfers the requested element
count, so `ns/op` means nanoseconds per transferred element. Burst permits partial progress.
The root module's bulk benchmarks require all-or-nothing batches and measure another workload.

## Read the measurements

A row measures the queue together with its callers' waiting behavior. The harness calls
`runtime.Gosched` when a polling call moves nothing. Ordered ring publication also uses bounded
spinning before yielding. RTS uses its configured head-to-tail distance wait. Channels park
in the runtime. The mutex row waits for its lock and yields after full or empty calls.
Workiva yields inside its library loop. xsync's try calls can return false after losing a
compare-and-swap, so its harness also yields on contention.

xsync and Workiva use their MPMC algorithms in every topology. The ring selects an API for
each topology. Batch-32 ring rows provide batching context because the other queues expose
single-element operations. Compare `ring-single` with those rows when comparing single calls.
The ring, xsync, Workiva, and mutex adapters clear dequeued slots.

Workiva stores `interface{}` values. Its `int64` rows box nearly every value at about eight bytes
each. Go rounds `allocs/op` down to a whole number per element, so read `B/op` as well.
The pointer rows fit directly in an interface and do not need boxing allocations.

Binary layout and scheduling affect results. The runners record the offset of `runtime.sched`
modulo 128, but equal offsets do not prove equivalent layouts. Apple silicon mixes performance
and efficiency cores, and macOS cannot pin goroutines. Under WSL2, `taskset` pins virtual CPUs
and host scheduling adds noise. Record the machine, toolchain, CPU policy, and workload beside
published results. `benchstat` does not adjust p-values across rows and metrics, so individual
significant comparisons can occur by chance.

The comparison harness loads an abort flag in each worker loop and builds a separate binary
from the root module. Its channel adapter also differs from the root harness. Compare rows
within the same campaign. Do not combine the two modules' tables.

## Provenance and local evidence

Before timing, `TestBenchmarkManifest` exports all 84 workload contracts. The runner checks
names, multiplicity, CPU suffixes, and required metadata. Ordinary, padded, and race builds
have separate source snapshots and independent postbuild discovery. Changes to selected inputs
stop finalization. Snapshots include dirty and untracked compiled inputs, correctness tests,
dependency sources, module inputs, and the runner bundle. Validation gates precede repeated
pre-timing checks. Failed runs retain gate, export, discovery, and round logs.

When padding applies, the runner records both ordinary and padded binary identities, plus the
separate unpadded race identity. It selects layout before timing. A methodology change requires
new measurements of all compared implementations. Verification tests establish correctness,
not a performance improvement. See [runner documentation](../scripts/README.md) for artifact
names, source reconstruction, units, and the quick runner's admission policy.

Keep raw data, source snapshots, profiles, binaries, and archives outside tracked files.
`benchmark/results/`, `benchmark/baselines/`, and `benchmark/.local/` are ignored.
Use `.local/` for temporary development evidence. Delete superseded experiments and evidence
for resolved investigations as soon as they are no longer needed. Retain accepted baselines
and evidence needed for unresolved investigations until they are replaced or resolved.
