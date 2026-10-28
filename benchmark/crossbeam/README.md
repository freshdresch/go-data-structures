# Crossbeam comparison

This suite runs the [`ring/dpdk`](../../ring/dpdk/README.md) and
[`ring/frontier`](../../ring/README.md#frontier) queues in crossbeam's channel benchmark suite.
It runs them beside crossbeam's Rust queues and Go's buffered channel. Every program prints the
suite's output format, so each result lines up with the matching result from every other program.

The benchmark programs come from the `crossbeam-channel/benchmarks/` directory of
`crossbeam-rs/crossbeam` at commit `099d0469efcfacb32527bd9451a4fb62586ae537`. They are used under
the MIT option, as [LICENSE-MIT](LICENSE-MIT) states. Each file copied or derived from upstream
starts with a header that names the upstream path and commit and says what changed. The library
code is the crates.io release of each crate pinned in `Cargo.lock`.

## Workloads

Each workload is named `<prefix>_<topology>`. The prefix sets the queue capacity.

| Prefix | Capacity | Notes |
|---|---|---|
| `bounded1` | 1 | One slot |
| `bounded1024` | 1024 (2^10) | Same capacity as the [ring comparison](../README.md) |
| `bounded2p18` | 262,144 (2^18) | Medium |
| `bounded2p23` | 8,388,608 (2^23) | Holds all 5,000,000 messages without overflow |

The topologies are:

- `seq`: one thread sends every message, then receives them. It runs only with `bounded2p23`,
  because the queue must hold every message.
- `spsc`: one sender and one receiver.
- `mpsc`: four senders and one receiver.
- `mpmc`: four senders and four receivers.

Each message is 8 bytes. In `spsc`, the sender sends all 5,000,000 messages. In `mpsc` and `mpmc`,
each sender sends a quarter of them.

That gives 13 workloads. The suite drops upstream's `bounded0`, `unbounded`, and 5,000,000-slot
`bounded` capacities, because rings accept only powers of two, and `bounded2p23` replaces
`bounded`. It also drops the `select` workloads, because the rings have no select. The suite has
no SPMC workload.

## Programs

| Program | Kind | Topologies |
|---|---|---|
| `src/crossbeam_channel.rs` | `Rust crossbeam-channel` | All |
| `src/arrayqueue.rs` | `Rust arrayqueue` | All |
| `src/arrayqueue.rs` | `Rust arrayqueue-backoff` | All |
| `gochan/main.go` | `Go chan` | All |
| `ring/main.go` | `Go dpdk` | All |
| `ring/main.go` | `Go frontier` | `mpsc`, `mpmc` |
| `ring/main.go` | `Go dpdk-mpmc` | `seq`, `spsc`, `mpsc` |
| `ring/main.go` | `Go frontier-mpmc` | `seq`, `spsc`, `mpsc` |

- `arrayqueue` polls `ArrayQueue::push` and `pop`. It calls `thread::yield_now()` after each
  failed attempt, as upstream's `segqueue` program does. `arrayqueue-backoff` calls
  `Backoff::snooze()` after a failed attempt and `Backoff::reset()` after a success. It has no Go
  counterpart. The summaries show it beside `arrayqueue`, but no significance test compares the
  two.
- `ring/main.go` runs the topologies in the table with goroutines and a `sync.WaitGroup`. It
  sends `uintptr` messages with the same values as `gochan`. It calls the single-element
  `Enqueue` and `Dequeue` with default queue options, and it calls `runtime.Gosched()` after
  `ErrFull` or `ErrEmpty`. Any other error ends the run with exit status 1, after the first error
  is printed to stderr.
- Each queue type has its own `send` and `recv` loops that take the concrete type, so every
  `Enqueue` and `Dequeue` inside them is a direct call. A generic loop would call them through
  the generic dictionary.
- `dpdk` and `frontier` use the queue specialized for each topology. See
  [Reading the MPMC columns](#reading-the-mpmc-columns).

## Changes from upstream

Every program works the same way.

1. **One workload per process.** Rust programs and `gochan` take `<workload>`, and `ring` takes
   `<family> <workload>`. An unknown or missing argument prints usage to stderr and exits with
   status 2. Each measurement starts in a fresh process, so the wrapper can shuffle them.
   Upstream ran every workload in one process in a fixed order. That let heap reuse, garbage
   collection pacing, and CPU frequency drift depend on position.
2. **Setup and teardown outside the timer.** Each program builds the queue, then one thread
   sends `capacity` messages and receives them. Go programs then call `runtime.GC()`. The timer
   covers only the start of the threads or goroutines, the workload, and the join. Rust programs
   drop the queue after the timer stops. Upstream timed construction and the drop. Freeing 128 MB
   takes about 6 to 7 ms at 2^23, about 1.3 ns per message, and upstream's structure frees it
   inside the timed call. Also, crossbeam writes every slot at construction, while Go's `make`
   returns zeroed pages that it does not touch. At 2^23, upstream's timing included page-fault and
   slot-writing work that differs between the languages.
3. **Six decimal places.** Seconds print with six decimals. Three decimals would be a 0.2 ns grid
   per message. `plot.py` parses both formats. Every reader splits on whitespace, so the columns
   need not line up.

The capacity for each prefix comes from one table per language. The Rust programs read the table
in `src/lib.rs`, and the Go programs read the `workloads` package.

## Requirements

- `go`, `git`, `benchstat`, `cargo`, and `rustc` on the path. Linux also needs `lscpu` and
  `taskset`.
- Rust 1.99.1. Run `rustup toolchain install 1.99.1` once. `rust-toolchain.toml` pins that
  version, and the wrapper stops unless `rustc -V` in `benchmark/crossbeam/` prints it.
- Run `cargo fetch --locked` once in `benchmark/crossbeam/`. The wrapper builds with `--frozen`,
  which works offline, so the crate sources must already be downloaded.
- `uv`, for the plot. Without it, the wrapper skips the plot. The first `uv run` may download
  Python and the locked packages. That happens after the timed rounds.
- Linux needs at least 8 CPUs with one thread per core. The wrapper pins every invocation to the
  first 8 of them. macOS needs `hw.ncpu` of at least 8 and AC power.

## Run the comparison

Run this from the repository root:

```bash
scripts/crossbeam-bench.sh [-rounds N] [-seed N] [-timeout S]
```

The defaults are 20 rounds, the script's process ID as the seed, and 120 seconds for each
invocation. Each round runs all 91 invocations in an order shuffled with `seed + round`. A seed
repeats an order only with the same `awk` implementation.

Before timing, the wrapper builds the Rust crate and both Go programs, and it runs the Rust tests
and the Go tests with `-race`. It stops if any step fails.

### What the wrapper refuses

The wrapper refuses to run if any of these variables is set, because each one changes the timed
programs without leaving a trace that the build checks can find:

- `GOMAXPROCS`, `GOGC`, `GODEBUG`, `GOMEMLIMIT`, and `GOTRACEBACK` change the Go rows at run
  time. `GOTRACEBACK=crash` would make a timed-out Go program dump core.
- `GLIBC_TUNABLES`, `LD_PRELOAD`, and any `MALLOC_*` variable can change the allocator that the
  Rust programs use.
- `RUSTFLAGS`, `CARGO_BUILD_RUSTFLAGS`, `CARGO_ENCODED_RUSTFLAGS`, `RUSTC`, `RUSTC_WRAPPER`,
  `RUSTC_WORKSPACE_WRAPPER`, and any `CARGO_PROFILE_*` or `CARGO_BUILD_RUSTC*` variable change
  how the Rust programs are compiled.

### Build checks

After the build, the wrapper checks the `rustc` command lines that `cargo build -v` prints.
Cargo's configuration files can change the flags, and `Cargo.toml` writes out `[profile.release]`
with Cargo's defaults, so the release settings are visible in the repository. The wrapper checks:

- Every line runs the toolchain's own `rustc`, from `rustc --print sysroot`.
- Every crate line, other than a build script's, sets each expected codegen option exactly once.
  The expected options are `opt-level=3` and `codegen-units=16`, and the options that cargo's
  release profile writes out. No other codegen option may appear.
- The Rust `MESSAGES` constant equals `workloads.Messages`.
- `go version -m` for each Go binary shows no `GOEXPERIMENT` and no instruction level other than
  the default for its architecture, `GOAMD64=v1` or `GOARM64=v8.0`.

## Output

Results go to `~/crossbeam-compare/<timestamp>-<pid>/`. The directory holds these files:

- `state.txt`: the command line, seed, rounds, timeout, commit, and whether the tree is clean. It
  also records the CPU model, the pin set, the transparent huge page settings on Linux or the core
  counts on macOS, load averages, the toolchain and crate versions, each Go binary's
  `runtime.sched` offset, the full `go version -m` output, and the `rustc` command lines. Its last
  line is `status:`, and it lists the findings, incomplete cells, and excluded cells.
- `rounds/`: one `.txt` and one `.err` file for each invocation, named
  `<round>-<Lang>-<impl>-<workload>`, for example `rounds/3-Go-dpdk-bounded1_mpsc.txt`.
- `samples.txt`: each kept sample as a Go benchmark line, for `benchstat`.
- `median.txt`: the median of each cell, in the suite's line format.
- `summary-<impl>.txt`: one `benchstat` table for each Go implementation, with that
  implementation as the first column. Each table shows medians, 95% confidence intervals, sample
  counts, and p-values against the first column.
- `significant.txt`: one line for each pair of cells that `benchstat` marks significant, in the
  form `<prefix> <topology> <impl A> <impl B> <faster impl>`.
- `plot.png`: the plot, when `uv` is present and plotting succeeds.
- `cargo-build.txt` and `target/`: the Rust build output.

### Sample and cell rules

A sample is kept if the process exits with status 0 before the timeout, writes nothing to stderr,
and writes exactly one line with the expected name, kind, a decimal number, and `sec`. Otherwise
the wrapper records a finding and drops only that sample.

A cell with fewer than 6 kept samples is incomplete, because `benchstat` needs at least 6 samples
for a confidence interval. A cell with at least 6 samples and any dropped sample is excluded. A
timeout or failure is part of that implementation's behavior, and the kept samples would show
only the successful runs. Cells that no program runs, such as `frontier` at `spsc`, are not
expected cells.

### Status and exit codes

The `status:` line in `state.txt` has one of these values:

- `complete`
- `complete with findings`
- `aborted <step>`, where `<step>` names the step that failed
- `interrupted`, after `INT`, `TERM`, or `HUP`

The wrapper exits with status 1 on any finding, incomplete cell, or `benchstat` failure. A run with
fewer than 6 rounds therefore always exits with status 1, because every cell is incomplete. If no
sample is kept, the wrapper stops with `no invocation succeeded`. An interrupted campaign never
counts toward the decision rule, whatever files it left.

## Decision rule

A difference between two cells in the same row counts only if `benchstat` reports p < 0.05, in
the same direction, in two campaigns with different seeds. Run this to list the differences that
pass:

```bash
LC_ALL=C comm -12 a/significant.txt b/significant.txt
```

Use `LC_ALL=C`, because the wrapper sorts its files under that locale, so two campaigns sort the
same way.

The two campaigns must meet these conditions:

- They come from the same commit with a clean tree. Their `state.txt` files record the commit and
  the tree status.
- Their `state.txt` files show the same toolchains, CPU, pin set, and transparent huge page
  settings.
- Neither campaign has an incomplete cell, and `benchstat` succeeded in both. A campaign that
  dropped samples still counts. Its excluded cells are left out of `significant.txt`, and its
  `state.txt` lists them with their findings.

The two seeds must differ by more than the round count, because round `r` uses `seed + r`. Closer
seeds repeat most round orders.

One campaign runs a few hundred tests, so more than ten pairs can reach p < 0.05 by chance alone.
The rule does not adjust p-values for multiple comparisons. Replication takes that role.
Ignore the geomean rows in the summaries, because the families cover different rows.

## Reading the MPMC columns

The `dpdk` and `frontier` rows use the queue specialized for each topology. `dpdk` uses
`SPSCQueue` for `seq` and `spsc`, `MPSCQueue` for `mpsc`, and `MPMCQueue` for `mpmc`. `frontier`
has no SPSC queue, so it runs only `mpsc` and `mpmc`.

The `Go dpdk-mpmc` and `Go frontier-mpmc` rows use `MPMCQueue` for `seq`, `spsc`, and `mpsc`.
Those rows give one structure in every topology, as crossbeam and Go chan do. They do not run
`mpmc`, because the `Go dpdk` and `Go frontier` rows already measure that topology with the same
queue.

To compare a ring MPMC queue with crossbeam's MPMC structures in every topology, read the
`dpdk-mpmc` and `frontier-mpmc` columns for `seq`, `spsc`, and `mpsc`, and the `dpdk` and
`frontier` columns for `mpmc`. The Rust `arrayqueue` columns are MPMC structures too.

## Caveats

- **Workers.** Go workers are goroutines, and Rust workers are OS threads. On the 8-CPU Linux pin
  set, each `mpmc` worker can have its own CPU. On a Mac with fewer than 8 performance cores,
  some workers run on efficiency cores. Under WSL2, `taskset` pins virtual CPUs, not physical
  cores.
- **Waiting.** `runtime.Gosched()` stays in user space, and `thread::yield_now()` is a system
  call. The `bounded1` rows mostly measure the waiting mechanism, not the queue structure.
- **Slot clearing.** The rings and Go chan clear each dequeued slot. crossbeam moves the value
  out and does not clear the slot.
- **Memory layout.** Go heap addresses are the same in every run of a binary, and Rust's are
  randomized for each process. Two campaigns with different seeds share the Go binary's layout,
  so replication does not rule out a layout effect. `gochan` and `ring` are separate binaries, so
  a difference between Go chan and a ring can come from where each binary's `runtime.sched` lands.
  The comment on `sched_offset` in [`scripts/lib.sh`](../../scripts/lib.sh) reports MPMC rows on
  Apple silicon that moved by 30% or more from that offset alone. The campaigns record each
  binary's offset in `state.txt`. Only Linux results have been checked.
- **Specialized queues.** `dpdk` uses specialized SPSC, MPSC, and MPMC queues, and `frontier` uses
  specialized MPSC and MPMC queues. crossbeam and Go chan use one MPMC structure in every
  topology. The `-mpmc` rows compare like structures.
- **Calls measured.** The suite moves one message per call, so the rings' batch calls are not
  measured.
- **Atomics on Apple silicon.** Rust's `aarch64-apple-darwin` target enables LSE atomics at
  compile time. Go chooses them at run time behind a check, under its default `GOARM64=v8.0`. The
  build checks accept both defaults, so on a Mac the two languages can run different atomic
  instructions. Only Linux x86-64 results have been checked.
