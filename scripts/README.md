# Scripts

`ring-quick-bench.sh` compares the ring benchmarks of two committed refs in a couple of minutes.
It builds each ref's test binary, runs 24 fixed throughput and latency rows for both sides in
alternating rounds, and prints a `benchstat` comparison. A quick run is directional. It shows
whether a change helped and roughly how much, and it never by itself decides whether to keep a
change.

```bash
scripts/ring-quick-bench.sh [-rounds N] [-benchtime D] <before-ref> <after-ref>
scripts/ring-quick-bench.sh [-rounds N] [-benchtime D] -reuse <dir>
```

The script needs `git`, `go`, and `benchstat` on the path. Install `benchstat` with
`go install golang.org/x/perf/cmd/benchstat@latest`. On Linux it also needs `lscpu` and `taskset`,
and it pins every run to one CPU per core. On macOS it needs at least 10 CPUs.

**Run it only while plugged into AC power.** On battery, macOS throttles the CPU and the
results are meaningless.

Run timing campaigns serially on an otherwise idle machine. After methodology changes, establish
both sides under the new methodology. Agree on refs, rounds, durations, and exclusion rules before
collecting results. Unit tests and fixtures establish no speedup.

Results go to `~/ring-quick/<timestamp>-<pid>/`, with the two binaries, every round's raw output,
the comparison, and a `state.txt` that records how the run was made. `-reuse` reruns the binaries
saved in an earlier results directory. The script only reads git state and never changes the
working tree, the index, refs, or remotes. The printed comparison leaves out the Handoff p99.9 and
max percentiles, because short runs make them very noisy, but the raw files keep them.

The quick runner admits a default implementation comparison only when both complete origins have
the same measurement, runner, toolchain, dependency, build-setting, and workload identities. Ring
implementation identities may differ. Changed measurement paths appear by class in the saved
admission decision. An old ref without the metadata export requires a new compatible baseline.
The runner never copies a new harness onto an old API.

`-methodology-comparison` permits known methodology differences and labels the presentation
`methodology-comparison`. It cannot establish a ring implementation speedup. On Darwin/arm64,
unequal or unknown `runtime.sched` offsets modulo 128 stop before timing. The explicit
`-allow-layout-confounded` diagnostic override adds `layout-confounded`. It does not bypass any
provenance or workload gate. Linux and other supported Darwin architectures record and warn about
unequal or unknown offsets because offset sensitivity is unestablished there. Equal offsets do
not establish equivalent layouts. A stopped run starts no placement investigation or padding
search.

`lib.sh` holds helpers that `ring-quick-bench.sh` and `ring-compare-bench.sh` source. It is not
run on its own.

`ring-spin-bound.sh` compares one committed ref built with several publish wait spin bounds, the
`defaultSpinsBeforeYield` constant in `ring/ring.go`, or `tailSpins` in commits before it was
renamed. It builds one binary per bound, with the first bound as the base, runs the MPSC, SPMC, and
MPMC throughput rows for every side in rotating order, and prints a `benchstat` comparison. Run it
to choose the spin bound after a change to the publish wait or to the ring's layout.

```bash
scripts/ring-spin-bound.sh [-rounds N] [-benchtime D] <ref> <bound> <bound>...
scripts/ring-spin-bound.sh 8e3e968 256 64 512 1024
```

A default run of four bounds takes about 11 minutes on macOS and about 8 under WSL2. Results go
to `~/ring-spin/<timestamp>-<pid>/`. Like the quick bench, run it only while plugged into AC power.

`ring-compare-bench.sh` compares the ring with other Go queues through the `benchmark` module. It
runs the module's tests first and stops if any fails. It then runs the 84 benchmark rows in
shuffled rounds, checks each run's rows, and prints two `benchstat` comparisons, one led by
`ring` and one led by `ring-single`. The module's README says what each row measures.

```bash
scripts/ring-compare-bench.sh [-rounds N] [-benchtime D] [-pad N] [-seed N]
```

Results go to `~/ring-compare/<timestamp>-<pid>/`. On the Mac, the script stops unless
`pmset -g batt` reports AC Power, so run it only while plugged into AC power.

## Saved identities and units

Both runners build a standard-library metadata helper once outside timing. Before each test
binary build, `snapshot` discovers the selected inputs with `go list -deps -test -json` under
`GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly`. It saves input membership and exact bytes.
After the build, `finalize` independently discovers the inputs again and rejects additions,
removals, changed bytes, changed settings, or discovery errors. It then attaches the binary,
build information, scheduler offset, and metadata-only workload export. A final check runs before
timing. An edit that changes and reverts during a build remains outside this observed-source
check.

`provenance/*.snapshot.json` records incomplete prebuild origins. `provenance/*.json` records
finalized origins, decisions, requests, and manifests. Verification reports preserve before/after
inventories and changes. `provenance/sources/<sha256>` stores deduplicated bytes for dirty,
untracked, module, dependency, and actual runner inputs. The helper binary has a separate digest.
Source reconstruction does not promise reproducible binaries or archival toolchain availability.
Discovery logs, metadata export logs, and complete `go version -m` output remain beside the
manifests. `state.txt` records invocation environment, CPU selection, commands, order, and status.
Worker counts, actual GOMAXPROCS, and affinity are distinct observations.

`-reuse` requires complete versioned origins, snapshots, source bytes, manifests, and binaries.
The runner verifies their digests and checks the binary's metadata-only export again. It copies
original provenance unchanged and records the reuse parent. A changed current runner requires the
methodology flag. Legacy directories retain their historical meaning, but cannot be admitted
under this protocol, even with overrides. Their source identity is never backfilled from today's
checkout.

The quick selection remains 24 rows. The comparison selection remains 84 rows. Exact metadata
row checks run before timing, and exact result row checks remain after every invocation.
Concurrent RingAPI rows report canonical `ns/elem` from the stopped timed interval divided by
verified transfers. Bulk rounds to a multiple of batch times producer count and also reports
`elem/op`. Raw `ns/op`, `MB/s`, `B/op`, and `allocs/op` retain their Go iteration denominator.
In particular, raw MB/s is iterations times element size divided by elapsed time. Go rounds raw
allocation counts to whole allocations per iteration. GC metrics retain their definitions.

`before.txt`, `after.txt`, and round files retain raw results. `before-primary.txt` and
`after-primary.txt` remove only concurrent RingAPI `ns/op` and retain `ns/elem`.
`comparison.txt` uses benchstat's normalized `sec/elem` and `sec/op` units and latency metrics.
`comparison-raw.txt` preserves interpretation of iteration, byte-rate, allocation, and ratio
metrics. Sequential RingAPI `ns/op` measures an enqueue/dequeue pair. Handoff `ns/op` measures a
round trip. Its half-round-trip percentiles estimate handoff latency rather than measure one-way
latency directly.

Throughput excludes queue/buffer allocation and pre-run GC. It includes worker startup, payload
fill, queue operations, retries, completion, drain, and joins. Producers yield on an unsuccessful
enqueue attempt. Throughput consumers yield after every unsuccessful dequeue attempt. Latency
polling allows 64 attempts before yielding. Handoff excludes its warm-up round trip and ponger
startup. Loaded latency retains its existing start barrier for the precreated workers. Loaded
rows are not added to either runner.

The comparison runner retains its one-binary and padding policy. It saves
`compare-unpadded.test`, optional `compare-padded.test`, and `compare-race.test`, each with its own
prebuild snapshot and finalization. Generated padding source and the overlay mapping precede the
padded snapshot. `compare.test` is a checked copy of the selected finalized binary.
`provenance/compare.json` preserves that variant's identity. Original/final offsets and the padding
choice remain recorded. The existing padding failure message suggests another explicit `-pad`,
but investigation, automatic retries, and new padding searches remain deferred.

For verification without performance measurements, run `bash scripts/bench-runners-test.sh`.
It compiles only the metadata helper. Temporary shell binaries emit fixed metadata and results,
and markers establish that rejected inputs never dispatch timed invocations.

## Bounded SPSC comparison corpus

The SPSC proof uses one frozen `go test -c ./ring` binary for both implementations.
`-spsc-manifest=concurrent` exports exactly 22 rows: ten existing RingAPI rows, ten SPSCAPI
rows, and two `BenchmarkSPSCComparisonHandoff/impl={ring,spsc}` rows. The API rows use
SPSC ownership, `int64` and `ptr`, single calls, and bulk/burst batches of 1 and 32.
`-spsc-manifest=sequential` exports four
`BenchmarkSPSCConcrete/impl={ring,spsc}/api=seq/elem={int64,ptr}` rows. Every queue has
capacity 1024 and default options. Concurrent API pointer rows rotate through the existing
1024-item pool. Sequential pointer rows allocate the same pool and repeatedly use its first item.
The default selector remains `legacy`, with the original 24-row contract.

Shared API rows cross the same queue interface and report canonical `ns/elem` per verified
transfer. Bulk also reports actual `elem/op`. Concrete sequential rows call concrete queue
pointers and report `ns/op` per enqueue/dequeue pair. Handoff reports `ns/op` per round trip
and estimates one-way latency with half-round-trip histogram samples. It uses two int64 queues.
The manifest records each family's participants, waiting, storage, options, and lifecycle timing.
Keep shared dispatch and concrete results separate. Sequential direct calls answer the compiler
question. Concurrent shared dispatch answers the transfer question.

The parent's approved protocol sets CPU placement, durations, rounds, and acceptance criteria.
Use the existing `benchmeta snapshot`, `finalize`, and `finalize --check-only` commands with
that protocol's request and artifact JSON files. Snapshot before building, finalize after building,
and check again before timing. Source discovery through `go list -deps -test -json` includes the
new compiled files. Preserve discovery logs, source bytes, build information, binary hashes,
manifest export logs, exact commands, affinity, environment, raw output, and each process status.
Both selected manifests must come from that frozen binary at their intended CPU settings.
For example, after the protocol creates its snapshot request:

```bash
export GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly
go build -o /tmp/spsc-proof/benchmeta ./scripts/benchmeta
/tmp/spsc-proof/benchmeta snapshot \
  --request=/tmp/spsc-proof/request.json --output=/tmp/spsc-proof/snapshot.json
go test -c -o /tmp/spsc-proof/ring.test ./ring
/tmp/spsc-proof/ring.test -test.run='^TestBenchmarkManifest$' -test.v \
  -test.cpu=2 -spsc-manifest=concurrent > /tmp/spsc-proof/concurrent.export.log
/tmp/spsc-proof/ring.test -test.run='^TestBenchmarkManifest$' -test.v \
  -test.cpu=1 -spsc-manifest=sequential > /tmp/spsc-proof/sequential.export.log
```

Extract the single `BENCHMARK_MANIFEST ` JSON payload from each export log into its manifest
file. Keep the explicit test PASS in the original log. The protocol supplies exact expected row
files, including Go's CPU suffix when the requested CPU count exceeds one. Admit every selected
row before timing:

```bash
/tmp/spsc-proof/benchmeta rows --selection=all --cpu=2 \
  --manifest=/tmp/spsc-proof/concurrent.json --expected=/tmp/spsc-proof/concurrent.rows
/tmp/spsc-proof/benchmeta rows --selection=all --cpu=1 \
  --manifest=/tmp/spsc-proof/sequential.json --expected=/tmp/spsc-proof/sequential.rows
/tmp/spsc-proof/benchmeta finalize --snapshot=/tmp/spsc-proof/snapshot.json \
  --artifacts=/tmp/spsc-proof/artifacts.json --output=/tmp/spsc-proof/provenance.json \
  --report=/tmp/spsc-proof/finalize-report.json
/tmp/spsc-proof/benchmeta finalize --snapshot=/tmp/spsc-proof/snapshot.json \
  --artifacts=/tmp/spsc-proof/artifacts.json --output=/tmp/spsc-proof/provenance.json \
  --report=/tmp/spsc-proof/check-report.json --check-only
```

After every invocation, require successful process status and exact equality between observed
benchmark row names and the expected selection. Reject missing, extra, duplicate, or wrong-CPU
rows. A `-test.benchtime=1x` run of each selected row checks wiring only. Preserve raw output and
project a separate file before comparing canonical transfer metrics:

```bash
/tmp/spsc-proof/benchmeta project \
  --input=/tmp/spsc-proof/raw.txt --output=/tmp/spsc-proof/primary.txt
```

Projection removes raw concurrent RingAPI and SPSCAPI `ns/op`, requires exactly one finite,
nonnegative `ns/elem`, and preserves other metrics. It preserves concrete sequential and handoff
`ns/op` and latency metrics. Benchstat may present the shared API implementation in `.name` with
`/mode,/api,/batch,/elem` as rows. Concrete comparisons use `/impl` columns.

The parent's protocol also measures unchanged Ring with the original quick corpus before and
after this harness extension. Label this a harness/build control. Its compiled-test hashes differ.
Do not overlay SPSC tests on the old baseline or attribute this control's differences to a Ring
algorithm change. Existing quick and compare runners retain their selections and admission rules.
