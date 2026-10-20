# Scripts

`ring-quick-bench.sh` compares the ring benchmarks of two committed refs in a couple of minutes.
It builds each ref's test binary, runs six fixed throughput and latency rows for both sides in
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

Results go to `~/ring-quick/<timestamp>-<pid>/`, with the two binaries, every round's raw output,
the comparison, and a `state.txt` that records how the run was made. `-reuse` reruns the binaries
saved in an earlier results directory. The script only reads git state and never changes the
working tree, the index, refs, or remotes. The printed comparison leaves out the Handoff p99.9 and
max percentiles, because short runs make them very noisy, but the raw files keep them.
