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

A default run takes about 2.5 minutes. Measurement checkpoints, such as the run after each ring
round, use `-rounds 20`, which takes about 5 minutes.

Results go to `~/ring-quick/<timestamp>-<pid>/`, with the two binaries, every round's raw output,
the comparison, and a `state.txt` that records how the run was made. `-reuse` reruns the binaries
saved in an earlier results directory. The script only reads git state and never changes the
working tree, the index, refs, or remotes. The printed comparison leaves out the Handoff p99.9 and
max percentiles, because short runs make them very noisy, but the raw files keep them.

`ring-spin-bound.sh` compares one committed ref built with several publish wait spin bounds, the
`tailSpins` constant in `ring/ring.go`. It builds one binary per bound, with the first bound as the
base, runs the MPSC, SPMC, and MPMC throughput rows for every side in rotating order, and prints a
`benchstat` comparison. Run it to choose the spin bound after a change to the publish wait or to
the ring's layout.

```bash
scripts/ring-spin-bound.sh [-rounds N] [-benchtime D] <ref> <bound> <bound>...
scripts/ring-spin-bound.sh 8e3e968 256 64 512 1024
```

A default run of four bounds takes about 11 minutes on macOS and about 8 under WSL2. Results go
to `~/ring-spin/<timestamp>-<pid>/`. Like the quick bench, run it only while plugged into AC power.
