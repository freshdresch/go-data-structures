# MPSCQueue comparisons

No third-party measurements for the current `MPSCQueue` API are published yet. No performance
ranking is established. Crossbeam comparisons are future work.

## Workload

This report covers 4 producers and 1 consumer, with 1024 usable queue slots.
The harness transfers `int64` values and pointers. The `ring-single` row uses `Enqueue` and
`Dequeue`. It compares with buffered Go channels, a mutex queue, xsync, and Workiva.
The `ring` and `ring-exact` rows provide burst context at batches 1 and 32.

The `ring-rts` row also measures relaxed tail synchronization on the concurrent sides.
The `frontier` and `frontier-single` rows measure `frontier.MPSCQueue`, which publishes completed
ranges without waiting for earlier producers, through burst and single-element calls.

See [the methodology](../README.md) for dependency versions, timing boundaries, waiting behavior,
allocation caveats, capacity checks, and provenance requirements.

## Run

From the repository root, run the full comparison, which includes the `MPSC` rows:

```bash
scripts/ring-compare-bench.sh -rounds 10 -benchtime 200ms -seed 12345
```

Read [the runner requirements](../README.md#run-the-comparison) before collecting timings.
Future results must record the machine, toolchain, configuration, and uncertainty alongside
measurements from the same campaign.
