# SPSCQueue comparisons

The [results](#results) below come from one 10-round campaign. Crossbeam comparisons are future
work.

## Workload

This report covers 1 producer and 1 consumer, with 1024 usable queue slots.
The harness transfers `int64` values and pointers. The `ring-single` row uses `Enqueue` and
`Dequeue`. It compares with buffered Go channels, a mutex queue, xsync, and Workiva.
The `ring` and `ring-exact` rows provide burst context at batches 1 and 32.

See [the methodology](../README.md) for dependency versions, timing boundaries, waiting behavior,
allocation caveats, capacity checks, and provenance requirements.

## Results

Measured at commit 38330b3 on an AMD Ryzen 7 5800X under WSL2, with Go 1.27.3, GOMAXPROCS 8, and
CPUs 0, 2, 4, 6, 8, 10, 12, and 14 (one per physical core). The campaign ran 10 rotating rounds
with a 200 ms benchtime. Values are median ns per transferred element, with the range across rounds
in parentheses.

| Row | Batch | `int64` | Pointer |
|---|---|---|---|
| `ring` | 1 | 10.5 (10.3–10.8) | 11.1 (10.8–11.3) |
| `ring-exact` | 1 | 32.4 (29.6–32.6) | 31.2 (30.1–32.1) |
| `ring-single` | 1 | 10.7 (10.4–10.9) | 11.3 (11.1–11.6) |
| `chan` | 1 | 31.6 (30.6–31.9) | 31.7 (31.3–32.5) |
| `mutex` | 1 | 21.4 (20.6–22.3) | 22.6 (21.5–23.1) |
| `xsync` | 1 | 8.94 (8.74–9.28) | 9.88 (9.55–10.3) |
| `workiva` | 1 | 19.7 (19.5–19.9) | 10.1 (10.00–14.2) |
| `ring` | 32 | 1.41 (1.39–1.44) | 2.57 (2.55–2.62) |
| `ring-exact` | 32 | 1.98 (1.95–2.13) | 2.68 (2.51–2.87) |

[The ring queue guide](../../ring/README.md#choosing-a-queue) discusses what these results mean
for choosing a queue.

## Run

From the repository root, run the full comparison, which includes the `SPSC` rows:

```bash
scripts/ring-compare-bench.sh -rounds 10 -benchtime 200ms -seed 12345
```

Read [the runner requirements](../README.md#run-the-comparison) before collecting timings.
Results must record the machine, toolchain, configuration, and uncertainty alongside
measurements from the same campaign.
