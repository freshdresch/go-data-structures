# SPMCQueue comparisons

The [results](#results) below come from one 10-round campaign. Crossbeam comparisons are future
work.

## Workload

This report covers 1 producer and 4 consumers, with 1024 usable queue slots.
The harness transfers `int64` values and pointers. The `ring-single` row uses `Enqueue` and
`Dequeue`. It compares with buffered Go channels, a mutex queue, xsync, and Workiva.
The `ring` and `ring-exact` rows provide burst context at batches 1 and 32.

The `ring-rts` row also measures relaxed tail synchronization on the concurrent sides.
The `frontier` and `frontier-single` rows measure `frontier.SPMCQueue`, whose consumers release
their slots without waiting for earlier consumers, through burst and single-element calls.

See [the methodology](../README.md) for dependency versions, timing boundaries, waiting behavior,
allocation caveats, capacity checks, and provenance requirements.

## Results

Measured at commit 38330b3 on an AMD Ryzen 7 5800X under WSL2, with Go 1.27.3, GOMAXPROCS 8, and
CPUs 0, 2, 4, 6, 8, 10, 12, and 14 (one per physical core). The campaign ran 10 rotating rounds
with a 200 ms benchtime. Values are median ns per transferred element, with the range across rounds
in parentheses.

| Row | Batch | `int64` | Pointer |
|---|---|---|---|
| `ring` | 1 | 88.5 (88.0–89.3) | 88.8 (88.4–89.3) |
| `ring-exact` | 1 | 100.0 (99.4–100.8) | 99.9 (99.0–100.5) |
| `ring-single` | 1 | 89.4 (88.7–90.1) | 88.7 (88.0–89.5) |
| `ring-rts` | 1 | 127.7 (110.4–134.1) | 127.3 (121.3–136.0) |
| `frontier` | 1 | 32.4 (31.9–32.5) | 34.5 (34.1–34.7) |
| `frontier-single` | 1 | 31.7 (31.6–32.0) | 31.6 (31.3–44.8) |
| `chan` | 1 | 43.9 (43.4–44.3) | 43.2 (42.1–44.3) |
| `mutex` | 1 | 70.2 (67.1–94.0) | 88.4 (60.9–90.7) |
| `xsync` | 1 | 25.0 (24.8–25.4) | 26.4 (26.3–26.5) |
| `workiva` | 1 | 24.9 (24.5–31.4) | 18.8 (18.6–19.2) |
| `ring` | 32 | 2.99 (2.88–3.04) | 2.95 (2.91–2.98) |
| `ring-exact` | 32 | 3.38 (3.36–3.45) | 3.77 (3.74–3.82) |
| `ring-rts` | 32 | 4.40 (4.36–4.54) | 4.23 (4.18–4.33) |
| `frontier` | 32 | 1.67 (1.65–1.80) | 2.66 (2.65–2.79) |

[The ring queue guide](../../ring/README.md#choosing-a-queue) discusses what these results mean
for choosing a queue.

## Run

From the repository root, run the full comparison, which includes the `SPMC` rows:

```bash
scripts/ring-compare-bench.sh -rounds 10 -benchtime 200ms -seed 12345
```

Read [the runner requirements](../README.md#run-the-comparison) before collecting timings.
Results must record the machine, toolchain, configuration, and uncertainty alongside
measurements from the same campaign.
