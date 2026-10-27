# MPMCQueue comparisons

The [results](#results) below come from one 10-round campaign. Crossbeam comparisons are future
work.

## Workload

This report covers 4 producers and 4 consumers, with 1024 usable queue slots.
The harness transfers `int64` values and pointers. The `ring-single` row uses `Enqueue` and
`Dequeue`. It compares with buffered Go channels, a mutex queue, xsync, and Workiva.
The `ring` and `ring-exact` rows provide burst context at batches 1 and 32.

The `ring-rts` row also measures relaxed tail synchronization on the concurrent sides.
The `frontier` and `frontier-single` rows measure `frontier.MPMCQueue`, whose producers and
consumers never wait for earlier callers on their side, through burst and single-element calls.

See [the methodology](../README.md) for dependency versions, timing boundaries, waiting behavior,
allocation caveats, capacity checks, and provenance requirements.

## Results

Measured at commit 38330b3 on an AMD Ryzen 7 5800X under WSL2, with Go 1.27.3, GOMAXPROCS 8, and
CPUs 0, 2, 4, 6, 8, 10, 12, and 14 (one per physical core). The campaign ran 10 rotating rounds
with a 200 ms benchtime. Values are median ns per transferred element, with the range across rounds
in parentheses.

| Row | Batch | `int64` | Pointer |
|---|---|---|---|
| `ring` | 1 | 80.4 (77.3–81.1) | 81.1 (80.1–81.8) |
| `ring-exact` | 1 | 150.3 (142.3–153.1) | 149.9 (148.7–151.1) |
| `ring-single` | 1 | 79.9 (78.8–86.9) | 81.0 (79.2–88.1) |
| `ring-rts` | 1 | 120.2 (117.1–122.0) | 119.5 (115.7–123.8) |
| `frontier` | 1 | 24.4 (23.7–28.9) | 32.2 (31.7–32.6) |
| `frontier-single` | 1 | 30.8 (29.9–31.6) | 36.8 (36.3–37.0) |
| `chan` | 1 | 49.6 (49.2–49.8) | 49.6 (47.9–50.8) |
| `mutex` | 1 | 62.0 (61.7–63.0) | 64.2 (63.3–70.6) |
| `xsync` | 1 | 18.0 (17.4–18.5) | 19.7 (18.8–20.1) |
| `workiva` | 1 | 30.0 (29.8–31.3) | 18.4 (18.1–19.0) |
| `ring` | 32 | 3.14 (3.06–3.21) | 3.01 (2.86–3.11) |
| `ring-exact` | 32 | 4.16 (4.13–4.26) | 4.11 (4.08–4.15) |
| `ring-rts` | 32 | 4.21 (4.16–4.35) | 4.18 (4.13–4.29) |
| `frontier` | 32 | 2.01 (1.98–2.02) | 2.59 (2.56–2.64) |

[The ring queue guide](../../ring/README.md#choosing-a-queue) discusses what these results mean
for choosing a queue.

## Run

From the repository root, run the full comparison, which includes the `MPMC` rows:

```bash
scripts/ring-compare-bench.sh -rounds 10 -benchtime 200ms -seed 12345
```

Read [the runner requirements](../README.md#run-the-comparison) before collecting timings.
Results must record the machine, toolchain, configuration, and uncertainty alongside
measurements from the same campaign.
