# MPSCQueue comparisons

The [results](#results) below come from one 10-round campaign. Crossbeam comparisons are future
work.

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

## Results

Measured at commit 38330b3 on an AMD Ryzen 7 5800X under WSL2, with Go 1.27.3, GOMAXPROCS 8, and
CPUs 0, 2, 4, 6, 8, 10, 12, and 14 (one per physical core). The campaign ran 10 rotating rounds
with a 200 ms benchtime. Values are median ns per transferred element, with the range across rounds
in parentheses.

| Row | Batch | `int64` | Pointer |
|---|---|---|---|
| `ring` | 1 | 87.1 (86.8–87.6) | 78.4 (77.8–79.0) |
| `ring-exact` | 1 | 98.3 (96.8–100.9) | 102.5 (101.4–103.8) |
| `ring-single` | 1 | 87.7 (87.2–88.1) | 77.3 (76.8–78.1) |
| `ring-rts` | 1 | 114.0 (111.2–117.0) | 115.5 (112.6–117.6) |
| `frontier` | 1 | 32.2 (31.6–33.8) | 31.4 (30.4–34.5) |
| `frontier-single` | 1 | 31.4 (30.9–32.4) | 33.4 (32.4–34.7) |
| `chan` | 1 | 43.7 (43.4–44.6) | 44.0 (43.0–45.4) |
| `mutex` | 1 | 41.4 (38.0–46.1) | 42.9 (39.5–43.4) |
| `xsync` | 1 | 29.3 (29.1–29.6) | 30.7 (30.3–41.5) |
| `workiva` | 1 | 35.6 (35.4–35.9) | 21.8 (21.4–22.4) |
| `ring` | 32 | 2.70 (2.60–2.71) | 2.03 (1.80–2.15) |
| `ring-exact` | 32 | 2.75 (2.72–2.78) | 2.66 (2.64–2.69) |
| `ring-rts` | 32 | 4.09 (4.05–4.12) | 3.99 (3.90–4.04) |
| `frontier` | 32 | 1.86 (1.83–1.90) | 2.03 (2.01–2.04) |

[The ring queue guide](../../ring/README.md#choosing-a-queue) discusses what these results mean
for choosing a queue.

## Run

From the repository root, run the full comparison, which includes the `MPSC` rows:

```bash
scripts/ring-compare-bench.sh -rounds 10 -benchtime 200ms -seed 12345
```

Read [the runner requirements](../README.md#run-the-comparison) before collecting timings.
Results must record the machine, toolchain, configuration, and uncertainty alongside
measurements from the same campaign.
