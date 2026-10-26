# Go data structures

Generic data structures and collection helpers for Go: fixed-capacity buffers, concurrent ring
queues, map-backed sets, and functions that complement the standard `maps` and `slices` packages.
Import the packages you need. There is no Go package at the module root.

## Packages

| Package | Use it for |
|---|---|
| [`buffer`](buffer/) | Reuse fixed-capacity storage and consume elements from either end. |
| [`ring/dpdk`](ring/dpdk/README.md) | Transfer values or batches, modeled on DPDK's `rte_ring`. |
| [`ring/frontier`](ring/README.md) | Transfer values or batches with non-waiting producers. |
| [`set`](set/) | Track membership and compute unions, intersections, and differences. |
| [`xmaps`](xmaps/) | Collect keys or values, invert a map, or merge maps into a new result. |
| [`xslices`](xslices/) | Remove duplicates, partition elements, and find or reverse elements. |

The library packages use only the Go standard library. Tests and the separate benchmark
module have additional dependencies.

## Install

Use Go 1.25.5 or later, as declared in [go.mod](go.mod). In your own Go module, add the package
you want:

```sh
go get github.com/freshdresch/go-data-structures/set
```

Replace `set` with `buffer`, `ring/dpdk`, `ring/frontier`, `xmaps`, or `xslices` for another
package. Import paths share
the prefix `github.com/freshdresch/go-data-structures/`.

## Example

This program uses five of the packages. It runs sequentially so that the output is reproducible.

```go
package main

import (
	"fmt"
	"log"

	"github.com/freshdresch/go-data-structures/buffer"
	"github.com/freshdresch/go-data-structures/ring/dpdk"
	"github.com/freshdresch/go-data-structures/set"
	"github.com/freshdresch/go-data-structures/xmaps"
	"github.com/freshdresch/go-data-structures/xslices"
)

func main() {
	pending := buffer.NewBuffer[int](4)
	pending.Append(10, 20, 30)
	pending.Consume(2)
	pending.Append(40, 50, 60)
	fmt.Println("buffer:", pending.Slice())

	queue, err := dpdk.NewSPSCQueue[string](4)
	if err != nil {
		log.Fatal(err)
	}
	if err := queue.Enqueue("job"); err != nil {
		log.Fatal(err)
	}
	job, err := queue.Dequeue()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("queue:", job)

	wanted := set.Of("read", "write")
	granted := set.Of("read")
	missing := wanted.Difference(granted)
	fmt.Println("missing write:", missing.Contains("write"))
	fmt.Println("wanted unchanged:", wanted.Len())

	defaults := map[string]int{"workers": 2}
	overrides := map[string]int{"workers": 4}
	config := xmaps.Merge(defaults, overrides)
	fmt.Println("workers:", config["workers"])
	byCount, err := xmaps.Invert(config)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("setting:", byCount[4])

	values := xslices.Unique([]int{3, 1, 3, 2, 1})
	odd, even := xslices.Partition(values, func(value int) bool {
		return value%2 != 0
	})
	fmt.Println("unique:", values)
	fmt.Println("odd:", odd, "even:", even)
}
```

Output:

```text
buffer: [30 40 50 60]
queue: job
missing write: true
wanted unchanged: 2
workers: 4
setting: workers
unique: [3 1 2]
odd: [3 1] even: [2]
```

## Buffers

[`Buffer[T]`](buffer/buffer.go) separates the live elements from the backing array's capacity.
`Consume` advances the front without copying. When an append needs more space at the end,
the buffer compacts live elements to the front. Capacity stays fixed. `Append` and `Grow` panic
if the result would exceed it.

Use `Grow` to reserve elements to fill directly, or `AvailableBuffer` to build a slice for an
immediately following append. `Grow` can expose stale values. `GrowAndZero` clears new elements.
Removal and reset methods also have `AndZero` variants. `CompactAndZero` moves the live elements
and clears all other previously written storage.

[`BytesBuffer`](buffer/bytes_buffer.go) adds `io.Reader` and `io.Writer` methods to a byte buffer.
Unlike a growing `bytes.Buffer`, it has fixed capacity. `Write` and `WriteString` append as much
as fits, then return the number written and `buffer.ErrFull` if the input did not fit.

[`Pool[T]`](buffer/pool.go) reuses buffers of one capacity. Construct it with `NewPool`.
`Get` returns an empty buffer and can allocate. With `zeroOnPut` set to `true`, `Put` also clears
previously written elements, including elements already removed. Without it, stale values can
remain in the backing array.

`Slice`, `Bytes`, and slices returned by `Grow` share the buffer's storage. Follow their documented
lifetimes and aliasing restrictions. After `Put`, stop using both the buffer and any slices into it.

## Ring queues

The [ring queues](ring/README.md) come in two packages with the same type names and methods.
[`ring/dpdk`](ring/dpdk/README.md) is modeled on DPDK's `rte_ring`: a returned enqueue's values
are visible to consumers, and DPDK's relaxed tail sync is available as an option.
`ring/frontier` lets producers publish without waiting for one another, which is much faster
for single-element transfers under contention, but a returned enqueue's values can stay invisible
while an earlier producer is still copying. `ring/frontier` implements `MPSCQueue` and
`SPMCQueue` so far.
Choose a type according to which sides your application serializes:

| Type | Producers | Consumers |
|---|---|---|
| `SPSCQueue[T]` | One at a time | One at a time |
| `SPMCQueue[T]` | One at a time | Concurrent |
| `MPSCQueue[T]` | Concurrent | One at a time |
| `MPMCQueue[T]` | Concurrent | Concurrent |

SPSC means single producer, single consumer. The other names substitute multiple producers,
multiple consumers, or both. Opposite sides can run concurrently in every type.
Use the matching `NewSPSCQueue`, `NewSPMCQueue`, `NewMPSCQueue`, or `NewMPMCQueue` constructor.
Capacity must be a power of two from 1 through `2^30`, and every slot is usable.

`Enqueue` and `Dequeue` transfer one value or report `ErrFull` and `ErrEmpty`, respectively.
Bulk methods transfer a whole batch or nothing. Burst methods transfer a prefix and report how
many values moved. Construction options control batch limits, slot clearing, counts, and
publication behavior on concurrent sides.

A call can wait internally for another call's progress. The queues have no close, cancellation,
or notification API. Read the [`ring/dpdk` guide](ring/dpdk/README.md) for ownership rules,
waiting behavior, options, and the limits of count snapshots before using a queue concurrently.

## Sets

[`Set[K comparable]`](set/set.go) stores unique elements in a Go map. Its zero value is ready to
use. `Of`, `FromSlice`, and `Collect` construct sets from values, slices, and iterators.

`Union`, `Intersection`, `Difference`, and `Disjunction` return new sets without modifying their
inputs. `Disjunction` is the symmetric difference: elements present in exactly one input.
`Update` adds another set in place. `DifferenceUpdate`, `IntersectionUpdate`, and
`DisjunctionUpdate` apply the corresponding operation in place.

Use `Contains` for membership, `IsSubset` and `IsSuperset` for containment, and `All` to iterate.
`All` and `ToSlice` have unspecified order. Sort a slice when order matters.
Do not copy a set after first use, and do not modify it while iterating with `All`.

## Map and slice helpers

[`xmaps`](xmaps/maps.go) complements the standard `maps` package:

- `Keys` and `Values` return new slices with capacity allocated for the source map's length.
  Their order is unspecified, as with map iteration.
- `Invert` swaps keys and values into a new map. If values repeat, it returns a nil map and an
  error that wraps `ErrDuplicateValue`. Match it with `errors.Is`.
- `Merge` returns a new map. When keys repeat, later input maps win.

[`xslices`](xslices/slices.go) complements the standard `slices` package:

- `Unique` removes all duplicates and preserves first-seen order.
- `Partition` returns matching and nonmatching elements, each in its original order.
- `Reversed` returns a reversed copy. `Reverse` changes the input in place.
- `IndexOf`, `LastIndexOf`, and `IndicesOf` find the first, last, or all matching positions.
  The first two return `-1` when an element is absent.

`Reverse`, `IndexOf`, and `Contains` delegate to the corresponding standard `slices` functions.
`Unique`, `Reversed`, and `Partition` return new slices and leave the input unchanged.
Copies of elements are shallow throughout these packages.

## Concurrency and ownership

Coordinate concurrent access to buffers, sets, and the maps and slices passed to helper functions.
`Pool.Get` and `Pool.Put` support concurrent callers, but an individual buffer still needs an owner
or external synchronization. Do not copy a pool after first use.

Construct ring queues before sharing them, and do not copy them after use. Synchronize handoffs of
a serialized producer or consumer side. `Reset` requires exclusive access against all queue use.
Values copied through a collection can still refer to shared objects that need synchronization.

## Development and benchmarks

From the repository root, run the library tests and race checks:

```sh
go test ./...
go test -race ./...
```

The comparison benchmarks live in a [separate module](benchmark/README.md). Run its tests and
lint checks separately. With `golangci-lint` installed, check both modules:

```sh
golangci-lint run
(cd benchmark && go test ./... && go test -race ./... && golangci-lint run)
```

Bazel is optional. [.bazelversion](.bazelversion) pins Bazel 9.3.1, and [MODULE.bazel](MODULE.bazel)
selects the Go SDK from `go.mod`. With Bazelisk installed, test the packages:

```sh
bazelisk test //buffer/... //ring/... //set/... //xmaps/... //xslices/...
```

The [benchmark methodology](benchmark/README.md) describes workloads and comparison limits.
The [scripts guide](scripts/README.md) covers benchmark runners and their requirements.
No third-party measurements or performance rankings for the current queue APIs are published yet.

Licensed under the [Apache License 2.0](LICENSE).
