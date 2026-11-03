# buffer

`buffer` provides fixed-capacity buffers that you reuse instead of reallocating.
`Buffer[T]` keeps its live elements in a window of a fixed backing array. You can append at the
end and consume from either end without copying. `BytesBuffer` adds `io.Reader` and `io.Writer`
methods for bytes, and `Pool[T]` recycles buffers of one capacity.

```sh
go get github.com/freshdresch/go-data-structures/buffer
```

## Example

```go
package main

import (
	"fmt"

	"github.com/freshdresch/go-data-structures/buffer"
)

func main() {
	pending := buffer.NewBuffer[int](4)
	pending.Append(10, 20, 30)
	pending.Consume(2) // Removes 10 and 20 without copying.

	// Only one slot is free at the end, so Append moves 30 to the front first.
	pending.Append(40, 50, 60)
	fmt.Println(pending.Slice())
}
```

Output:

```text
[30 40 50 60]
```

## How a buffer works

A `Buffer` tracks a start offset and a length inside a backing array of size `Cap()`.
`Consume` advances the start, and `ConsumeRight` and `Truncate` shorten the length. Neither moves
data. When `Append` or `Grow` needs more room at the end than remains, but the total free space is
enough, the buffer compacts. That is, it moves the live elements to index 0. Capacity never
changes. `Append` and `Grow` panic if the result would exceed `Cap()`.

The methods without `AndZero` in their names leave removed elements in the backing array. Those
stale values keep any referenced memory alive, and `Grow` can expose them. Use an `AndZero`
variant when `T` holds pointers or sensitive data.

A `Buffer` is not safe for concurrent use. A `Pool` is safe for concurrent use.

## Aliasing rules

`Slice`, `Bytes`, `Grow`, and `GrowAndZero` return slices that share the buffer's storage.

- Each slice is valid only until the next call that may compact the buffer.
- Each slice's capacity equals its length, so `append` on it never writes into the buffer.
- Do not pass a slice that shares the backing array to `Append` or `Write`. A compaction can
  overwrite it before it is copied. The slice from `AvailableBuffer` is the exception.
- After `Pool.Put`, stop using the buffer and every slice into it. Another goroutine may get the
  buffer next.

## API

### Buffer[T]

`NewBuffer[T](capacity)` returns an empty buffer. It panics if `capacity` is negative.

| Method | Effect |
|---|---|
| `Len()`, `Cap()`, `Available()` | Live elements, backing capacity, and `Cap() - Len()`. |
| `Slice()` | The live elements. The slice shares the buffer's storage. |
| `Append(values...)` | Adds values at the end. It compacts first if the end lacks room. |
| `Grow(count)` | Adds `count` elements at the end and returns them to fill. They may hold stale values. |
| `AvailableBuffer()` | An empty slice over the free space at the end, for building values to pass to the next `Append`. It does not compact. |
| `Consume(count)` | Removes `count` elements from the front. |
| `ConsumeRight(count)` | Removes `count` elements from the end. |
| `Truncate(newLength)` | Shortens the buffer to `newLength`. |
| `Reset()` | Empties the buffer. |
| `Compact()` | Moves the live elements to index 0. |

`Grow`, `Consume`, `ConsumeRight`, `Truncate`, `Reset`, and `Compact` each have an `AndZero`
variant. `GrowAndZero` returns zeroed elements. The removal variants zero the elements they
remove. `CompactAndZero` also zeroes every element outside the live region that the buffer ever
wrote. Its cost is proportional to how much of the buffer was ever written, not to `Cap()`.

Removal methods panic if `count` or `newLength` is negative or greater than `Len()`.
`String` and `GoString` print diagnostic views of the buffer.

### BytesBuffer

`NewBytesBuffer(capacity)` returns an empty `BytesBuffer`. It embeds `*Buffer[byte]`, so every
`Buffer` method is available.

| Method | Effect |
|---|---|
| `Write(p)`, `WriteString(text)` | Appends as much as fits. If the input does not fit, it returns the count written and `ErrFull`. |
| `WriteByte(value)` | Appends one byte, or returns `ErrFull`. |
| `Read(p)` | Copies bytes into `p` and consumes them. It returns `io.EOF` when the buffer is empty. |
| `Bytes()` | The live bytes. The slice shares the buffer's storage. |
| `String()` | A copy of the live bytes as a string. |

Unlike `bytes.Buffer`, a `BytesBuffer` never grows. `ErrFull` and `io.EOF` are returned unwrapped.

### Pool[T]

`NewPool[T](capacity, zeroOnPut)` returns a pool of buffers with that capacity, backed by
`sync.Pool`. The zero value of `Pool` is not usable.

| Method | Effect |
|---|---|
| `Get()` | Returns an empty buffer of the pool's capacity. It may allocate. |
| `Put(released)` | Resets the buffer and may keep it for reuse. `Put(nil)` does nothing. |

If `zeroOnPut` is true, `Put` zeroes every element the buffer ever held, including elements
already removed. Without it, a buffer from `Get` can hold stale values that `Grow` exposes.
`Put` discards a buffer whose capacity differs from the pool's.
