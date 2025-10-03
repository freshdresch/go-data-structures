// package buffer provides a buffer primitive that encourages re-use of the same
// fixed length buffer, as opposed to the dynamic nature of slices.
package buffer

// Buffer is a slice-like structure that decouples length and capacity.
// It reuses backing memory across resets without reallocating.
type Buffer[T any] struct {
        data  []T
        len   int
}

// NewBuffer returns a new Buffer with the given capacity.
func NewBuffer[T any](cap int) *Buffer[T] {
        return &Buffer[T]{
                data: make([]T, cap),
        }
}

// Len returns the logical length.
func (b *Buffer[T]) Len() int {
        return b.len
}

// Cap returns the capacity.
func (b *Buffer[T]) Cap() int {
        return cap(b.data)
}

// Slice returns the initialized elements as a regular slice.
func (b *Buffer[T]) Slice() []T {
        return b.data[:b.len]
}

// Reset sets length back to zero.
func (b *Buffer[T]) Reset() {
        b.len = 0
}

// ResetAndClear wipes the memory being used and then resets the length to zero.
// Note: it only clears up the occupied length, not the entire backing memory.
func (b *Buffer[T]) ResetAndClear() {
        var zero T
        for i := 0; i < b.len; i++ {
                b.data[i] = zero
        }
}

// Append adds elements, growing length but not exceeding capacity.
func (b *Buffer[T]) Append(vals ...T) {
        if b.len + len(vals) > cap(b.data) {
                panic("buffer: append would exceed capacity")
        }
        copy(b.data[b.len:], vals)
        b.len += len(vals)
}

// Grow expands the logical length by n, returning a slice to fill.
func (b *Buffer[T]) Grow(n int) []T {
        if b.len+n > cap(b.data) {
                panic("buffer: grow would exceed capacity")
        }
        start := b.len
        end := b.len+n
        b.len = end
        return b.data[start:end]
}

// Truncate shrinks the logical length to n (must be <= current length).
func (b *Buffer[T]) Truncate(n int) {
        if n < 0 || n > b.len {
                panic("buffer: truncate out of range")
        }
        b.len = n
}

// TruncateAndClear shrinks the logical length to n (must be <= current length), and clears the
// entries that were removed by the shrinking.
func (b *Buffer[T]) TruncateAndClear(n int) {
        if n < 0 || n > b.len {
                panic("buffer: truncate out of range")
        }

        var zero T
        for i := n; i < b.len; i++ {
                b.data[i] = zero
        }

        b.len = n
}
