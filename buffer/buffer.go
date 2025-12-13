// package buffer provides a different buffer primitive that encourages reuse of the same
// fixed-length buffer, as opposed to the dynamic nature of slices.
package buffer

import (
	"fmt"
	"strings"
)

// Buffer is a slice-like structure that decouples length and capacity,
// with a movable front offset so you can consume from the front without copying.
type Buffer[T any] struct {
	data  []T
	start int // index of first initialized element
	len   int // logical length
}

// NewBuffer creates a Buffer with the given capacity.
func NewBuffer[T any](cap int) *Buffer[T] {
	if cap < 0 {
		panic("buffer: negative capacity")
	}
	return &Buffer[T]{
		data: make([]T, cap),
	}
}

// Len returns the logical length (number of initialized items).
func (b *Buffer[T]) Len() int {
	return b.len
}

// Cap returns the backing capacity.
func (b *Buffer[T]) Cap() int {
	return cap(b.data)
}

// Slice returns the initialized elements as a single contiguous slice.
// It is always safe (cannot reference beyond Len).
func (b *Buffer[T]) Slice() []T {
	return b.data[b.start : b.start+b.len]
}

// Reset resets `start` to 0, but does not take the performance hit of zeroing out memory.
func (b *Buffer[T]) Reset() {
	b.start = 0
	b.len = 0
}

// ResetAndZero zeroes out the content currently in the buffer and resets `start` to 0.
func (b *Buffer[T]) ResetAndZero() {
	var zero T
	for i := 0; i < b.len; i++ {
		b.data[b.start+i] = zero
	}

	b.start = 0
	b.len = 0
}

// Truncate shrinks the logical length to n (0 <= n <= Len()).
func (b *Buffer[T]) Truncate(n int) {
	if n < 0 || n > b.len {
		panic("buffer: truncate out of range")
	}

	b.len = n
	if b.len == 0 {
		// move start back to zero to keep things tidy
		b.start = 0
	}
}

// TruncateAndZero shrinks the logical length to n (0 <= n <= Len()) and zeroes any elements that
// were truncated out.
func (b *Buffer[T]) TruncateAndZero(n int) {
	if n < 0 || n > b.len {
		panic("buffer: truncate out of range")
	}

	var zero T
	for i := n; i < b.len; i++ {
		b.data[b.start+i] = zero
	}

	b.len = n
	if b.len == 0 {
		b.start = 0
	}
}

// Consume advances the front by n (removes first n items) without copying.
func (b *Buffer[T]) Consume(n int) {
	if n < 0 || n > b.len {
		panic("buffer: consume out of range")
	}

	b.start += n
	b.len -= n
	if b.len == 0 {
		// collapse offset when empty to avoid unbounded growth of start
		b.start = 0
	}
}

// ConsumeAndZero advances the front by n (removes first n items) without copying, and zeroes the
// elements that were consumed.
func (b *Buffer[T]) ConsumeAndZero(n int) {
	if n < 0 || n > b.len {
		panic("buffer: consume out of range")
	}

	var zero T
	for i := 0; i < n; i++ {
		b.data[b.start+i] = zero
	}

	b.start += n
	b.len -= n
	if b.len == 0 {
		b.start = 0
	}
}

// ConsumeRight removes n elements from the end of the live region.
// It panics if n > b.len.
func (b *Buffer[T]) ConsumeRight(n int) {
	if n < 0 || n > b.len {
		panic("buffer: consume out of range")
	}
	b.len -= n
}

// ConsumeRightAndZero removes n elements from the end of the live region
// and zeroes the removed portion of the backing array.
// It panics if n > b.len.
func (b *Buffer[T]) ConsumeRightAndZero(n int) {
	if n < 0 || n > b.len {
		panic("buffer: consume out of range")
	}

	zeroStart := b.start + (b.len - n)
	var zero T
	for i := 0; i < n; i++ {
		b.data[zeroStart+i] = zero
	}
	b.len -= n
}

// Append adds vals onto the end of the buffer.
//
// Important: it will compact (move live region to index 0) if tail space is insufficient but total
// free space is enough. It panics if capacity would be exceeded.
func (b *Buffer[T]) Append(vals ...T) {
	n := len(vals)
	if n == 0 {
		return
	}

	if b.len+n > cap(b.data) {
		panic("buffer: append would exceed capacity")
	}

	tail := cap(b.data) - (b.start + b.len)
	if tail < n {
		// compact to create tail space
		b.Compact()
	}

	copy(b.data[b.start+b.len:], vals)
	b.len += n
}

// Grow increases logical length by n and returns a slice to fill.
// It panics if capacity would be exceeded.
func (b *Buffer[T]) Grow(n int) []T {
	return b.grow(n, false)
}

// GrowAndZero increases logical length by n, zeroes that memory, and then returns it as a slice to
// fill. It panics if capacity would be exceeded.
func (b *Buffer[T]) GrowAndZero(n int) []T {
	return b.grow(n, true)
}

func (b *Buffer[T]) grow(n int, shouldZero bool) []T {
	if n < 0 {
		panic("buffer: negative grow")
	} else if b.len+n > cap(b.data) {
		panic("buffer: grow would exceed capacity")
	}

	shouldCompact := false
	tail := cap(b.data) - (b.start + b.len)
	if tail < n {
		shouldCompact = true
	}

	if shouldCompact {
		switch shouldZero {
		case true:
			b.CompactAndZero()
		case false:
			b.Compact()
		}
	}

	start := b.start + b.len
	end := start + n
	b.len += n

	// If we didn't do a compact, then we still need to zero the entries
	if shouldZero && !shouldCompact {
		var zero T
		for i := start; i < end; i++ {
			b.data[i] = zero
		}
	}

	return b.data[start:end]
}

// Compact force-moves the live region to the front (index 0).
func (b *Buffer[T]) Compact() {
	if b.start == 0 || b.len == 0 {
		// if len == 0 we still want to ensure start = 0
		b.start = 0
		return
	}

	oldStart := b.start
	oldLen := b.len
	copy(b.data[0:oldLen], b.data[oldStart:oldStart+oldLen])
	b.start = 0
}

// CompactAndZero force-moves the live region to the front (index 0), and zeroes out the previous live region.
func (b *Buffer[T]) CompactAndZero() {
	if b.start == 0 || b.len == 0 {
		// if len == 0 we still want to ensure start = 0
		b.start = 0
		return
	}

	oldStart := b.start
	oldLen := b.len
	copy(b.data[0:oldLen], b.data[oldStart:oldStart+oldLen])

	// Zero everything after the new live region
	var zero T
	for i := oldLen; i < cap(b.data); i++ {
		b.data[i] = zero
	}
	b.start = 0
}

// String implements fmt.Stringer for debugging and diagnostics.
func (b *Buffer[T]) String() string {
	// Handle nil receiver gracefully
	if b == nil {
		return "<nil Buffer>"
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Buffer(len=%d, cap=%d, start=%d)\n", b.len, cap(b.data), b.start)

	if len(b.data) == 0 {
		sb.WriteString("  data: []\n")
		return sb.String()
	}

	// Mark the buffer visually, showing where live elements are.
	sb.WriteString("  data: [")

	for i := range b.data {
		var marker string
		if i >= b.start && i < b.start+b.len {
			// Live element region
			marker = "*"
		} else {
			marker = " "
		}

		// For printing each element safely, use %v
		fmt.Fprintf(&sb, "%s%v", marker, b.data[i])

		if i != len(b.data)-1 {
			sb.WriteString(", ")
		}
	}

	sb.WriteString("]\n")
	sb.WriteString("         ^ live region marked with '*'\n")
	return sb.String()
}

// GoString implements a more concise representation for the %#v format specifier.
func (b *Buffer[T]) GoString() string {
	return fmt.Sprintf(
		"Buffer{start:%d, len:%d, cap:%d, slice:%v}",
		b.start, b.len, cap(b.data), b.Slice(),
	)
}
