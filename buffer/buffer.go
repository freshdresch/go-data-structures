// Package buffer provides fixed-capacity buffers that are reused instead of reallocated.
package buffer

import (
	"fmt"
	"strings"
)

// Buffer is a slice-like structure that decouples length and capacity, with a movable front
// offset so you can consume from the front without copying.
//
// CompactAndZero, GrowAndZero when it compacts, and a Pool's zeroOnPut cost time in proportion
// to how much of the buffer was ever written, not to Cap(). Using AvailableBuffer counts as
// writing up to Cap(). An empty Buffer always has start 0, which keeps offsets bounded.
type Buffer[T any] struct {
	data   []T
	start  int // index of first initialized element
	length int // logical length
	// highWater bounds the written region. Every element at an index at or above highWater is
	// zero, and highWater <= cap(data). Append, Grow, and GrowAndZero raise it to the end of the
	// region they write, and AvailableBuffer raises it to cap(data) because the caller may write
	// anywhere in the free space it hands out. Only full wipes lower it. CompactAndZero lowers it
	// to Len(), and resetAndZeroAll lowers it to 0. Every other method leaves it unchanged, which
	// is safe because it is only an upper bound.
	highWater int
}

// NewBuffer returns an empty Buffer with the given capacity. It panics if capacity is negative.
func NewBuffer[T any](capacity int) *Buffer[T] {
	if capacity < 0 {
		panic("buffer: negative capacity")
	}
	return &Buffer[T]{
		data: make([]T, capacity),
	}
}

// Len returns the number of initialized elements in the buffer.
func (b *Buffer[T]) Len() int {
	return b.length
}

// Cap returns the capacity of the backing array.
func (b *Buffer[T]) Cap() int {
	return cap(b.data)
}

// Available returns the number of elements that can be added without exceeding Cap().
func (b *Buffer[T]) Available() int {
	return cap(b.data) - b.length
}

// AvailableBuffer returns an empty slice whose capacity is the free space after the live
// elements. It is meant to be appended to and passed to an immediately following Append, or
// Write for BytesBuffer. The slice is valid only until the next call that modifies the buffer.
//
// It does not compact, so call Compact first to make all free space available. After it is used,
// CompactAndZero and a Pool with zeroOnPut zero up to Cap().
//
//	dst := b.AvailableBuffer()
//	dst = strconv.AppendInt(dst, 42, 10)
//	b.Append(dst...) // BytesBuffer: b.Write(dst)
func (b *Buffer[T]) AvailableBuffer() []T {
	end := b.start + b.length
	b.raiseHighWater(cap(b.data))
	return b.data[end:end]
}

// Slice returns the initialized elements as a slice. The slice shares memory with the buffer,
// so later changes to the buffer are visible through it. Its capacity equals its length, so
// append on it never writes into the buffer. It is valid only until the next call that may
// compact the buffer.
func (b *Buffer[T]) Slice() []T {
	return b.data[b.start : b.start+b.length : b.start+b.length]
}

// Reset sets Len() to 0 without zeroing the old elements.
func (b *Buffer[T]) Reset() {
	b.start = 0
	b.length = 0
}

// ResetAndZero sets Len() to 0 and zeroes the elements that were in the buffer.
func (b *Buffer[T]) ResetAndZero() {
	clear(b.data[b.start : b.start+b.length])
	b.start = 0
	b.length = 0
}

// Truncate shrinks Len() to newLength without zeroing the removed elements.
// It panics if newLength is negative or greater than Len().
func (b *Buffer[T]) Truncate(newLength int) {
	b.checkCount(newLength, "truncate")
	b.length = newLength
	b.resetStartIfEmpty()
}

// TruncateAndZero shrinks Len() to newLength and zeroes the elements that were removed.
// It panics if newLength is negative or greater than Len().
func (b *Buffer[T]) TruncateAndZero(newLength int) {
	b.checkCount(newLength, "truncate")
	clear(b.data[b.start+newLength : b.start+b.length])
	b.length = newLength
	b.resetStartIfEmpty()
}

// Consume removes the first count elements without copying and without zeroing them.
// It panics if count is negative or greater than Len().
func (b *Buffer[T]) Consume(count int) {
	b.checkCount(count, "consume")
	b.start += count
	b.length -= count
	b.resetStartIfEmpty()
}

// ConsumeAndZero removes the first count elements without copying, and zeroes them.
// It panics if count is negative or greater than Len().
func (b *Buffer[T]) ConsumeAndZero(count int) {
	b.checkCount(count, "consume")
	clear(b.data[b.start : b.start+count])
	b.start += count
	b.length -= count
	b.resetStartIfEmpty()
}

// ConsumeRight removes the last count elements without zeroing them.
// It panics if count is negative or greater than Len().
func (b *Buffer[T]) ConsumeRight(count int) {
	b.checkCount(count, "consume")
	b.Truncate(b.length - count)
}

// ConsumeRightAndZero removes the last count elements and zeroes them.
// It panics if count is negative or greater than Len().
func (b *Buffer[T]) ConsumeRightAndZero(count int) {
	b.checkCount(count, "consume")
	b.TruncateAndZero(b.length - count)
}

// Append adds values to the end of the buffer.
//
// It compacts the buffer, moving the live elements to index 0, when the tail space is
// insufficient but the total free space is enough. It panics if the result would exceed Cap().
// Values must not share memory with the buffer's backing array, such as a slice from Slice or
// Grow, because a compaction may overwrite it before it is copied. A slice returned by
// AvailableBuffer is the exception, since it lies past the live elements.
func (b *Buffer[T]) Append(values ...T) {
	count := len(values)
	if count == 0 {
		return
	}

	if b.length+count > cap(b.data) {
		panic("buffer: append would exceed capacity")
	}

	tail := cap(b.data) - (b.start + b.length)
	if tail < count {
		b.Compact()
	}

	copy(b.data[b.start+b.length:], values)
	b.length += count
	b.raiseHighWater(b.start + b.length)
}

// Grow increases Len() by count and returns the new elements as a slice to fill. The new
// elements are not zeroed, so they may hold stale values. It panics if count is negative or
// if the result would exceed Cap(). The returned slice shares memory with the buffer. Its
// capacity equals its length, so append on it never writes into the buffer. It is valid only
// until the next call that may compact the buffer.
func (b *Buffer[T]) Grow(count int) []T {
	return b.grow(count, false)
}

// GrowAndZero increases Len() by count and returns the new elements, zeroed, as a slice to fill.
// It panics if count is negative or if the result would exceed Cap(). The returned slice shares
// memory with the buffer. Its capacity equals its length, so append on it never writes into the
// buffer. It is valid only until the next call that may compact the buffer.
func (b *Buffer[T]) GrowAndZero(count int) []T {
	return b.grow(count, true)
}

func (b *Buffer[T]) grow(count int, shouldZero bool) []T {
	if count < 0 {
		panic("buffer: negative grow")
	}
	if b.length+count > cap(b.data) {
		panic("buffer: grow would exceed capacity")
	}

	tail := cap(b.data) - (b.start + b.length)
	if tail < count {
		if shouldZero {
			b.CompactAndZero()
		} else {
			b.Compact()
		}
	}

	start := b.start + b.length
	end := start + count
	b.length += count

	if shouldZero {
		clear(b.data[start:end])
	}

	b.raiseHighWater(end)
	return b.data[start:end:end]
}

// Compact moves the live elements to the front of the backing array, so that start is 0.
// It moves nothing when the buffer is empty or already starts at 0.
func (b *Buffer[T]) Compact() {
	if b.start == 0 || b.length == 0 {
		b.start = 0
		return
	}

	oldStart := b.start
	oldLength := b.length
	copy(b.data[0:oldLength], b.data[oldStart:oldStart+oldLength])
	b.start = 0
}

// CompactAndZero moves the live elements to the front of the backing array.
// Afterward the live elements start at index 0 and every other element is zero, whether or not
// anything moved. The cost is proportional to how much of the buffer was ever written, not to
// Cap().
func (b *Buffer[T]) CompactAndZero() {
	if b.start > 0 && b.length > 0 {
		copy(b.data[0:b.length], b.data[b.start:b.start+b.length])
	}
	b.start = 0

	if b.highWater > b.length {
		clear(b.data[b.length:b.highWater])
	}
	b.highWater = b.length
}

// resetAndZeroAll sets Len() to 0 and zeroes every element ever written, including ones removed
// by non-zeroing methods such as Consume, Truncate, and ConsumeRight.
func (b *Buffer[T]) resetAndZeroAll() {
	clear(b.data[:b.highWater])
	b.start = 0
	b.length = 0
	b.highWater = 0
}

// raiseHighWater raises the high-water mark to end when end is greater. Callers pass the
// exclusive end of the region they wrote.
func (b *Buffer[T]) raiseHighWater(end int) {
	if end > b.highWater {
		b.highWater = end
	}
}

// checkCount panics with a message naming operation if count is negative or greater than Len().
func (b *Buffer[T]) checkCount(count int, operation string) {
	if count < 0 || count > b.length {
		panic("buffer: " + operation + " out of range")
	}
}

// resetStartIfEmpty sets start to 0 when the buffer is empty, so offsets stay bounded as the
// buffer is repeatedly consumed and refilled.
func (b *Buffer[T]) resetStartIfEmpty() {
	if b.length == 0 {
		b.start = 0
	}
}

// String returns a diagnostic view of the buffer that marks the live elements with '*'.
// It returns "<nil Buffer>" for a nil receiver.
func (b *Buffer[T]) String() string {
	if b == nil {
		return "<nil Buffer>"
	}

	var builder strings.Builder
	fmt.Fprintf(&builder, "Buffer(len=%d, cap=%d, start=%d)\n", b.length, cap(b.data), b.start)

	if len(b.data) == 0 {
		builder.WriteString("  data: []\n")
		return builder.String()
	}

	builder.WriteString("  data: [")

	for i := range b.data {
		var marker string
		isLive := i >= b.start && i < b.start+b.length
		if isLive {
			marker = "*"
		} else {
			marker = " "
		}

		fmt.Fprintf(&builder, "%s%v", marker, b.data[i])

		if i != len(b.data)-1 {
			builder.WriteString(", ")
		}
	}

	builder.WriteString("]\n")
	builder.WriteString("         ^ live region marked with '*'\n")
	return builder.String()
}

// GoString returns a concise representation of the buffer for the %#v format verb.
func (b *Buffer[T]) GoString() string {
	return fmt.Sprintf(
		"Buffer{start:%d, len:%d, cap:%d, slice:%v}",
		b.start, b.length, cap(b.data), b.Slice(),
	)
}
