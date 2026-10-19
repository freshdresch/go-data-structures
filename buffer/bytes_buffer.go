package buffer

import (
	"errors"
	"io"
)

// ErrFull is returned unwrapped by Write, WriteString, and WriteByte when the buffer cannot hold
// all of the input. Write and WriteString still append as much of the input as fits.
var ErrFull = errors.New("buffer: full")

// BytesBuffer is a specialization of Buffer[byte] with convenience methods for I/O-like usage.
type BytesBuffer struct {
	*Buffer[byte]
}

// NewBytesBuffer returns an empty BytesBuffer with the given capacity. It panics if capacity is
// negative.
func NewBytesBuffer(capacity int) *BytesBuffer {
	return &BytesBuffer{Buffer: NewBuffer[byte](capacity)}
}

// Write appends p to the buffer and returns the number of bytes appended. If the buffer cannot
// hold all of p, it appends as much as fits and returns that count with ErrFull. p must not
// share memory with the buffer's backing array, such as a slice from Bytes, except a slice
// returned by AvailableBuffer. It satisfies io.Writer.
func (b *BytesBuffer) Write(p []byte) (int, error) {
	available := b.Available()
	if available < len(p) {
		b.Append(p[:available]...)
		return available, ErrFull
	}
	b.Append(p...)
	return len(p), nil
}

// WriteString appends text to the buffer. It behaves like Write, including its ErrFull result.
func (b *BytesBuffer) WriteString(text string) (int, error) {
	return b.Write([]byte(text))
}

// WriteByte appends value to the buffer. It returns ErrFull when the buffer is full.
func (b *BytesBuffer) WriteByte(value byte) error {
	if b.Available() == 0 {
		return ErrFull
	}
	b.Append(value)
	return nil
}

// Read reads up to len(p) bytes into p and removes them from the front of the buffer. It
// returns (0, nil) when len(p) is 0, and io.EOF, unwrapped, when the buffer is empty.
// It satisfies io.Reader.
func (b *BytesBuffer) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.Len() == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.Slice())
	b.Consume(n)
	return n, nil
}

// Bytes returns the live bytes as a slice that shares memory with the buffer. Its capacity
// equals its length, so append on it never writes into the buffer. See Slice.
func (b *BytesBuffer) Bytes() []byte {
	return b.Slice()
}

// String returns a copy of the live bytes as a string.
func (b *BytesBuffer) String() string {
	return string(b.Slice())
}
