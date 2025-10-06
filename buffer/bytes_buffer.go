// package buffer provides a buffer primitive that encourages re-use of the same
// fixed length buffer, as opposed to the dynamic nature of slices.
package buffer

import "io"

// BytesBuffer is a specialization of Buffer[byte] with convenience methods for I/O-like usage.
type BytesBuffer struct {
        *Buffer[byte]
}

// NewBytesBuffer creates a new BytesBuffer with the given capacity and options.
func NewBytesBuffer(cap int) *BytesBuffer {
        return &BytesBuffer{Buffer: NewBuffer[byte](cap)}
}

// Write appends bytes (satisfies io.Writer).
func (b *BytesBuffer) Write(p []byte) (int, error) {
        b.Append(p...)
        return len(p), nil
}

// WriteByte appends a single byte.
func (b *BytesBuffer) WriteByte(c byte) error {
        b.Append(c)
        return nil
}

// WriteString appends a string (like bytes.Buffer.WriteString).
func (b *BytesBuffer) WriteString(s string) (int, error) {
        b.Append([]byte(s)...)
        return len(s), nil
}

// Read reads up to len(p) bytes into p, consuming from the front.
// It behaves like io.Reader.
func (b *BytesBuffer) Read(p []byte) (int, error) {
        if b.Len() == 0 {
                return 0, io.EOF
        }
        n := copy(p, b.Slice())
        b.Consume(n)
        return n, nil
}

// Bytes returns the underlying slice (like bytes.Buffer.Bytes).
func (b *BytesBuffer) Bytes() []byte {
        return b.Slice()
}

// String returns the buffer as a string.
func (b *BytesBuffer) String() string {
        return string(b.Slice())
}
