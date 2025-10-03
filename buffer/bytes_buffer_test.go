// package buffer provides a buffer primitive that encourages re-use of the same
// fixed length buffer, as opposed to the dynamic nature of slices.
package buffer

import (
        "testing"
        "github.com/stretchr/testify/assert"
)

func TestBytesBuffer(t *testing.T) {
        bb := NewBytesBuffer(64)
        bb.WriteString("hello ")
        bb.Write([]byte("world"))
        bb.WriteByte('!')

        assert.Equal(t, "hello world!", bb.String())

        p := make([]byte, 5)
        n, _ := bb.Read(p)
        assert.Equal(t, "hello", string(p[:n]))
        assert.Equal(t, " world!", bb.String())
}
