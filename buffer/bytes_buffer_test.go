// package buffer provides a buffer primitive that encourages re-use of the same
// fixed length buffer, as opposed to the dynamic nature of slices.
package buffer

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBytesBuffer(t *testing.T) {
	bb := NewBytesBuffer(64)
	bb.WriteString("hello ")
	bb.Write([]byte("world"))
	bb.WriteByte('!')

	assert.Equal(t, "hello world!", bb.String())

	p := make([]byte, 5)
	n, err := bb.Read(p)
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	assert.Equal(t, "hello", string(p[:n]))
	assert.Equal(t, " world!", bb.String())
}
