// package buffer provides a buffer primitive that encourages re-use of the same
// fixed length buffer, as opposed to the dynamic nature of slices.
package buffer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBufferPool(t *testing.T) {
	cap := 256
	p := NewPool[int](cap, true)

	buf := p.Get()
	buf.Append(42, 99)
	assert.Equal(t, []int{42, 99}, buf.Slice())

	p.Put(buf)

	buf = p.Get()
	assert.Empty(t, buf.Slice())

	buf.Grow(2)
	assert.Equal(t, []int{0, 0}, buf.Slice())
}
