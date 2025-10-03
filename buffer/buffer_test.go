// package buffer provides a buffer primitive that encourages re-use of the same
// fixed length buffer, as opposed to the dynamic nature of slices.
package buffer

import (
        "testing"

        "github.com/stretchr/testify/assert"
)

func TestBuffer(t *testing.T) {
        buf := NewBuffer[int](100)
        buf.Append(1, 2, 3)
        assert.Equal(t, []int{1, 2, 3}, buf.Slice())
        assert.Equal(t, 3, buf.Len())

        // reset without clearing, contents are still in memory
        buf.Reset()
        assert.Equal(t, 0, buf.Len())

        buf.len += 3
        assert.Equal(t, []int{1, 2, 3}, buf.Slice())

        // now contents should be wiped from memory
        buf.ResetAndClear()
        assert.Equal(t, 0, buf.Len())

        s := buf.Grow(3)
        assert.Equal(t, []int{0, 0, 0}, s)

        for _, val := range []int{4, 5, 6} {
                s = append(s, val)
        }
        assert.Equal(t, []int{4, 5, 6}, buf.Slice())

        buf.Truncate(1)
        assert.Equal(t, []int{4, 5}, buf.Slice())

        _ = buf.Grow(1)
        assert.Equal(t, []int{4, 5, 6}, buf.Slice())

        buf.TruncateAndClear(1)
        assert.Equal(t, []int{4, 5}, buf.Slice())

        buf.len++
        assert.Equal(t, []int{4, 5, 0}, buf.Slice())
}
