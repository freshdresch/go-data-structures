package buffer

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuffer_EmptyInit(t *testing.T) {
	b := NewBuffer[int](0)
	require.NotNil(t, b)

	assert.Zero(t, b.Len())
	assert.Zero(t, b.Cap())
	assert.Empty(t, b.Slice())

	// make sure we can't grow
	assert.Panics(t, func() { b.Grow(1) })
	assert.Zero(t, b.Len())
	assert.Zero(t, b.Cap())
	assert.Empty(t, b.Slice())
}

func TestBuffer_Append(t *testing.T) {
	t.Run("append zero elements", func(t *testing.T) {
		b := NewBuffer[int](5)
		b.Append()

		assert.Equal(t, []int{}, b.Slice())
		assert.Equal(t, 0, b.Len())
	})

	t.Run("append some elements", func(t *testing.T) {
		b := NewBuffer[int](5)
		b.Append(1, 2, 3)

		assert.Equal(t, []int{1, 2, 3}, b.Slice())
		assert.Equal(t, 3, b.Len())
	})

	t.Run("append all elements", func(t *testing.T) {
		b := NewBuffer[int](5)
		b.Append(1, 2, 3, 4, 5)

		assert.Equal(t, []int{1, 2, 3, 4, 5}, b.Slice())
		assert.Equal(t, 5, b.Len())
	})

	t.Run("append after shrinking", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.ConsumeRight(3)

		b.Append(6, 7)
		assert.Equal(t, []int{1, 2, 6, 7}, b.Slice())
		assert.Equal(t, 4, b.Len())

		// stale value is still there
		b.Grow(1)
		assert.Equal(t, []int{1, 2, 6, 7, 5}, b.Slice())
		assert.Equal(t, 5, b.Len())
	})

	t.Run("cause append to compact", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		t.Logf("Starting buffer: %s\n", b)

		b.Consume(3)
		t.Logf("After Consume: %s\n", b)
		assert.Equal(t, []int{4, 5}, b.Slice())
		assert.Equal(t, 2, b.Len())

		b.Append(6)
		t.Logf("After Append: %s\n", b)
		assert.Equal(t, []int{4, 5, 6}, b.Slice())
		assert.Equal(t, 3, b.Len())

		// should be stale values on the end after a grow
		b.Grow(2)
		t.Logf("After Grow: %s\n", b)
		assert.Equal(t, []int{4, 5, 6, 4, 5}, b.Slice())
		assert.Equal(t, 5, b.Len())
	})

	t.Run("append more elements than buffer length", func(t *testing.T) {
		b := NewBuffer[int](5)
		assert.Panics(t, func() { b.Append(1, 2, 3, 4, 5, 6) })
	})
}

func TestBuffer_Compact(t *testing.T) {
	t.Run("compact a zero length buffer", func(t *testing.T) {
		b := NewBuffer[int](0)
		b.Compact()
		assert.Equal(t, 0, b.start)
		assert.Equal(t, 0, b.Len())
	})

	t.Run("compact an already compact buffer", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3}, 5)
		b.Compact()
		assert.Equal(t, 0, b.start)
		assert.Equal(t, 3, b.Len())
	})

	t.Run("compact with a small gap", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.Consume(2) // left with {X, X, 3, 4, 5} where X are stale values from the consume
		assert.Equal(t, 2, b.start)
		assert.Equal(t, 3, b.Len())

		b.Compact() // left with {3, 4, 5, Y, Y} where Y are stale values from the compact
		assert.Equal(t, 0, b.start)
		assert.Equal(t, 3, b.Len())

		b.Grow(2)
		assert.Equal(t, []int{3, 4, 5, 4, 5}, b.Slice())
		assert.Equal(t, 5, b.Len())
	})

	t.Run("compact with a large gap", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.Consume(4) // left with {X, X, X, X, 5} where X are stale values from the consume
		assert.Equal(t, 4, b.start)
		assert.Equal(t, 1, b.Len())

		b.Compact() // left with {5, Y, Y, Y, Y} where Y are stale values from the compact
		assert.Equal(t, 0, b.start)
		assert.Equal(t, 1, b.Len())

		b.Grow(4)
		assert.Equal(t, []int{5, 2, 3, 4, 5}, b.Slice())
		assert.Equal(t, 5, b.Len())
	})
}

func TestBuffer_CompactAndZero(t *testing.T) {
	t.Run("compact and zero a zero length buffer", func(t *testing.T) {
		b := NewBuffer[int](0)
		b.CompactAndZero()
		assert.Equal(t, 0, b.start)
		assert.Equal(t, 0, b.Len())
	})

	t.Run("compact and zero an already compact buffer", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.Truncate(3) // leaves 4 and 5 as stale values in the tail
		b.CompactAndZero()
		assert.Equal(t, 0, b.start)
		assert.Equal(t, 3, b.Len())
		assert.Equal(t, []int{1, 2, 3, 0, 0}, b.data)
		assert.Equal(t, 3, b.highWater)
	})

	t.Run("compact and zero an empty buffer that was written", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3}, 5)
		b.Truncate(0)
		b.CompactAndZero()
		assert.Equal(t, 0, b.start)
		assert.Equal(t, 0, b.Len())
		assert.Equal(t, []int{0, 0, 0, 0, 0}, b.data)
		assert.Equal(t, 0, b.highWater)
	})

	t.Run("compact and zero with a small gap", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.Consume(2)
		// left with {X, X, 3, 4, 5} where X are stale values from the consume
		assert.Equal(t, 2, b.start)
		assert.Equal(t, 3, b.Len())

		b.CompactAndZero()
		assert.Equal(t, 0, b.start)
		assert.Equal(t, 3, b.Len())

		b.Grow(2)
		assert.Equal(t, []int{3, 4, 5, 0, 0}, b.Slice())
		assert.Equal(t, 5, b.Len())
	})

	t.Run("compact and zero with a large gap", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.Consume(4)
		// left with {X, X, X, X, 5} where X are stale values from the consume
		assert.Equal(t, 4, b.start)
		assert.Equal(t, 1, b.Len())

		b.CompactAndZero()
		assert.Equal(t, 0, b.start)
		assert.Equal(t, 1, b.Len())

		b.Grow(4)
		assert.Equal(t, []int{5, 0, 0, 0, 0}, b.Slice())
		assert.Equal(t, 5, b.Len())
	})
}

func TestBuffer_Truncate(t *testing.T) {
	b := makeBuffer[int](t, []int{1, 2, 3, 4}, 4)
	b.Truncate(2)
	assert.Equal(t, []int{1, 2}, b.Slice())
	assert.Equal(t, 2, b.Len())
}

func TestBuffer_TruncateAndZero(t *testing.T) {
	b := makeBuffer[int](t, []int{1, 2, 3, 4}, 4)
	b.TruncateAndZero(3)
	assert.Equal(t, []int{1, 2, 3}, b.Slice())
}

func TestBuffer_Consume(t *testing.T) {
	t.Run("consume zero elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.Consume(0)

		assert.Equal(t, 5, b.Len())
		assert.Equal(t, []int{1, 2, 3, 4, 5}, b.Slice())
	})

	t.Run("consume some elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.Consume(2)

		assert.Equal(t, 3, b.Len())
		assert.Equal(t, []int{3, 4, 5}, b.Slice())
	})

	t.Run("consume all elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.Consume(5)

		assert.Equal(t, 0, b.Len())
		assert.Equal(t, []int{}, b.Slice())

		b.Grow(5)
		require.Equal(t, 5, b.Len())
		assert.Equal(t, []int{1, 2, 3, 4, 5}, b.Slice())
	})

	t.Run("consume more elements than buffer length", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Panics(t, func() { b.Consume(6) })
	})

	t.Run("consume negative elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Panics(t, func() { b.Consume(-1) })
	})
}

func TestBuffer_ConsumeAndZero(t *testing.T) {
	t.Run("consume zero elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.ConsumeAndZero(0)

		assert.Equal(t, 5, b.Len())
		assert.Equal(t, []int{1, 2, 3, 4, 5}, b.Slice())
	})

	t.Run("consume some elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.ConsumeAndZero(2)

		assert.Equal(t, 3, b.Len())
		assert.Equal(t, []int{3, 4, 5}, b.Slice())
	})

	t.Run("consume all elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.ConsumeAndZero(5)

		assert.Equal(t, 0, b.Len())
		assert.Equal(t, []int{}, b.Slice())

		b.Grow(5)
		require.Equal(t, 5, b.Len())
		assert.Equal(t, []int{0, 0, 0, 0, 0}, b.Slice())
	})

	t.Run("consume more elements than buffer length", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Panics(t, func() { b.ConsumeAndZero(6) })
	})

	t.Run("consume negative elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Panics(t, func() { b.ConsumeAndZero(-1) })
	})
}

func TestBuffer_ConsumeRight(t *testing.T) {
	t.Run("consume zero elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.ConsumeRight(0)

		assert.Equal(t, 5, b.Len())
		assert.Equal(t, []int{1, 2, 3, 4, 5}, b.Slice())
	})

	t.Run("consume some elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.ConsumeRight(2)

		assert.Equal(t, 3, b.Len())
		assert.Equal(t, []int{1, 2, 3}, b.Slice())
	})

	t.Run("consume all elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.ConsumeRight(5)

		assert.Equal(t, 0, b.Len())
		assert.Equal(t, []int{}, b.Slice())

		b.Grow(5)
		require.Equal(t, 5, b.Len())
		assert.Equal(t, []int{1, 2, 3, 4, 5}, b.Slice())

	})

	t.Run("consume more elements than buffer length", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Panics(t, func() { b.ConsumeRight(6) })
	})

	t.Run("consume negative elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Panics(t, func() { b.ConsumeRight(-1) })
	})
}

func TestBuffer_ConsumeRightAndZero(t *testing.T) {
	t.Run("consume zero elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.ConsumeRightAndZero(0)

		assert.Equal(t, 5, b.Len())
		assert.Equal(t, []int{1, 2, 3, 4, 5}, b.Slice())
	})

	t.Run("consume some elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.ConsumeRightAndZero(2)

		assert.Equal(t, 3, b.Len())
		assert.Equal(t, []int{1, 2, 3}, b.Slice())
	})

	t.Run("consume all elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.ConsumeRightAndZero(5)

		assert.Equal(t, 0, b.Len())
		assert.Equal(t, []int{}, b.Slice())

		b.Grow(5)
		require.Equal(t, 5, b.Len())
		assert.Equal(t, []int{0, 0, 0, 0, 0}, b.Slice())
	})

	t.Run("consume more elements than buffer length", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Panics(t, func() { b.ConsumeRightAndZero(6) })
	})

	t.Run("consume negative elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Panics(t, func() { b.ConsumeRightAndZero(-1) })
	})
}

func TestBuffer_PanicMessages(t *testing.T) {
	const truncateMessage = "buffer: truncate out of range"
	const consumeMessage = "buffer: consume out of range"

	methods := []struct {
		name    string
		message string
		call    func(b *Buffer[int], count int)
	}{
		{"Truncate", truncateMessage, func(b *Buffer[int], count int) { b.Truncate(count) }},
		{"TruncateAndZero", truncateMessage, func(b *Buffer[int], count int) {
			b.TruncateAndZero(count)
		}},
		{"Consume", consumeMessage, func(b *Buffer[int], count int) { b.Consume(count) }},
		{"ConsumeAndZero", consumeMessage, func(b *Buffer[int], count int) {
			b.ConsumeAndZero(count)
		}},
		{"ConsumeRight", consumeMessage, func(b *Buffer[int], count int) { b.ConsumeRight(count) }},
		{"ConsumeRightAndZero", consumeMessage, func(b *Buffer[int], count int) {
			b.ConsumeRightAndZero(count)
		}},
	}

	for _, method := range methods {
		t.Run(method.name+" with negative count", func(t *testing.T) {
			b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
			assert.PanicsWithValue(t, method.message, func() { method.call(b, -1) })
		})

		t.Run(method.name+" with count above Len", func(t *testing.T) {
			b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
			assert.PanicsWithValue(t, method.message, func() { method.call(b, 6) })
		})
	}
}

func TestBuffer_Grow(t *testing.T) {
	t.Run("grow zero elements", func(t *testing.T) {
		b := NewBuffer[int](5)
		growSlice := b.Grow(0)

		assert.Equal(t, 0, b.Len())
		assert.Equal(t, 0, len(growSlice))

		assert.Equal(t, []int{}, b.Slice())
		assert.Equal(t, []int{}, growSlice)
	})

	t.Run("grow some elements", func(t *testing.T) {
		b := NewBuffer[int](5)
		growSlice := b.Grow(3)

		assert.Equal(t, 3, b.Len())
		assert.Equal(t, 3, len(growSlice))

		// should be the zero val
		assert.Equal(t, []int{0, 0, 0}, b.Slice())
		assert.Equal(t, []int{0, 0, 0}, growSlice)

		// make sure we can access elements after growing
		copy(growSlice, []int{1, 2, 3})
		assert.Equal(t, 3, b.Len())
		assert.Equal(t, 3, len(growSlice))
	})

	t.Run("grow all elements", func(t *testing.T) {
		b := NewBuffer[int](5)
		growSlice := b.Grow(5)

		assert.Equal(t, 5, b.Len())
		assert.Equal(t, 5, len(growSlice))

		// should be the zero val
		assert.Equal(t, []int{0, 0, 0, 0, 0}, b.Slice())
		assert.Equal(t, []int{0, 0, 0, 0, 0}, growSlice)

		// make sure we can access elements after growing
		copy(growSlice, []int{1, 2, 3, 4, 5})
		assert.Equal(t, 5, b.Len())
		assert.Equal(t, 5, len(growSlice))

		assert.Equal(t, []int{1, 2, 3, 4, 5}, b.Slice())
		assert.Equal(t, []int{1, 2, 3, 4, 5}, growSlice)
	})

	t.Run("grow into stale values", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.ConsumeRight(2)

		assert.Equal(t, []int{1, 2, 3}, b.Slice())
		assert.Equal(t, 3, b.Len())

		growSlice := b.Grow(2)
		assert.Equal(t, 5, b.Len())
		assert.Equal(t, 2, len(growSlice))
		assert.Equal(t, []int{1, 2, 3, 4, 5}, b.Slice())

		growSlice[0] = 6
		growSlice[1] = 7
		assert.Equal(t, []int{1, 2, 3, 6, 7}, b.Slice())
	})

	t.Run("cause grow to compact", func(t *testing.T) {
		b := makeBuffer[int](t, []int{0, 1, 2, 3, 4, 5, 6, 7}, 8)

		// Manually shift start so a compaction is necessary
		b.start = 1
		b.length = 4
		require.Equal(t, []int{1, 2, 3, 4}, b.Slice())

		// Only 3 slots in the tail, so growing by 4 requires compacting.
		growSlice := b.Grow(4)

		// After compaction, [1, 2, 3, 4] has been copied to the front of the backing array. The fifth
		// entry is the old location of 4 before the copy. Nothing overwrote it, so it still
		// holds 4. The rest of the slice keeps its pre-existing contents, 5, 6, and 7.
		assert.Equal(t, 8, b.Len())
		assert.Equal(t, 4, len(growSlice))
		assert.Equal(t, []int{1, 2, 3, 4, 4, 5, 6, 7}, b.Slice())

		// Ensure the returned slice is writable and correctly mapped
		growSlice[0] = 5
		growSlice[1] = 6
		growSlice[2] = 7
		growSlice[3] = 8

		assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, b.Slice())
	})

	t.Run("grow more elements than buffer length", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Panics(t, func() { b.Grow(6) })
	})

	t.Run("grow negative elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Panics(t, func() { b.Grow(-1) })
	})
}

func TestBuffer_GrowAndZero(t *testing.T) {
	t.Run("grow zero elements", func(t *testing.T) {
		b := NewBuffer[int](5)
		growSlice := b.GrowAndZero(0)

		assert.Equal(t, 0, b.Len())
		assert.Equal(t, 0, len(growSlice))

		assert.Equal(t, []int{}, b.Slice())
		assert.Equal(t, []int{}, growSlice)
	})

	t.Run("grow some elements", func(t *testing.T) {
		b := NewBuffer[int](5)
		growSlice := b.GrowAndZero(3)

		assert.Equal(t, 3, b.Len())
		assert.Equal(t, 3, len(growSlice))

		// should be the zero val
		assert.Equal(t, []int{0, 0, 0}, b.Slice())
		assert.Equal(t, []int{0, 0, 0}, growSlice)

		// make sure we can access elements after growing
		copy(growSlice, []int{1, 2, 3})
		assert.Equal(t, 3, b.Len())
		assert.Equal(t, 3, len(growSlice))
	})

	t.Run("grow all elements", func(t *testing.T) {
		b := NewBuffer[int](5)
		growSlice := b.GrowAndZero(5)

		assert.Equal(t, 5, b.Len())
		assert.Equal(t, 5, len(growSlice))

		// should be the zero val
		assert.Equal(t, []int{0, 0, 0, 0, 0}, b.Slice())
		assert.Equal(t, []int{0, 0, 0, 0, 0}, growSlice)

		// make sure we can access elements after growing
		copy(growSlice, []int{1, 2, 3, 4, 5})
		assert.Equal(t, 5, b.Len())
		assert.Equal(t, 5, len(growSlice))

		assert.Equal(t, []int{1, 2, 3, 4, 5}, b.Slice())
		assert.Equal(t, []int{1, 2, 3, 4, 5}, growSlice)
	})

	t.Run("grow into stale values", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		b.ConsumeRight(2)

		assert.Equal(t, []int{1, 2, 3}, b.Slice())
		assert.Equal(t, 3, b.Len())

		growSlice := b.GrowAndZero(2)
		assert.Equal(t, 5, b.Len())
		assert.Equal(t, 2, len(growSlice))
		assert.Equal(t, []int{1, 2, 3, 0, 0}, b.Slice())

		growSlice[0] = 6
		growSlice[1] = 7
		assert.Equal(t, []int{1, 2, 3, 6, 7}, b.Slice())
	})

	t.Run("cause grow to compact", func(t *testing.T) {
		b := makeBuffer[int](t, []int{0, 1, 2, 3, 4}, 8)

		// Manually shift start so a compaction is necessary
		b.start = 1
		b.length = 4
		require.Equal(t, []int{1, 2, 3, 4}, b.Slice())

		// Only 3 slots in the tail, so growing by 4 requires compacting.
		growSlice := b.GrowAndZero(4)

		// After compaction, [1, 2, 3, 4] has been moved to the front of the backing array, and
		// the grown region is zero.
		assert.Equal(t, 8, b.Len())
		assert.Equal(t, 4, len(growSlice))
		assert.Equal(t, []int{1, 2, 3, 4, 0, 0, 0, 0}, b.Slice())

		// Ensure the returned slice is writable and correctly mapped
		growSlice[0] = 5
		growSlice[1] = 6
		growSlice[2] = 7
		growSlice[3] = 8

		assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8}, b.Slice())
	})

	t.Run("grow more elements than buffer length", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Panics(t, func() { b.GrowAndZero(6) })
	})

	t.Run("grow negative elements", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Panics(t, func() { b.GrowAndZero(-1) })
	})
}

func TestBuffer_Available(t *testing.T) {
	t.Run("empty buffer", func(t *testing.T) {
		b := NewBuffer[int](5)
		assert.Equal(t, 5, b.Available())
	})

	t.Run("partly full buffer", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2}, 5)
		assert.Equal(t, 3, b.Available())
	})

	t.Run("full buffer", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
		assert.Equal(t, 0, b.Available())
	})
}

func TestBuffer_GrowAndZero_ZeroesAfterCompact(t *testing.T) {
	b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 5)
	b.Truncate(3) // leaves 4 and 5 as stale values in the tail
	b.Consume(1)  // start is 1, and the live elements are {2, 3}
	require.Equal(t, 1, b.start)

	// Only 2 slots remain in the tail, so growing by 3 compacts first.
	grown := b.GrowAndZero(3)
	assert.Equal(t, []int{2, 3, 0, 0, 0}, b.Slice())
	for index, value := range grown {
		assert.Zero(t, value, "grown element at index %d", index)
	}
}

func TestBuffer_HighWaterTracksWrites(t *testing.T) {
	b := NewBuffer[int](8)
	assert.Equal(t, 0, b.highWater)

	b.Append(1, 2)
	assert.Equal(t, 2, b.highWater)

	b.Grow(2)
	assert.Equal(t, 4, b.highWater)

	b.GrowAndZero(3)
	assert.Equal(t, 7, b.highWater)

	b.Truncate(1)
	assert.Equal(t, 7, b.highWater)
}

func TestBuffer_ReturnedSlicesAreCapped(t *testing.T) {
	t.Run("Slice", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3}, 5)
		slice := b.Slice()
		assert.Equal(t, len(slice), cap(slice))

		_ = append(b.Slice(), 9)
		assert.Equal(t, []int{1, 2, 3, 0, 0}, b.data)
	})

	t.Run("Grow", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1}, 5)
		grown := b.Grow(2)
		assert.Equal(t, len(grown), cap(grown))
	})

	t.Run("GrowAndZero", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1}, 5)
		grown := b.GrowAndZero(2)
		assert.Equal(t, len(grown), cap(grown))
	})
}

func TestBuffer_AppendToSliceDoesNotDefeatGrowAndZero(t *testing.T) {
	b := NewBuffer[int](5)
	b.Append(1, 2)
	_ = append(b.Slice(), 7, 8, 9)
	b.Consume(1)

	assert.Equal(t, []int{0, 0, 0, 0}, b.GrowAndZero(4))
}

func TestBuffer_AvailableBuffer(t *testing.T) {
	t.Run("capacity is the tail free space", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3}, 6)
		b.Consume(1)

		available := b.AvailableBuffer()
		assert.Equal(t, 0, len(available))
		assert.Equal(t, 3, cap(available))
	})

	t.Run("raises highWater to Cap", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2}, 6)
		require.Equal(t, 2, b.highWater)

		b.AvailableBuffer()
		assert.Equal(t, 6, b.highWater)
	})

	t.Run("append within capacity is committed by Append", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2}, 6)

		dst := b.AvailableBuffer()
		dst = append(dst, 3, 4, 5)
		assert.Same(t, &b.data[2], &dst[0])
		b.Append(dst...)

		assert.Equal(t, []int{1, 2, 3, 4, 5}, b.Slice())
		assert.Equal(t, 5, b.Len())
	})

	t.Run("append that outgrows the tail is committed by Append", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2, 3, 4}, 6)
		b.Consume(2) // the live elements are {3, 4}, with 2 slots in the tail and 4 free in all

		dst := b.AvailableBuffer()
		dst = append(dst, 5, 6, 7)
		b.Append(dst...)

		assert.Equal(t, []int{3, 4, 5, 6, 7}, b.Slice())
		assert.Equal(t, 5, b.Len())
	})

	t.Run("abandoned write is zeroed by CompactAndZero", func(t *testing.T) {
		b := makeBuffer[int](t, []int{1, 2}, 6)
		_ = append(b.AvailableBuffer(), 3, 4, 5)
		require.Equal(t, []int{1, 2, 3, 4, 5, 0}, b.data)

		b.CompactAndZero()

		assert.Equal(t, []int{1, 2, 0, 0, 0, 0}, b.data)
	})
}

func TestBuffer_AvailableBufferAllocationFree(t *testing.T) {
	b := NewBuffer[byte](16)

	allocations := testing.AllocsPerRun(100, func() {
		dst := b.AvailableBuffer()
		dst = strconv.AppendInt(dst, 12345, 10)
		b.Append(dst...)
		b.Reset()
	})

	assert.Equal(t, 0.0, allocations)
}

func TestBuffer_RandomOperationsKeepInvariants(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))

	for iteration := range 2000 {
		b := NewBuffer[int](random.IntN(9))
		var expected []int
		for step := range 50 {
			operation := random.IntN(operationCount)
			expected = applyRandomOperation(t, random, b, operation, expected)
			assertInvariants(t, b)
			assert.Equal(t, len(expected), b.Len(),
				"iteration %d, step %d, operation %d", iteration, step, operation)
			assert.True(t, slices.Equal(expected, b.Slice()),
				"iteration %d, step %d, operation %d, contents: got %v, want %v",
				iteration, step, operation, b.Slice(), expected)
			if t.Failed() {
				t.FailNow()
			}
		}
	}
}

const operationCount = 16

// applyRandomOperation applies the mutating method selected by operation to b, with counts chosen
// so the call never panics. Every value it writes is nonzero, so a stale value is distinguishable
// from a zeroed one. It returns the live contents that b should hold after the operation. It does
// not modify expected.
func applyRandomOperation(
	t *testing.T, random *rand.Rand, b *Buffer[int], operation int, expected []int,
) []int {
	t.Helper()

	switch operation {
	case 0:
		count := random.IntN(b.Available() + 1)
		values := make([]int, count)
		fillNonzero(random, values)
		b.Append(values...)
		return append(slices.Clone(expected), values...)
	case 1:
		grown := b.Grow(random.IntN(b.Available() + 1))
		fillNonzero(random, grown)
		return append(slices.Clone(expected), grown...)
	case 2:
		grown := b.GrowAndZero(random.IntN(b.Available() + 1))
		for index, value := range grown {
			assert.Zero(t, value, "GrowAndZero element at index %d", index)
		}
		fillNonzero(random, grown)
		return append(slices.Clone(expected), grown...)
	case 3:
		newLength := random.IntN(b.Len() + 1)
		b.Truncate(newLength)
		return slices.Clone(expected[:newLength])
	case 4:
		newLength := random.IntN(b.Len() + 1)
		b.TruncateAndZero(newLength)
		return slices.Clone(expected[:newLength])
	case 5:
		count := random.IntN(b.Len() + 1)
		b.Consume(count)
		return slices.Clone(expected[count:])
	case 6:
		count := random.IntN(b.Len() + 1)
		b.ConsumeAndZero(count)
		return slices.Clone(expected[count:])
	case 7:
		count := random.IntN(b.Len() + 1)
		b.ConsumeRight(count)
		return slices.Clone(expected[:len(expected)-count])
	case 8:
		count := random.IntN(b.Len() + 1)
		b.ConsumeRightAndZero(count)
		return slices.Clone(expected[:len(expected)-count])
	case 9:
		b.Compact()
		return expected
	case 10:
		b.CompactAndZero()
		return expected
	case 11:
		b.Reset()
		return nil
	case 12:
		b.ResetAndZero()
		return nil
	case 13:
		b.resetAndZeroAll()
		return nil
	case 14:
		// An abandoned write leaves the live contents unchanged.
		dst := b.AvailableBuffer()
		values := make([]int, cap(dst))
		fillNonzero(random, values)
		_ = append(dst, values...)
		return expected
	// A commit that still aliases the buffer always fits the tail, so only a reallocated
	// commit can make Append compact.
	case 15:
		dst := b.AvailableBuffer()
		values := make([]int, random.IntN(b.Available()+1))
		fillNonzero(random, values)
		b.Append(append(dst, values...)...)
		return append(slices.Clone(expected), values...)
	}
	return expected
}

func fillNonzero(random *rand.Rand, values []int) {
	for index := range values {
		values[index] = random.IntN(100) + 1
	}
}

func TestBuffer_Reset(t *testing.T) {
	b := makeBuffer[int](t, []int{1, 2, 3}, 3)
	b.Reset()
	assert.Zero(t, b.Len())
	assert.Equal(t, 0, len(b.Slice()))

	growSlice := b.Grow(3)
	assert.Equal(t, 3, b.Len())
	assert.Equal(t, 3, len(growSlice))

	// make sure the stale contents are still there
	assert.Equal(t, []int{1, 2, 3}, growSlice)
	assert.Equal(t, []int{1, 2, 3}, b.Slice())
}

func TestBuffer_ResetAndZero(t *testing.T) {
	b := makeBuffer[int](t, []int{1, 2, 3}, 3)
	b.ResetAndZero()
	assert.Zero(t, b.Len())
	assert.Equal(t, 0, len(b.Slice()))

	growSlice := b.Grow(3)
	assert.Equal(t, 3, b.Len())
	assert.Equal(t, 3, len(growSlice))

	// make sure the contents were zeroed out
	assert.Equal(t, []int{0, 0, 0}, growSlice)
	assert.Equal(t, []int{0, 0, 0}, b.Slice())
}

func TestBuffer_Invariants_Basic(t *testing.T) {
	b := NewBuffer[int](3)
	assertInvariants(t, b)

	b.Append(1, 2, 3)
	assertInvariants(t, b)

	b.Consume(1)
	assertInvariants(t, b)

	b.Compact()
	assertInvariants(t, b)

	b.Truncate(1)
	assertInvariants(t, b)

	b.Reset()
	assertInvariants(t, b)
}

func TestBuffer_DataIntegrity_Roundtrip(t *testing.T) {
	b := NewBuffer[int](5)
	b.Append(1, 2, 3, 4, 5)
	assert.Equal(t, []int{1, 2, 3, 4, 5}, b.Slice())

	b.Consume(2)
	b.Append(6, 7)
	assert.Equal(t, []int{3, 4, 5, 6, 7}, b.Slice())

	before := append([]int(nil), b.Slice()...)
	b.Compact()
	assert.Equal(t, before, b.Slice(), "Compact must preserve order and content")

	b.Truncate(3)
	slice := b.Grow(2)
	copy(slice, []int{8, 9})
	assert.Equal(t, []int{3, 4, 5, 8, 9}, b.Slice())

	b.ResetAndZero()
	assert.Zero(t, b.Len())
	assertInvariants(t, b)
}

func TestBuffer_AndZeroVariants_PreserveLiveData(t *testing.T) {
	b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 8)

	b.ConsumeAndZero(2)
	assert.Equal(t, []int{3, 4, 5}, b.Slice(), "ConsumeAndZero preserves live data")

	before := append([]int(nil), b.Slice()...)
	b.TruncateAndZero(2)
	assert.Equal(t, before[:2], b.Slice(), "TruncateAndZero preserves prefix")
}

func TestBuffer_CompactAndZero_PreservesLiveData(t *testing.T) {
	b := makeBuffer[int](t, []int{1, 2, 3, 4, 5}, 8)
	b.Consume(2)
	before := append([]int(nil), b.Slice()...)
	b.CompactAndZero()
	assert.Equal(t, before, b.Slice(), "CompactAndZero preserves content and order")
}

func FuzzBuffer_Invariants(f *testing.F) {
	f.Add(8, 12, 2)
	f.Fuzz(func(t *testing.T, capHint, opCount, growSize int) {
		if capHint <= 0 || opCount <= 0 || growSize < 0 {
			return
		}
		b := NewBuffer[int](capHint)
		require.NotNil(t, b)

		for i := 0; i < opCount; i++ {
			switch i % 6 {
			case 0:
				if b.Len() < b.Cap() {
					b.Append(i)
				}
			case 1:
				if b.Len() > 0 {
					b.Consume(1)
				}
			case 2:
				if b.Len() > 0 {
					b.Truncate(b.Len() / 2)
				}
			case 3:
				if growSize > 0 && b.Len()+growSize <= b.Cap() {
					_ = b.Grow(growSize)
				}
			case 4:
				b.Compact()
			case 5:
				b.Reset()
			}
			assertInvariants(t, b)
		}
	})
}

func makeBuffer[T any](t *testing.T, vals []T, capacity int) *Buffer[T] {
	t.Helper()
	b := NewBuffer[T](capacity)
	require.NotNil(t, b)
	b.Append(vals...)
	return b
}

func assertInvariants[T any](t *testing.T, b *Buffer[T]) {
	t.Helper()
	slice := b.Slice()

	assert.Equal(t, b.Len(), len(slice), "Len() must equal len(Slice())")
	assert.LessOrEqual(t, b.Len(), b.Cap(), "Len() <= Cap()")
	assert.GreaterOrEqual(t, b.start, 0, "start >= 0")
	assert.GreaterOrEqual(t, b.Len(), 0, "len >= 0")
	assert.LessOrEqual(t, b.start+b.Len(), b.Cap(), "start+len <= capacity")

	if b.Len() == 0 {
		assert.Equal(t, 0, b.start, "start must be 0 when buffer is empty")
	}

	assert.LessOrEqual(t, b.highWater, b.Cap(), "highWater <= Cap()")
	var zero T
	for index, value := range b.data[b.highWater:] {
		assert.Equal(t, zero, value, "element at index %d is above highWater", b.highWater+index)
	}
}
