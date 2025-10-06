// package buffer provides a different buffer primitve that encourages reuse of the same fixed-length
// buffer, as opposed to the dynamic nature of slices.
package buffer

import (
        "fmt"
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
                fmt.Printf("Starting buffer: %s\n", b)

                b.Consume(3)
                fmt.Printf("After Consume: %s\n", b)
                assert.Equal(t, []int{4, 5}, b.Slice())
                assert.Equal(t, 2, b.Len())

                b.Append(6)
                fmt.Printf("After Append: %s\n", b)
                assert.Equal(t, []int{4, 5, 6}, b.Slice())
                assert.Equal(t, 3, b.Len())

                // should be stale values on the end after a grow
                b.Grow(2)
                fmt.Printf("After Grow: %s\n", b)
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
                b := makeBuffer[int](t, []int{1, 2, 3}, 5)
                b.CompactAndZero()
                assert.Equal(t, 0, b.start)
                assert.Equal(t, 3, b.Len())
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
                for i, elem := range []int{1, 2, 3} {
                        growSlice[i] = elem
                }
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
                for i, elem := range []int{1, 2, 3, 4, 5} {
                        growSlice[i] = elem
                }
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
                // TODO
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
                for i, elem := range []int{1, 2, 3} {
                        growSlice[i] = elem
                }
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
                for i, elem := range []int{1, 2, 3, 4, 5} {
                        growSlice[i] = elem
                }
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
                // TODO
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
                                b.Append(i)
                        case 1:
                                if b.Len() > 0 {
                                        b.Consume(1)
                                }
                        case 2:
                                if b.Len() > 0 {
                                        b.Truncate(b.Len() / 2)
                                }
                        case 3:
                                if growSize > 0 {
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

func makeBuffer[T any](t *testing.T, vals []T, cap int) *Buffer[T] {
        t.Helper()
        b := NewBuffer[T](cap)
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
}
