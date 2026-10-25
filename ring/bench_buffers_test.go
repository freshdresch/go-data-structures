package ring_test

import (
	"fmt"
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBuffers(t *testing.T) {
	value := &item{value: 1}
	for _, size := range []int{1, 32} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			t.Run("ptr", func(t *testing.T) { checkNewBuffers(t, size, value) })
			t.Run("int64", func(t *testing.T) { checkNewBuffers(t, size, int64(1)) })
			t.Run("int64x8", func(t *testing.T) { checkNewBuffers(t, size, int64x8{1}) })
			t.Run("stamped", func(t *testing.T) { checkNewBuffers(t, size, stamped{1, 2}) })
			t.Run("bytes3", func(t *testing.T) { checkNewBuffers(t, size, [3]byte{1, 2, 3}) })
		})
	}
}

func checkNewBuffers[T comparable](t *testing.T, size int, value T) {
	t.Helper()
	const count = 24
	calls := [2][][]T{newBuffers[T](count, size), newBuffers[T](count, size)}
	var zero T
	var buffers [][]T
	for _, call := range calls {
		require.Len(t, call, count)
		buffers = append(buffers, call...)
		for _, buffer := range call {
			require.Len(t, buffer, size)
			assert.Equal(t, size, cap(buffer))
			for _, element := range buffer {
				assert.Equal(t, zero, element)
			}
		}
	}

	for i, buffer := range buffers {
		for j := range buffer {
			buffer[j] = value
		}
		for j, other := range buffers {
			expected := zero
			if j <= i {
				expected = value
			}
			for _, element := range other {
				require.Equal(t, expected, element, "write to buffer %d affected buffer %d", i, j)
			}
		}
	}

	elementSize := unsafe.Sizeof(zero)
	for _, width := range []uintptr{64, 128} {
		for i, buffer := range buffers {
			first := uintptr(unsafe.Pointer(unsafe.SliceData(buffer)))
			last := first + uintptr(len(buffer))*elementSize - 1
			for j := 0; j < i; j++ {
				otherFirst := uintptr(unsafe.Pointer(unsafe.SliceData(buffers[j])))
				otherLast := otherFirst + uintptr(len(buffers[j]))*elementSize - 1
				assert.True(
					t,
					last/width < otherFirst/width || otherLast/width < first/width,
					"buffers %d and %d share an occupied %d-byte line", i, j, width,
				)
			}
		}
	}
	runtime.KeepAlive(calls)
}

func TestNewBuffersEmptyAndInvalidDimensions(t *testing.T) {
	assert.Empty(t, newBuffers[int64](0, 1))
	assert.Empty(t, newBuffers[int64](0, -1))
	emptyBuffers := newBuffers[int64](24, 0)
	require.Len(t, emptyBuffers, 24)
	for _, buffer := range emptyBuffers {
		assert.Len(t, buffer, 0)
		assert.Zero(t, cap(buffer))
	}
	for _, size := range []int{0, 1, 32} {
		buffers := newBuffers[struct{}](24, size)
		require.Len(t, buffers, 24)
		for _, buffer := range buffers {
			assert.Len(t, buffer, size)
			assert.Equal(t, size, cap(buffer))
		}
	}
	assert.Panics(t, func() { newBuffers[int64](-1, 1) })
	assert.Panics(t, func() { newBuffers[int64](1, -1) })
	assert.Panics(t, func() { newBuffers[struct{}](1, -1) })
}

func TestNewBuffersGuardGeometry(t *testing.T) {
	elementSizes := []uintptr{
		unsafe.Sizeof((*item)(nil)),
		unsafe.Sizeof(int64(0)),
		unsafe.Sizeof(int64x8{}),
		unsafe.Sizeof(stamped{}),
		unsafe.Sizeof([3]byte{}),
	}
	const guardBytes = 128
	for _, elementSize := range elementSizes {
		guard := 1 + (guardBytes-1)/elementSize
		for _, size := range []uintptr{1, 32} {
			for _, width := range []uintptr{64, 128} {
				for phase := range width {
					backingFirst := width + phase
					activeFirst := backingFirst + guard*elementSize
					activeEnd := activeFirst + size*elementSize
					backingEnd := activeEnd + guard*elementSize
					assert.GreaterOrEqual(t, activeFirst-backingFirst, uintptr(guardBytes))
					assert.GreaterOrEqual(t, backingEnd-activeEnd, uintptr(guardBytes))
					assert.Greater(t, activeFirst/width, (backingFirst-1)/width)
					assert.Less(t, (activeEnd-1)/width, backingEnd/width)
				}
			}
		}
	}
}
