//go:build !asan

package dpdk

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// checkedRing holds each ring TestRingAlignment checks, so the ring escapes to the heap however
// the compiler inlines New.
var checkedRing any

// fortyBytes is a struct element type for TestRingAlignment.
type fortyBytes struct {
	_ [5]uint64
}

// TestRingAlignment checks that rings from New keep each group of fields on a cache line of its
// own, shared with no other group. The allocator's header leaves the ring off a line boundary,
// so this checks what the layout needs rather than alignment. The offset comes from the
// allocator's size classes, not from a language guarantee, so this catches a Go release that
// changes it.
//
// It is excluded from -asan builds, because the address sanitizer pads each allocation, which
// moves the ring to an offset the layout was not built for.
func TestRingAlignment(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) == 8 {
		require.Equal(t, uintptr(24), unsafe.Sizeof(prodHeadLine{}))
		require.Equal(t, uintptr(24), unsafe.Sizeof(consHeadLine{}))
		require.Equal(t, uintptr(640), unsafe.Sizeof(Ring[byte]{}))
		require.Equal(t, uintptr(0), unsafe.Offsetof(Ring[byte]{}.readOnly))
		require.Equal(t, uintptr(128), unsafe.Offsetof(Ring[byte]{}.prodHeadLine))
		require.Equal(t, uintptr(256), unsafe.Offsetof(Ring[byte]{}.prodTailLine))
		require.Equal(t, uintptr(384), unsafe.Offsetof(Ring[byte]{}.consHeadLine))
		require.Equal(t, uintptr(512), unsafe.Offsetof(Ring[byte]{}.consTailLine))
	}
	t.Run("int", assertGroupsOnOwnLines[int])
	t.Run("pointer", assertGroupsOnOwnLines[*int])
	t.Run("fortyBytes", assertGroupsOnOwnLines[fortyBytes])
	t.Run("string", assertGroupsOnOwnLines[string])
}

// assertGroupsOnOwnLines allocates rings of T with New and checks that each group of fields fits
// in one cache line, on a line after the previous group's.
func assertGroupsOnOwnLines[T any](t *testing.T) {
	const rings = 1000
	for range rings {
		ring, err := New[T](8)
		require.NoError(t, err)
		checkedRing = ring

		address := uintptr(unsafe.Pointer(ring))
		groups := []struct {
			name  string
			start uintptr
			size  uintptr
		}{
			{
				"readOnly",
				uintptr(unsafe.Pointer(&ring.readOnly)),
				unsafe.Sizeof(ring.readOnly),
			},
			{
				"prodHeadLine",
				uintptr(unsafe.Pointer(&ring.prodHeadLine)),
				unsafe.Sizeof(ring.prodHeadLine),
			},
			{
				"prodTailLine",
				uintptr(unsafe.Pointer(&ring.prodTailLine)),
				unsafe.Sizeof(ring.prodTailLine),
			},
			{
				"consHeadLine",
				uintptr(unsafe.Pointer(&ring.consHeadLine)),
				unsafe.Sizeof(ring.consHeadLine),
			},
			{
				"consTailLine",
				uintptr(unsafe.Pointer(&ring.consTailLine)),
				unsafe.Sizeof(ring.consTailLine),
			},
		}

		var previousName string
		var previousLine uintptr
		for i, group := range groups {
			first, last := linesOf(group.start, group.size)
			require.Equal(t, first, last,
				"ring at %#x: %s spans cache lines %d to %d, not one line",
				address, group.name, first, last)
			if i > 0 {
				require.Greater(t, first, previousLine,
					"ring at %#x: %s is on cache line %d, not after %s on line %d",
					address, group.name, first, previousName, previousLine)
			}
			previousName = group.name
			previousLine = first
		}
	}
}

// linesOf returns the first and last cache line numbers that a span of size bytes at start
// touches. They are equal when the span fits in one line.
func linesOf(start, size uintptr) (first, last uintptr) {
	return start / cacheLine, (start + size - 1) / cacheLine
}
