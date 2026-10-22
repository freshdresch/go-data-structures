//go:build !asan

package ring

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// alignedRing holds each ring TestRingAlignment checks, so the ring escapes to the heap however
// the compiler inlines New.
var alignedRing any

// fortyBytes is a struct element type for TestRingAlignment.
type fortyBytes struct {
	_ [5]uint64
}

// TestRingAlignment checks that both allocations New makes for a ring, the ring itself and its
// index block, start on a cache line, so each line of the ring is shared with neither the other
// lines nor neighboring heap objects. The ring is aligned because at one cache line it is under
// the size above which the allocator puts a malloc header in front of an object holding a
// pointer. The index block is aligned because it holds no pointers, so it never gets a header. The
// alignment comes from the allocator's size classes, not from a language guarantee, so this
// catches a Go release that changes it.
//
// It is excluded from -asan builds, because the address sanitizer pads each allocation, which
// moves the ring into a larger size class whose objects are not 128-byte aligned.
func TestRingAlignment(t *testing.T) {
	t.Run("int", assertRingsAligned[int])
	t.Run("pointer", assertRingsAligned[*int])
	t.Run("fortyBytes", assertRingsAligned[fortyBytes])
	t.Run("string", assertRingsAligned[string])
}

// assertRingsAligned allocates rings of T with New and checks that each ring and its index block
// are cacheLine aligned.
func assertRingsAligned[T any](t *testing.T) {
	const rings = 1000
	for range rings {
		ring, err := New[T](8)
		require.NoError(t, err)
		alignedRing = ring

		address := uintptr(unsafe.Pointer(ring))
		require.Zero(t, address%cacheLine, "ring at %#x is not %d-byte aligned", address, cacheLine)
		indicesAddress := uintptr(unsafe.Pointer(ring.indices))
		require.Zero(
			t,
			indicesAddress%cacheLine,
			"index block at %#x is not %d-byte aligned",
			indicesAddress,
			cacheLine,
		)
	}
}
