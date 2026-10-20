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

// TestRingAlignment checks that rings from New start on a cache line, so the producer and
// consumer sides never share one. The alignment comes from the allocator's size classes, not from
// a language guarantee, so this catches a Go release that changes it.
//
// It is excluded from -asan builds, because the address sanitizer pads each allocation, which
// moves the ring into a larger size class whose objects are not 128-byte aligned.
func TestRingAlignment(t *testing.T) {
	t.Run("int", assertRingsAligned[int])
	t.Run("pointer", assertRingsAligned[*int])
	t.Run("fortyBytes", assertRingsAligned[fortyBytes])
	t.Run("string", assertRingsAligned[string])
}

// assertRingsAligned allocates rings of T with New and checks that each is cacheLine aligned.
func assertRingsAligned[T any](t *testing.T) {
	const rings = 1000
	for range rings {
		ring, err := New[T](8)
		require.NoError(t, err)
		alignedRing = ring

		address := uintptr(unsafe.Pointer(ring))
		require.Zero(t, address%cacheLine, "ring at %#x is not %d-byte aligned", address, cacheLine)
	}
}
