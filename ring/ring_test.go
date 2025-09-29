package ring

import (
        "testing"

        "github.com/stretchr/testify/assert"
)

func TestRingCreation(t *testing.T) {
        // not a power of 2, should error
        _, err := New[uintptr](13)
        assert.Error(t, err)

        simple, err := New[uintptr](64)
        assert.NoError(t, err)
        assertNewRing[uintptr](t, simple, 64)
        assert.False(t, simple.multiProdEnqueue)
        assert.False(t, simple.multiConsDequeue)

        mpsc, err := New[uintptr](64, WithMultiProdEnqueue[uintptr]())
        assert.NoError(t, err)
        assertNewRing[uintptr](t, mpsc, 64)
        assert.True(t, mpsc.multiProdEnqueue)
        assert.False(t, mpsc.multiConsDequeue)

        spmc, err := New[uintptr](64, WithMultiConsDequeue[uintptr]())
        assert.NoError(t, err)
        assertNewRing[uintptr](t, spmc, 64)
        assert.False(t, spmc.multiProdEnqueue)
        assert.True(t, spmc.multiConsDequeue)

        mpmc, err := New[uintptr](
                64,
                WithMultiProdEnqueue[uintptr](),
                WithMultiConsDequeue[uintptr](),
        )
        assert.NoError(t, err)
        assertNewRing[uintptr](t, mpmc, 64)
        assert.True(t, mpmc.multiProdEnqueue)
        assert.True(t, mpmc.multiConsDequeue)

        large, err := New[uintptr](1 << 16)
        assert.NoError(t, err)
        assertNewRing(t, large, 1<<16)
}

func assertNewRing[T any](t *testing.T, ring *Ring[T], count uint32) {
        assert.NotNil(t, ring.entries)
        assert.Equal(t, ring.size, count)
        assert.Equal(t, ring.mask, count-1)
        assert.Equal(t, ring.capacity, count-1)

        assert.Zero(t, *ring.prodHead)
        assert.Zero(t, *ring.prodTail)
        assert.Zero(t, *ring.consHead)
        assert.Zero(t, *ring.consTail)
}
