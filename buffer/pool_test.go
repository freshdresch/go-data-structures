package buffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests do not check what Get returns after Put. sync.Pool drops items at random under
// -race, so the returned buffer may or may not be the one that was released.

func TestPool_PutZeroesWhenZeroOnPut(t *testing.T) {
	pool := NewPool[int](4, true)
	released := pool.Get()
	released.Append(1, 2, 3)

	pool.Put(released)

	assert.Zero(t, released.Len())
	assert.Equal(t, []int{0, 0, 0, 0}, released.data)
}

func TestPool_ZeroOnPutWipesRemovedElements(t *testing.T) {
	pool := NewPool[int](6, true)
	released := pool.Get()
	released.Append(1, 2, 3, 4, 5, 6)
	released.Consume(2)  // leaves 1 and 2 as stale values before the live elements
	released.Truncate(2) // leaves 5 and 6 as stale values after the live elements

	pool.Put(released)

	assert.Equal(t, []int{0, 0, 0, 0, 0, 0}, released.data)
	assert.Equal(t, 0, released.highWater)
}

func TestPool_PutAppliesCapacityCheck(t *testing.T) {
	pool := NewPool[int](4, false)
	pool.Put(NewBuffer[int](8))

	// This check cannot fail on correct code. Under -race it can miss a regression, because
	// sync.Pool drops items at random, so Get may allocate instead of returning the rejected buffer.
	assert.Equal(t, 4, pool.Get().Cap())
}

func TestPool_PutKeepsElementsWhenNotZeroOnPut(t *testing.T) {
	pool := NewPool[int](4, false)
	released := pool.Get()
	released.Append(1, 2, 3)

	pool.Put(released)

	assert.Zero(t, released.Len())
	assert.Equal(t, []int{1, 2, 3, 0}, released.data)
}

func TestPool_WrongCapacityIsResetButNotAccepted(t *testing.T) {
	pool := NewPool[int](4, true)
	wrong := NewBuffer[int](3)
	wrong.Append(1, 2)
	assert.False(t, pool.accepts(wrong))

	pool.Put(wrong)

	assert.Zero(t, wrong.Len())
	assert.Equal(t, []int{0, 0, 0}, wrong.data)
}

func TestPool_RightCapacityIsAccepted(t *testing.T) {
	pool := NewPool[int](4, true)

	assert.True(t, pool.accepts(NewBuffer[int](4)))
}

func TestPool_PutNilDoesNotPanic(t *testing.T) {
	pool := NewPool[int](4, true)

	assert.NotPanics(t, func() { pool.Put(nil) })
}

func TestNewPool_NegativeCapacityPanics(t *testing.T) {
	assert.PanicsWithValue(t, "buffer: negative capacity", func() {
		NewPool[int](-1, false)
	})
}

func TestPool_GetReturnsPoolCapacity(t *testing.T) {
	pool := NewPool[int](7, false)

	assert.Equal(t, 7, pool.Get().Cap())
}

func TestPool_ZeroOnPutWipesAbandonedAvailableBuffer(t *testing.T) {
	pool := NewPool[int](6, true)
	released := pool.Get()
	released.Append(1, 2)
	_ = append(released.AvailableBuffer(), 3, 4, 5)
	require.Equal(t, []int{1, 2, 3, 4, 5, 0}, released.data)

	pool.Put(released)

	assert.Equal(t, []int{0, 0, 0, 0, 0, 0}, released.data)
}
